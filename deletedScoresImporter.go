package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/VictoriaMetrics/fastcache"
	dekanatEvents "github.com/kneu-messenger-pigeon/dekanat-events"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"io"
	"sync"
	"time"
)

const DeletedScoreQuery = ScoreSelect + ` WHERE XI_2 IN (?) AND ID_T_PD_CMS IS NOT NULL ` + ScoreSelectOrderBy
const CustomGroupDeletedScoreQuery = ScoreSelect + ` WHERE ID_ZANCG IN (?) AND ID_T_PD_CMS IS NOT NULL ` + ScoreSelectOrderBy

type DeletedScoresImporterInterface interface {
	Execute(context context.Context)
	AddEvent(event dekanatEvents.LessonDeletedEvent)
	GetConfirmed() <-chan dekanatEvents.LessonDeletedEvent
}

type DeletedScoresImporter struct {
	out             io.Writer
	db              *sql.DB
	cache           *fastcache.Cache
	writer          events.WriterInterface
	currentYear     CurrentYearGetterInterface
	eventQueue      []dekanatEvents.LessonDeletedEvent
	eventQueueMutex sync.Mutex
	confirmed       chan dekanatEvents.LessonDeletedEvent
}

func (importer *DeletedScoresImporter) Execute(context context.Context) {
	if importer.confirmed == nil {
		importer.confirmed = make(chan dekanatEvents.LessonDeletedEvent)
	}

	var err error
	nextTick := time.Tick(defaultPollInterval)
	for {
		if len(importer.eventQueue) != 0 {
			err = importer.pullDeletedScores()
			if err != nil {
				fmt.Fprintf(importer.out, "[%s] Failed to fetch deleted scores: %s \n", t(), err)
			}
			importer.determineConfirmedEvents()
		}

		select {
		case <-context.Done():
			return

		case <-nextTick:
		}
	}
}

func (importer *DeletedScoresImporter) AddEvent(event dekanatEvents.LessonDeletedEvent) {
	if !importer.putIntoConfirmedIfSatisfy(&event) {
		importer.eventQueueMutex.Lock()
		importer.eventQueue = append(importer.eventQueue, event)
		importer.eventQueueMutex.Unlock()
	}
}

func (importer *DeletedScoresImporter) GetConfirmed() <-chan dekanatEvents.LessonDeletedEvent {
	return importer.confirmed
}

func (importer *DeletedScoresImporter) determineConfirmedEvents() {
	length := len(importer.eventQueue)
	for i := 0; i < length; i++ {
		importer.putIntoConfirmedIfSatisfy(&importer.eventQueue[i])
	}

	importer.eventQueueMutex.Lock()
	importer.eventQueue = importer.eventQueue[length:len(importer.eventQueue)]
	importer.eventQueueMutex.Unlock()
}

func (importer *DeletedScoresImporter) putIntoConfirmedIfSatisfy(event *dekanatEvents.LessonDeletedEvent) bool {
	isDeletedFlag, exist := importer.cache.HasGet(
		[]byte{},
		idToBytes(event.GetLessonId(), event.IsCustomGroup()),
	)

	if exist && isDeletedFlag[0] == 1 {
		importer.confirmed <- *event
		fmt.Fprintf(
			importer.out, "[%s] %T confirmed: %d \n",
			t(), event, event.GetLessonId(),
		)
		return true
	}
	return false
}

func (importer *DeletedScoresImporter) pullDeletedScores() error {
	regularGroupLessonIds := make([]any, 0, len(importer.eventQueue))
	customGroupLessonIds := make([]any, 0, len(importer.eventQueue)/4)

	for _, event := range importer.eventQueue {
		if event.IsCustomGroup() {
			customGroupLessonIds = append(customGroupLessonIds, event.GetLessonId())
		} else {
			regularGroupLessonIds = append(regularGroupLessonIds, event.GetLessonId())
		}
	}

	var err error

	if len(regularGroupLessonIds) != 0 {
		fmt.Fprintf(importer.out, "[%s] Check deleted scores in regular group %v \n", t(), regularGroupLessonIds)
		err = importer.doPullDeletedScores(DeletedScoreQuery, regularGroupLessonIds)
	}

	if len(customGroupLessonIds) != 0 && err == nil {
		fmt.Fprintf(importer.out, "[%s] Check deleted scores in custom group %v \n", t(), customGroupLessonIds)
		err = importer.doPullDeletedScores(CustomGroupDeletedScoreQuery, customGroupLessonIds)
	}

	return err
}

func (importer *DeletedScoresImporter) doPullDeletedScores(query string, lessonIds []any) error {
	tx, rows, err := queryRowsInTransaction(
		importer.db, extractInPlaceHolder(query, len(lessonIds)), lessonIds...,
	)
	defer closeRowsAndTransaction(rows, tx)
	if err != nil {
		return err
	}

	lessonUpdatedMap := make(map[uint]bool)
	customLessonUpdatedMap := make(map[uint]bool)

	var messages []kafka.Message
	message := kafka.Message{
		Key: []byte(events.ScoreEventName),
	}

	var event events.ScoreEvent
	event.SyncedAt = time.Now()
	event.ScoreSource = events.Realtime

	customGroupLessonId := sql.NullInt32{}

	for rows.Next() {
		err = rows.Scan(
			&event.Id, &event.StudentId,
			&event.LessonId, &event.LessonPart,
			&customGroupLessonId,
			&event.DisciplineId, &event.Semester,
			&event.Value, &event.IsAbsent,
			&event.UpdatedAt, &event.IsDeleted,
		)
		if err != nil {
			fmt.Fprintf(importer.out, "[%s] Error with fetching score: %s \n", t(), err)
			continue
		}
		event.Year = importer.currentYear.GetYear()
		message.Value, _ = json.Marshal(event)
		messages = append(messages, message)
		if event.IsDeleted {
			lessonUpdatedMap[event.LessonId] = true
			if customGroupLessonId.Valid {
				customLessonUpdatedMap[uint(customGroupLessonId.Int32)] = true
			}
		}
	}
	err = nil
	fmt.Fprintf(
		importer.out, "[%s] Finished importing deleted scores for %d lessons - get %d records \n", t(),
		len(lessonIds), len(messages),
	)

	if len(messages) != 0 {
		err = importer.writer.WriteMessages(context.Background(), messages...)
		if err == nil {
			var lessonId uint
			for lessonId, _ = range lessonUpdatedMap {
				importer.cache.Set(idToBytes(lessonId, false), []byte{1})
			}

			for lessonId, _ = range customLessonUpdatedMap {
				importer.cache.Set(idToBytes(lessonId, true), []byte{1})
			}
		}
	}
	return err
}
