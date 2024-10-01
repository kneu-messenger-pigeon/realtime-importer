package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	dekanatEvents "github.com/kneu-messenger-pigeon/dekanat-events"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/kneu-messenger-pigeon/fileStorage"
	"github.com/segmentio/kafka-go"
	"io"
	"sync"
	"time"
)

const StorageTimeFormat = time.RFC3339

const UpdateScoreQuery = ScoreSelect + ` WHERE REGDATE > ? AND ID_T_PD_CMS IS NOT NULL ` + ScoreSelectOrderBy

type UpdatedScoresImporterInterface interface {
	Execute(context context.Context)
	AddEvent(event dekanatEvents.ScoreEditEvent)
	GetConfirmed() <-chan dekanatEvents.ScoreEditEvent
}

type UpdatedScoresImporter struct {
	out             io.Writer
	db              *sql.DB
	cache           *timeCache
	writer          events.WriterInterface
	storage         fileStorage.Interface
	currentYear     CurrentYearGetterInterface
	eventQueue      []dekanatEvents.ScoreEditEvent
	eventQueueMutex sync.Mutex
	confirmed       chan dekanatEvents.ScoreEditEvent
	lastRegDate     time.Time
	maxLessonId     MaxLessonIdSetterInterface
}

func (importer *UpdatedScoresImporter) Execute(context context.Context) {
	if importer.confirmed == nil {
		importer.confirmed = make(chan dekanatEvents.ScoreEditEvent)
	}

	var err error
	nextTick := time.Tick(defaultPollInterval)
	nextRequiredTick := time.Tick(defaultForcePollInterval)

	requiredPoll := true
	for {
		if requiredPoll || len(importer.eventQueue) != 0 {
			requiredPoll = false
			err = importer.pullUpdatedScores()
			if err != nil {
				fmt.Fprintf(importer.out, "[%s] Failed to fetch updated scores: %s \n", t(), err)
			}
			importer.determineConfirmedEvents()
		}

		select {
		case <-context.Done():
			return

		case <-nextRequiredTick:
			requiredPoll = true

		case <-nextTick:
		}
	}
}

func (importer *UpdatedScoresImporter) AddEvent(event dekanatEvents.ScoreEditEvent) {
	if !importer.putIntoConfirmedIfSatisfy(&event) {
		importer.eventQueueMutex.Lock()
		importer.eventQueue = append(importer.eventQueue, event)
		importer.eventQueueMutex.Unlock()

		if !event.IsCustomGroup() {
			importer.maxLessonId.Set(event.GetLessonId())
		}
	}
}

func (importer *UpdatedScoresImporter) GetConfirmed() <-chan dekanatEvents.ScoreEditEvent {
	return importer.confirmed
}

func (importer *UpdatedScoresImporter) determineConfirmedEvents() {
	length := len(importer.eventQueue)
	for i := 0; i < length; i++ {
		if !importer.putIntoConfirmedIfSatisfy(&importer.eventQueue[i]) {
			importer.putIntoConfirmedIfHasNoChanges(&importer.eventQueue[i])
		}
	}

	importer.eventQueueMutex.Lock()
	importer.eventQueue = importer.eventQueue[length:len(importer.eventQueue)]
	importer.eventQueueMutex.Unlock()
}

func (importer *UpdatedScoresImporter) putIntoConfirmedIfSatisfy(event *dekanatEvents.ScoreEditEvent) bool {
	if event.Timestamp <= importer.cache.Get(event.GetLessonId(), event.IsCustomGroup()) {
		importer.confirmed <- *event
		return true
	}
	return false
}

func (importer *UpdatedScoresImporter) putIntoConfirmedIfHasNoChanges(event *dekanatEvents.ScoreEditEvent) bool {
	if !event.HasChanges {
		importer.confirmed <- *event
		return true
	}

	return false
}

func (importer *UpdatedScoresImporter) pullUpdatedScores() error {
	fmt.Fprintf(importer.out, "[%s] Check new scores changes after %s \n", t(), importer.getLastRegDate().Format(FirebirdTimeFormat))
	tx, rows, err := queryRowsInTransaction(importer.db, UpdateScoreQuery, importer.getLastRegDate().Format(FirebirdTimeFormat))
	defer closeRowsAndTransaction(rows, tx)
	if err != nil {
		return err
	}
	lessonUpdatedMap := make(map[uint]time.Time)
	customLessonUpdatedMap := make(map[uint]time.Time)

	var messages []kafka.Message
	message := kafka.Message{
		Key: []byte(events.ScoreEventName),
	}
	var event events.ScoreEvent
	event.SyncedAt = time.Now()
	event.ScoreSource = events.Realtime
	nextLastRegDate := importer.getLastRegDate()

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
		nextLastRegDate = event.UpdatedAt
		message.Value, _ = json.Marshal(event)
		messages = append(messages, message)
		lessonUpdatedMap[event.LessonId] = event.UpdatedAt
		importer.maxLessonId.Set(event.LessonId)

		if customGroupLessonId.Valid {
			customLessonUpdatedMap[uint(customGroupLessonId.Int32)] = event.UpdatedAt
		}
	}
	err = nil
	fmt.Fprintf(
		importer.out, "[%s] Finished importing updated scores since %s to %s - get %d updated records \n", t(),
		importer.getLastRegDate().Format(FirebirdTimeFormat), nextLastRegDate.Format(FirebirdTimeFormat), len(messages),
	)

	if len(messages) != 0 {
		err = importer.setLastRegDate(nextLastRegDate)
		if err == nil {
			err = importer.writer.WriteMessages(context.Background(), messages...)
		}
		if err == nil {
			var lessonId uint
			var updatedAt time.Time
			for lessonId, updatedAt = range lessonUpdatedMap {
				importer.cache.Set(lessonId, false, updatedAt.Unix())
			}

			for lessonId, updatedAt = range customLessonUpdatedMap {
				importer.cache.Set(lessonId, true, updatedAt.Unix())
			}
		}
	}
	return err
}

func (importer *UpdatedScoresImporter) getLastRegDate() time.Time {
	if importer.lastRegDate.IsZero() {
		bytesValue, err := importer.storage.Get()

		if err == nil {
			err = importer.lastRegDate.UnmarshalBinary(bytesValue)
		}

		if err != nil {
			fmt.Fprintf(importer.out, "[%s] Failed to get score Last Rag Date from file %v \n", t(), err)
		}

		if importer.lastRegDate.IsZero() {
			// set non zero for prevent repeat error
			importer.lastRegDate = time.Now().Add(-time.Minute)
		}
	}

	return importer.lastRegDate
}

func (importer *UpdatedScoresImporter) setLastRegDate(newLastRegDate time.Time) (err error) {
	if !importer.lastRegDate.Equal(newLastRegDate) {
		importer.lastRegDate = newLastRegDate.In(time.Local)

		value, _ := importer.lastRegDate.MarshalBinary()
		err = importer.storage.Set(value)

		if err != nil {
			fmt.Fprintf(importer.out, "[%s] Failed to write setLastRegDate %v \n", t(), err)
		}
	}
	return
}
