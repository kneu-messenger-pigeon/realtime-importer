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

const UpdateScoreQuery = ScoreSelect + ` WHERE REGDATE > ? ` + ScoreSelectOrderBy

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
	importer.initConfirmed()

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

		importer.maxLessonId.Set(event.GetLessonId())
	}
}

func (importer *UpdatedScoresImporter) GetConfirmed() <-chan dekanatEvents.ScoreEditEvent {
	importer.initConfirmed()
	return importer.confirmed
}

func (importer *UpdatedScoresImporter) initConfirmed() {
	if importer.confirmed == nil {
		importer.confirmed = make(chan dekanatEvents.ScoreEditEvent)
	}
}

func (importer *UpdatedScoresImporter) determineConfirmedEvents() {
	length := len(importer.eventQueue)
	for i := 0; i < length; i++ {
		importer.putIntoConfirmedIfSatisfy(&importer.eventQueue[i])
	}

	importer.eventQueueMutex.Lock()
	importer.eventQueue = importer.eventQueue[length:len(importer.eventQueue)]
	importer.eventQueueMutex.Unlock()
}

func (importer *UpdatedScoresImporter) putIntoConfirmedIfSatisfy(event *dekanatEvents.ScoreEditEvent) bool {
	if event.Timestamp <= importer.cache.Get(event.GetLessonId()) {
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
	var messages []kafka.Message
	message := kafka.Message{
		Key: []byte(events.ScoreEventName),
	}
	var event events.ScoreEvent
	event.SyncedAt = time.Now()
	event.ScoreSource = events.Realtime
	nextLastRegDate := importer.getLastRegDate()
	for rows.Next() {
		err = rows.Scan(
			&event.Id, &event.StudentId,
			&event.LessonId, &event.LessonPart,
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
			for lessonId, updatedAt := range lessonUpdatedMap {
				importer.cache.Set(lessonId, updatedAt.Unix())
			}
		}
	}
	return err
}

func (importer *UpdatedScoresImporter) getLastRegDate() time.Time {
	if importer.lastRegDate.IsZero() {
		stringValue, err := importer.storage.Get()
		if stringValue == "" && err == nil { // storage not exist or empty. Make initial value
			importer.lastRegDate = time.Now().Add(-time.Minute)

		} else if err == nil {
			importer.lastRegDate, err = time.ParseInLocation(StorageTimeFormat, stringValue, time.Local)
		}

		if err != nil {
			fmt.Fprintf(importer.out, "[%s] Failed to get score Last Rag Date from file %v \n", t(), err)
			// set non zero for prevent repeat error
			importer.lastRegDate = time.Now()
		}
	}

	return importer.lastRegDate
}

func (importer *UpdatedScoresImporter) setLastRegDate(newLastRegDate time.Time) (err error) {
	if importer.lastRegDate != newLastRegDate {
		newLastRegDate.In(time.Local)
		importer.lastRegDate = newLastRegDate
		err = importer.storage.Set(newLastRegDate.Format(StorageTimeFormat))
		if err != nil {
			fmt.Fprintf(importer.out, "[%s] Failed to write LessonMaxId %v \n", t(), err)
		}
	}
	return
}
