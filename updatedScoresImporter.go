package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/kneu-messenger-pigeon/fileStorage"
	"github.com/segmentio/kafka-go"
	"io"
	"time"
)

const StorageTimeFormat = time.RFC3339

const UpdateScoreQuery = ScoreSelect + ` WHERE REGDATE > ? ORDER BY REGDATE ASC;`

type UpdatedScoresImporterInterface interface {
	execute(context context.Context)
	addEvent(event ScoreEditEvent)
	getConfirmed() <-chan ScoreEditEvent
}

type UpdatedScoresImporter struct {
	out         io.Writer
	db          *sql.DB
	cache       *timeCache
	writer      events.WriterInterface
	storage     fileStorage.Interface
	currentYear CurrentYearGetterInterface
	eventQueue  []ScoreEditEvent
	confirmed   chan ScoreEditEvent
	lastRegDate time.Time
}

func (importer *UpdatedScoresImporter) execute(context context.Context) {
	importer.initConfirmed()

	var err error
	nextRun := time.NewTimer(0)
	nextRequiredRun := time.Now()
	for {
		select {
		case <-context.Done():
			return

		case <-nextRun.C:
			nextRun = time.NewTimer(time.Second * 3)
			if len(importer.eventQueue) != 0 || time.Now().After(nextRequiredRun) {
				nextRequiredRun = time.Now().Add(time.Minute * 30)

				err = importer.pullUpdatedScores()
				if err != nil {
					fmt.Fprintf(importer.out, "[%s] Failed to fetch updated scores: %s \n", t(), err)
				}
				importer.determineConfirmedEvents()
			}
		}
	}
}

func (importer *UpdatedScoresImporter) addEvent(event ScoreEditEvent) {
	if !importer.putIntoConfirmedIfSatisfy(&event) {
		importer.eventQueue = append(importer.eventQueue, event)
	}
}

func (importer *UpdatedScoresImporter) getConfirmed() <-chan ScoreEditEvent {
	importer.initConfirmed()
	return importer.confirmed
}

func (importer *UpdatedScoresImporter) initConfirmed() {
	if importer.confirmed == nil {
		importer.confirmed = make(chan ScoreEditEvent)
	}
}

func (importer *UpdatedScoresImporter) determineConfirmedEvents() {
	length := len(importer.eventQueue)
	for i := 0; i < length; i++ {
		importer.putIntoConfirmedIfSatisfy(&importer.eventQueue[i])
	}

	importer.eventQueue = importer.eventQueue[length:len(importer.eventQueue)]
}

func (importer *UpdatedScoresImporter) putIntoConfirmedIfSatisfy(event *ScoreEditEvent) bool {
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
	var event events.ScoreEvent
	event.SyncedAt = time.Now()
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
		} else {
			event.Year = importer.currentYear.getYear()
			nextLastRegDate = event.UpdatedAt
			payload, _ := json.Marshal(event)
			messages = append(messages, kafka.Message{
				Key:   []byte(events.ScoreEventName),
				Value: payload,
			})
			lessonUpdatedMap[event.LessonId] = event.UpdatedAt
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
