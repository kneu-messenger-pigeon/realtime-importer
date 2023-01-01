package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/VictoriaMetrics/fastcache"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"io"
	"time"
)

const DeletedScoreQuery = ScoreSelect + ` WHERE XI_2 IN (?) ORDER BY ID ASC;`

type DeletedScoresImporter struct {
	out        io.Writer
	db         *sql.DB
	cache      *fastcache.Cache
	writer     events.WriterInterface
	eventQueue []LessonDeletedEvent
	confirmed  chan LessonDeletedEvent
}

func (importer *DeletedScoresImporter) execute(context context.Context) {
	if importer.confirmed == nil {
		importer.confirmed = make(chan LessonDeletedEvent)
	}

	var err error
	nextRun := time.NewTimer(0)
	for {
		select {
		case <-context.Done():
			return

		case <-nextRun.C:
			nextRun = time.NewTimer(time.Second * 3)
			if len(importer.eventQueue) != 0 {
				err = importer.pullDeletedScores()
				if err != nil {
					fmt.Fprintf(importer.out, "[%s] Failed to fetch updated scores: %s \n", t(), err)
				}
				importer.determineConfirmedEvents()
			}
		}
	}
}

func (importer *DeletedScoresImporter) addEvent(event LessonDeletedEvent) {
	if !importer.putIntoConfirmedIfSatisfy(&event) {
		importer.eventQueue = append(importer.eventQueue, event)
	}
}

func (importer *DeletedScoresImporter) determineConfirmedEvents() {
	length := len(importer.eventQueue)
	for i := 0; i < length; i++ {
		importer.putIntoConfirmedIfSatisfy(&importer.eventQueue[i])
	}

	importer.eventQueue = importer.eventQueue[length:len(importer.eventQueue)]
}

func (importer *DeletedScoresImporter) putIntoConfirmedIfSatisfy(event *LessonDeletedEvent) bool {
	isDeletedFlag, exist := importer.cache.HasGet([]byte{}, uintToBytes(event.GetLessonId()))

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
	lessonIds := make([]any, 0)
	for _, event := range importer.eventQueue {
		lessonIds = append(lessonIds, event.GetLessonId())
	}

	fmt.Fprintf(importer.out, "[%s] Check deleted scores %v \n", t(), lessonIds)
	tx, rows, err := queryRowsInTransaction(
		importer.db, extractInPlaceHolder(DeletedScoreQuery, len(lessonIds)), lessonIds...,
	)
	defer closeRowsAndTransaction(rows, tx)
	if err != nil {
		return err
	}

	lessonUpdatedMap := make(map[uint]bool)
	var messages []kafka.Message
	var event events.ScoreEvent
	event.SyncedAt = time.Now()
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
		payload, _ := json.Marshal(event)
		messages = append(messages, kafka.Message{
			Key:   []byte(events.ScoreEventName),
			Value: payload,
		})
		if event.IsDeleted {
			lessonUpdatedMap[event.LessonId] = true
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
			for lessonId, _ := range lessonUpdatedMap {
				importer.cache.Set(uintToBytes(lessonId), []byte{1})
			}
		}
	}
	return err
}
