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

const LessonsEditedQuery = LessonsSelect + ` WHERE ID IN (?) ORDER BY ID ASC`

type EditedLessonsImporterInterface interface {
	execute(context context.Context)
	addEvent(event LessonEditEvent)
	getConfirmed() <-chan LessonEditEvent
}

type EditedLessonsImporter struct {
	out        io.Writer
	db         *sql.DB
	cache      *fastcache.Cache
	writer     events.WriterInterface
	eventQueue []LessonEditEvent
	confirmed  chan LessonEditEvent
}

func (importer *EditedLessonsImporter) execute(context context.Context) {
	if importer.confirmed == nil {
		importer.confirmed = make(chan LessonEditEvent)
	}

	var err error
	nextRun := time.NewTimer(0)
	for {
		select {
		case <-context.Done():
			close(importer.confirmed)
			importer.confirmed = nil
			return

		case <-nextRun.C:
			nextRun = time.NewTimer(time.Second * 3)
			if len(importer.eventQueue) != 0 {
				err = importer.pullEditedLessons()
				if err != nil {
					fmt.Fprintf(importer.out, "[%s] Failed to fetch created lessons: %s \n", t(), err)
				}
				importer.determineConfirmedEvents()
			}
		}
	}
}

func (importer *EditedLessonsImporter) addEvent(event LessonEditEvent) {
	if !importer.putIntoConfirmedIfSatisfy(&event) {
		importer.eventQueue = append(importer.eventQueue, event)

		fmt.Printf(
			"[%s] receive LessonEditEvent - discipline: %d; lesson: %d; added to processing queue \n",
			t(), event.GetDisciplineId(), event.GetLessonId(),
		)
	}
}

func (importer *EditedLessonsImporter) getConfirmed() <-chan LessonEditEvent {
	if importer.confirmed == nil {
		importer.confirmed = make(chan LessonEditEvent)
	}

	return importer.confirmed
}

func (importer *EditedLessonsImporter) determineConfirmedEvents() {
	length := len(importer.eventQueue)
	for i := 0; i < length; i++ {
		importer.putIntoConfirmedIfSatisfy(&importer.eventQueue[i])
	}

	importer.eventQueue = importer.eventQueue[length:len(importer.eventQueue)]
}

func (importer *EditedLessonsImporter) putIntoConfirmedIfSatisfy(event *LessonEditEvent) bool {
	cachedState, exist := importer.cache.HasGet([]byte{}, uintToBytes(event.GetLessonId()))

	if exist && cachedState[0] == importer.makeLessonState(event.GetDate(), event.GetTypeId(), event.IsDeleted) {
		fmt.Fprintf(
			importer.out, "[%s] %T confirmed: %d \n",
			t(), event, event.GetLessonId(),
		)
		importer.confirmed <- *event
		return true
	}
	return false
}

func (importer *EditedLessonsImporter) pullEditedLessons() error {
	lessonIds := make([]any, 0)
	for _, event := range importer.eventQueue {
		lessonIds = append(lessonIds, event.GetLessonId())
	}

	fmt.Fprintf(importer.out, "[%s] Check edited lessons ids: %v \n", t(), lessonIds)
	tx, rows, err := queryRowsInTransaction(
		importer.db, extractInPlaceHolder(LessonsEditedQuery, len(lessonIds)), lessonIds...,
	)
	defer closeRowsAndTransaction(rows, tx)
	if err != nil {
		return err
	}

	lessonsUpdatedMap := make(map[uint]byte)
	var event events.LessonEvent
	var messages []kafka.Message
	for rows.Next() {
		err = rows.Scan(&event.Id, &event.DisciplineId, &event.Date, &event.TypeId, &event.Semester, &event.IsDeleted)
		if err != nil {
			fmt.Fprintf(importer.out, "[%s] Error with fetching new lesson: %s \n", t(), err)
			continue
		}
		payload, _ := json.Marshal(event)
		messages = append(messages, kafka.Message{
			Key:   []byte(events.LessonEventName),
			Value: payload,
		})
		lessonsUpdatedMap[event.Id] = importer.makeLessonState(event.Date, event.TypeId, event.IsDeleted)
	}
	err = nil
	fmt.Fprintf(
		importer.out, "[%s] Finished importing edited %d lessons - get %d lessons \n",
		t(), len(lessonIds), len(messages),
	)

	if len(messages) != 0 {
		err = importer.writer.WriteMessages(context.Background(), messages...)
		if err == nil {
			for lessonId, state := range lessonsUpdatedMap {
				importer.cache.Set(uintToBytes(lessonId), []byte{state})
			}
		}
	}
	return err
}

const BitsForLessonYearDay = 6

func (importer *EditedLessonsImporter) makeLessonState(date time.Time, typeId uint8, isDeleted bool) (state byte) {
	if isDeleted {
		return 255
	}
	return byte(date.YearDay()%(1<<BitsForLessonYearDay)) | (typeId << BitsForLessonYearDay)
}
