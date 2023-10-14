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

const LessonsEditedQuery = LessonsSelect + ` WHERE ID IN (?) ORDER BY ID ASC`
const CustomGroupLessonsEditedQuery = LessonsSelect + ` WHERE ID_ZANCG IN (?) ORDER BY ID ASC`

type EditedLessonsImporterInterface interface {
	Execute(context context.Context)
	AddEvent(event dekanatEvents.LessonEditEvent)
	GetConfirmed() <-chan dekanatEvents.LessonEditEvent
}

type EditedLessonsImporter struct {
	out             io.Writer
	db              *sql.DB
	cache           *fastcache.Cache
	writer          events.WriterInterface
	currentYear     CurrentYearGetterInterface
	eventQueue      []dekanatEvents.LessonEditEvent
	eventQueueMutex sync.Mutex
	confirmed       chan dekanatEvents.LessonEditEvent
}

func (importer *EditedLessonsImporter) Execute(context context.Context) {
	if importer.confirmed == nil {
		importer.confirmed = make(chan dekanatEvents.LessonEditEvent)
	}

	var err error
	nextTick := time.Tick(defaultPollInterval)
	for {
		if len(importer.eventQueue) != 0 {
			err = importer.pullEditedLessons()
			if err != nil {
				fmt.Fprintf(importer.out, "[%s] Failed to fetch edited lessons: %s \n", t(), err)
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

func (importer *EditedLessonsImporter) AddEvent(event dekanatEvents.LessonEditEvent) {
	if !importer.putIntoConfirmedIfSatisfy(&event) {
		importer.eventQueueMutex.Lock()
		importer.eventQueue = append(importer.eventQueue, event)
		importer.eventQueueMutex.Unlock()

		fmt.Fprintf(
			importer.out, "[%s] receive dekanatEvents.LessonEditEvent - discipline: %d; lesson: %d; added to processing queue \n",
			t(), event.GetDisciplineId(), event.GetLessonId(),
		)
	}
}

func (importer *EditedLessonsImporter) GetConfirmed() <-chan dekanatEvents.LessonEditEvent {
	return importer.confirmed
}

func (importer *EditedLessonsImporter) determineConfirmedEvents() {
	length := len(importer.eventQueue)
	for i := 0; i < length; i++ {
		importer.putIntoConfirmedIfSatisfy(&importer.eventQueue[i])
	}

	importer.eventQueueMutex.Lock()
	importer.eventQueue = importer.eventQueue[length:len(importer.eventQueue)]
	importer.eventQueueMutex.Unlock()
}

func (importer *EditedLessonsImporter) putIntoConfirmedIfSatisfy(event *dekanatEvents.LessonEditEvent) bool {
	cachedState, exist := importer.cache.HasGet(
		[]byte{},
		idToBytes(event.GetLessonId(), event.IsCustomGroup()),
	)

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
		fmt.Fprintf(importer.out, "[%s] Check edited lessons ids in regular groups: %v \n", t(), regularGroupLessonIds)
		err = importer.doPullEditedLessons(LessonsEditedQuery, regularGroupLessonIds)
	}

	if len(customGroupLessonIds) != 0 && err == nil {
		fmt.Fprintf(importer.out, "[%s] Check edited lessons ids in custom groups: %v \n", t(), regularGroupLessonIds)
		err = importer.doPullEditedLessons(CustomGroupLessonsEditedQuery, customGroupLessonIds)
	}

	return err
}

func (importer *EditedLessonsImporter) doPullEditedLessons(query string, lessonIds []any) error {
	tx, rows, err := queryRowsInTransaction(
		importer.db, extractInPlaceHolder(query, len(lessonIds)),
		lessonIds...,
	)
	defer closeRowsAndTransaction(rows, tx)
	if err != nil {
		return err
	}

	lessonsUpdatedMap := make(map[uint]byte)
	customGroupLessonUpdatedMap := make(map[uint]byte)

	var event events.LessonEvent
	var messages []kafka.Message
	message := kafka.Message{
		Key: []byte(events.LessonEventName),
	}

	customGroupLessonId := sql.NullInt32{}
	var state byte
	for rows.Next() {
		err = rows.Scan(
			&event.Id, &customGroupLessonId,
			&event.DisciplineId, &event.Date,
			&event.TypeId, &event.Semester, &event.IsDeleted)
		if err != nil {
			fmt.Fprintf(importer.out, "[%s] Error with fetching new lesson: %s \n", t(), err)
			continue
		}
		event.Year = importer.currentYear.GetYear()
		message.Value, _ = json.Marshal(event)
		messages = append(messages, message)
		state = importer.makeLessonState(event.Date, event.TypeId, event.IsDeleted)
		lessonsUpdatedMap[event.Id] = state
		if customGroupLessonId.Valid {
			customGroupLessonUpdatedMap[uint(customGroupLessonId.Int32)] = state
		}
	}
	err = nil
	fmt.Fprintf(
		importer.out, "[%s] Finished importing edited %d lessons - get %d lessons \n",
		t(), len(lessonIds), len(messages),
	)

	if len(messages) != 0 {
		err = importer.writer.WriteMessages(context.Background(), messages...)
		if err == nil {
			var lessonId uint
			for lessonId, state = range lessonsUpdatedMap {
				importer.cache.Set(idToBytes(lessonId, false), []byte{state})
			}

			for lessonId, state = range customGroupLessonUpdatedMap {
				importer.cache.Set(idToBytes(lessonId, true), []byte{state})
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
