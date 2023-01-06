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
	"strconv"
	"time"
)

const LessonsCreatedQuery = LessonsSelect + ` WHERE ID > ? ORDER BY ID ASC`

const LessonDefaultLastIdQuery = `SELECT FIRST 1 ID FROM T_PRJURN WHERE REGDATE < ? ORDER BY ID DESC`

type CreatedLessonsImporterInterface interface {
	execute(context context.Context)
	addEvent(event LessonCreateEvent)
	getConfirmed() <-chan LessonCreateEvent
}

type CreatedLessonsImporter struct {
	out             io.Writer
	db              *sql.DB
	cache           *timeCache
	writer          events.WriterInterface
	storage         fileStorage.Interface
	eventQueue      []LessonCreateEvent
	confirmed       chan LessonCreateEvent
	lessonMaxId     uint
	lessonMaxIdChan chan uint
}

func (importer *CreatedLessonsImporter) execute(context context.Context) {
	importer.initConfirmed()

	var err error
	nextRun := time.NewTimer(0)
	nextRequiredRun := time.Now()
	for {
		select {
		case <-context.Done():
			close(importer.confirmed)
			return

		case <-nextRun.C:
			nextRun = time.NewTimer(time.Second * 3)
			if len(importer.eventQueue) != 0 || time.Now().After(nextRequiredRun) {
				nextRequiredRun = time.Now().Add(time.Minute * 30)

				err = importer.pullCreatedLessons()
				if err != nil {
					fmt.Fprintf(importer.out, "[%s] Failed to fetch created lessons: %s \n", t(), err)
				}
				importer.determineConfirmedEvents()
			}
		}
	}
}

func (importer *CreatedLessonsImporter) addEvent(event LessonCreateEvent) {
	if !importer.putIntoConfirmedIfSatisfy(&event) {
		importer.eventQueue = append(importer.eventQueue, event)

		fmt.Fprintf(
			importer.out, "[%s] receive %T - discipline: %d; added to processing queue \n",
			t(), event, event.GetDisciplineId(),
		)
	}
}

func (importer *CreatedLessonsImporter) getConfirmed() <-chan LessonCreateEvent {
	importer.initConfirmed()
	return importer.confirmed
}

func (importer *CreatedLessonsImporter) initConfirmed() {
	if importer.confirmed == nil {
		importer.confirmed = make(chan LessonCreateEvent)
	}
}

func (importer *CreatedLessonsImporter) determineConfirmedEvents() {
	length := len(importer.eventQueue)
	for i := 0; i < length; i++ {
		importer.putIntoConfirmedIfSatisfy(&importer.eventQueue[i])
	}

	importer.eventQueue = importer.eventQueue[length:len(importer.eventQueue)]
}

func (importer *CreatedLessonsImporter) putIntoConfirmedIfSatisfy(event *LessonCreateEvent) bool {
	if event.Timestamp <= importer.cache.Get(event.GetDisciplineId()) {
		fmt.Fprintf(
			importer.out, "[%s] %T confirmed, discipline: %d \n",
			t(), event, event.GetDisciplineId(),
		)
		importer.confirmed <- *event
		return true
	}
	return false
}

func (importer *CreatedLessonsImporter) pullCreatedLessons() error {
	fmt.Fprintf(importer.out, "[%s] Check new lessons created after the lesson #%d \n", t(), importer.getLessonMaxId())
	tx, rows, err := queryRowsInTransaction(importer.db, LessonsCreatedQuery, importer.getLessonMaxId())
	defer closeRowsAndTransaction(rows, tx)
	if err != nil {
		return err
	}

	disciplineUpdatedMap := make(map[uint]time.Time)
	now := time.Now()

	var event events.LessonEvent
	var messages []kafka.Message
	newLastId := importer.getLessonMaxId()
	for rows.Next() {
		err = rows.Scan(&event.Id, &event.DisciplineId, &event.Date, &event.TypeId, &event.Semester, &event.IsDeleted)
		if err != nil {
			fmt.Fprintf(importer.out, "[%s] Error with fetching new lesson: %s \n", t(), err)
			continue
		}
		newLastId = event.Id
		payload, _ := json.Marshal(event)
		messages = append(messages, kafka.Message{
			Key:   []byte(events.LessonEventName),
			Value: payload,
		})
		disciplineUpdatedMap[event.DisciplineId] = now
	}
	err = nil
	fmt.Fprintf(
		importer.out, "[%s] Finished importing created lessons from id %d to %d - get %d new lessons \n",
		t(), importer.getLessonMaxId(), newLastId, len(messages),
	)

	if len(messages) != 0 {
		err = importer.setLessonMaxId(newLastId)
		if err == nil {
			err = importer.writer.WriteMessages(context.Background(), messages...)
		}
		if err == nil {
			for disciplineId, updatedAt := range disciplineUpdatedMap {
				importer.cache.Set(disciplineId, updatedAt.Unix())
			}
		}
	}
	return err
}

func (importer *CreatedLessonsImporter) getLessonMaxId() uint {
	if importer.lessonMaxId == 0 {
		var uint64Value uint64
		stringValue, err := importer.storage.Get()

		if stringValue != "" {
			uint64Value, err = strconv.ParseUint(stringValue, 10, 0)
			importer.lessonMaxId = uint(uint64Value)

		} else if err == nil {
			// storage not exist or empty
			row := importer.db.QueryRow(LessonDefaultLastIdQuery, getTodayString())
			err = row.Err()
			if err == nil {
				err = row.Scan(&importer.lessonMaxId)
			}
			fmt.Fprintf(
				importer.out,
				"[%s] Loaded default max lesson id: %d, error: %v \n",
				t(), importer.lessonMaxId, err,
			)
		}

		if err != nil {
			fmt.Fprintf(importer.out, "[%s] Failed to get LessonMax from file %v \n", t(), err)
			// set non zero for prevent repeat error
			importer.lessonMaxId = 1
		}
	}

	return importer.lessonMaxId
}

func (importer *CreatedLessonsImporter) setLessonMaxId(newLastId uint) (err error) {
	if importer.lessonMaxId < newLastId {
		importer.lessonMaxId = newLastId
		err = importer.storage.Set(strconv.FormatUint(uint64(newLastId), 10))
		if err != nil {
			fmt.Fprintf(importer.out, "[%s] Failed to write LessonMaxId %v \n", t(), err)
		}
		if importer.lessonMaxIdChan != nil {
			importer.lessonMaxIdChan <- newLastId
		}
	}
	return
}

func (importer *CreatedLessonsImporter) waitForNewLessonMaxId() chan uint {
	importer.lessonMaxIdChan = make(chan uint)
	return importer.lessonMaxIdChan
}
