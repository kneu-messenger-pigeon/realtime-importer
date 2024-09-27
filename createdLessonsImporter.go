package main

import (
	"context"
	"database/sql"
	"encoding/binary"
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

const LessonsCreatedQuery = LessonsSelect + ` WHERE ID > ? ORDER BY ID ASC`

const LessonDefaultLastIdQuery = `SELECT FIRST 1 ID FROM T_PRJURN WHERE REGDATE < ? ORDER BY ID DESC`

type CreatedLessonsImporterInterface interface {
	Execute(context context.Context)
	AddEvent(event dekanatEvents.LessonCreateEvent)
	GetConfirmed() <-chan dekanatEvents.LessonCreateEvent
}

type CreatedLessonsImporter struct {
	out                   io.Writer
	db                    *sql.DB
	cache                 *timeCache
	writer                events.WriterInterface
	storage               fileStorage.Interface
	currentYear           CurrentYearWatcherInterface
	eventQueue            []dekanatEvents.LessonCreateEvent
	eventQueueMutex       sync.Mutex
	confirmed             chan dekanatEvents.LessonCreateEvent
	lessonMaxId           uint
	lessonMaxIdChan       chan uint
	editScoresMaxLessonId MaxLessonIdGetterInterface
}

func (importer *CreatedLessonsImporter) Execute(context context.Context) {
	importer.initConfirmed()

	var err error
	nextTick := time.Tick(defaultPollInterval)
	nextRequiredTick := time.Tick(defaultForcePollInterval)

	requiredPoll := true
	for {
		if requiredPoll || len(importer.eventQueue) != 0 || importer.editScoresMaxLessonId.Get() > importer.getLessonMaxId() {
			requiredPoll = false

			err = importer.pullCreatedLessons()
			if err != nil {
				fmt.Fprintf(importer.out, "[%s] Failed to fetch created lessons: %s \n", t(), err)
			}
			importer.determineConfirmedEvents()
		}

		select {
		case <-context.Done():
			close(importer.confirmed)
			return

		case <-nextRequiredTick:
			requiredPoll = true
		case <-nextTick:
		}
	}
}

func (importer *CreatedLessonsImporter) AddEvent(event dekanatEvents.LessonCreateEvent) {
	if !importer.putIntoConfirmedIfSatisfy(&event) {
		importer.eventQueueMutex.Lock()
		importer.eventQueue = append(importer.eventQueue, event)
		importer.eventQueueMutex.Unlock()

		fmt.Fprintf(
			importer.out, "[%s] receive %T - discipline: %d; added to processing queue \n",
			t(), event, event.GetDisciplineId(),
		)
	}
}

func (importer *CreatedLessonsImporter) GetConfirmed() <-chan dekanatEvents.LessonCreateEvent {
	importer.initConfirmed()
	return importer.confirmed
}

func (importer *CreatedLessonsImporter) initConfirmed() {
	if importer.confirmed == nil {
		importer.confirmed = make(chan dekanatEvents.LessonCreateEvent)
	}
}

func (importer *CreatedLessonsImporter) determineConfirmedEvents() {
	length := len(importer.eventQueue)
	for i := 0; i < length; i++ {
		importer.putIntoConfirmedIfSatisfy(&importer.eventQueue[i])
	}

	importer.eventQueueMutex.Lock()
	importer.eventQueue = importer.eventQueue[length:len(importer.eventQueue)]
	importer.eventQueueMutex.Unlock()
}

func (importer *CreatedLessonsImporter) putIntoConfirmedIfSatisfy(event *dekanatEvents.LessonCreateEvent) bool {
	if event.Timestamp <= importer.cache.Get(event.GetDisciplineId(), event.IsCustomGroup()) {
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
	message := kafka.Message{
		Key: []byte(events.LessonEventName),
	}
	newLastId := importer.getLessonMaxId()

	customGroupLessonId := sql.NullInt32{}
	isLessonInCustomGroupCreated := false
	for rows.Next() {
		err = rows.Scan(
			&event.Id, &customGroupLessonId,
			&event.DisciplineId, &event.Date,
			&event.TypeId, &event.Semester, &event.IsDeleted,
		)
		if err != nil {
			fmt.Fprintf(importer.out, "[%s] Error with fetching new lesson: %s \n", t(), err)
			continue
		}
		event.Year = importer.currentYear.GetYear()
		newLastId = event.Id
		message.Value, _ = json.Marshal(event)
		messages = append(messages, message)
		disciplineUpdatedMap[event.DisciplineId] = now

		if customGroupLessonId.Valid {
			isLessonInCustomGroupCreated = true
		}
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
				importer.cache.Set(disciplineId, false, updatedAt.Unix())
			}

			if isLessonInCustomGroupCreated {
				// we don't receive discipline id in case with custom group.
				// So we need to force confirm all events with empty disciplineId
				importer.cache.Set(0, true, now.Unix())
			}
		}
	}
	return err
}

func (importer *CreatedLessonsImporter) getLessonMaxId() uint {
	if importer.lessonMaxId == 0 {
		storageValue, err := importer.storage.Get()

		if storageValue != nil && len(storageValue) >= 8 {
			importer.lessonMaxId = uint(binary.LittleEndian.Uint64(storageValue))

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

		storageValue := make([]byte, 8)
		binary.LittleEndian.PutUint64(storageValue, uint64(newLastId))
		err = importer.storage.Set(storageValue)
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
