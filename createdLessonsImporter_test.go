package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/kneu-messenger-pigeon/fileStorage"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"math/rand"
	"regexp"
	"strconv"
	"testing"
	"time"
)

var LessonsSelectExpectedColumns = []string{
	"ID", "NUM_PREDM", "DATEZAN", "NUM_VARZAN", "HALF", "isDeleted",
}

func TestExecuteImportCreatedLesson(t *testing.T) {
	var out bytes.Buffer
	var ctx context.Context
	var cancel context.CancelFunc
	var expectedEvent events.LessonEvent

	var matchContext = mock.MatchedBy(func(ctx context.Context) bool { return true })

	disciplineId := 215

	expectLessonEventMessage := func(expected events.LessonEvent) func(kafka.Message) bool {
		var actualEvent events.LessonEvent

		return func(message kafka.Message) bool {
			_ = json.Unmarshal(message.Value, &actualEvent)

			return assert.Equal(t, events.LessonEventName, string(message.Key)) &&
				assert.Equalf(
					t, expected, actualEvent,
					"Unexpected event: %v \n", actualEvent,
				)
		}
	}

	t.Run("New valid lesson", func(t *testing.T) {
		lastLessonId := 10

		expectedEvent = events.LessonEvent{
			Id:           uint(lastLessonId) + 1,
			DisciplineId: uint(disciplineId),
			TypeId:       uint8(rand.Intn(10) + 1),
			Date:         time.Date(2022, 12, 20, 14, 36, 0, 0, time.Local),
			Semester:     2,
			IsDeleted:    false,
		}

		lessonCreatedEvent := LessonCreateEvent{
			CommonEventData: CommonEventData{
				ReceiptHandle: nil,
				Timestamp:     time.Now().Unix(),
				LessonId:      "0",
				DisciplineId:  strconv.Itoa(int(expectedEvent.DisciplineId)),
				Semester:      strconv.Itoa(int(expectedEvent.Semester)),
			},
			TypeId:    strconv.Itoa(int(expectedEvent.TypeId)),
			Date:      "04.05.2023",
			TeacherId: "9999",
		}

		db, dbMock, _ := sqlmock.New()
		dbMock.ExpectQuery(regexp.QuoteMeta(LessonDefaultLastIdQuery)).WithArgs(
			getTodayString(),
		).WillReturnRows(
			sqlmock.NewRows([]string{"ID"}).AddRow(strconv.Itoa(lastLessonId)),
		)
		dbMock.ExpectBegin()
		dbMock.ExpectQuery(regexp.QuoteMeta(LessonsCreatedQuery)).WithArgs(
			lastLessonId,
		).WillReturnRows(
			sqlmock.NewRows(LessonsSelectExpectedColumns).AddRow(
				expectedEvent.Id, expectedEvent.DisciplineId, expectedEvent.Date,
				expectedEvent.TypeId, expectedEvent.Semester, expectedEvent.IsDeleted,
			),
		)
		dbMock.ExpectRollback()

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Get").Once().Return("", nil)
		fileStorageMock.On("Set", strconv.Itoa(int(expectedEvent.Id))).Once().Return(nil)

		writerMock := events.NewMockWriterInterface(t)
		writerMock.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(expectLessonEventMessage(expectedEvent)),
		).Return(nil)

		createdLessonsImporter := &CreatedLessonsImporter{
			out:     &out,
			db:      db,
			cache:   NewTimeCache(1),
			storage: fileStorageMock,
			writer:  writerMock,
		}

		createdLessonsImporter.addEvent(lessonCreatedEvent)

		var confirmed LessonCreateEvent

		ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*100)
		go func() {
			confirmed = <-createdLessonsImporter.confirmed
			cancel()
		}()

		go createdLessonsImporter.execute(ctx)
		<-ctx.Done()
		close(createdLessonsImporter.confirmed)

		assert.Equalf(t, lessonCreatedEvent, confirmed, "Expect that event will be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)
		fileStorageMock.AssertExpectations(t)
	})

	t.Run("Error rows and Error write to Kafka", func(t *testing.T) {
		out.Reset()
		expectedError := errors.New("expected error")

		lastLessonId := 10

		expectedEvent = events.LessonEvent{
			Id:           uint(lastLessonId) + 1,
			DisciplineId: uint(disciplineId),
			TypeId:       uint8(rand.Intn(10) + 1),
			Date:         time.Date(2022, 12, 20, 14, 36, 0, 0, time.Local),
			Semester:     2,
			IsDeleted:    false,
		}

		lessonCreatedEvent := LessonCreateEvent{
			CommonEventData: CommonEventData{
				ReceiptHandle: nil,
				Timestamp:     time.Now().Unix(),
				LessonId:      "0",
				DisciplineId:  strconv.Itoa(int(expectedEvent.DisciplineId)),
				Semester:      strconv.Itoa(int(expectedEvent.Semester)),
			},
			TypeId:    strconv.Itoa(int(expectedEvent.TypeId)),
			Date:      "04.05.2023",
			TeacherId: "9999",
		}

		db, dbMock, _ := sqlmock.New()
		dbMock.ExpectBegin()
		dbMock.ExpectQuery(regexp.QuoteMeta(LessonsCreatedQuery)).WithArgs(
			lastLessonId,
		).WillReturnRows(
			sqlmock.NewRows([]string{
				"ID", "NUM_PREDM", "DATEZAN", "NUM_VARZAN", "HALF", "isDeleted",
			}).AddRow(
				expectedEvent.Id, expectedEvent.DisciplineId, expectedEvent.Date,
				expectedEvent.TypeId, expectedEvent.Semester, expectedEvent.IsDeleted,
			).AddRow( // emulate row error
				999, 222, nil,
				nil, nil, nil,
			),
		)
		dbMock.ExpectRollback()

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Get").Once().Return(strconv.Itoa(lastLessonId), nil)
		fileStorageMock.On("Set", strconv.Itoa(int(expectedEvent.Id))).Once().Return(nil)

		writerMock := events.NewMockWriterInterface(t)
		writerMock.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(expectLessonEventMessage(expectedEvent)),
		).Return(expectedError)

		createdLessonsImporter := &CreatedLessonsImporter{
			out:     &out,
			db:      db,
			cache:   NewTimeCache(1),
			storage: fileStorageMock,
			writer:  writerMock,
		}

		var confirmed LessonCreateEvent
		ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*50)
		createdLessonsImporter.addEvent(lessonCreatedEvent)

		go func() {
			confirmed = <-createdLessonsImporter.confirmed
			cancel()
		}()
		go createdLessonsImporter.execute(ctx)
		<-ctx.Done()
		close(createdLessonsImporter.confirmed)

		assert.Equalf(t, LessonCreateEvent{}, confirmed, "Expect that event will be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)
		fileStorageMock.AssertExpectations(t)

		assert.Contains(t, out.String(), "Error with fetching new lesson: sql")
		assert.Contains(t, out.String(), expectedError.Error())
	})
}

func TestImportCreatedLesson(t *testing.T) {
	var out bytes.Buffer
	var ctx context.Context
	var cancel context.CancelFunc

	t.Run("Transaction Begin error", func(t *testing.T) {
		out.Reset()
		expectedError := errors.New("expected error")

		lastLessonId := 10

		db, dbMock, _ := sqlmock.New()
		dbMock.ExpectBegin().WillReturnError(expectedError)

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Get").Once().Return(strconv.Itoa(lastLessonId), nil)

		writerMock := events.NewMockWriterInterface(t)

		createdLessonsImporter := &CreatedLessonsImporter{
			out:     &out,
			db:      db,
			cache:   NewTimeCache(1),
			storage: fileStorageMock,
			writer:  writerMock,
		}

		var confirmed LessonCreateEvent
		ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*50)

		go func() {
			confirmed = <-createdLessonsImporter.confirmed
			cancel()
		}()
		go createdLessonsImporter.execute(ctx)
		<-ctx.Done()
		close(createdLessonsImporter.confirmed)

		assert.Equalf(t, LessonCreateEvent{}, confirmed, "Expect that event will be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)
		writerMock.AssertNotCalled(t, "WriteMessages")
		fileStorageMock.AssertExpectations(t)
		fileStorageMock.AssertNotCalled(t, "Set")

		assert.Contains(t, out.String(), expectedError.Error())
	})
}

func TestGetMaxLessonId(t *testing.T) {
	var out bytes.Buffer

	t.Run("Fail get max Lesson id", func(t *testing.T) {
		expectedError := errors.New("expected error")

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Get").Once().Return("", expectedError)

		createdLessonsImporter := &CreatedLessonsImporter{
			out:     &out,
			storage: fileStorageMock,
		}

		actualLessonMaxId := createdLessonsImporter.getLessonMaxId()

		assert.Contains(t, out.String(), expectedError.Error())
		assert.Equal(t, uint(1), actualLessonMaxId)
	})
}

func TestSetMaxLessonId(t *testing.T) {
	var out bytes.Buffer

	t.Run("Fail set max Lesson id", func(t *testing.T) {
		expectedError := errors.New("expected error")
		expectedLessonMaxId := uint(20)

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Set", strconv.Itoa(int(expectedLessonMaxId))).Once().Return(expectedError)

		createdLessonsImporter := &CreatedLessonsImporter{
			out:     &out,
			storage: fileStorageMock,
		}

		actualError := createdLessonsImporter.setLessonMaxId(expectedLessonMaxId)

		assert.Contains(t, out.String(), expectedError.Error())
		assert.Error(t, actualError)
		assert.Equal(t, expectedError, actualError)
	})

	t.Run("Lesson max id chan", func(t *testing.T) {
		expectedLessonMaxId := uint(50)

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Set", strconv.Itoa(int(expectedLessonMaxId))).Once().Return(nil)

		createdLessonsImporter := &CreatedLessonsImporter{
			out:     &out,
			storage: fileStorageMock,
		}

		lessonMaxIdChan := createdLessonsImporter.waitForNewLessonMaxId()

		var actualLessonMaxId uint
		go func() {
			actualLessonMaxId = <-lessonMaxIdChan
		}()

		err := createdLessonsImporter.setLessonMaxId(expectedLessonMaxId)
		close(lessonMaxIdChan)
		time.Sleep(time.Microsecond * 50)

		assert.NoError(t, err)
		assert.Equal(t, expectedLessonMaxId, actualLessonMaxId)
	})
}
