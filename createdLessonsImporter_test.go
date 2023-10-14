package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	dekanatEvents "github.com/kneu-messenger-pigeon/dekanat-events"
	"github.com/kneu-messenger-pigeon/events"
	eventsMocks "github.com/kneu-messenger-pigeon/events/mocks"
	"github.com/kneu-messenger-pigeon/fileStorage"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"math/rand"
	"realtime-importer/mocks"
	"regexp"
	"runtime"
	"strconv"
	"testing"
	"time"
)

var LessonsSelectExpectedColumns = []string{
	"ID", "CUSTOM_GROUP_LESSON_ID",
	"NUM_PREDM", "DATEZAN",
	"NUM_VARZAN", "HALF", "isDeleted",
}

func TestExecuteImportCreatedLesson(t *testing.T) {
	var out bytes.Buffer

	var matchContext = mock.MatchedBy(func(ctx context.Context) bool { return true })
	disciplineId := 215

	defaultPollInterval = time.Millisecond * 100
	defaultForcePollInterval = time.Millisecond * 150

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

		expectedEvent := events.LessonEvent{
			Id:           uint(lastLessonId) + 1,
			DisciplineId: uint(disciplineId),
			TypeId:       uint8(rand.Intn(10) + 1),
			Date:         time.Date(2022, 12, 20, 14, 36, 0, 0, time.Local),
			Year:         2030,
			Semester:     2,
			IsDeleted:    false,
		}

		lessonCreatedEvent := dekanatEvents.LessonCreateEvent{
			CommonEventData: dekanatEvents.CommonEventData{
				ReceiptHandle: nil,
				Timestamp:     time.Now().Unix(),
				HasChanges:    true,
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
				expectedEvent.Id, sql.NullInt32{},
				expectedEvent.DisciplineId, expectedEvent.Date,
				expectedEvent.TypeId, expectedEvent.Semester, expectedEvent.IsDeleted,
			),
		)
		dbMock.ExpectRollback()

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Get").Once().Return("", nil)
		fileStorageMock.On("Set", strconv.Itoa(int(expectedEvent.Id))).Once().Return(nil)

		writerMock := eventsMocks.NewWriterInterface(t)
		writerMock.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(expectLessonEventMessage(expectedEvent)),
		).Return(nil)

		editScoresMaxLessonId := mocks.NewMaxLessonIdGetterInterface(t)
		editScoresMaxLessonId.On("Get").Return(uint(0))

		createdLessonsImporter := &CreatedLessonsImporter{
			out:                   &out,
			db:                    db,
			cache:                 NewTimeCache(1),
			storage:               fileStorageMock,
			writer:                writerMock,
			currentYear:           NewMockCurrentYearGetter(t, expectedEvent.Year),
			editScoresMaxLessonId: editScoresMaxLessonId,
		}

		createdLessonsImporter.AddEvent(lessonCreatedEvent)
		var confirmed dekanatEvents.LessonCreateEvent

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
		go createdLessonsImporter.Execute(ctx)
		runtime.Gosched()

		select {
		case confirmed = <-createdLessonsImporter.GetConfirmed():
			time.Sleep(defaultForcePollInterval)
		case <-ctx.Done():
		}
		cancel()

		assert.Equalf(t, lessonCreatedEvent, confirmed, "Expect that event will be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)
		fileStorageMock.AssertExpectations(t)
	})

	t.Run("LessonIdFromEditedScore", func(t *testing.T) {
		lastLessonId := 10

		expectedEvent := events.LessonEvent{
			Id:           uint(lastLessonId) + 1,
			DisciplineId: uint(disciplineId),
			TypeId:       uint8(rand.Intn(10) + 1),
			Date:         time.Date(2022, 12, 20, 14, 36, 0, 0, time.Local),
			Year:         2030,
			Semester:     2,
			IsDeleted:    false,
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
				expectedEvent.Id, sql.NullInt32{},
				expectedEvent.DisciplineId, expectedEvent.Date,
				expectedEvent.TypeId, expectedEvent.Semester, expectedEvent.IsDeleted,
			),
		)
		dbMock.ExpectRollback()

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Get").Once().Return("", nil)
		fileStorageMock.On("Set", strconv.Itoa(int(expectedEvent.Id))).Once().Return(nil)

		writerMock := eventsMocks.NewWriterInterface(t)
		writerMock.On("WriteMessages", matchContext, mock.MatchedBy(expectLessonEventMessage(expectedEvent))).
			Return(nil)

		editScoresMaxLessonId := mocks.NewMaxLessonIdGetterInterface(t)
		editScoresMaxLessonId.On("Get").Return(expectedEvent.Id)

		createdLessonsImporter := &CreatedLessonsImporter{
			out:                   &out,
			db:                    db,
			cache:                 NewTimeCache(1),
			storage:               fileStorageMock,
			writer:                writerMock,
			currentYear:           NewMockCurrentYearGetter(t, expectedEvent.Year),
			editScoresMaxLessonId: editScoresMaxLessonId,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
		defer cancel()

		go createdLessonsImporter.Execute(ctx)
		<-ctx.Done()

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)
		fileStorageMock.AssertExpectations(t)
	})

	t.Run("VirtualGroupWithEmptyDiscipline", func(t *testing.T) {
		lastLessonId := 10

		expectedEvent := events.LessonEvent{
			Id:           uint(lastLessonId) + 1,
			DisciplineId: uint(disciplineId),
			TypeId:       uint8(rand.Intn(10) + 1),
			Date:         time.Date(2022, 12, 20, 14, 36, 0, 0, time.Local),
			Year:         2030,
			Semester:     2,
			IsDeleted:    false,
		}

		lessonCreatedEvent := dekanatEvents.LessonCreateEvent{
			CommonEventData: dekanatEvents.CommonEventData{
				ReceiptHandle: nil,
				Timestamp:     time.Now().Unix(),
				HasChanges:    true,
				LessonId:      "0",
				DisciplineId:  "-1",
				Semester:      "0",
			},
			TypeId:    strconv.Itoa(rand.Intn(10) + 1),
			Date:      "04.05.2023",
			TeacherId: "9999",
		}

		db, dbMock, _ := sqlmock.New()
		dbMock.ExpectQuery(regexp.QuoteMeta(LessonDefaultLastIdQuery)).WithArgs(
			getTodayString(),
		).WillReturnRows(
			sqlmock.NewRows([]string{"ID"}).AddRow(strconv.Itoa(lastLessonId)),
		)

		customGroupLessonId := sql.NullInt32{
			Int32: 999,
			Valid: true,
		}

		dbMock.ExpectBegin()
		dbMock.ExpectQuery(regexp.QuoteMeta(LessonsCreatedQuery)).WithArgs(
			lastLessonId,
		).WillReturnRows(
			sqlmock.NewRows(LessonsSelectExpectedColumns).
				AddRow(
					expectedEvent.Id, customGroupLessonId,
					expectedEvent.DisciplineId, expectedEvent.Date,
					expectedEvent.TypeId, expectedEvent.Semester, expectedEvent.IsDeleted,
				),
		)
		dbMock.ExpectRollback()

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Get").Once().Return("", nil)
		fileStorageMock.On("Set", strconv.Itoa(int(expectedEvent.Id))).Once().Return(nil)

		writerMock := eventsMocks.NewWriterInterface(t)
		writerMock.On("WriteMessages", matchContext, mock.MatchedBy(expectLessonEventMessage(expectedEvent))).
			Return(nil)

		editScoresMaxLessonId := mocks.NewMaxLessonIdGetterInterface(t)
		editScoresMaxLessonId.On("Get").Return(uint(0))

		createdLessonsImporter := &CreatedLessonsImporter{
			out:                   &out,
			db:                    db,
			cache:                 NewTimeCache(1),
			storage:               fileStorageMock,
			writer:                writerMock,
			currentYear:           NewMockCurrentYearGetter(t, expectedEvent.Year),
			editScoresMaxLessonId: editScoresMaxLessonId,
		}

		var confirmed dekanatEvents.LessonCreateEvent
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)

		createdLessonsImporter.AddEvent(lessonCreatedEvent)
		go createdLessonsImporter.Execute(ctx)
		runtime.Gosched()

		select {
		case confirmed = <-createdLessonsImporter.GetConfirmed():
			time.Sleep(defaultForcePollInterval)
		case <-ctx.Done():
		}
		cancel()
		runtime.Gosched()

		assert.Equalf(t, lessonCreatedEvent, confirmed, "Expect that event will be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		fileStorageMock.AssertExpectations(t)
	})

	t.Run("Error rows and Error write to Kafka", func(t *testing.T) {
		out.Reset()
		expectedError := errors.New("expected error")

		lastLessonId := 10

		expectedEvent := events.LessonEvent{
			Id:           uint(lastLessonId) + 1,
			DisciplineId: uint(disciplineId),
			TypeId:       uint8(rand.Intn(10) + 1),
			Date:         time.Date(2022, 12, 20, 14, 36, 0, 0, time.Local),
			Year:         2030,
			Semester:     2,
			IsDeleted:    false,
		}

		lessonCreatedEvent := dekanatEvents.LessonCreateEvent{
			CommonEventData: dekanatEvents.CommonEventData{
				ReceiptHandle: nil,
				Timestamp:     time.Now().Unix(),
				HasChanges:    true,
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
			sqlmock.NewRows(LessonsSelectExpectedColumns).AddRow(
				expectedEvent.Id, sql.NullInt32{},
				expectedEvent.DisciplineId, expectedEvent.Date,
				expectedEvent.TypeId, expectedEvent.Semester, expectedEvent.IsDeleted,
			).AddRow( // emulate row error
				999, nil,
				222, nil,
				nil, nil, nil,
			),
		)
		dbMock.ExpectRollback()

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Get").Once().Return(strconv.Itoa(lastLessonId), nil)
		fileStorageMock.On("Set", strconv.Itoa(int(expectedEvent.Id))).Once().Return(nil)

		writerMock := eventsMocks.NewWriterInterface(t)
		writerMock.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(expectLessonEventMessage(expectedEvent)),
		).Return(expectedError)

		createdLessonsImporter := &CreatedLessonsImporter{
			out:         &out,
			db:          db,
			cache:       NewTimeCache(1),
			storage:     fileStorageMock,
			currentYear: NewMockCurrentYearGetter(t, expectedEvent.Year),
			writer:      writerMock,
		}

		var confirmed dekanatEvents.LessonCreateEvent
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		createdLessonsImporter.AddEvent(lessonCreatedEvent)

		go createdLessonsImporter.Execute(ctx)
		runtime.Gosched()

		select {
		case confirmed = <-createdLessonsImporter.GetConfirmed():
		case <-ctx.Done():
		}
		cancel()

		assert.Equalf(t, dekanatEvents.LessonCreateEvent{}, confirmed, "Expect that event will be confirmed")

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

	t.Run("Transaction Begin error", func(t *testing.T) {
		out.Reset()
		expectedError := errors.New("expected error")

		lastLessonId := 10

		db, dbMock, _ := sqlmock.New()
		dbMock.ExpectBegin().WillReturnError(expectedError)

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Get").Once().Return(strconv.Itoa(lastLessonId), nil)

		writerMock := eventsMocks.NewWriterInterface(t)

		createdLessonsImporter := &CreatedLessonsImporter{
			out:     &out,
			db:      db,
			cache:   NewTimeCache(1),
			storage: fileStorageMock,
			writer:  writerMock,
		}

		var confirmed dekanatEvents.LessonCreateEvent
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)

		go createdLessonsImporter.Execute(ctx)
		runtime.Gosched()

		select {
		case confirmed = <-createdLessonsImporter.GetConfirmed():
			time.Sleep(defaultPollInterval)
		case <-ctx.Done():
		}

		cancel()
		runtime.Gosched()

		assert.Equalf(t, dekanatEvents.LessonCreateEvent{}, confirmed, "Expect that event will be confirmed")

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
		runtime.Gosched()

		assert.NoError(t, err)
		assert.Equal(t, expectedLessonMaxId, actualLessonMaxId)
	})
}
