package main

import (
	"bytes"
	"context"
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
	"realtime-importer/mocks"
	"regexp"
	"runtime"
	"strconv"
	"testing"
	"time"
)

var scoreSelectExpectedColumns = []string{
	"ID", "STUDENT_ID", "LESSON_ID", "LESSON_PART", "DISCIPLINE_ID", "SEMESTER",
	"SCORE", "IS_ABSENT", "REGDATE", "IS_DELETED",
}

func TestExecuteImportUpdatedScores(t *testing.T) {
	var out bytes.Buffer
	var ctx context.Context
	var cancel context.CancelFunc
	var expectedEvent events.ScoreEvent

	defaultPollInterval = time.Millisecond * 100
	defaultForcePollInterval = time.Millisecond * 150

	var matchContext = mock.MatchedBy(func(ctx context.Context) bool { return true })

	lastRegDate := time.Now().Add(-time.Minute * 20)

	syncedAtRewrite := time.Now()
	syncedAtRewrite = time.Date(
		syncedAtRewrite.Year(), syncedAtRewrite.Month(), syncedAtRewrite.Day(),
		syncedAtRewrite.Hour(), syncedAtRewrite.Minute(), syncedAtRewrite.Second(),
		0, syncedAtRewrite.Location(),
	)

	expectScoreEventMessage := func(expected events.ScoreEvent) func(kafka.Message) bool {
		return func(message kafka.Message) bool {
			var actualEvent events.ScoreEvent
			_ = json.Unmarshal(message.Value, &actualEvent)
			actualEvent.SyncedAt = syncedAtRewrite

			return assert.Equal(t, events.ScoreEventName, string(message.Key)) &&
				assert.Equalf(t, expected, actualEvent, "Unexpected event: %v \n", actualEvent)
		}
	}

	t.Run("Score updated", func(t *testing.T) {
		expectedEvent = events.ScoreEvent{
			Id:           501,
			LessonId:     130,
			DisciplineId: 110,
			Year:         2030,
			Semester:     2,
			ScoreValue: events.ScoreValue{
				Value:     3,
				IsAbsent:  false,
				IsDeleted: true,
			},
			SyncedAt:    syncedAtRewrite,
			ScoreSource: events.Realtime,
		}

		updatedScoreEvent := dekanatEvents.ScoreEditEvent{
			CommonEventData: dekanatEvents.CommonEventData{
				ReceiptHandle: nil,
				Timestamp:     time.Now().Unix(),
				LessonId:      strconv.Itoa(int(expectedEvent.LessonId)),
				DisciplineId:  strconv.Itoa(int(expectedEvent.DisciplineId)),
				Semester:      strconv.Itoa(int(expectedEvent.Semester)),
			},
		}

		db, dbMock, _ := sqlmock.New()

		dbMock.ExpectBegin()
		dbMock.ExpectQuery(regexp.QuoteMeta(UpdateScoreQuery)).WithArgs(
			sqlmock.AnyArg(),
		).WillReturnRows(
			sqlmock.NewRows(scoreSelectExpectedColumns).AddRow(
				expectedEvent.Id, expectedEvent.StudentId, expectedEvent.LessonId, expectedEvent.LessonPart,
				expectedEvent.DisciplineId, expectedEvent.Semester, expectedEvent.Value, expectedEvent.IsAbsent,
				expectedEvent.UpdatedAt, expectedEvent.IsDeleted,
			),
		)
		dbMock.ExpectRollback()

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Get").Times(2).Return("", nil)
		fileStorageMock.On("Set", expectedEvent.UpdatedAt.Format(StorageTimeFormat)).Once().Return(nil)

		writerMock := eventsMocks.NewWriterInterface(t)
		writerMock.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(expectScoreEventMessage(expectedEvent)),
		).Return(nil)

		maxLessonIdSetter := mocks.NewMaxLessonIdSetterInterface(t)
		maxLessonIdSetter.On("Set", expectedEvent.LessonId).Times(2).Return(nil)

		updatedLessonsImporter := &UpdatedScoresImporter{
			out:         &out,
			db:          db,
			cache:       NewTimeCache(1),
			writer:      writerMock,
			storage:     fileStorageMock,
			currentYear: NewMockCurrentYearGetter(t, expectedEvent.Year),
			maxLessonId: maxLessonIdSetter,
		}

		var confirmed dekanatEvents.ScoreEditEvent

		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		updatedLessonsImporter.AddEvent(updatedScoreEvent)
		maxLessonIdSetter.AssertCalled(t, "Set", expectedEvent.LessonId)

		go updatedLessonsImporter.Execute(ctx)
		runtime.Gosched()
		select {
		case confirmed = <-updatedLessonsImporter.GetConfirmed():
			time.Sleep(defaultForcePollInterval)
			cancel()
			runtime.Gosched()
		case <-ctx.Done():
		}

		assert.Equalf(t, updatedScoreEvent, confirmed, "Expect that event will be confirmed")
		assert.NoError(t, dbMock.ExpectationsWereMet())

		writerMock.AssertExpectations(t)
		fileStorageMock.AssertExpectations(t)
	})

	t.Run("Score not created in DB", func(t *testing.T) {
		TestScoreNotCreated := func(t *testing.T, hasChanges bool) {
			expectedEvent = events.ScoreEvent{
				Id:           501,
				LessonId:     130,
				DisciplineId: 110,
				Semester:     2,
				ScoreValue: events.ScoreValue{
					Value:     3,
					IsAbsent:  false,
					IsDeleted: false,
				},
				SyncedAt:    syncedAtRewrite,
				ScoreSource: events.Realtime,
			}

			updateScoreEvent := dekanatEvents.ScoreEditEvent{
				CommonEventData: dekanatEvents.CommonEventData{
					ReceiptHandle: nil,
					Timestamp:     time.Now().Unix(),
					HasChanges:    hasChanges,
					LessonId:      strconv.Itoa(int(expectedEvent.LessonId)),
					DisciplineId:  strconv.Itoa(int(expectedEvent.DisciplineId)),
					Semester:      strconv.Itoa(int(expectedEvent.Semester)),
				},
			}

			db, dbMock, _ := sqlmock.New()

			dbMock.ExpectBegin()
			dbMock.ExpectQuery(regexp.QuoteMeta(UpdateScoreQuery)).WithArgs(
				lastRegDate.Format(FirebirdTimeFormat),
			).WillReturnRows(
				sqlmock.NewRows(scoreSelectExpectedColumns),
			)
			dbMock.ExpectRollback()

			fileStorageMock := fileStorage.NewMockInterface(t)
			fileStorageMock.On("Get").Once().Return(lastRegDate.Format(StorageTimeFormat), nil)

			writerMock := eventsMocks.NewWriterInterface(t)

			maxLessonIdSetter := mocks.NewMaxLessonIdSetterInterface(t)
			maxLessonIdSetter.On("Set", expectedEvent.LessonId).Once().Return(nil)

			updatedLessonsImporter := &UpdatedScoresImporter{
				out:         &out,
				db:          db,
				cache:       NewTimeCache(1),
				writer:      writerMock,
				storage:     fileStorageMock,
				maxLessonId: maxLessonIdSetter,
			}

			updatedLessonsImporter.AddEvent(updateScoreEvent)
			maxLessonIdSetter.AssertCalled(t, "Set", expectedEvent.LessonId)

			var confirmed dekanatEvents.ScoreEditEvent

			ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*300)
			go updatedLessonsImporter.Execute(ctx)
			runtime.Gosched()

			select {
			case confirmed = <-updatedLessonsImporter.GetConfirmed():
				cancel()
			case <-ctx.Done():
			}

			if hasChanges {
				assert.Empty(t, confirmed.LessonId, "Expect that event will not be confirmed - changes not found in DB")
			} else {
				assert.Equalf(t, updateScoreEvent, confirmed, "Expect that event will be confirmed - no changes in DB os nothing to found and confirm")
			}

			err := dbMock.ExpectationsWereMet()
			assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

			writerMock.AssertExpectations(t)
			writerMock.AssertNotCalled(t, "WriteMessages")
			fileStorageMock.AssertExpectations(t)
		}

		t.Run("Score not created in DB - no changes", func(t *testing.T) {
			TestScoreNotCreated(t, false)
		})

		t.Run("Score not created in DB - has changes", func(t *testing.T) {
			TestScoreNotCreated(t, true)
		})
	})

	t.Run("Error rows and Error write to Kafka", func(t *testing.T) {
		out.Reset()
		expectedError := errors.New("expected error")

		expectedEvent = events.ScoreEvent{
			Id:           501,
			LessonId:     130,
			DisciplineId: 110,
			Semester:     2,
			ScoreValue: events.ScoreValue{
				Value:     3,
				IsAbsent:  false,
				IsDeleted: false,
			},
			SyncedAt:    syncedAtRewrite,
			UpdatedAt:   syncedAtRewrite.Add(-time.Minute),
			ScoreSource: events.Realtime,
		}

		updateScoreEvent := dekanatEvents.ScoreEditEvent{
			CommonEventData: dekanatEvents.CommonEventData{
				ReceiptHandle: nil,
				Timestamp:     time.Now().Unix(),
				HasChanges:    true,
				LessonId:      strconv.Itoa(int(expectedEvent.LessonId)),
				DisciplineId:  strconv.Itoa(int(expectedEvent.DisciplineId)),
				Semester:      strconv.Itoa(int(expectedEvent.Semester)),
			},
		}

		db, dbMock, _ := sqlmock.New()

		dbMock.ExpectBegin()
		dbMock.ExpectQuery(regexp.QuoteMeta(UpdateScoreQuery)).WithArgs(
			lastRegDate.Format(FirebirdTimeFormat),
		).WillReturnRows(
			sqlmock.NewRows(scoreSelectExpectedColumns).AddRow(
				expectedEvent.Id, expectedEvent.StudentId, expectedEvent.LessonId, expectedEvent.LessonPart,
				expectedEvent.DisciplineId, expectedEvent.Semester, expectedEvent.Value, expectedEvent.IsAbsent,
				expectedEvent.UpdatedAt, expectedEvent.IsDeleted,
			).AddRow(
				nil, expectedEvent.StudentId, expectedEvent.LessonId, false,
				expectedEvent.DisciplineId, "", expectedEvent.Value, expectedEvent.IsAbsent,
				nil, expectedEvent.IsDeleted,
			),
		)
		dbMock.ExpectRollback()

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Get").Once().Return(lastRegDate.Format(StorageTimeFormat), nil)
		fileStorageMock.On(
			"Set", expectedEvent.UpdatedAt.Format(StorageTimeFormat),
		).Once().Return(nil)

		writerMock := eventsMocks.NewWriterInterface(t)
		writerMock.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(expectScoreEventMessage(expectedEvent)),
		).Return(expectedError)

		maxLessonIdSetter := mocks.NewMaxLessonIdSetterInterface(t)
		maxLessonIdSetter.On("Set", expectedEvent.LessonId).Times(2).Return(nil)

		updatedLessonsImporter := &UpdatedScoresImporter{
			out:         &out,
			db:          db,
			cache:       NewTimeCache(1),
			writer:      writerMock,
			storage:     fileStorageMock,
			currentYear: NewMockCurrentYearGetter(t, expectedEvent.Year),
			maxLessonId: maxLessonIdSetter,
		}

		updatedLessonsImporter.AddEvent(updateScoreEvent)

		var confirmed dekanatEvents.ScoreEditEvent

		ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*50)
		go updatedLessonsImporter.Execute(ctx)
		runtime.Gosched()

		select {
		case confirmed = <-updatedLessonsImporter.GetConfirmed():
			cancel()
		case <-ctx.Done():
		}

		assert.Empty(t, confirmed.LessonId, "Expect that event will not be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)

		assert.Contains(t, out.String(), "Error with fetching score: sql")
		assert.Contains(t, out.String(), expectedError.Error())
	})
}

func TestImportUpdatedScoresLesson(t *testing.T) {
	var out bytes.Buffer
	var ctx context.Context
	var cancel context.CancelFunc

	t.Run("Transaction Begin error", func(t *testing.T) {

		out.Reset()
		expectedError := errors.New("expected error")

		expectedEvent := events.ScoreEvent{
			Id:           501,
			LessonId:     130,
			DisciplineId: 110,
			Semester:     2,
			ScoreValue: events.ScoreValue{
				Value:     3,
				IsAbsent:  false,
				IsDeleted: false,
			},
			ScoreSource: events.Realtime,
		}

		updateScoreEvent := dekanatEvents.ScoreEditEvent{
			CommonEventData: dekanatEvents.CommonEventData{
				ReceiptHandle: nil,
				HasChanges:    true,
				Timestamp:     time.Now().Unix(),
				LessonId:      strconv.Itoa(int(expectedEvent.LessonId)),
				DisciplineId:  strconv.Itoa(int(expectedEvent.DisciplineId)),
				Semester:      strconv.Itoa(int(expectedEvent.Semester)),
			},
		}

		db, dbMock, _ := sqlmock.New()
		dbMock.ExpectBegin().WillReturnError(expectedError)

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Get").Once().Return("", nil)

		writerMock := eventsMocks.NewWriterInterface(t)

		maxLessonIdSetter := mocks.NewMaxLessonIdSetterInterface(t)
		maxLessonIdSetter.On("Set", expectedEvent.LessonId).Once().Return(nil)

		updatedLessonsImporter := &UpdatedScoresImporter{
			out:         &out,
			db:          db,
			cache:       NewTimeCache(1),
			writer:      writerMock,
			storage:     fileStorageMock,
			maxLessonId: maxLessonIdSetter,
		}

		updatedLessonsImporter.AddEvent(updateScoreEvent)

		var confirmed dekanatEvents.ScoreEditEvent

		ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*200)
		go func() {
			confirmed = <-updatedLessonsImporter.GetConfirmed()
			cancel()
		}()

		go updatedLessonsImporter.Execute(ctx)
		runtime.Gosched()

		select {
		case confirmed = <-updatedLessonsImporter.GetConfirmed():
			cancel()
		case <-ctx.Done():
		}

		assert.Empty(t, confirmed.LessonId, "Expect that event will not be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)
		writerMock.AssertNotCalled(t, "WriteMessages")

		assert.Contains(t, out.String(), expectedError.Error())
	})
}

func TestGetLastRegDate(t *testing.T) {
	var out bytes.Buffer

	t.Run("Get last reg date - storage error", func(t *testing.T) {
		out.Reset()
		expectedError := errors.New("expected error")

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Get").Once().Return("", expectedError)

		updatedLessonsImporter := &UpdatedScoresImporter{
			out:     &out,
			storage: fileStorageMock,
		}

		mixExpectedLastRegDate := time.Now()
		actualLastRegDate := updatedLessonsImporter.getLastRegDate()

		assert.True(t, actualLastRegDate.After(mixExpectedLastRegDate))
		assert.Contains(t, out.String(), "Failed to get score Last Rag Date from file "+expectedError.Error())
	})
}

func TestSetLastRegDate(t *testing.T) {
	var out bytes.Buffer

	t.Run("Set last reg date - storage error", func(t *testing.T) {
		out.Reset()
		expectedError := errors.New("expected error")

		now := time.Now()
		newLastRegDate := time.Date(
			now.Year(), now.Month(), now.Day(),
			now.Hour(), now.Minute(), now.Second(),
			0, now.Location(),
		).Add(-time.Minute * 10)

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On(
			"Set", newLastRegDate.Format(StorageTimeFormat),
		).Once().Return(expectedError)

		updatedLessonsImporter := &UpdatedScoresImporter{
			out:     &out,
			storage: fileStorageMock,
		}

		actualError := updatedLessonsImporter.setLastRegDate(newLastRegDate)

		assert.Error(t, actualError)
		assert.Equal(t, expectedError, actualError)

		assert.Contains(t, out.String(), "Failed to write LessonMaxId "+expectedError.Error())
	})
}
