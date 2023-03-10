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
	"regexp"
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
			Value:        3,
			Year:         2030,
			Semester:     2,
			IsAbsent:     false,
			IsDeleted:    true,
			SyncedAt:     syncedAtRewrite,
		}

		updatedScoreEvent := ScoreEditEvent{
			CommonEventData: CommonEventData{
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
		fileStorageMock.On("Get").Once().Return("", nil)
		fileStorageMock.On("Set", expectedEvent.UpdatedAt.Format(StorageTimeFormat)).Once().Return(nil)

		writerMock := events.NewMockWriterInterface(t)
		writerMock.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(expectScoreEventMessage(expectedEvent)),
		).Return(nil)

		updatedLessonsImporter := &UpdatedScoresImporter{
			out:         &out,
			db:          db,
			cache:       NewTimeCache(1),
			writer:      writerMock,
			storage:     fileStorageMock,
			currentYear: NewMockCurrentYearGetter(t, expectedEvent.Year),
		}

		var confirmed ScoreEditEvent

		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		updatedLessonsImporter.addEvent(updatedScoreEvent)
		go updatedLessonsImporter.execute(ctx)
		time.Sleep(time.Nanosecond * 200)
		go func() {
			confirmed = <-updatedLessonsImporter.getConfirmed()
			cancel()
		}()
		<-ctx.Done()

		assert.Equalf(t, updatedScoreEvent, confirmed, "Expect that event will be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)
		fileStorageMock.AssertExpectations(t)
	})

	t.Run("Score not created in DB", func(t *testing.T) {
		expectedEvent = events.ScoreEvent{
			Id:           501,
			LessonId:     130,
			DisciplineId: 110,
			Value:        3,
			Semester:     2,
			IsAbsent:     false,
			IsDeleted:    false,
			SyncedAt:     syncedAtRewrite,
		}

		updateScoreEvent := ScoreEditEvent{
			CommonEventData: CommonEventData{
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
			lastRegDate.Format(FirebirdTimeFormat),
		).WillReturnRows(
			sqlmock.NewRows(scoreSelectExpectedColumns),
		)
		dbMock.ExpectRollback()

		fileStorageMock := fileStorage.NewMockInterface(t)
		fileStorageMock.On("Get").Once().Return(lastRegDate.Format(StorageTimeFormat), nil)

		writerMock := events.NewMockWriterInterface(t)

		updatedLessonsImporter := &UpdatedScoresImporter{
			out:     &out,
			db:      db,
			cache:   NewTimeCache(1),
			writer:  writerMock,
			storage: fileStorageMock,
		}

		updatedLessonsImporter.addEvent(updateScoreEvent)

		var confirmed ScoreEditEvent

		ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*100)
		go func() {
			confirmed = <-updatedLessonsImporter.getConfirmed()
			cancel()
		}()

		go updatedLessonsImporter.execute(ctx)
		<-ctx.Done()

		assert.Equalf(t, ScoreEditEvent{}, confirmed, "Expect that event will be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)
		writerMock.AssertNotCalled(t, "WriteMessages")
		fileStorageMock.AssertExpectations(t)
	})

	t.Run("Error rows and Error write to Kafka", func(t *testing.T) {
		out.Reset()
		expectedError := errors.New("expected error")

		expectedEvent = events.ScoreEvent{
			Id:           501,
			LessonId:     130,
			DisciplineId: 110,
			Value:        3,
			Semester:     2,
			IsAbsent:     false,
			IsDeleted:    false,
			SyncedAt:     syncedAtRewrite,
			UpdatedAt:    syncedAtRewrite.Add(-time.Minute),
		}

		updateScoreEvent := ScoreEditEvent{
			CommonEventData: CommonEventData{
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

		writerMock := events.NewMockWriterInterface(t)
		writerMock.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(expectScoreEventMessage(expectedEvent)),
		).Return(expectedError)

		updatedLessonsImporter := &UpdatedScoresImporter{
			out:         &out,
			db:          db,
			cache:       NewTimeCache(1),
			writer:      writerMock,
			storage:     fileStorageMock,
			currentYear: NewMockCurrentYearGetter(t, expectedEvent.Year),
		}

		updatedLessonsImporter.addEvent(updateScoreEvent)

		var confirmed ScoreEditEvent

		ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*50)
		go func() {
			confirmed = <-updatedLessonsImporter.getConfirmed()
			cancel()
		}()

		go updatedLessonsImporter.execute(ctx)
		<-ctx.Done()

		assert.Equalf(t, ScoreEditEvent{}, confirmed, "Expect that event will be confirmed")

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
			Value:        3,
			Semester:     2,
			IsAbsent:     false,
			IsDeleted:    false,
		}

		updateScoreEvent := ScoreEditEvent{
			CommonEventData: CommonEventData{
				ReceiptHandle: nil,
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

		writerMock := events.NewMockWriterInterface(t)

		updatedLessonsImporter := &UpdatedScoresImporter{
			out:     &out,
			db:      db,
			cache:   NewTimeCache(1),
			writer:  writerMock,
			storage: fileStorageMock,
		}

		updatedLessonsImporter.addEvent(updateScoreEvent)

		var confirmed ScoreEditEvent

		ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*50)
		go func() {
			confirmed = <-updatedLessonsImporter.getConfirmed()
			cancel()
		}()

		go updatedLessonsImporter.execute(ctx)
		<-ctx.Done()

		assert.Equalf(t, ScoreEditEvent{}, confirmed, "Expect that event will be confirmed")

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
