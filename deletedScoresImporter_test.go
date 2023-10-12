package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/VictoriaMetrics/fastcache"
	dekanatEvents "github.com/kneu-messenger-pigeon/dekanat-events"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/kneu-messenger-pigeon/events/mocks"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"regexp"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestExecuteImportDeletedScores(t *testing.T) {
	var out bytes.Buffer
	var expectedEvent events.ScoreEvent

	defaultPollInterval = time.Millisecond * 100

	var matchContext = mock.MatchedBy(func(ctx context.Context) bool { return true })

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

	t.Run("Score deleted", func(t *testing.T) {
		expectedEvent = events.ScoreEvent{
			Id:           501,
			LessonId:     130,
			DisciplineId: 110,
			Semester:     2,
			ScoreValue: events.ScoreValue{
				Value:     3,
				IsAbsent:  false,
				IsDeleted: true,
			},
			SyncedAt:    syncedAtRewrite,
			ScoreSource: events.Realtime,
		}

		lessonDeletedEvent := dekanatEvents.LessonDeletedEvent{
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
		dbMock.ExpectQuery(regexp.QuoteMeta(DeletedScoreQuery)).WithArgs(
			expectedEvent.LessonId,
		).WillReturnRows(
			sqlmock.NewRows(scoreSelectExpectedColumns).AddRow(
				expectedEvent.Id, expectedEvent.StudentId, expectedEvent.LessonId, expectedEvent.LessonPart,
				expectedEvent.DisciplineId, expectedEvent.Semester, expectedEvent.Value, expectedEvent.IsAbsent,
				expectedEvent.UpdatedAt, expectedEvent.IsDeleted,
			),
		)
		dbMock.ExpectRollback()

		writerMock := mocks.NewWriterInterface(t)
		writerMock.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(expectScoreEventMessage(expectedEvent)),
		).Return(nil)

		deletedLessonsImporter := &DeletedScoresImporter{
			out:         &out,
			db:          db,
			cache:       fastcache.New(1),
			writer:      writerMock,
			currentYear: NewMockCurrentYearGetter(t, expectedEvent.Year),
		}

		var confirmed dekanatEvents.LessonDeletedEvent
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*300)
		go func() {
			confirmed = <-deletedLessonsImporter.GetConfirmed()
			time.Sleep(defaultPollInterval)
			cancel()
			runtime.Gosched()
		}()
		deletedLessonsImporter.AddEvent(lessonDeletedEvent)
		time.Sleep(time.Nanosecond * 100)
		go deletedLessonsImporter.Execute(ctx)
		<-ctx.Done()

		assert.Equalf(t, lessonDeletedEvent, confirmed, "Expect that event will be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)
	})

	t.Run("Score not deleted", func(t *testing.T) {
		expectedEvent = events.ScoreEvent{
			Id:           501,
			LessonId:     130,
			DisciplineId: 110,
			Year:         2030,
			Semester:     2,
			ScoreValue: events.ScoreValue{
				Value:     3,
				IsAbsent:  false,
				IsDeleted: false,
			},
			SyncedAt:    syncedAtRewrite,
			ScoreSource: events.Realtime,
		}

		lessonDeletedEvent := dekanatEvents.LessonDeletedEvent{
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
		dbMock.ExpectQuery(regexp.QuoteMeta(DeletedScoreQuery)).WithArgs(
			expectedEvent.LessonId,
		).WillReturnRows(
			sqlmock.NewRows(scoreSelectExpectedColumns).AddRow(
				expectedEvent.Id, expectedEvent.StudentId, expectedEvent.LessonId, expectedEvent.LessonPart,
				expectedEvent.DisciplineId, expectedEvent.Semester, expectedEvent.Value, expectedEvent.IsAbsent,
				expectedEvent.UpdatedAt, expectedEvent.IsDeleted,
			),
		)
		dbMock.ExpectRollback()

		writerMock := mocks.NewWriterInterface(t)
		writerMock.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(expectScoreEventMessage(expectedEvent)),
		).Return(nil)

		deletedLessonsImporter := &DeletedScoresImporter{
			out:         &out,
			db:          db,
			cache:       fastcache.New(1),
			writer:      writerMock,
			currentYear: NewMockCurrentYearGetter(t, expectedEvent.Year),
		}

		deletedLessonsImporter.AddEvent(lessonDeletedEvent)

		var confirmed dekanatEvents.LessonDeletedEvent

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		go func() {
			confirmed = <-deletedLessonsImporter.GetConfirmed()
			cancel()
		}()

		go deletedLessonsImporter.Execute(ctx)
		<-ctx.Done()

		assert.Equalf(t, dekanatEvents.LessonDeletedEvent{}, confirmed, "Expect that event will be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)
	})

	t.Run("Error rows and Error write to Kafka", func(t *testing.T) {
		out.Reset()
		expectedError := errors.New("expected error")

		expectedEvent = events.ScoreEvent{
			Id:           501,
			LessonId:     130,
			DisciplineId: 110,
			Year:         2030,
			Semester:     2,
			ScoreValue: events.ScoreValue{
				Value:     3,
				IsAbsent:  false,
				IsDeleted: false,
			},
			SyncedAt:    syncedAtRewrite,
			ScoreSource: events.Realtime,
		}

		lessonDeletedEvent := dekanatEvents.LessonDeletedEvent{
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
		dbMock.ExpectQuery(regexp.QuoteMeta(DeletedScoreQuery)).WithArgs(
			expectedEvent.LessonId,
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

		writerMock := mocks.NewWriterInterface(t)
		writerMock.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(expectScoreEventMessage(expectedEvent)),
		).Return(expectedError)

		deletedLessonsImporter := &DeletedScoresImporter{
			out:         &out,
			db:          db,
			cache:       fastcache.New(1),
			writer:      writerMock,
			currentYear: NewMockCurrentYearGetter(t, expectedEvent.Year),
		}

		deletedLessonsImporter.AddEvent(lessonDeletedEvent)

		var confirmed dekanatEvents.LessonDeletedEvent

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		go func() {
			confirmed = <-deletedLessonsImporter.GetConfirmed()
			cancel()
		}()

		go deletedLessonsImporter.Execute(ctx)
		<-ctx.Done()

		assert.Equalf(t, dekanatEvents.LessonDeletedEvent{}, confirmed, "Expect that event will be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)

		assert.Contains(t, out.String(), "Error with fetching score: sql")
		assert.Contains(t, out.String(), expectedError.Error())
	})
}

func TestImportDeletedScoresLesson(t *testing.T) {
	var out bytes.Buffer

	t.Run("Transaction Begin error", func(t *testing.T) {

		out.Reset()
		expectedError := errors.New("expected error")

		lessonDeletedEvent := dekanatEvents.LessonDeletedEvent{
			CommonEventData: dekanatEvents.CommonEventData{
				ReceiptHandle: nil,
				Timestamp:     time.Now().Unix(),
				LessonId:      "130",
				DisciplineId:  "110",
				Semester:      "2",
			},
		}

		db, dbMock, _ := sqlmock.New()
		dbMock.ExpectBegin().WillReturnError(expectedError)

		writerMock := mocks.NewWriterInterface(t)

		deletedLessonsImporter := &DeletedScoresImporter{
			out:    &out,
			db:     db,
			cache:  fastcache.New(1),
			writer: writerMock,
		}

		deletedLessonsImporter.AddEvent(lessonDeletedEvent)

		var confirmed dekanatEvents.LessonDeletedEvent

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		go func() {
			confirmed = <-deletedLessonsImporter.GetConfirmed()
			cancel()
		}()

		go deletedLessonsImporter.Execute(ctx)
		<-ctx.Done()

		assert.Equalf(t, dekanatEvents.LessonDeletedEvent{}, confirmed, "Expect that event will be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)
		writerMock.AssertNotCalled(t, "WriteMessages")

		assert.Contains(t, out.String(), expectedError.Error())
	})
}
