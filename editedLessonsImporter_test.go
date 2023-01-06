package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/VictoriaMetrics/fastcache"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"math/rand"
	"regexp"
	"strconv"
	"testing"
	"time"
)

func TestExecuteImportEditedLesson(t *testing.T) {
	var out bytes.Buffer
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

	t.Run("Edit valid lesson", func(t *testing.T) {
		lessonId := 65

		expectedEvent = events.LessonEvent{
			Id:           uint(lessonId),
			DisciplineId: uint(disciplineId),
			TypeId:       uint8(rand.Intn(10) + 1),
			Date:         time.Date(2022, 12, 20, 14, 36, 0, 0, time.Local),
			Semester:     2,
			IsDeleted:    false,
		}

		lessonEditedEvent := LessonEditEvent{
			CommonEventData: CommonEventData{
				ReceiptHandle: nil,
				Timestamp:     time.Now().Unix(),
				LessonId:      strconv.Itoa(lessonId),
				DisciplineId:  strconv.Itoa(int(expectedEvent.DisciplineId)),
				Semester:      strconv.Itoa(int(expectedEvent.Semester)),
			},
			TypeId:    strconv.Itoa(int(expectedEvent.TypeId)),
			Date:      expectedEvent.Date.Format("02.01.2006"),
			TeacherId: "9999",
		}

		db, dbMock, _ := sqlmock.New()
		dbMock.ExpectBegin()
		dbMock.ExpectQuery(regexp.QuoteMeta(LessonsEditedQuery)).WithArgs(
			lessonId,
		).WillReturnRows(
			sqlmock.NewRows(LessonsSelectExpectedColumns).AddRow(
				expectedEvent.Id, expectedEvent.DisciplineId, expectedEvent.Date,
				expectedEvent.TypeId, expectedEvent.Semester, expectedEvent.IsDeleted,
			),
		)
		dbMock.ExpectRollback()

		writerMock := events.NewMockWriterInterface(t)
		writerMock.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(expectLessonEventMessage(expectedEvent)),
		).Return(nil)

		editedLessonsImporter := &EditedLessonsImporter{
			out:    &out,
			db:     db,
			cache:  fastcache.New(1),
			writer: writerMock,
		}

		var confirmed LessonEditEvent

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		go func() {
			confirmed = <-editedLessonsImporter.getConfirmed()
			cancel()
		}()
		go editedLessonsImporter.execute(ctx)
		editedLessonsImporter.addEvent(lessonEditedEvent)
		<-ctx.Done()

		assert.Equalf(t, lessonEditedEvent, confirmed, "Expect that event will be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)
	})

	t.Run("Lesson deleted", func(t *testing.T) {
		lessonId := 65

		expectedEvent = events.LessonEvent{
			Id:           uint(lessonId),
			DisciplineId: uint(disciplineId),
			TypeId:       uint8(rand.Intn(10) + 1),
			Date:         time.Date(2022, 12, 20, 14, 36, 0, 0, time.Local),
			Semester:     2,
			IsDeleted:    true,
		}

		lessonEditedEvent := LessonEditEvent{
			CommonEventData: CommonEventData{
				ReceiptHandle: nil,
				Timestamp:     time.Now().Unix(),
				LessonId:      strconv.Itoa(lessonId),
				DisciplineId:  strconv.Itoa(int(expectedEvent.DisciplineId)),
				Semester:      strconv.Itoa(int(expectedEvent.Semester)),
			},
			TypeId:    strconv.Itoa(int(expectedEvent.TypeId)),
			Date:      expectedEvent.Date.Format("02.01.2006"),
			TeacherId: "9999",
			IsDeleted: expectedEvent.IsDeleted,
		}

		db, dbMock, _ := sqlmock.New()
		dbMock.ExpectBegin()
		dbMock.ExpectQuery(regexp.QuoteMeta(LessonsEditedQuery)).WithArgs(
			lessonId,
		).WillReturnRows(
			sqlmock.NewRows(LessonsSelectExpectedColumns).AddRow(
				expectedEvent.Id, expectedEvent.DisciplineId, expectedEvent.Date,
				expectedEvent.TypeId, expectedEvent.Semester, expectedEvent.IsDeleted,
			),
		)
		dbMock.ExpectRollback()

		writerMock := events.NewMockWriterInterface(t)
		writerMock.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(expectLessonEventMessage(expectedEvent)),
		).Return(nil)

		editedLessonsImporter := &EditedLessonsImporter{
			out:    &out,
			db:     db,
			cache:  fastcache.New(1),
			writer: writerMock,
		}

		var confirmed LessonEditEvent

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
		editedLessonsImporter.addEvent(lessonEditedEvent)
		go editedLessonsImporter.execute(ctx)
		time.Sleep(time.Nanosecond * 30)
		go func() {
			confirmed = <-editedLessonsImporter.getConfirmed()
			cancel()
		}()
		time.Sleep(time.Nanosecond * 30)

		<-ctx.Done()

		assert.Equalf(t, lessonEditedEvent, confirmed, "Expect that event will be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)
	})

	t.Run("Error rows and Error write to Kafka", func(t *testing.T) {
		out.Reset()
		expectedError := errors.New("expected error")

		lessonId := 92

		expectedEvent = events.LessonEvent{
			Id:           uint(lessonId),
			DisciplineId: uint(disciplineId),
			TypeId:       uint8(rand.Intn(10) + 1),
			Date:         time.Date(2022, 12, 20, 14, 36, 0, 0, time.Local),
			Semester:     2,
			IsDeleted:    false,
		}

		lessonEditedEvent := LessonEditEvent{
			CommonEventData: CommonEventData{
				ReceiptHandle: nil,
				Timestamp:     time.Now().Unix(),
				LessonId:      strconv.Itoa(lessonId),
				DisciplineId:  strconv.Itoa(int(expectedEvent.DisciplineId)),
				Semester:      strconv.Itoa(int(expectedEvent.Semester)),
			},
			TypeId:    strconv.Itoa(int(expectedEvent.TypeId)),
			Date:      expectedEvent.Date.Format("02.01.2006"),
			TeacherId: "9999",
		}

		db, dbMock, _ := sqlmock.New()
		dbMock.ExpectBegin()
		dbMock.ExpectQuery(regexp.QuoteMeta(LessonsEditedQuery)).WithArgs(
			lessonId,
		).WillReturnRows(
			sqlmock.NewRows(LessonsSelectExpectedColumns).AddRow(
				expectedEvent.Id, expectedEvent.DisciplineId, expectedEvent.Date,
				expectedEvent.TypeId, expectedEvent.Semester, expectedEvent.IsDeleted,
			).AddRow( // emulate row error
				999, 222, nil,
				nil, nil, nil,
			),
		)
		dbMock.ExpectRollback()

		writerMock := events.NewMockWriterInterface(t)
		writerMock.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(expectLessonEventMessage(expectedEvent)),
		).Return(expectedError)

		editedLessonsImporter := &EditedLessonsImporter{
			out:    &out,
			db:     db,
			cache:  fastcache.New(1),
			writer: writerMock,
		}

		var confirmed LessonEditEvent
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		editedLessonsImporter.addEvent(lessonEditedEvent)

		go func() {
			confirmed = <-editedLessonsImporter.getConfirmed()
			cancel()
		}()
		go editedLessonsImporter.execute(ctx)
		<-ctx.Done()

		assert.Equalf(t, LessonEditEvent{}, confirmed, "Expect that event will be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)

		assert.Contains(t, out.String(), "Error with fetching new lesson: sql")
		assert.Contains(t, out.String(), expectedError.Error())
	})
}

func TestImportEditedLesson(t *testing.T) {
	var out bytes.Buffer

	t.Run("Transaction Begin error", func(t *testing.T) {
		out.Reset()
		expectedError := errors.New("expected error")

		db, dbMock, _ := sqlmock.New()
		dbMock.ExpectBegin().WillReturnError(expectedError)

		writerMock := events.NewMockWriterInterface(t)

		editedLessonsImporter := &EditedLessonsImporter{
			out:    &out,
			db:     db,
			cache:  fastcache.New(1),
			writer: writerMock,
		}

		var confirmed LessonEditEvent

		editedLessonsImporter.addEvent(LessonEditEvent{})

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)

		go func() {
			confirmed = <-editedLessonsImporter.getConfirmed()
			cancel()
		}()
		go editedLessonsImporter.execute(ctx)
		<-ctx.Done()

		assert.Equalf(t, LessonEditEvent{}, confirmed, "Expect that event will be confirmed")

		err := dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)

		writerMock.AssertExpectations(t)
		writerMock.AssertNotCalled(t, "WriteMessages")

		assert.Contains(t, out.String(), expectedError.Error())
	})
}
