package main

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/mock"
	"syscall"
	"testing"
	"time"
)

func TestEventLoopExecute(t *testing.T) {
	t.Run("EventLoop execute", func(t *testing.T) {
		out := &bytes.Buffer{}

		matchContext := mock.MatchedBy(func(ctx context.Context) bool { return true })

		editedLessonsImporter := NewMockEditedLessonsImporterInterface(t)
		createdLessonsImporter := NewMockCreatedLessonsImporterInterface(t)
		updatedScoresImporter := NewMockUpdatedScoresImporterInterface(t)
		deletedScoresImporter := NewMockDeletedScoresImporterInterface(t)

		deleter := NewMockEventDeleterInterface(t)
		fetcher := NewMockEventFetcherInterface(t)

		editedLessonsImporter.On("getConfirmed").Return(func() <-chan LessonEditEvent {
			return make(chan LessonEditEvent)
		}).Maybe()
		createdLessonsImporter.On("getConfirmed").Return(func() <-chan LessonCreateEvent {
			return make(chan LessonCreateEvent)
		}).Maybe()
		updatedScoresImporter.On("getConfirmed").Return(func() <-chan ScoreEditEvent {
			return make(chan ScoreEditEvent)
		}).Maybe()
		deletedScoresImporter.On("getConfirmed").Return(func() <-chan LessonDeletedEvent {
			return make(chan LessonDeletedEvent)
		}).Maybe()

		editedLessonsImporter.On("execute", matchContext).Once().Return()
		createdLessonsImporter.On("execute", matchContext).Once().Return()
		updatedScoresImporter.On("execute", matchContext).Once().Return()
		deletedScoresImporter.On("execute", matchContext).Once().Return()

		deleter.On("execute", matchContext).Once().Return()

		eventLoop := EventLoop{
			out:                    out,
			fetcher:                fetcher,
			deleter:                deleter,
			editedLessonsImporter:  editedLessonsImporter,
			createdLessonsImporter: createdLessonsImporter,
			updatedScoresImporter:  updatedScoresImporter,
			deletedScoresImporter:  deletedScoresImporter,
		}

		fetcher.On("Fetch", matchContext).Return(func(ctx context.Context) interface{} {
			_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
			return nil
		})

		eventLoop.execute()

		editedLessonsImporter.AssertExpectations(t)
		createdLessonsImporter.AssertExpectations(t)
		updatedScoresImporter.AssertExpectations(t)
		deletedScoresImporter.AssertExpectations(t)

		deleter.AssertExpectations(t)
		fetcher.AssertExpectations(t)
	})
}

func TestEventDispatchIncomingEvent(t *testing.T) {
	t.Run("EventLoop dispatchIncomingEvent", func(t *testing.T) {
		out := &bytes.Buffer{}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()

		editedLessonsImporter := NewMockEditedLessonsImporterInterface(t)
		createdLessonsImporter := NewMockCreatedLessonsImporterInterface(t)
		updatedScoresImporter := NewMockUpdatedScoresImporterInterface(t)
		deletedScoresImporter := NewMockDeletedScoresImporterInterface(t)

		deleter := NewMockEventDeleterInterface(t)
		fetcher := NewMockEventFetcherInterface(t)

		scoreEditEvent := ScoreEditEvent{}
		lessonCreateEvent := LessonCreateEvent{}
		lessonEditEvent := LessonEditEvent{}
		lessonDeletedEvent := LessonDeletedEvent{}

		updatedScoresImporter.On("addEvent", scoreEditEvent).Once().Return()
		createdLessonsImporter.On("addEvent", lessonCreateEvent).Once().Return()
		deletedScoresImporter.On("addEvent", lessonDeletedEvent).Once().Return()
		editedLessonsImporter.On("addEvent", lessonEditEvent).Once().Return()
		// extra call based on lessonDeletedEvent
		editedLessonsImporter.On("addEvent", LessonEditEvent{
			CommonEventData: lessonDeletedEvent.CommonEventData,
			IsDeleted:       true,
		}).Once().Return()

		fetcher.On("Fetch", ctx).Return(scoreEditEvent).Once()
		fetcher.On("Fetch", ctx).Return(lessonCreateEvent).Once()
		fetcher.On("Fetch", ctx).Return(lessonEditEvent).Once()
		fetcher.On("Fetch", ctx).Return(lessonDeletedEvent).Once()
		fetcher.On("Fetch", ctx).Return(func(ctx context.Context) interface{} {
			cancel()
			return nil
		})

		eventLoop := EventLoop{
			out:                    out,
			fetcher:                fetcher,
			deleter:                deleter,
			editedLessonsImporter:  editedLessonsImporter,
			createdLessonsImporter: createdLessonsImporter,
			updatedScoresImporter:  updatedScoresImporter,
			deletedScoresImporter:  deletedScoresImporter,
		}

		eventLoop.dispatchIncomingEvent(ctx)

		editedLessonsImporter.AssertExpectations(t)
		createdLessonsImporter.AssertExpectations(t)
		updatedScoresImporter.AssertExpectations(t)
		deletedScoresImporter.AssertExpectations(t)

		deleter.AssertExpectations(t)
		fetcher.AssertExpectations(t)
	})
}

func TestEventLoopDispatchConfirmedEvent(t *testing.T) {
	t.Run("EventLoop dispatchConfirmedEvent", func(t *testing.T) {
		out := &bytes.Buffer{}

		editedLessonsImporter := NewMockEditedLessonsImporterInterface(t)
		createdLessonsImporter := NewMockCreatedLessonsImporterInterface(t)
		updatedScoresImporter := NewMockUpdatedScoresImporterInterface(t)
		deletedScoresImporter := NewMockDeletedScoresImporterInterface(t)

		deleter := NewMockEventDeleterInterface(t)
		fetcher := NewMockEventFetcherInterface(t)

		scoreEditEvent := ScoreEditEvent{}
		lessonCreateEvent := LessonCreateEvent{}
		lessonEditEvent := LessonEditEvent{}
		lessonDeletedEvent := LessonDeletedEvent{}

		lessonCreateEventConfirmed := make(chan LessonCreateEvent)
		lessonEditEventConfirmed := make(chan LessonEditEvent)
		lessonDeletedEventConfirmed := make(chan LessonDeletedEvent)
		scoreEditEventConfirmed := make(chan ScoreEditEvent)

		editedLessonsImporter.On("getConfirmed").Return(func() <-chan LessonEditEvent {
			return lessonEditEventConfirmed
		})
		createdLessonsImporter.On("getConfirmed").Return(func() <-chan LessonCreateEvent {
			return lessonCreateEventConfirmed
		})
		updatedScoresImporter.On("getConfirmed").Return(func() <-chan ScoreEditEvent {
			return scoreEditEventConfirmed
		})
		deletedScoresImporter.On("getConfirmed").Return(func() <-chan LessonDeletedEvent {
			return lessonDeletedEventConfirmed
		})

		deleter.On("Delete", lessonEditEvent).Once().Return()
		deleter.On("Delete", lessonCreateEvent).Once().Return()
		deleter.On("Delete", scoreEditEvent).Once().Return()
		deleter.On("Delete", lessonDeletedEvent).Once().Return()

		eventLoop := EventLoop{
			out:                    out,
			fetcher:                fetcher,
			deleter:                deleter,
			editedLessonsImporter:  editedLessonsImporter,
			createdLessonsImporter: createdLessonsImporter,
			updatedScoresImporter:  updatedScoresImporter,
			deletedScoresImporter:  deletedScoresImporter,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()

		go func() {
			lessonCreateEventConfirmed <- lessonCreateEvent
			lessonEditEventConfirmed <- lessonEditEvent
			lessonDeletedEventConfirmed <- lessonDeletedEvent
			scoreEditEventConfirmed <- scoreEditEvent

			cancel()
		}()
		time.Sleep(time.Nanosecond * 10)
		eventLoop.dispatchConfirmedEvent(ctx)

		editedLessonsImporter.AssertExpectations(t)
		createdLessonsImporter.AssertExpectations(t)
		updatedScoresImporter.AssertExpectations(t)
		deletedScoresImporter.AssertExpectations(t)

		deleter.AssertExpectations(t)
		fetcher.AssertExpectations(t)
	})
}
