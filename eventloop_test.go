package main

import (
	"bytes"
	"context"
	dekanatEvents "github.com/kneu-messenger-pigeon/dekanat-events"
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

		currentYearWatcher := NewMockCurrentYearWatcherInterface(t)

		confirmedCalled := make(chan bool)

		editedLessonsImporter.On("getConfirmed").Return(func() <-chan dekanatEvents.LessonEditEvent {
			confirmedCalled <- true
			return make(chan dekanatEvents.LessonEditEvent)
		}).Once()
		createdLessonsImporter.On("getConfirmed").Return(func() <-chan dekanatEvents.LessonCreateEvent {
			confirmedCalled <- true
			return make(chan dekanatEvents.LessonCreateEvent)
		}).Once()
		updatedScoresImporter.On("getConfirmed").Return(func() <-chan dekanatEvents.ScoreEditEvent {
			confirmedCalled <- true
			return make(chan dekanatEvents.ScoreEditEvent)
		}).Once()
		deletedScoresImporter.On("getConfirmed").Return(func() <-chan dekanatEvents.LessonDeletedEvent {
			confirmedCalled <- true
			return make(chan dekanatEvents.LessonDeletedEvent)
		}).Once()

		fetcher.On("Fetch", matchContext).Return(func(ctx context.Context) interface{} {
			// wait for call all  `getConfirmed`and then send POSIX-signal to stop eventloop
			expectedConfirmedCallCount := 4
			for expectedConfirmedCallCount > 0 {
				select {
				case <-confirmedCalled:
					expectedConfirmedCallCount--
				case <-time.After(time.Millisecond * 200):
					expectedConfirmedCallCount = 0
				}
			}

			_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
			return nil
		})

		editedLessonsImporter.On("execute", matchContext).Once().Return()
		createdLessonsImporter.On("execute", matchContext).Once().Return()
		updatedScoresImporter.On("execute", matchContext).Once().Return()
		deletedScoresImporter.On("execute", matchContext).Once().Return()

		deleter.On("execute", matchContext).Once().Return()
		currentYearWatcher.On("execute", matchContext).Once().Return()

		eventLoop := EventLoop{
			out:                    out,
			fetcher:                fetcher,
			deleter:                deleter,
			editedLessonsImporter:  editedLessonsImporter,
			createdLessonsImporter: createdLessonsImporter,
			updatedScoresImporter:  updatedScoresImporter,
			deletedScoresImporter:  deletedScoresImporter,
			currentYearWatcher:     currentYearWatcher,
		}

		timeout := time.After(time.Millisecond * 500)
		done := make(chan bool)
		go func() {
			eventLoop.execute()
			done <- true
		}()

		select {
		case <-timeout:
			t.Fatal("Test didn't finish in time")
		case <-done:
		}

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

		scoreEditEvent := dekanatEvents.ScoreEditEvent{}
		lessonCreateEvent := dekanatEvents.LessonCreateEvent{}
		lessonEditEvent := dekanatEvents.LessonEditEvent{}
		lessonDeletedEvent := dekanatEvents.LessonDeletedEvent{}

		updatedScoresImporter.On("addEvent", scoreEditEvent).Once().Return()
		createdLessonsImporter.On("addEvent", lessonCreateEvent).Once().Return()
		deletedScoresImporter.On("addEvent", lessonDeletedEvent).Once().Return()
		editedLessonsImporter.On("addEvent", lessonEditEvent).Once().Return()
		// extra call based on dekanatEvents.LessonDeletedEvent
		editedLessonsImporter.On("addEvent", dekanatEvents.LessonEditEvent{
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

		scoreEditEvent := dekanatEvents.ScoreEditEvent{}
		lessonCreateEvent := dekanatEvents.LessonCreateEvent{}
		lessonEditEvent := dekanatEvents.LessonEditEvent{}
		lessonDeletedEvent := dekanatEvents.LessonDeletedEvent{}

		lessonCreateEventConfirmed := make(chan dekanatEvents.LessonCreateEvent)
		lessonEditEventConfirmed := make(chan dekanatEvents.LessonEditEvent)
		lessonDeletedEventConfirmed := make(chan dekanatEvents.LessonDeletedEvent)
		scoreEditEventConfirmed := make(chan dekanatEvents.ScoreEditEvent)

		editedLessonsImporter.On("getConfirmed").Return(func() <-chan dekanatEvents.LessonEditEvent {
			return lessonEditEventConfirmed
		})
		createdLessonsImporter.On("getConfirmed").Return(func() <-chan dekanatEvents.LessonCreateEvent {
			return lessonCreateEventConfirmed
		})
		updatedScoresImporter.On("getConfirmed").Return(func() <-chan dekanatEvents.ScoreEditEvent {
			return scoreEditEventConfirmed
		})
		deletedScoresImporter.On("getConfirmed").Return(func() <-chan dekanatEvents.LessonDeletedEvent {
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

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
		defer cancel()

		go func() {
			lessonCreateEventConfirmed <- lessonCreateEvent
			lessonEditEventConfirmed <- lessonEditEvent
			lessonDeletedEventConfirmed <- lessonDeletedEvent
			scoreEditEventConfirmed <- scoreEditEvent

			cancel()
		}()
		time.Sleep(time.Nanosecond * 10)

		timeout := time.After(5 * time.Second)
		done := make(chan bool)
		go func() {
			// testing with timeout
			eventLoop.dispatchConfirmedEvent(ctx)
			done <- true
		}()

		select {
		case <-timeout:
			cancel()
			t.Fatal("Test didn't finish in time")
		case <-done:
		}

		editedLessonsImporter.AssertExpectations(t)
		createdLessonsImporter.AssertExpectations(t)
		updatedScoresImporter.AssertExpectations(t)
		deletedScoresImporter.AssertExpectations(t)

		deleter.AssertExpectations(t)
		fetcher.AssertExpectations(t)
	})
}
