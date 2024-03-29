package main

import (
	"context"
	"fmt"
	dekanatEvents "github.com/kneu-messenger-pigeon/dekanat-events"
	"io"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

type EventLoop struct {
	out                    io.Writer
	fetcher                EventFetcherInterface
	deleter                EventDeleterInterface
	createdLessonsImporter CreatedLessonsImporterInterface
	editedLessonsImporter  EditedLessonsImporterInterface
	updatedScoresImporter  UpdatedScoresImporterInterface
	deletedScoresImporter  DeletedScoresImporterInterface
	currentYearWatcher     CurrentYearWatcherInterface
}

func (eventLoop *EventLoop) execute() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	go eventLoop.deleter.Execute(ctx)
	go eventLoop.currentYearWatcher.Execute(ctx)
	go eventLoop.createdLessonsImporter.Execute(ctx)
	go eventLoop.editedLessonsImporter.Execute(ctx)
	go eventLoop.updatedScoresImporter.Execute(ctx)
	go eventLoop.deletedScoresImporter.Execute(ctx)

	// wait while all importers will be ready, confirmed chan initialized
	runtime.Gosched()
	time.Sleep(time.Millisecond * 500)

	go eventLoop.dispatchConfirmedEvent(ctx)
	eventLoop.dispatchIncomingEvent(ctx)
}

func (eventLoop *EventLoop) dispatchIncomingEvent(ctx context.Context) {
	var lessonDeletedEvent dekanatEvents.LessonDeletedEvent
	var lessonEditEvent dekanatEvents.LessonEditEvent

	for ctx.Err() == nil {
		event := eventLoop.fetcher.Fetch(ctx)
		if event != nil {
			fmt.Fprintf(eventLoop.out, "[%s] Receive event %T \n", t(), event)
		}

		switch event.(type) {
		case dekanatEvents.ScoreEditEvent:
			eventLoop.updatedScoresImporter.AddEvent(event.(dekanatEvents.ScoreEditEvent))

		case dekanatEvents.LessonCreateEvent:
			eventLoop.createdLessonsImporter.AddEvent(event.(dekanatEvents.LessonCreateEvent))

		case dekanatEvents.LessonEditEvent:
			eventLoop.editedLessonsImporter.AddEvent(event.(dekanatEvents.LessonEditEvent))

		case dekanatEvents.LessonDeletedEvent:
			lessonDeletedEvent = event.(dekanatEvents.LessonDeletedEvent)
			eventLoop.deletedScoresImporter.AddEvent(lessonDeletedEvent)

			lessonEditEvent = dekanatEvents.LessonEditEvent{
				CommonEventData: lessonDeletedEvent.CommonEventData,
				IsDeleted:       true,
			}
			lessonEditEvent.ReceiptHandle = nil
			eventLoop.editedLessonsImporter.AddEvent(lessonEditEvent)

		}
	}
}

func (eventLoop *EventLoop) dispatchConfirmedEvent(ctx context.Context) {
	var event interface{}
	for {
		select {
		case event = <-eventLoop.createdLessonsImporter.GetConfirmed():
		case event = <-eventLoop.updatedScoresImporter.GetConfirmed():
		case event = <-eventLoop.editedLessonsImporter.GetConfirmed():
		case event = <-eventLoop.deletedScoresImporter.GetConfirmed():
		case <-ctx.Done():
			return
		}

		eventLoop.deleter.Delete(event)
	}
}
