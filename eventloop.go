package main

import (
	"context"
	"fmt"
	dekanatEvents "github.com/kneu-messenger-pigeon/dekanat-events"
	"io"
	"os/signal"
	"syscall"
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

	go eventLoop.deleter.execute(ctx)
	go eventLoop.currentYearWatcher.execute(ctx)
	go eventLoop.createdLessonsImporter.execute(ctx)
	go eventLoop.editedLessonsImporter.execute(ctx)
	go eventLoop.updatedScoresImporter.execute(ctx)
	go eventLoop.deletedScoresImporter.execute(ctx)

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
			eventLoop.updatedScoresImporter.addEvent(event.(dekanatEvents.ScoreEditEvent))

		case dekanatEvents.LessonCreateEvent:
			eventLoop.createdLessonsImporter.addEvent(event.(dekanatEvents.LessonCreateEvent))

		case dekanatEvents.LessonEditEvent:
			eventLoop.editedLessonsImporter.addEvent(event.(dekanatEvents.LessonEditEvent))

		case dekanatEvents.LessonDeletedEvent:
			lessonDeletedEvent = event.(dekanatEvents.LessonDeletedEvent)
			eventLoop.deletedScoresImporter.addEvent(lessonDeletedEvent)

			lessonEditEvent = dekanatEvents.LessonEditEvent{
				CommonEventData: lessonDeletedEvent.CommonEventData,
				IsDeleted:       true,
			}
			lessonEditEvent.ReceiptHandle = nil
			eventLoop.editedLessonsImporter.addEvent(lessonEditEvent)
		}
	}
}

func (eventLoop *EventLoop) dispatchConfirmedEvent(ctx context.Context) {
	var event interface{}
	for {
		select {
		case event = <-eventLoop.createdLessonsImporter.getConfirmed():
		case event = <-eventLoop.updatedScoresImporter.getConfirmed():
		case event = <-eventLoop.editedLessonsImporter.getConfirmed():
		case event = <-eventLoop.deletedScoresImporter.getConfirmed():
		case <-ctx.Done():
			return
		}

		eventLoop.deleter.Delete(event)
	}
}
