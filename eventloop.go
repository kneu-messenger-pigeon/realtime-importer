package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
)

type EventLoop struct {
	fetcher                EventFetcherInterface
	deleter                EventDeleterInterface
	createdLessonsImporter *CreatedLessonsImporter
	editedLessonsImporter  *EditedLessonsImporter
	updatedScoresImporter  *UpdatedScoresImporter
	deletedScoresImporter  *DeletedScoresImporter
}

func (eventLoop *EventLoop) execute() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	go eventLoop.deleter.execute(ctx)
	go eventLoop.createdLessonsImporter.execute(ctx)
	go eventLoop.editedLessonsImporter.execute(ctx)
	go eventLoop.updatedScoresImporter.execute(ctx)
	go eventLoop.deletedScoresImporter.execute(ctx)

	go eventLoop.dispatchConfirmedEvent(ctx)
	eventLoop.dispatchIncomingEvent(ctx)
}

func (eventLoop *EventLoop) dispatchIncomingEvent(ctx context.Context) {
	var lessonDeleteEvent LessonDeletedEvent
	var lessonEditEvent LessonEditEvent

	for ctx.Err() == nil {
		event := eventLoop.fetcher.Fetch(ctx)
		if event == nil {
			continue
		}
		fmt.Printf("[%s] Receive event %T \n", t(), event)

		switch event.(type) {
		case ScoreEditEvent:
			eventLoop.updatedScoresImporter.addEvent(event.(ScoreEditEvent))

		case LessonCreateEvent:
			eventLoop.createdLessonsImporter.addEvent(event.(LessonCreateEvent))

		case LessonEditEvent:
			eventLoop.editedLessonsImporter.addEvent(event.(LessonEditEvent))

		case LessonDeletedEvent:
			lessonDeleteEvent = event.(LessonDeletedEvent)
			eventLoop.deletedScoresImporter.addEvent(lessonDeleteEvent)

			lessonEditEvent = LessonEditEvent{
				CommonEventData: lessonDeleteEvent.CommonEventData,
				IsDeleted:       true,
			}
			lessonEditEvent.ReceiptHandle = nil
			eventLoop.editedLessonsImporter.addEvent(lessonEditEvent)
		}
	}
}

func (eventLoop *EventLoop) dispatchConfirmedEvent(ctx context.Context) {
	var event interface{}
	for ctx.Err() == nil {
		select {
		case event = <-eventLoop.createdLessonsImporter.confirmed:
		case event = <-eventLoop.updatedScoresImporter.confirmed:
		case event = <-eventLoop.editedLessonsImporter.confirmed:
		case event = <-eventLoop.deletedScoresImporter.confirmed:
		}

		eventLoop.deleter.Delete(event)
	}

}
