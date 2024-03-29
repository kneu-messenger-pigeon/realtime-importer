package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	dekanatEvents "github.com/kneu-messenger-pigeon/dekanat-events"
	"io"
)

type EventDeleterInterface interface {
	Execute(ctx context.Context)
	Delete(event any)
}

type EventDeleter struct {
	out         io.Writer
	client      SqsApiClientInterface
	sqsQueueUrl *string
	queue       chan *string
}

func (deleter *EventDeleter) Execute(ctx context.Context) {
	deleter.queue = make(chan *string)
	var receiptHandle *string

	for {
		select {
		case <-ctx.Done():
			close(deleter.queue)
			return

		case receiptHandle = <-deleter.queue:
			fmt.Fprintf(deleter.out, "[%s] Deleting message with receipt: %s... \n", t(), (*receiptHandle)[0:10])
			dMInput := &sqs.DeleteMessageInput{
				QueueUrl:      deleter.sqsQueueUrl,
				ReceiptHandle: receiptHandle,
			}

			_, err := deleter.client.DeleteMessage(context.Background(), dMInput)
			if err != nil {
				fmt.Fprintf(deleter.out, "[%s] Failed to remove message %s: %v \n", t(), *receiptHandle, err)
			}
		}
	}
}

func (deleter *EventDeleter) Delete(event any) {
	var receiptHandle *string
	switch event.(type) {
	case *string:
		receiptHandle = event.(*string)

	case dekanatEvents.ScoreEditEvent:
		receiptHandle = event.(dekanatEvents.ScoreEditEvent).ReceiptHandle

	case dekanatEvents.LessonCreateEvent:
		receiptHandle = event.(dekanatEvents.LessonCreateEvent).ReceiptHandle

	case dekanatEvents.LessonEditEvent:
		receiptHandle = event.(dekanatEvents.LessonEditEvent).ReceiptHandle
	case dekanatEvents.LessonDeletedEvent:
		receiptHandle = event.(dekanatEvents.LessonDeletedEvent).ReceiptHandle

	default:
		fmt.Fprintf(deleter.out, "Wrong input event type: %T \n", event)
		return
	}

	if receiptHandle != nil {
		deleter.queue <- receiptHandle
	}
}
