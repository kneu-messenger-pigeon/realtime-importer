package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	dekanatEvents "github.com/kneu-messenger-pigeon/dekanat-events"
	"io"
)

type EventFetcherInterface interface {
	Fetch(context context.Context) interface{}
}

type eventFetcher struct {
	out         io.Writer
	client      SqsApiClientInterface
	sqsQueueUrl *string
	deleter     EventDeleterInterface
}

func (fetcher eventFetcher) Fetch(context context.Context) (event interface{}) {
	gMInput := &sqs.ReceiveMessageInput{
		QueueUrl:            fetcher.sqsQueueUrl,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     20,
	}
	var err error
	var msgResult *sqs.ReceiveMessageOutput
	var message *dekanatEvents.Message

	for context.Err() == nil {
		msgResult, err = fetcher.client.ReceiveMessage(context, gMInput)
		if err != nil && context.Err() == nil {
			fmt.Fprintf(fetcher.out, "Failed to get message: %v \n", err)
			break
		}

		if msgResult == nil || len(msgResult.Messages) == 0 {
			continue
		}

		message, err = dekanatEvents.CreateMessage(msgResult.Messages[0].Body, msgResult.Messages[0].ReceiptHandle)
		if err == nil {
			event, err = message.ToEvent()
		}

		if err == nil && event != nil {
			return event
		}

		fmt.Fprintf(fetcher.out, "Failed to decode Event message: %v \n%+v\n", err, message)
		fetcher.deleter.Delete(message.ReceiptHandle)
	}

	return nil
}
