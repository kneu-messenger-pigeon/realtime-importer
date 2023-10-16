package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
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
	countCache  *countCache
}

func (fetcher eventFetcher) Fetch(context context.Context) (event interface{}) {
	gMInput := &sqs.ReceiveMessageInput{
		QueueUrl:            fetcher.sqsQueueUrl,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     20,
	}
	var err error
	var msgResult *sqs.ReceiveMessageOutput
	var sqsMessage *types.Message
	var message *dekanatEvents.Message
	var eventReceiveCount uint8

	for context.Err() == nil {
		msgResult, err = fetcher.client.ReceiveMessage(context, gMInput)
		if err != nil && context.Err() == nil {
			fmt.Fprintf(fetcher.out, "Failed to get message: %v \n", err)
			break
		}

		if msgResult == nil || len(msgResult.Messages) == 0 {
			continue
		}

		sqsMessage = &msgResult.Messages[0]

		message, err = dekanatEvents.CreateMessage(sqsMessage.Body, sqsMessage.ReceiptHandle)
		if err == nil {
			event, err = message.ToEvent()
		}

		if err == nil && event != nil {
			eventReceiveCount = fetcher.countCache.Get(sqsMessage.MessageId)
			fetcher.countCache.Set(sqsMessage.MessageId, eventReceiveCount+1)
			if eventReceiveCount == 0 {
				trackEventMetrics(event)
			}

			return event
		}

		fmt.Fprintf(fetcher.out, "Failed to decode Event message: %v \n%+v\n", err, message)
		fetcher.deleter.Delete(message.ReceiptHandle)
	}

	return nil
}
