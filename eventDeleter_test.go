package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestEventDeleter(t *testing.T) {
	var out bytes.Buffer

	sqsQueueUrl := "test-sqs-url"
	receiptHandle := "TestReceiptHandle"

	expectedInput := &sqs.DeleteMessageInput{
		QueueUrl:      &sqsQueueUrl,
		ReceiptHandle: &receiptHandle,
	}
	matchContext := mock.MatchedBy(func(ctx context.Context) bool { return true })

	toDeleteEvents := append(make([]interface{}, 0),
		ScoreEditEvent{
			CommonEventData: CommonEventData{
				ReceiptHandle: &receiptHandle,
			},
		},
		LessonCreateEvent{
			CommonEventData: CommonEventData{
				ReceiptHandle: &receiptHandle,
			},
		},
		LessonEditEvent{
			CommonEventData: CommonEventData{
				ReceiptHandle: &receiptHandle,
			},
		},
		LessonDeletedEvent{
			CommonEventData: CommonEventData{
				ReceiptHandle: &receiptHandle,
			},
		},
		&receiptHandle,
	)

	for _, event := range toDeleteEvents {
		t.Run(fmt.Sprintf("Delete %T", event), func(t *testing.T) {
			sqsClientMock := NewMockSqsApiClientInterface(t)
			deleter := EventDeleter{
				out:         &out,
				sqsQueueUrl: &sqsQueueUrl,
				client:      sqsClientMock,
			}

			sqsClientMock.On("DeleteMessage", matchContext, expectedInput).Once().
				Return(&sqs.DeleteMessageOutput{}, nil)

			callDelete(deleter, event)
			sqsClientMock.AssertExpectations(t)
		})
	}

	t.Run("Wrong input for deleter", func(t *testing.T) {
		sqsClientMock := NewMockSqsApiClientInterface(t)
		deleter := EventDeleter{
			out:         &out,
			sqsQueueUrl: &sqsQueueUrl,
			client:      sqsClientMock,
		}

		callDelete(deleter, out)

		sqsClientMock.AssertExpectations(t)
		sqsClientMock.AssertNotCalled(t, "DeleteMessage")
	})

	t.Run("Error response from SqsClient", func(t *testing.T) {
		out.Reset()
		expectedError := errors.New("expected error")

		sqsClientMock := NewMockSqsApiClientInterface(t)
		deleter := EventDeleter{
			out:         &out,
			sqsQueueUrl: &sqsQueueUrl,
			client:      sqsClientMock,
		}

		sqsClientMock.On("DeleteMessage", matchContext, expectedInput).Once().
			Return(&sqs.DeleteMessageOutput{}, expectedError)
		callDelete(deleter, toDeleteEvents[0])

		sqsClientMock.AssertExpectations(t)
		assert.Contains(t, out.String(), expectedError.Error())
	})

}

func callDelete(deleter EventDeleter, event interface{}) {
	var ctx context.Context
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(context.Background())
	go deleter.execute(ctx)
	time.Sleep(time.Millisecond)
	deleter.Delete(event)
	time.Sleep(time.Millisecond)
	cancel()
}
