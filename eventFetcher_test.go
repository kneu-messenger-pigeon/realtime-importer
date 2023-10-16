package main

import (
	"bytes"
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	dekanatEvents "github.com/kneu-messenger-pigeon/dekanat-events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"realtime-importer/mocks"
	"strings"
	"testing"
	"time"
)

var sqsQueueUrl = "test-sqs-url"

func TestEventFetcher_Fetch(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		incomingLessonCreateTotal.Set(0)

		expectedEvent := dekanatEvents.LessonCreateEvent{
			CommonEventData: dekanatEvents.CommonEventData{
				Timestamp:    1673000000,
				SessionId:    "00AB0000-0000-0000-0000-000CD0000AA0",
				LessonId:     "0",
				DisciplineId: "193000",
				Semester:     "0",
			},
			TypeId:    "1",
			Date:      "23.12.2022",
			TeacherId: "9999",
		}

		actualEvent := fetchMessage(t, expectedEvent.ToMessage().ToJson(), false)

		assert.IsType(t, dekanatEvents.LessonCreateEvent{}, actualEvent)
		event := actualEvent.(dekanatEvents.LessonCreateEvent)

		assert.Equal(t, uint(0), event.GetLessonId())
		assert.Equal(t, uint(193000), event.GetDisciplineId())
		assert.Equal(t, "0", event.Semester)

		assert.Equal(t, uint64(1), incomingLessonCreateTotal.Get())

	})

	t.Run("Wrong form", func(t *testing.T) {
		testEvent := dekanatEvents.ScoreEditEvent{
			CommonEventData: dekanatEvents.CommonEventData{
				Timestamp:    1673000000,
				SessionId:    "99FED80A-2E33-40CB-9CEF-01E25B5AA66B",
				LessonId:     "999999",
				DisciplineId: "188619",
				Semester:     "0",
			},
			Date: "18.12.2022",
			Scores: map[int]map[uint8]string{
				110030: {
					1: "",
				},
				110054: {
					1: "нб/нп",
					2: "",
				},
				110055: {
					1: "",
					2: "",
				},
			},
		}

		testEventJson := testEvent.ToMessage().ToJson()
		*testEventJson = strings.Replace(*testEventJson, `"st`, `"__`, -1)
		actualEvent := fetchMessage(t, testEventJson, true)

		assert.Nil(t, actualEvent)
	})

	t.Run("Fetch wrong message", func(t *testing.T) {
		wrongJson := `test`
		actualEvent := fetchMessage(t, &wrongJson, true)

		assert.Nil(t, actualEvent)
	})

	t.Run("failed to fetch message from SQS", func(t *testing.T) {
		out := &bytes.Buffer{}
		expectedError := errors.New("expected error")

		sqsClientMock := mocks.NewSqsApiClientInterface(t)

		sqsClientMock.On(
			"ReceiveMessage",
			mock.MatchedBy(func(ctx context.Context) bool { return true }),
			mock.IsType(&sqs.ReceiveMessageInput{}),
		).Return(nil, expectedError).Once()

		fetcher := eventFetcher{
			out:         out,
			sqsQueueUrl: &sqsQueueUrl,
			client:      sqsClientMock,
			countCache:  NewCountCache(1),
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		actualEvent := fetcher.Fetch(ctx)
		cancel()

		sqsClientMock.AssertExpectations(t)
		assert.Nil(t, actualEvent)
		assert.Contains(t, out.String(), expectedError.Error())
	})
}

func fetchMessage(t *testing.T, messageJSON *string, expectDelete bool) interface{} {
	var out bytes.Buffer
	var ctx context.Context
	var cancel context.CancelFunc

	sqsClientMock := mocks.NewSqsApiClientInterface(t)
	deleterMock := mocks.NewEventDeleterInterface(t)

	receiptHandle := "ReceiptHandle"
	messageId := "MessageId"
	sqsMessageOutput := &sqs.ReceiveMessageOutput{
		Messages: []sqstypes.Message{
			{
				ReceiptHandle: &receiptHandle,
				Body:          aws.String(*messageJSON),
				MessageId:     &messageId,
			},
		},
	}

	sqsClientMock.On(
		"ReceiveMessage",
		mock.MatchedBy(func(ctx context.Context) bool { return true }),
		mock.IsType(&sqs.ReceiveMessageInput{}),
	).Return(sqsMessageOutput, nil).Once()

	sqsClientMock.On(
		"ReceiveMessage",
		mock.MatchedBy(func(ctx context.Context) bool { return true }),
		mock.IsType(&sqs.ReceiveMessageInput{}),
	).Return(&sqs.ReceiveMessageOutput{}, nil)

	if expectDelete {
		deleterMock.On("Delete", sqsMessageOutput.Messages[0].ReceiptHandle).Once().Return(nil)
	}

	fetcher := eventFetcher{
		out:         &out,
		sqsQueueUrl: &sqsQueueUrl,
		client:      sqsClientMock,
		deleter:     deleterMock,
		countCache:  NewCountCache(1),
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	actualEvent := fetcher.Fetch(ctx)
	cancel()

	sqsClientMock.AssertExpectations(t)
	deleterMock.AssertExpectations(t)

	return actualEvent
}
