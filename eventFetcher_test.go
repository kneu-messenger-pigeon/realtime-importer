package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"strings"
	"testing"
	"time"
)

const EventMessageJSON = `{
	"timestamp": 1673000000,
	"ip": "127.0.0.1",
	"referer": "http://example.com",
	"form": %s
}`

const LessonCreateEventFormJSON = `{
	"hlf":"0",
	"prt":"193000",
	"prti":"0",
	"teacher":"9999",
	"action":"insert",
	"n":"10",
	"sesID":"00AB0000-0000-0000-0000-000CD0000AA0",
	"m":"-1",
	"date_z":"23.12.2022",
	"tzn":"1",
	"result":"3",
	"grade":""
}`

const LessonEditEventFormJSON = `{
	"hlf":"0",
	"prt":"193000",
	"prti":"999999",
	"teacher":"9999",
	"action":"edit",
	"n":"10",
	"sesID":"00AB0000-0000-0000-0000-000CD0000AA0",
	"m":"-1",
	"date_z":"12.12.2022",
	"tzn":"1",
	"result":"",
	"grade":"2"
}`

const LessonDeletedEventFormJSON = `{
	"sesID":"00AB0000-0000-0000-0000-000CD0000AA0",
	"n":"11",
	"action":"delete",
	"prti":"999999",
	"prt":"193000",
	"d1":"",
	"d2":"",
	"m":"-1",
	"hlf":"0",
	"course":"undefined"
}`

const ScoreEditEventFormJSON = `{
        "hlf":"0",
        "prt":"188619",
        "prti":"999999",
        "action":"",
        "n":"4",
        "sesID":"99FED80A-2E33-40CB-9CEF-01E25B5AA66B",
        "d1":"09.09.2022",
        "course":"3",
        "m":"-1",
        "d2":"18.12.2022",
        "st110030_1-999999":"",
        "st110043_2-999999":"",
        "st110044_1-999999":"-11",
        "st110044_2-999999":"",
        "st110054_1-999999":"нб/нп",
        "st110054_2-999999":"",
        "st110055_1-999999":"",
        "st110055_2-999999":"",
        "AddEstim":"0"
    }`

var sqsQueueUrl = "test-sqs-url"

func TestEventFetcher(t *testing.T) {
	t.Run("Fetch LessonCreateEvent", func(t *testing.T) {
		actualEvent := fetchMessage(t, LessonCreateEventFormJSON, false)

		assert.IsType(t, LessonCreateEvent{}, actualEvent)
		event := actualEvent.(LessonCreateEvent)

		assert.Equal(t, uint(0), event.GetLessonId())
		assert.Equal(t, uint(193000), event.GetDisciplineId())
		assert.Equal(t, "0", event.Semester)
	})

	t.Run("Fetch LessonEditEvent", func(t *testing.T) {
		actualEvent := fetchMessage(t, LessonEditEventFormJSON, false)

		assert.IsType(t, LessonEditEvent{}, actualEvent)
		event := actualEvent.(LessonEditEvent)

		assert.Equal(t, uint(999999), event.GetLessonId())
		assert.Equal(t, uint(193000), event.GetDisciplineId())
		assert.Equal(t, "0", event.Semester)
	})

	t.Run("Fetch LessonDeletedEvent", func(t *testing.T) {
		actualEvent := fetchMessage(t, LessonDeletedEventFormJSON, false)

		assert.IsType(t, LessonDeletedEvent{}, actualEvent)
		event := actualEvent.(LessonDeletedEvent)

		assert.Equal(t, uint(999999), event.GetLessonId())
		assert.Equal(t, uint(193000), event.GetDisciplineId())
		assert.Equal(t, "0", event.Semester)
	})

	t.Run("Fetch ScoreEditEvent", func(t *testing.T) {
		actualEvent := fetchMessage(t, ScoreEditEventFormJSON, false)

		assert.IsType(t, ScoreEditEvent{}, actualEvent)
		event := actualEvent.(ScoreEditEvent)

		assert.Equal(t, uint(999999), event.GetLessonId())
		assert.Equal(t, uint(188619), event.GetDisciplineId())
		assert.Equal(t, "0", event.Semester)
		assert.Equal(t, "-11", event.Scores[110044][1])
		assert.Equal(t, "", event.Scores[110044][2])
		assert.Equal(t, "нб/нп", event.Scores[110054][1])
		assert.Equal(t, "", event.Scores[110054][2])
	})

	t.Run("Wrong form", func(t *testing.T) {
		eventFormJSON := strings.Replace(ScoreEditEventFormJSON, `"st`, `"__`, -1)
		actualEvent := fetchMessage(t, eventFormJSON, true)

		assert.Nil(t, actualEvent)
	})

	t.Run("Fetch wrong message", func(t *testing.T) {
		actualEvent := fetchMessage(t, "test", true)

		assert.Nil(t, actualEvent)
	})

	t.Run("failed to fetch message from SQS", func(t *testing.T) {
		out := &bytes.Buffer{}
		expectedError := errors.New("expected error")

		sqsClientMock := NewMockSqsApiClientInterface(t)

		sqsClientMock.On(
			"ReceiveMessage",
			mock.MatchedBy(func(ctx context.Context) bool { return true }),
			mock.IsType(&sqs.ReceiveMessageInput{}),
		).Return(nil, expectedError).Once()

		fetcher := eventFetcher{
			out:         out,
			sqsQueueUrl: &sqsQueueUrl,
			client:      sqsClientMock,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		actualEvent := fetcher.Fetch(ctx)
		cancel()

		sqsClientMock.AssertExpectations(t)
		assert.Nil(t, actualEvent)
		assert.Contains(t, out.String(), expectedError.Error())
	})
}

func fetchMessage(t *testing.T, formJSON string, expectDelete bool) interface{} {
	var out bytes.Buffer
	var ctx context.Context
	var cancel context.CancelFunc

	sqsClientMock := NewMockSqsApiClientInterface(t)
	deleterMock := NewMockEventDeleterInterface(t)

	sqsMessageOutput := buildSqsMessageOutput(formJSON)

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
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	actualEvent := fetcher.Fetch(ctx)
	cancel()

	sqsClientMock.AssertExpectations(t)
	deleterMock.AssertExpectations(t)

	return actualEvent
}

func buildSqsMessageOutput(formJSON string) *sqs.ReceiveMessageOutput {
	receiptHandle := "ReceiptHandle"
	return &sqs.ReceiveMessageOutput{
		Messages: []sqstypes.Message{
			{
				ReceiptHandle: &receiptHandle,
				Body:          aws.String(fmt.Sprintf(EventMessageJSON, formJSON)),
			},
		},
	}
}
