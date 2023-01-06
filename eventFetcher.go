package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/mitchellh/mapstructure"
	"io"
	"regexp"
	"strconv"
)

type EventFetcherInterface interface {
	Fetch(context context.Context) interface{}
}

type EventMessage struct {
	ReceiptHandle *string
	Timestamp     int64             `json:"timestamp"`
	Ip            string            `json:"ip"`
	Referer       string            `json:"referer"`
	Form          map[string]string `json:"form"`
}

type eventFetcher struct {
	out         io.Writer
	client      SqsApiClientInterface
	sqsQueueUrl *string
	deleter     EventDeleterInterface
}

func (fetcher eventFetcher) Fetch(context context.Context) (event interface{}) {
	var isLessonForm bool
	var actionNumber int
	var actionString string
	var commonEventData CommonEventData

	var err error

	for context.Err() == nil {
		eventMessage := fetcher.fetchMessages(context)
		if eventMessage == nil {
			break
		}

		_ = mapstructure.Decode(eventMessage.Form, &commonEventData)
		commonEventData.ReceiptHandle = eventMessage.ReceiptHandle
		commonEventData.Timestamp = eventMessage.Timestamp

		actionNumber, _ = strconv.Atoi(eventMessage.Form["n"])
		actionString, _ = eventMessage.Form["action"]
		if _, isLessonForm = eventMessage.Form["tzn"]; isLessonForm {
			_, isLessonForm = eventMessage.Form["date_z"]
		}

		if actionNumber == 10 && isLessonForm && actionString == "edit" {
			event, err = parseLessonEditEvent(&eventMessage.Form, commonEventData)
		} else if actionNumber == 10 && isLessonForm && actionString == "insert" {
			event, err = parseLessonCreateEvent(&eventMessage.Form, commonEventData)
		} else if actionNumber == 11 && actionString == "delete" {
			event, err = parseLessonDeleteEvent(&eventMessage.Form, commonEventData)
		} else if actionNumber == 4 && formDataHasStudentScoreField(&eventMessage.Form) {
			event, err = parseScoreEditEvent(&eventMessage.Form, commonEventData)
		} else {
			err = errors.New(fmt.Sprintf("Unknown form: n=%d, action=%s", actionNumber, actionString))
		}

		if err != nil || event == nil {
			fmt.Fprintf(fetcher.out, "Failed to decode Event message: %v (message: - %v) \n", err, eventMessage)
			fetcher.deleter.Delete(eventMessage.ReceiptHandle)
		} else {
			return event
		}
	}

	return nil
}

func (fetcher eventFetcher) fetchMessages(context context.Context) (eventMessage *EventMessage) {
	gMInput := &sqs.ReceiveMessageInput{
		QueueUrl:            fetcher.sqsQueueUrl,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     20,
	}
	var err error
	var msgResult *sqs.ReceiveMessageOutput

	for context.Err() == nil {
		msgResult, err = fetcher.client.ReceiveMessage(context, gMInput)

		if err != nil && context.Err() == nil {
			fmt.Fprintf(fetcher.out, "Failed to get message: %v \n", err)
			break
		}

		if msgResult != nil && len(msgResult.Messages) != 0 {
			err = json.Unmarshal([]byte(*msgResult.Messages[0].Body), &eventMessage)
			if err == nil && isValidEventForm(eventMessage.Form) {
				eventMessage.ReceiptHandle = msgResult.Messages[0].ReceiptHandle
				return eventMessage
			} else {
				fetcher.deleter.Delete(msgResult.Messages[0].ReceiptHandle)
			}
		}
	}

	return nil
}

func isValidEventForm(form map[string]string) (valid bool) {
	if _, valid = form["n"]; valid {
		if _, valid = form["prti"]; valid {
			_, valid = form["prt"]
		}
	}

	return
}

func parseLessonCreateEvent(form *map[string]string, eventData CommonEventData) (event LessonCreateEvent, err error) {
	err = mapstructure.Decode(form, &event)
	event.CommonEventData = eventData
	return
}

func parseLessonEditEvent(form *map[string]string, eventData CommonEventData) (event LessonEditEvent, err error) {
	err = mapstructure.Decode(form, &event)
	event.CommonEventData = eventData
	return
}

func parseLessonDeleteEvent(form *map[string]string, eventData CommonEventData) (event LessonDeletedEvent, err error) {
	err = mapstructure.Decode(form, &event)
	event.CommonEventData = eventData
	return
}

var StudentScoreFieldRegexp = regexp.MustCompile("^st([0-9]+)_(1|2)-[0-9]+$")

func parseScoreEditEvent(form *map[string]string, eventData CommonEventData) (event ScoreEditEvent, err error) {
	var matches []string
	var studentId int
	var lessonHalf uint64
	var hasKey bool

	err = mapstructure.Decode(form, &event)
	event.CommonEventData = eventData
	event.Scores = make(map[int]map[uint8]string)

	if err == nil {
		for key, value := range *form {
			matches = StudentScoreFieldRegexp.FindStringSubmatch(key)
			if len(matches) >= 3 {
				studentId, _ = strconv.Atoi(matches[1])
				lessonHalf, _ = strconv.ParseUint(matches[2], 10, 8)

				if _, hasKey = event.Scores[studentId]; !hasKey {
					event.Scores[studentId] = make(map[uint8]string)
				}
				event.Scores[studentId][uint8(lessonHalf)] = value
			}
		}
	}
	return
}

func formDataHasStudentScoreField(formData *map[string]string) bool {
	for key := range *formData {
		if StudentScoreFieldRegexp.MatchString(key) {
			return true
		}
	}
	return false
}
