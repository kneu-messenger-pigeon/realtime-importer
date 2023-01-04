package main

import (
	"strconv"
	"time"
)

const LessonCreateEventName = "LessonCreateEvent"
const LessonEditEventName = "LessonEditEvent"
const LessonDeletedEventName = "LessonDeletedEvent"
const ScoreEditEventName = "ScoreEditEventName"

type CommonEventData struct {
	ReceiptHandle *string
	Timestamp     int64
	//	ActionNumber  int8   `mapstructure:"n"`
	SessionId    string `mapstructure:"sesID"`
	LessonId     string `mapstructure:"prti"`
	DisciplineId string `mapstructure:"prt"`
	Semester     string `mapstructure:"hlf"`
}

type LessonCreateEvent struct {
	CommonEventData
	// n=4 and action:insert
	TypeId    string `mapstructure:"tzn"`
	Date      string `mapstructure:"date_z"`
	TeacherId string `mapstructure:"teacher"`
}

type LessonEditEvent struct {
	CommonEventData
	// n=4 and 	action: edit
	TypeId    string `mapstructure:"tzn"`
	Date      string `mapstructure:"date_z"`
	TeacherId string `mapstructure:"teacher"`
	IsDeleted bool
}

type LessonDeletedEvent struct {
	CommonEventData
	// n = 11
}

type ScoreEditEvent struct {
	CommonEventData
	// n = 4 and have keys "^st[0-9]+_(1|2)_[0-9]+$" st119906_2-2616031
	Date   string `mapstructure:"d2"`
	Scores map[int]map[uint8]string
	/* map from form:
	st119905_1-2616031:
	st119905_2-2616031: нб/нп
	st119906_1-2616031:
	st119909_1-2616031:	6
	st119909_2-2616031:
	st119910_1-2616031:
	st119910_2-2616031:
	*/

}

func (data *CommonEventData) GetLessonId() uint {
	return parseUint(data.LessonId)
}

func (data *CommonEventData) GetDisciplineId() uint {
	return parseUint(data.DisciplineId)
}

func (data *LessonEditEvent) GetTypeId() uint8 {
	return uint8(parseUint(data.TypeId))
}

func (data *LessonEditEvent) GetDate() (date time.Time) {
	if data.Date != "" {
		year, _ := strconv.Atoi(data.Date[6:10])
		month, _ := strconv.Atoi(data.Date[3:5])
		day, _ := strconv.Atoi(data.Date[0:2])
		date = time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local)
	}
	return
}

func parseUint(s string) uint {
	value, _ := strconv.ParseUint(s, 10, 0)
	return uint(value)
}
