package main

import (
	"github.com/VictoriaMetrics/metrics"
	dekanatEvents "github.com/kneu-messenger-pigeon/dekanat-events"
)

var (
	incomingLessonCreateTotal  = metrics.NewCounter(`incoming_realtime_events_total{type="lesson_create"}`)
	incomingLessonEditTotal    = metrics.NewCounter(`incoming_realtime_events_total{type="lesson_edit"}`)
	incomingLessonDeletedTotal = metrics.NewCounter(`incoming_realtime_events_total{type="lesson_deleted"}`)
	incomingScoreEditTotal     = metrics.NewCounter(`incoming_realtime_events_total{type="score_edit"}`)
)

func trackEventMetrics(event interface{}) {
	switch event.(type) {

	case dekanatEvents.ScoreEditEvent, *dekanatEvents.ScoreEditEvent:
		incomingScoreEditTotal.Inc()

	case dekanatEvents.LessonCreateEvent, *dekanatEvents.LessonCreateEvent:
		incomingLessonCreateTotal.Inc()

	case dekanatEvents.LessonEditEvent, *dekanatEvents.LessonEditEvent:
		incomingLessonEditTotal.Inc()

	case dekanatEvents.LessonDeletedEvent, *dekanatEvents.LessonDeletedEvent:
		incomingLessonDeletedTotal.Inc()
	}
}
