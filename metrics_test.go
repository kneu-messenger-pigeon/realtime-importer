package main

import (
	dekanatEvents "github.com/kneu-messenger-pigeon/dekanat-events"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTrackEventMetrics(t *testing.T) {
	resetMetrics := func() {
		incomingLessonCreateTotal.Set(0)
		incomingLessonEditTotal.Set(0)
		incomingLessonDeletedTotal.Set(0)
		incomingScoreEditTotal.Set(0)
	}

	t.Run("LessonCreate", func(t *testing.T) {
		resetMetrics()
		trackEventMetrics(dekanatEvents.LessonCreateEvent{})

		assert.Equal(t, uint64(1), incomingLessonCreateTotal.Get())
		assert.Equal(t, uint64(0), incomingLessonEditTotal.Get())
		assert.Equal(t, uint64(0), incomingLessonDeletedTotal.Get())
		assert.Equal(t, uint64(0), incomingScoreEditTotal.Get())
	})

	t.Run("LessonEdit", func(t *testing.T) {
		resetMetrics()
		trackEventMetrics(dekanatEvents.LessonEditEvent{})

		assert.Equal(t, uint64(0), incomingLessonCreateTotal.Get())
		assert.Equal(t, uint64(1), incomingLessonEditTotal.Get())
		assert.Equal(t, uint64(0), incomingLessonDeletedTotal.Get())
		assert.Equal(t, uint64(0), incomingScoreEditTotal.Get())
	})

	t.Run("LessonDelete", func(t *testing.T) {
		resetMetrics()
		trackEventMetrics(dekanatEvents.LessonDeletedEvent{})

		assert.Equal(t, uint64(0), incomingLessonCreateTotal.Get())
		assert.Equal(t, uint64(0), incomingLessonEditTotal.Get())
		assert.Equal(t, uint64(1), incomingLessonDeletedTotal.Get())
		assert.Equal(t, uint64(0), incomingScoreEditTotal.Get())
	})

	t.Run("ScoreEdit", func(t *testing.T) {
		resetMetrics()
		trackEventMetrics(dekanatEvents.ScoreEditEvent{})

		assert.Equal(t, uint64(0), incomingLessonCreateTotal.Get())
		assert.Equal(t, uint64(0), incomingLessonEditTotal.Get())
		assert.Equal(t, uint64(0), incomingLessonDeletedTotal.Get())
		assert.Equal(t, uint64(1), incomingScoreEditTotal.Get())
	})
}
