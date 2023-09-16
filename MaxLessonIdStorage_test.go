package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMaxLessonId_Set(t *testing.T) {
	maxLessonId := &MaxLessonId{}

	maxLessonId.Set(1)
	assert.Equal(t, uint(1), maxLessonId.Get())

	maxLessonId.Set(5)
	assert.Equal(t, uint(5), maxLessonId.Get())

	maxLessonId.Set(3)
	assert.Equal(t, uint(5), maxLessonId.Get())
}
