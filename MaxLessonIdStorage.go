package main

import (
	"sync"
)

type MaxLessonId struct {
	maxLessonId uint
	sync.Mutex
}

type MaxLessonIdSetterInterface interface {
	Set(uint)
}

type MaxLessonIdGetterInterface interface {
	Get() uint
}

func (storage *MaxLessonId) Get() uint {
	return storage.maxLessonId
}

func (storage *MaxLessonId) Set(lessonId uint) {
	if lessonId > storage.maxLessonId {
		storage.Mutex.Lock()
		if lessonId > storage.maxLessonId {
			storage.maxLessonId = lessonId
		}
		storage.Mutex.Unlock()
	}
}
