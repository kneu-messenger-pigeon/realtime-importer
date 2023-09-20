package main

import (
	"realtime-importer/mocks"
	"testing"
)

func NewMockCurrentYearGetter(t *testing.T, currentYear int) CurrentYearWatcherInterface {
	currentYearWatcher := mocks.NewCurrentYearWatcherInterface(t)
	currentYearWatcher.On("GetYear").Return(currentYear)
	return currentYearWatcher
}
