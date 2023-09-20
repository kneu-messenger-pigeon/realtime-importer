// Code generated by mockery v2.14.1. DO NOT EDIT.

package main

import (
	context "context"
	dekanatEvents "github.com/kneu-messenger-pigeon/dekanat-events"

	mock "github.com/stretchr/testify/mock"
)

// MockEditedLessonsImporterInterface is an autogenerated mock type for the EditedLessonsImporterInterface type
type MockEditedLessonsImporterInterface struct {
	mock.Mock
}

// addEvent provides a mock function with given fields: event
func (_m *MockEditedLessonsImporterInterface) addEvent(event dekanatEvents.LessonEditEvent) {
	_m.Called(event)
}

// execute provides a mock function with given fields: _a0
func (_m *MockEditedLessonsImporterInterface) execute(_a0 context.Context) {
	_m.Called(_a0)
}

// getConfirmed provides a mock function with given fields:
func (_m *MockEditedLessonsImporterInterface) getConfirmed() <-chan dekanatEvents.LessonEditEvent {
	ret := _m.Called()

	var r0 <-chan dekanatEvents.LessonEditEvent
	if rf, ok := ret.Get(0).(func() <-chan dekanatEvents.LessonEditEvent); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan dekanatEvents.LessonEditEvent)
		}
	}

	return r0
}

type mockConstructorTestingTNewMockEditedLessonsImporterInterface interface {
	mock.TestingT
	Cleanup(func())
}

// NewMockEditedLessonsImporterInterface creates a new instance of MockEditedLessonsImporterInterface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMockEditedLessonsImporterInterface(t mockConstructorTestingTNewMockEditedLessonsImporterInterface) *MockEditedLessonsImporterInterface {
	mock := &MockEditedLessonsImporterInterface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
