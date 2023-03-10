// Code generated by mockery v2.14.1. DO NOT EDIT.

package main

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// MockEventFetcherInterface is an autogenerated mock type for the EventFetcherInterface type
type MockEventFetcherInterface struct {
	mock.Mock
}

// Fetch provides a mock function with given fields: _a0
func (_m *MockEventFetcherInterface) Fetch(_a0 context.Context) interface{} {
	ret := _m.Called(_a0)

	var r0 interface{}
	if rf, ok := ret.Get(0).(func(context.Context) interface{}); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	return r0
}

type mockConstructorTestingTNewMockEventFetcherInterface interface {
	mock.TestingT
	Cleanup(func())
}

// NewMockEventFetcherInterface creates a new instance of MockEventFetcherInterface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMockEventFetcherInterface(t mockConstructorTestingTNewMockEventFetcherInterface) *MockEventFetcherInterface {
	mock := &MockEventFetcherInterface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
