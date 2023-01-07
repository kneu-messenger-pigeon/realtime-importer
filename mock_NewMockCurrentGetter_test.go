package main

func NewMockCurrentYearGetter(t mockConstructorTestingTNewMockCurrentYearWatcherInterface, year int) *MockCurrentYearWatcherInterface {
	mock := NewMockCurrentYearWatcherInterface(t)
	mock.On("getYear").Return(year)

	return mock
}
