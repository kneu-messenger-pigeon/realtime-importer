package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/kneu-messenger-pigeon/fileStorage"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"strconv"
	"testing"
	"time"
)

func TestExecuteCurrentYearWatcher(t *testing.T) {
	t.Run("defaultYear", func(t *testing.T) {
		var out bytes.Buffer

		reader := events.NewMockReaderInterface(t)
		storage := fileStorage.NewMockInterface(t)
		storage.On("Get").Return("", nil)

		currentYearWatcher := CurrentYearWatcher{
			out:     &out,
			storage: storage,
			reader:  reader,
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		currentYearWatcher.execute(ctx)

		actualYear := currentYearWatcher.getYear()

		assert.GreaterOrEqual(t, actualYear, time.Now().Year()-1)
		assert.LessOrEqual(t, actualYear, time.Now().Year())

		reader.AssertExpectations(t)
		storage.AssertExpectations(t)
	})

	t.Run("yearFromKafka", func(t *testing.T) {
		expectedYear := 2026
		expectedError := errors.New("expected error")

		var out bytes.Buffer
		matchContext := mock.MatchedBy(func(ctx context.Context) bool { return true })

		event := events.SecondaryDbLoadedEvent{
			PreviousSecondaryDatabaseDatetime: time.Date(expectedYear, time.Month(4), 10, 4, 0, 0, 0, time.UTC),
			CurrentSecondaryDatabaseDatetime:  time.Date(expectedYear, time.Month(4), 11, 4, 0, 0, 0, time.UTC),
			Year:                              expectedYear,
		}

		payload, _ := json.Marshal(event)
		message := kafka.Message{
			Key:   []byte(events.SecondaryDbLoadedEventName),
			Value: payload,
		}

		ctx, cancel := context.WithCancel(context.Background())
		reader := events.NewMockReaderInterface(t)
		reader.On("FetchMessage", matchContext).Return(func(ctx context.Context) kafka.Message {
			cancel()
			return message
		}, nil)
		reader.On("CommitMessages", matchContext, message).Return(expectedError)

		storage := fileStorage.NewMockInterface(t)
		storage.On("Get").Return("2024", nil)
		storage.On("Set", strconv.Itoa(expectedYear)).Return(nil)

		currentYearWatcher := CurrentYearWatcher{
			out:     &out,
			storage: storage,
			reader:  reader,
		}

		go func() {
			time.Sleep(time.Millisecond * 50)
			cancel()
		}()
		currentYearWatcher.execute(ctx)

		actualYear := currentYearWatcher.getYear()
		assert.Equal(t, expectedYear, actualYear)

		stringOutput := out.String()
		assert.NotContains(t, stringOutput, expectedError.Error())

		reader.AssertExpectations(t)
		storage.AssertExpectations(t)
	})

	t.Run("Error save to storage", func(t *testing.T) {
		expectedYear := 2026
		expectedError := errors.New("expected error")

		var out bytes.Buffer
		matchContext := mock.MatchedBy(func(ctx context.Context) bool { return true })

		event := events.SecondaryDbLoadedEvent{
			PreviousSecondaryDatabaseDatetime: time.Date(expectedYear, time.Month(4), 10, 4, 0, 0, 0, time.UTC),
			CurrentSecondaryDatabaseDatetime:  time.Date(expectedYear, time.Month(4), 11, 4, 0, 0, 0, time.UTC),
			Year:                              expectedYear,
		}

		payload, _ := json.Marshal(event)
		message := kafka.Message{
			Key:   []byte(events.SecondaryDbLoadedEventName),
			Value: payload,
		}

		ctx, cancel := context.WithCancel(context.Background())
		reader := events.NewMockReaderInterface(t)
		reader.On("FetchMessage", matchContext).Return(func(ctx context.Context) kafka.Message {
			cancel()
			return message
		}, nil)

		storage := fileStorage.NewMockInterface(t)
		storage.On("Get").Return("2024", nil)
		storage.On("Set", strconv.Itoa(expectedYear)).Return(expectedError)

		currentYearWatcher := CurrentYearWatcher{
			out:     &out,
			storage: storage,
			reader:  reader,
		}

		go func() {
			time.Sleep(time.Millisecond * 50)
			cancel()
		}()
		currentYearWatcher.execute(ctx)

		actualYear := currentYearWatcher.getYear()
		assert.Equal(t, expectedYear, actualYear)

		stringOutput := out.String()
		assert.Contains(t, stringOutput, expectedError.Error())

		reader.AssertExpectations(t)
		storage.AssertExpectations(t)
	})
}
