package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/kneu-messenger-pigeon/fileStorage"
	"github.com/segmentio/kafka-go"
	"io"
	"time"
)

const YearStorageLength = 2

type CurrentYearGetterInterface interface {
	GetYear() int
}

type CurrentYearWatcherInterface interface {
	CurrentYearGetterInterface
	Execute(context.Context)
}

type CurrentYearWatcher struct {
	out     io.Writer
	storage fileStorage.Interface
	reader  events.ReaderInterface
	year    uint16
}

func (watcher *CurrentYearWatcher) Execute(ctx context.Context) {
	yearBytes, err := watcher.storage.Get()
	if yearBytes != nil && len(yearBytes) == YearStorageLength {
		watcher.year = binary.LittleEndian.Uint16(yearBytes)
	}
	if watcher.year < 2022 {
		watcher.year = uint16((time.Now().Year()*12 + int(time.Now().Month()) - 8) / 12)
	}

	var event events.SecondaryDbLoadedEvent
	var m kafka.Message
	for ctx.Err() == nil {
		m, err = watcher.reader.FetchMessage(ctx)
		if err == nil && string(m.Key) == events.SecondaryDbLoadedEventName {
			err = json.Unmarshal(m.Value, &event)
			if err == nil {
				watcher.year = uint16(event.Year)

				yearBytes = make([]byte, YearStorageLength)
				binary.LittleEndian.PutUint16(yearBytes, watcher.year)
				err = watcher.storage.Set(yearBytes)
			}
			fmt.Fprintf(watcher.out, "[%s] New year received: %d (err: %v)\n", t(), event.Year, err)
		}

		if err == nil {
			err = watcher.reader.CommitMessages(context.Background(), m)
		} else if !errors.Is(err, ctx.Err()) {
			fmt.Fprintf(watcher.out, "[%s] Year watcher error: %s \n", t(), err)
		}
	}
}

func (watcher *CurrentYearWatcher) GetYear() int {
	return int(watcher.year)
}
