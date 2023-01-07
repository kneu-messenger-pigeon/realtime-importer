package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/kneu-messenger-pigeon/fileStorage"
	"github.com/segmentio/kafka-go"
	"io"
	"strconv"
	"time"
)

type CurrentYearWatcherInterface interface {
	execute(context.Context)
	getYear() int
}

type CurrentYearWatcher struct {
	out     io.Writer
	storage fileStorage.Interface
	reader  events.ReaderInterface
	year    int
}

func (watcher *CurrentYearWatcher) execute(ctx context.Context) {
	yearString, err := watcher.storage.Get()
	if err == nil && yearString != "" {
		watcher.year, err = strconv.Atoi(yearString)
	}
	if watcher.year < 2022 {
		watcher.year = (time.Now().Year()*12 + int(time.Now().Month()) - 8) / 12
	}

	var event events.SecondaryDbLoadedEvent
	var m kafka.Message
	for ctx.Err() == nil {
		m, err = watcher.reader.FetchMessage(ctx)
		if err == nil && string(m.Key) == events.SecondaryDbLoadedEventName {
			err = json.Unmarshal(m.Value, &event)
			if err == nil {
				watcher.year = event.Year
				err = watcher.storage.Set(strconv.Itoa(event.Year))
			}
		}

		if err == nil {
			err = watcher.reader.CommitMessages(context.Background(), m)
		} else if ctx.Err() == nil {
			fmt.Fprintf(watcher.out, "[%s] Year watcher error: %s \n", t(), err)
		}
	}
}

func (watcher *CurrentYearWatcher) getYear() int {
	return watcher.year
}
