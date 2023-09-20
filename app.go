package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/VictoriaMetrics/fastcache"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	dekanatEvents "github.com/kneu-messenger-pigeon/dekanat-events"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/kneu-messenger-pigeon/fileStorage"
	_ "github.com/nakagami/firebirdsql"
	"github.com/segmentio/kafka-go"
	"io"
	"os"
	"time"
)

const ExitCodeMainError = 1

var defaultPollInterval = time.Second * 3
var defaultForcePollInterval = time.Minute * 30

func runApp(out io.Writer) error {
	var awsCfg aws.Config
	var primaryDekanatDbPool [ConnectionPoolSize]*sql.DB

	envFilename := ""
	if _, err := os.Stat(".env"); err == nil {
		envFilename = ".env"
	}
	appConfig, err := loadConfig(envFilename)

	if err == nil {
		awsCfg, err = awsConfig.LoadDefaultConfig(context.Background())
	}

	if err == nil {
		fmt.Fprintf(out, "[%s] Open connections to primary dekanat DB...", t())
		startTime := time.Now()
		primaryDekanatDbPool, err = createConnectionPool(&appConfig)
		defer closeConnectionPool(primaryDekanatDbPool)
		fmt.Fprintf(out, "done in %d ms (err: %v) \n", time.Since(startTime).Milliseconds(), err)
	}

	if err != nil {
		return err
	}
	MakeEventLoop(out, &primaryDekanatDbPool, &appConfig, awsCfg).execute()
	return nil
}

func MakeEventLoop(out io.Writer, dbPool *[ConnectionPoolSize]*sql.DB, appConfig *Config, awsCfg aws.Config) *EventLoop {
	client := sqs.NewFromConfig(awsCfg)
	deleter := &EventDeleter{
		out:         out,
		client:      client,
		sqsQueueUrl: &appConfig.sqsQueueUrl,
	}

	fetcher := &eventFetcher{
		out:         out,
		client:      client,
		sqsQueueUrl: &appConfig.sqsQueueUrl,
		deleter:     deleter,
	}

	lessonsWriter := &kafka.Writer{
		Addr:     kafka.TCP(appConfig.kafkaHost),
		Topic:    events.RawLessonsTopic,
		Balancer: &kafka.LeastBytes{},
	}

	scoresWriter := &kafka.Writer{
		Addr:     kafka.TCP(appConfig.kafkaHost),
		Topic:    events.RawScoresTopic,
		Balancer: &kafka.LeastBytes{},
	}

	currentYearWatcher := &CurrentYearWatcher{
		out: out,
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:     []string{appConfig.kafkaHost},
				GroupID:     "realtime-importer",
				Topic:       events.MetaEventsTopic,
				MinBytes:    10,
				MaxBytes:    10e3,
				MaxWait:     time.Second,
				MaxAttempts: appConfig.kafkaAttempts,
				Dialer: &kafka.Dialer{
					Timeout:   appConfig.kafkaTimeout,
					DualStack: kafka.DefaultDialer.DualStack,
				},
			},
		),
		storage: &fileStorage.Storage{
			File: appConfig.storageDir + "current-year.txt",
		},
	}

	editScoresLessonId := &MaxLessonId{}

	createdLessonsImporter := &CreatedLessonsImporter{
		out:   out,
		db:    dbPool[0],
		cache: NewTimeCache(1),
		storage: &fileStorage.Storage{
			File: appConfig.storageDir + "created-lessons-state.txt",
		},
		writer:                lessonsWriter,
		currentYear:           currentYearWatcher,
		eventQueue:            []dekanatEvents.LessonCreateEvent{},
		editScoresMaxLessonId: editScoresLessonId,
	}

	editedLessonsImporter := &EditedLessonsImporter{
		out:         out,
		db:          dbPool[1],
		cache:       fastcache.New(1),
		writer:      lessonsWriter,
		currentYear: currentYearWatcher,
		eventQueue:  []dekanatEvents.LessonEditEvent{},
	}

	updatedScoresImporter := &UpdatedScoresImporter{
		out:   out,
		db:    dbPool[2],
		cache: NewTimeCache(1),
		storage: &fileStorage.Storage{
			File: appConfig.storageDir + "update-scores-state.txt",
		},
		writer:      scoresWriter,
		currentYear: currentYearWatcher,
		eventQueue:  []dekanatEvents.ScoreEditEvent{},
		maxLessonId: editScoresLessonId,
	}

	deletedScoresImporter := &DeletedScoresImporter{
		out:         out,
		db:          dbPool[3],
		cache:       fastcache.New(1),
		writer:      scoresWriter,
		currentYear: currentYearWatcher,
		eventQueue:  []dekanatEvents.LessonDeletedEvent{},
	}

	eventLoop := EventLoop{
		out:                    out,
		fetcher:                fetcher,
		deleter:                deleter,
		editedLessonsImporter:  editedLessonsImporter,
		createdLessonsImporter: createdLessonsImporter,
		updatedScoresImporter:  updatedScoresImporter,
		deletedScoresImporter:  deletedScoresImporter,
		currentYearWatcher:     currentYearWatcher,
	}

	return &eventLoop
}

func handleExitError(errStream io.Writer, err error) int {
	if err != nil {
		_, _ = fmt.Fprintln(errStream, err)
	}

	if err != nil {
		return ExitCodeMainError
	}

	return 0
}

func t() string {
	return time.Now().Format("2006-01-02 15:04:05")
}
