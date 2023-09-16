package main

import (
	"bytes"
	"database/sql"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aws/aws-sdk-go-v2/aws"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestRunApp(t *testing.T) {
	t.Run("Run with mock config", func(t *testing.T) {
		_ = os.Setenv("KAFKA_HOST", expectedConfig.kafkaHost)
		_ = os.Setenv("DEKANAT_DB_DRIVER_NAME", "sqlite3")
		_ = os.Setenv("PRIMARY_DEKANAT_DB_DSN", os.TempDir()+"/test-sqlite.db")
		_ = os.Setenv("KAFKA_TIMEOUT", strconv.Itoa(int(expectedConfig.kafkaTimeout.Seconds())))
		_ = os.Setenv("AWS_SQS_QUEUE_URL", expectedConfig.sqsQueueUrl)
		_ = os.Setenv("STORAGE_DIR", expectedConfig.storageDir)
		_ = os.Setenv("PRIMARY_DEKANAT_PING_ATTEMPTS", "0")
		_ = os.Setenv("AWS_SECRET_ACCESS_KEY", "")
		_ = os.Setenv("AWS_ACCESS_KEY_ID", "")
		_ = os.Setenv("AWS_SQS_QUEUE_URL", "")

		go func() {
			time.Sleep(time.Millisecond * 100)
			_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
		}()

		var out bytes.Buffer
		err := runApp(&out)
		assert.NoError(t, err, "Expected for TooManyError, got %s", err)

		outputString := out.String()

		assert.Contains(t, outputString, "Open connections to primary dekanat DB")
		assert.Contains(t, outputString, "Check new scores changes after")
		assert.Contains(t, outputString, "Loaded default max lesson id")
		assert.Contains(t, outputString, "Failed to get LessonMax from file near")
		assert.Contains(t, outputString, "Check new lessons created after the lesson")
	})

	t.Run("Run with wrong env file", func(t *testing.T) {
		previousWd, err := os.Getwd()
		assert.NoErrorf(t, err, "Failed to get working dir: %s", err)
		tmpDir := os.TempDir() + "/secondary-db-watcher-run-dir"
		tmpEnvFilepath := tmpDir + "/.env"

		defer func() {
			_ = os.Chdir(previousWd)
			_ = os.Remove(tmpEnvFilepath)
			_ = os.Remove(tmpDir)
		}()

		if _, err := os.Stat(tmpDir); errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(tmpDir, os.ModePerm)
			assert.NoErrorf(t, err, "Failed to create tmp dir %s: %s", tmpDir, err)
		}
		if _, err := os.Stat(tmpEnvFilepath); errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(tmpEnvFilepath, os.ModePerm)
			assert.NoErrorf(t, err, "Failed to create tmp  %s/.env: %s", tmpDir, err)
		}

		err = os.Chdir(tmpDir)
		assert.NoErrorf(t, err, "Failed to change working dir: %s", err)

		var out bytes.Buffer
		err = runApp(&out)
		assert.Error(t, err, "Expected for error")
		assert.Containsf(
			t, err.Error(), "Error loading .env file",
			"Expected for Load config error, got: %s", err,
		)
	})

	t.Run("Run with wrong sql driver", func(t *testing.T) {
		_ = os.Setenv("DEKANAT_DB_DRIVER_NAME", "dummy-not-exist")
		_ = os.Setenv("PRIMARY_DEKANAT_DB_DSN", os.TempDir()+"/test-sqlite.db")
		defer os.Unsetenv("DEKANAT_DB_DRIVER_NAME")

		var out bytes.Buffer
		err := runApp(&out)

		expectedError := "sql: unknown driver \"dummy-not-exist\" (forgotten import?)"

		assert.Error(t, err, "Expected for error")
		assert.Equalf(t, expectedError, err.Error(), "Expected for another error, got %s", err)
	})
}

func TestMakeEventLoop(t *testing.T) {
	pool := [ConnectionPoolSize]*sql.DB{}
	for i := 0; i < ConnectionPoolSize; i++ {
		pool[i], _, _ = sqlmock.New()
	}

	config := &Config{
		dekanatDbDriverName:          "",
		kafkaHost:                    "",
		primaryDekanatDbDSN:          "",
		primaryDekanatPingAttempts:   0,
		primaryDekanatPingDelay:      0,
		primaryDekanatReconnectDelay: 0,
		kafkaTimeout:                 0,
		kafkaAttempts:                0,
		sqsQueueUrl:                  "",
		storageDir:                   "",
	}

	awsCfg := aws.Config{}

	eventloop := MakeEventLoop(&bytes.Buffer{}, &pool, config, awsCfg)

	assert.IsType(t, &EventLoop{}, eventloop, "Expected for EventLoop type")

	assert.IsType(t, &eventFetcher{}, eventloop.fetcher, "Expected for EventDeleter type")
	assert.IsType(t, &EventDeleter{}, eventloop.deleter, "Expected for EventDeleter type")

	assert.IsType(t, &CreatedLessonsImporter{}, eventloop.createdLessonsImporter, "Expected for CreatedLessonsImporter type")
	assert.IsType(t, &EditedLessonsImporter{}, eventloop.editedLessonsImporter, "Expected for EditedLessonsImporter type")
	assert.IsType(t, &UpdatedScoresImporter{}, eventloop.updatedScoresImporter, "Expected for UpdatedScoresImporter type")
	assert.IsType(t, &DeletedScoresImporter{}, eventloop.deletedScoresImporter, "Expected for DeletedScoresImporter type")

	assert.IsType(t, &CurrentYearWatcher{}, eventloop.currentYearWatcher, "Expected for CurrentYearWatcher type")

	createdLessonsImporter := eventloop.createdLessonsImporter.(*CreatedLessonsImporter)
	assert.Equal(t, eventloop.out, createdLessonsImporter.out, "Expected for same out")
	assert.Equal(t, pool[0], createdLessonsImporter.db, "Expected for same db")
	assert.IsType(t, &MaxLessonId{}, createdLessonsImporter.editScoresMaxLessonId, "Expected for MaxLessonId type")

	editedLessonsImporter := eventloop.editedLessonsImporter.(*EditedLessonsImporter)
	assert.Equal(t, eventloop.out, editedLessonsImporter.out, "Expected for same out")
	assert.Equal(t, pool[1], editedLessonsImporter.db, "Expected for same db")

	updatedScoresImporter := eventloop.updatedScoresImporter.(*UpdatedScoresImporter)
	assert.Equal(t, eventloop.out, updatedScoresImporter.out, "Expected for same out")
	assert.Equal(t, pool[2], updatedScoresImporter.db, "Expected for same db")
	assert.IsType(t, &MaxLessonId{}, updatedScoresImporter.maxLessonId, "Expected for MaxLessonId type")
	assert.Equal(t, createdLessonsImporter.editScoresMaxLessonId, updatedScoresImporter.maxLessonId)

	deletedScoresImporter := eventloop.deletedScoresImporter.(*DeletedScoresImporter)
	assert.Equal(t, eventloop.out, deletedScoresImporter.out, "Expected for same out")
	assert.Equal(t, pool[3], deletedScoresImporter.db, "Expected for same db")
}

func TestHandleExitError(t *testing.T) {
	t.Run("Handle exit error", func(t *testing.T) {
		var actualExitCode int
		var out bytes.Buffer

		testCases := map[error]int{
			errors.New("dummy error"): ExitCodeMainError,
			nil:                       0,
		}

		for err, expectedCode := range testCases {
			out.Reset()
			actualExitCode = handleExitError(&out, err)

			assert.Equalf(
				t, expectedCode, actualExitCode,
				"Expect handleExitError(%v) = %d, actual: %d",
				err, expectedCode, actualExitCode,
			)
			if err == nil {
				assert.Empty(t, out.String(), "Error is not empty")
			} else {
				assert.Contains(t, out.String(), err.Error(), "error output hasn't error description")
			}
		}
	})
}
