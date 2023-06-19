package main

import (
	"errors"
	"fmt"
	"github.com/joho/godotenv"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	dekanatDbDriverName          string
	kafkaHost                    string
	primaryDekanatDbDSN          string
	primaryDekanatPingAttempts   uint8
	primaryDekanatPingDelay      time.Duration
	primaryDekanatReconnectDelay time.Duration
	kafkaTimeout                 time.Duration
	kafkaAttempts                int
	sqsQueueUrl                  string
	storageDir                   string
}

func loadConfig(envFilename string) (Config, error) {
	if envFilename != "" {
		err := godotenv.Load(envFilename)
		if err != nil {
			return Config{}, errors.New(fmt.Sprintf("Error loading %s file: %s", envFilename, err))
		}
	}

	kafkaTimeout, err := strconv.Atoi(os.Getenv("KAFKA_TIMEOUT"))
	if kafkaTimeout == 0 || err != nil {
		kafkaTimeout = 10
	}

	kafkaAttempts, err := strconv.Atoi(os.Getenv("KAFKA_ATTEMPTS"))
	if kafkaAttempts == 0 || err != nil {
		kafkaAttempts = 0
	}

	primaryDekanatPingAttempts, err := strconv.ParseUint(os.Getenv("PRIMARY_DEKANAT_PING_ATTEMPTS"), 10, 8)
	if err != nil && primaryDekanatPingAttempts == 0 {
		primaryDekanatPingAttempts = 10
	}

	config := Config{
		dekanatDbDriverName:          os.Getenv("DEKANAT_DB_DRIVER_NAME"),
		primaryDekanatDbDSN:          os.Getenv("PRIMARY_DEKANAT_DB_DSN"),
		primaryDekanatPingAttempts:   uint8(primaryDekanatPingAttempts),
		kafkaHost:                    os.Getenv("KAFKA_HOST"),
		kafkaTimeout:                 time.Second * time.Duration(kafkaTimeout),
		kafkaAttempts:                kafkaAttempts,
		sqsQueueUrl:                  os.Getenv("AWS_SQS_QUEUE_URL"),
		storageDir:                   os.Getenv("STORAGE_DIR"),
		primaryDekanatPingDelay:      time.Millisecond * 100,
		primaryDekanatReconnectDelay: time.Second,
	}

	config.storageDir = strings.TrimRight(config.storageDir, "/") + "/"

	if config.dekanatDbDriverName == "" {
		config.dekanatDbDriverName = "firebirdsql"
	}

	if config.primaryDekanatDbDSN == "" {
		return Config{}, errors.New("empty PRIMARY_DEKANAT_DB_DSN")
	}

	if config.kafkaHost == "" {
		return Config{}, errors.New("empty KAFKA_HOST")
	}

	if config.storageDir == "" {
		config.storageDir = "/tmp/"
	}

	return config, nil
}
