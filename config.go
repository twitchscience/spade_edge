package main

import (
	"encoding/json"
	"os"

	"github.com/twitchscience/spade_edge/loggers"
)

var config struct {
	StatsdPrefix   string
	LoggingDir     string
	Port           string
	CorsOrigins    []string
	EventsLogger   *loggers.S3LoggerConfig
	AuditsLogger   *loggers.S3LoggerConfig
	FallbackLogger *loggers.S3LoggerConfig
	EventStream    *loggers.KinesisLoggerConfig
}

func loadConfig(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}

	p := json.NewDecoder(f)
	err = p.Decode(&config)
	if err != nil {
		return err
	}

	return nil
}
