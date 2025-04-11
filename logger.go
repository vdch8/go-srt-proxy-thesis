package main

import (
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

var log = logrus.New()

const (
	logTimestampFormat = "2006-01-02 15:04:05.000"
)

func SetupLogger(levelStr string) {
	log.SetOutput(os.Stderr)

	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: logTimestampFormat,
		PadLevelText:    true,
	})

	logLevel := logrus.InfoLevel
	effectiveLevelStr := logLevel.String()

	if levelStr != "" {
		parsedLevel, err := logrus.ParseLevel(strings.ToLower(levelStr))
		if err != nil {
			log.Warnf("Invalid log_level '%s' provided: %v. Using default level '%s'.",
				levelStr, err, effectiveLevelStr)
		} else {
			logLevel = parsedLevel
			effectiveLevelStr = logLevel.String()
			log.Infof("Log level set to '%s' based on configuration.", effectiveLevelStr)
		}
	} else {
		log.Infof("No log_level specified. Using default level '%s'.", effectiveLevelStr)
	}

	log.SetLevel(logLevel)

	log.Debugf("Logger initialization complete. Effective log level: %s", log.GetLevel().String())
}
