package main

import (
	"os"
	"strings"

	"github.com/rs/zerolog"
)

type loggerWrapper struct {
	zerolog.Logger
}

func (loggerWrapper) Close() error {
	return nil
}

func (l loggerWrapper) log(level zerolog.Level, msg string, args ...interface{}) {
	if len(args) > 0 {
		l.Logger.WithLevel(level).Msgf(strings.TrimRight(msg, "\n"), args...)
		return
	}
	lines := strings.Split(msg, "\n")
	for _, line := range lines {
		if len(line) > 0 {
			l.Logger.WithLevel(level).Msg(line)
		}
	}
}

func (l loggerWrapper) Debugf(msg string, args ...interface{}) {
	l.log(zerolog.DebugLevel, msg, args...)
}

func (l loggerWrapper) Errorf(msg string, args ...interface{}) {
	l.log(zerolog.ErrorLevel, msg, args...)
}

func (l loggerWrapper) Infof(msg string, args ...interface{}) {
	l.log(zerolog.InfoLevel, msg, args...)
}

func (l loggerWrapper) Warningf(msg string, args ...interface{}) {
	l.log(zerolog.WarnLevel, msg, args...)
}

func (l loggerWrapper) Fatalf(msg string, args ...interface{}) {
	l.log(zerolog.FatalLevel, msg, args...)
	os.Exit(1)
}
