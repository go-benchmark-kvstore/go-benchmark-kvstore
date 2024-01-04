package main

import (
	"github.com/rs/zerolog"
)

type loggerWrapper struct {
	zerolog.Logger
}

func (loggerWrapper) Close() error {
	return nil
}

func (l loggerWrapper) Debugf(msg string, args ...interface{}) {
	l.Logger.Debug().Msgf(msg, args...)
}

func (l loggerWrapper) Errorf(msg string, args ...interface{}) {
	l.Logger.Error().Msgf(msg, args...)
}

func (l loggerWrapper) Infof(msg string, args ...interface{}) {
	l.Logger.Info().Msgf(msg, args...)
}

func (l loggerWrapper) Warningf(msg string, args ...interface{}) {
	l.Logger.Warn().Msgf(msg, args...)
}

func (l loggerWrapper) Fatalf(msg string, args ...interface{}) {
	l.Logger.Fatal().Msgf(msg, args...)
}
