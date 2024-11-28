// Copyright (c) 2018 - 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package badger

import (
	logpkg "github.com/echa/log"
)

var log = logpkg.Disabled

// useLogger is the callback provided during driver registration that sets the
// current logger to the provided one.
func useLogger(logger logpkg.Logger) {
	log = logger
}

// Logger is a log wrapper compatible with badger's log interface
type Logger struct {
	logpkg.Logger
}

// Default logger hides badger info logs
func NewLogger() Logger {
	return Logger{log.Clone().WithTag("badger").SetLevel(logpkg.LevelWarn)}
}

func (l Logger) Warningf(f string, args ...any) {
	l.Logger.Warnf(f, args...)
}
