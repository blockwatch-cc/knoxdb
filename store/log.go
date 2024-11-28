// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package store

import (
	logpkg "github.com/echa/log"
)

// log is a logger that is initialized with no output filters.  This
// means the package will not perform any logging by default until the caller
// requests it.
var log logpkg.Logger

// The default amount of logging is none.
func init() {
	DisableLog()
}

// DisableLog disables all library log output.  Logging output is disabled
// by default until UseLogger is called.
func DisableLog() {
	log = logpkg.Disabled
}

// UseLogger uses a specified Logger to output package logging info.
func UseLogger(logger logpkg.Logger) {
	log = logger

	// Update the logger for the registered drivers.
	for _, drv := range drivers {
		if drv.UseLogger != nil {
			drv.UseLogger(logger)
		}
	}
}
