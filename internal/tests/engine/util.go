// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine_tests

import (
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	DeadlockTimeout = 2 * time.Second
	DeadlockPoll    = 10 * time.Millisecond
)

// NoDeadlock runs fn until success or DeadlockTimeout and on failure
// prints a stack trace of all active goroutines.
func NoDeadlock(t *testing.T, fn func() bool, args ...any) {
	t.Helper()
	ok := assert.Eventually(t, fn, DeadlockTimeout, DeadlockPoll, args...)
	if !ok {
		t.Fail()
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	}
}
