// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc
//
// TestWorkload6 test the write throughput for knoxdb
package scenarios

import (
	"context"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tests "blockwatch.cc/knoxdb/internal/tests/engine"
	"blockwatch.cc/knoxdb/pkg/knox"
	"github.com/stretchr/testify/require"
)

func TestWorkload7Seq(t *testing.T) {
	// setup deterministic seed
	SetupDeterministicRand(t)

	eng, cleanup := tests.NewTempDatabase(t, &Account{})
	t.Cleanup(func() {
		cleanup()
		tests.SaveDatabaseFiles(t, eng)
	})
	db := knox.WrapEngine(eng)
	table, err := db.FindTable("account")
	require.NoError(t, err, "Missing table")

	defer func() {
		if e := recover(); e != nil {
			t.Fatalf("error: %v", e)
			debug.PrintStack()
		}
	}()

	var numRecords int64
	startTime := time.Now()

	for i := range 10_000_000 {
		data := &Account{
			Balance:   int64(balance + i + i),
			FirstSeen: time.Unix(int64(i), 0),
		}
		ctx := context.Background()
		_, _, err2 := table.Insert(ctx, data)
		if err != nil {
			t.Fatalf("error: %v", err2)
		}
		numRecords++
	}

	dur := time.Since(startTime)
	t.Logf("runtime [%s], rate [%f]", dur, float64(numRecords)/(float64(dur)/float64(time.Second)))
}

func TestWorkload7Conc(t *testing.T) {
	// setup deterministic seed
	SetupDeterministicRand(t)

	eng, cleanup := tests.NewTempDatabase(t, &Account{})
	t.Cleanup(func() {
		cleanup()
		tests.SaveDatabaseFiles(t, eng)
	})
	db := knox.WrapEngine(eng)
	table, err := db.FindTable("account")
	require.NoError(t, err, "Missing table")

	defer func() {
		if e := recover(); e != nil {
			t.Fatalf("error: %v", e)
			debug.PrintStack()
		}
	}()

	errch := make(chan error)
	var wg sync.WaitGroup
	var numRecords atomic.Uint64

	startTime := time.Now()
	for i := range 10_000_000 {
		data := &Account{
			Balance:   int64(balance + i + i),
			FirstSeen: time.Unix(int64(i), 0),
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			_, _, err2 := table.Insert(ctx, data)
			if err != nil {
				errch <- err2
			}
			numRecords.Add(1)
		}()
	}

	wg.Wait()
	dur := time.Since(startTime)
	t.Logf("runtime [%s], rate [%f]", dur, float64(numRecords.Load())/(float64(dur)/float64(time.Second)))

	select {
	case err := <-errch:
		t.Logf("error: %v", err)
	default:
	}
}
