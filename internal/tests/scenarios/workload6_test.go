// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc
//
// TestWorkload6 test the write throughput for knoxdb
package scenarios

import (
	"context"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"

	tests "blockwatch.cc/knoxdb/internal/tests/engine"
	"blockwatch.cc/knoxdb/pkg/knox"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const (
	account_limit = 1024
	balance       = 1000
)

type Account struct {
	Id        uint64    `knox:"id,pk"`
	FirstSeen time.Time `knox:"first_seen"`
	Balance   int64     `knox:"balance"`
}

func genAccounts(startid int) []*Account {
	accounts := make([]*Account, 0, account_limit)
	for i := 0; i < account_limit; i++ {
		accounts = append(accounts, &Account{
			Balance:   int64(balance + startid + i),
			FirstSeen: time.Unix(int64(startid+i), 0),
		})
	}
	return accounts
}

func TestWorkload6(t *testing.T) {
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
			t.Logf("error: %v", e)
			debug.PrintStack()
		}
	}()

	var numRecords atomic.Uint64
	var errg errgroup.Group
	errg.SetLimit(32)

	startTime := time.Now()
	startid := 0
	for range 10_000 {
		data := genAccounts(startid)
		startid += len(data)

		errg.Go(func() error {
			ctx := context.Background()
			_, n, err := table.Insert(ctx, data)
			if err != nil {
				return err
			}
			numRecords.Add(uint64(n))
			return nil
		})
	}

	require.NoError(t, errg.Wait(), "table inserts should not fail")
	dur := time.Since(startTime)
	t.Logf("runtime [%s], rate [%f]", dur, float64(startid)/(float64(dur)/float64(time.Second)))
}
