// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc, alex@blockwatch.cc
//
// Benchmark2 test the transaction throughput for knoxdb
package benchmarks

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	tests "blockwatch.cc/knoxdb/internal/tests/engine"
	"blockwatch.cc/knoxdb/pkg/knox"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

func BenchmarkInsertSequential(b *testing.B) {
	log.SetLevel(log.LevelOff)
	eng, cleanup := tests.NewDatabase(b, &Account{})
	db := knox.WrapEngine(eng)
	table, err := db.FindTable("account")
	require.NoError(b, err, "Missing table")

	var (
		nrec int
		ntx  int
	)

	for b.Loop() {
		ctx, commit, _, err := db.Begin(context.Background(), knox.TxFlagNoWal)
		if err != nil {
			b.Fatalf("begin: %v", err)
		}
		data := &Account{
			Balance:   int64(1000 + b.N),
			FirstSeen: time.Unix(int64(b.N), 0),
		}
		_, n, err := table.Insert(ctx, data)
		if err != nil {
			b.Fatalf("insert: %v", err)
		}
		err = commit()
		if err != nil {
			b.Fatalf("commit: %v", err)
		}
		nrec += n
		ntx++
	}
	b.ReportAllocs()
	b.ReportMetric(float64(nrec)/float64(b.Elapsed().Seconds()), "rec/s")
	b.ReportMetric(float64(ntx)/float64(b.Elapsed().Seconds()), "tx/s")
	cleanup()
}

func BenchmarkInsertParallel(b *testing.B) {
	log.SetLevel(log.LevelOff)
	eng, cleanup := tests.NewDatabase(b, &Account{})
	db := knox.WrapEngine(eng)
	table, err := db.FindTable("account")
	require.NoError(b, err, "Missing table")

	var (
		nrec atomic.Uint64
		ntx  atomic.Uint64
	)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctx, commit, _, err := db.Begin(context.Background(), knox.TxFlagNoWal)
			if err != nil {
				b.Fatalf("begin: %v", err)
			}
			data := &Account{
				Balance:   int64(1000 + b.N),
				FirstSeen: time.Unix(int64(b.N), 0),
			}
			_, n, err := table.Insert(ctx, data)
			if err != nil {
				b.Fatalf("insert: %v", err)
			}
			err = commit()
			if err != nil {
				b.Fatalf("commit: %v", err)
			}
			nrec.Add(uint64(n))
			ntx.Add(1)
		}
	})

	b.ReportAllocs()
	b.ReportMetric(float64(nrec.Load())/float64(b.Elapsed().Seconds()), "rec/s")
	b.ReportMetric(float64(ntx.Load())/float64(b.Elapsed().Seconds()), "tx/s")
	cleanup()
}
