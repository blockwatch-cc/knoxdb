// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// Benchmark3 test read throughput (tx/s)
package benchmarks

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/tests"
	etests "blockwatch.cc/knoxdb/internal/tests/engine"
	"blockwatch.cc/knoxdb/pkg/knox"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

func BenchmarkQuerySequential(b *testing.B) {
	for _, sz := range tests.BenchmarkSizes {
		log.SetLevel(log.LevelOff)
		eng, cleanup := etests.NewDatabase(b, &Account{})
		db := knox.WrapEngine(eng)
		table, err := db.FindTable("account")
		require.NoError(b, err, "Missing table")

		// write 128x N records (128k .. 8M)
		for k := range 128 {
			data := genAccounts(sz.N, k)
			ctx, commit, _, err := db.Begin(context.Background(), knox.TxFlagNoWal)
			if err != nil {
				b.Fatalf("begin: %v", err)
			}
			_, _, err = table.Insert(ctx, data)
			if err != nil {
				b.Fatalf("begin: %v", err)
			}
			err = commit()
			if err != nil {
				b.Fatalf("commit: %v", err)
			}
		}

		// wait a bit until data is merged
		time.Sleep(2 * time.Second)

		// run benchmark
		b.Run(sz.Name, func(b *testing.B) {
			var (
				nrec int
				ntx  int
			)

			for b.Loop() {
				res, err := knox.NewQuery().
					WithTable(table).
					WithTag("bench").
					WithLimit(1).
					// WithDebug(true).
					AndEqual("balance", util.RandInt64n(int64(sz.N))).
					Run(context.Background())
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				nrec += res.Len()
				ntx++
				res.Close()
			}
			b.ReportAllocs()
			b.ReportMetric(float64(nrec)/float64(b.Elapsed().Seconds()), "rec/s")
			b.ReportMetric(float64(ntx)/float64(b.Elapsed().Seconds()), "tx/s")
		})
		cleanup()
	}
}

func BenchmarkQueryParallel(b *testing.B) {
	for _, sz := range tests.BenchmarkSizes {
		log.SetLevel(log.LevelOff)
		eng, cleanup := etests.NewDatabase(b, &Account{})
		db := knox.WrapEngine(eng)
		table, err := db.FindTable("account")
		require.NoError(b, err, "Missing table")

		// write 128x N records (128k .. 8M)
		for k := range 128 {
			data := genAccounts(sz.N, k)
			ctx, commit, _, err := db.Begin(context.Background(), knox.TxFlagNoWal)
			if err != nil {
				b.Fatalf("begin: %v", err)
			}
			_, _, err = table.Insert(ctx, data)
			if err != nil {
				b.Fatalf("begin: %v", err)
			}
			err = commit()
			if err != nil {
				b.Fatalf("commit: %v", err)
			}
		}
		// wait a bit until data is merged
		time.Sleep(2 * time.Second)

		// run benchmark
		b.Run(sz.Name, func(b *testing.B) {
			var (
				nrec atomic.Uint64
				ntx  atomic.Uint64
			)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					res, err := knox.NewQuery().
						WithTable(table).
						WithTag("bench").
						WithLimit(1).
						AndEqual("balance", util.RandInt64n(int64(sz.N))).
						Run(context.Background())
					if err != nil {
						b.Fatalf("query: %v", err)
					}
					nrec.Add(uint64(res.Len()))
					ntx.Add(1)
					res.Close()
				}
			})

			b.ReportAllocs()
			b.ReportMetric(float64(nrec.Load())/float64(b.Elapsed().Seconds()), "rec/s")
			b.ReportMetric(float64(ntx.Load())/float64(b.Elapsed().Seconds()), "tx/s")
		})
		cleanup()
	}
}
