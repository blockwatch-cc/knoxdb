// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc, alex@blockwatch.cc
//
// Benchmark1 tests bulk write throughput (max rec/s)
package benchmarks

import (
	"context"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/tests"
	etests "blockwatch.cc/knoxdb/internal/tests/engine"
	"blockwatch.cc/knoxdb/internal/tests/testutil"
	"blockwatch.cc/knoxdb/pkg/knox"
	"blockwatch.cc/knoxdb/pkg/schema/encode"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

type Account struct {
	Id        uint64    `knox:"id,pk"`
	FirstSeen time.Time `knox:"first_seen"`
	Balance   int64     `knox:"balance"`
}

func genAccounts(n, k int) []*Account {
	accounts := make([]*Account, 0, n)
	now := time.Now().UTC()
	for i := range n {
		accounts = append(accounts, &Account{
			Balance:   int64(testutil.RandIntn(1000 + k + i)),
			FirstSeen: now.Add(time.Second * time.Duration(k+i)),
		})
	}
	return accounts
}

func BenchmarkInsertBulk(b *testing.B) {
	for _, sz := range tests.BenchmarkSizes {
		b.Run(sz.Name, func(b *testing.B) {
			var (
				nrec int
				ntx  int
			)

			// log.SetLevel(log.LevelOff)
			eng, cleanup := etests.NewDatabase(b, &Account{},
				engine.WithNoSync(true),
			)
			defer cleanup()
			db := knox.WrapEngine(eng)
			table, err := knox.FindTableFor[Account](db, "account")
			require.NoError(b, err, "Missing table")
			data := genAccounts(sz.N, 1)

			for b.Loop() {
				ctx, commit, _, err := db.Begin(context.Background(), knox.TxFlagNoWal)
				if err != nil {
					b.Fatalf("begin: %v", err)
				}
				_, n, err := table.Insert(ctx, data...)
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
		})
	}
}

func BenchmarkInsertBatch(b *testing.B) {
	for _, sz := range tests.BenchmarkSizes {
		b.Run(sz.Name, func(b *testing.B) {
			var (
				nrec int
				ntx  int
			)

			log.SetLevel(log.LevelOff)
			eng, cleanup := etests.NewDatabase(b, &Account{},
				// engine.WithDriverType("mem"),
				// engine.WithJournalSegments(2),
				engine.WithNoSync(true),
				// engine.WithPageSize(1<<14),
			)

			defer cleanup()
			db := knox.WrapEngine(eng)
			table, err := knox.FindTableFor[Account](db, "account")
			require.NoError(b, err, "Missing table")
			data := genAccounts(sz.N, 1)
			enc := encode.NewEncoderFor[Account]()
			wr := enc.NewBatchWriter(sz.N)
			require.NoError(b, enc.EncodeBatch(wr.Buffer(), data))

			for b.Loop() {
				ctx, commit, _, err := db.Begin(context.Background(), knox.TxFlagNoWal)
				if err != nil {
					b.Fatalf("begin: %v", err)
				}
				_, n, err := table.Table().Insert(ctx, wr.Batch())
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
		})
	}
}
