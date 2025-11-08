// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc, alex@blockwatch.cc
//
// Benchmark1 test the write throughput for knoxdb
package benchmarks

import (
	"context"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/tests"
	etests "blockwatch.cc/knoxdb/internal/tests/engine"
	"blockwatch.cc/knoxdb/pkg/knox"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

type Account struct {
	Id        uint64    `knox:"id,pk"`
	FirstSeen time.Time `knox:"first_seen"`
	Balance   int64     `knox:"balance"`
}

func genAccounts(n int) []*Account {
	accounts := make([]*Account, 0, n)
	for i := range n {
		accounts = append(accounts, &Account{
			Balance:   int64(1000 + 1 + i),
			FirstSeen: time.Unix(int64(1+i), 0),
		})
	}
	return accounts
}

func BenchmarkInsertBulk(b *testing.B) {
	for _, sz := range tests.BenchmarkSizes {
		log.SetLevel(log.LevelOff)
		eng, cleanup := etests.NewDatabase(b, &Account{})
		db := knox.WrapEngine(eng)
		table, err := db.FindTable("account")
		require.NoError(b, err, "Missing table")
		data := genAccounts(sz.N)

		b.Run(sz.Name, func(b *testing.B) {
			var (
				nrec int
				ntx  int
			)
			for b.Loop() {
				ctx, commit, _, err := db.Begin(context.Background(), knox.TxFlagNoWal)
				require.NoError(b, err, "begin")
				_, n, err := table.Insert(ctx, data)
				require.NoError(b, err, "insert")
				require.NoError(b, commit())
				nrec += n
				ntx++
			}
			b.ReportAllocs()
			b.ReportMetric(float64(nrec)/float64(b.Elapsed().Seconds()), "rec/s")
			b.ReportMetric(float64(ntx)/float64(b.Elapsed().Seconds()), "tx/s")
		})
		cleanup()
	}
}
