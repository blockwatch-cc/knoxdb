// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc
//
// TestWorkload2 tests KnoxDB's handling of parallel transactions across multiple threads.
// Ensures:
// - no data loss, corruption, or race conditions across threads.
// - total row count and content correctness are verified post-insertion.

package scenarios

import (
	"context"
	"sync"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/knox"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

func TestWorkload2(t *testing.T) {
	eng, cleanup := tests.NewDatabase(t, &tests.Types{})
	defer cleanup()
	db := knox.WrapEngine(eng)
	table, err := db.UseTable("types")
	require.NoError(t, err, "Missing table")

	ctx := context.Background()
	const txnSize = 50
	const numThreads = 4

	var wg sync.WaitGroup
	insertedData := sync.Map{}

	// Concurrent inserts
	for threadID := 0; threadID < numThreads; threadID++ {
		wg.Add(1)
		go func(threadID int) {
			start := time.Now()
			defer func() {
				log.Infof("Goroutine %d completed in %s", threadID, time.Since(start))
				wg.Done()
			}()
			for i := 0; i < txnSize; i++ {
				record := tests.NewRandomTypes(threadID*txnSize + i)
				pk, err := table.Insert(ctx, []*tests.Types{record})
				require.NoError(t, err, "Failed to insert data")
				record.Id = pk
				insertedData.Store(record.Id, record)
			}
		}(threadID)
	}

	// Wait for threads to finish
	wg.Wait()

	// Validate all rows are inserted correctly
	count := 0
	err = knox.NewGenericQuery[tests.Types]().
		WithTable(table).
		Stream(ctx, func(res *tests.Types) error {
			val, ok := insertedData.Load(res.Id)
			require.True(t, ok, "Missing record for Id: %d", res.Id)
			expected := val.(*tests.Types)
			require.Equal(t, expected.Int64, res.Int64)
			require.Equal(t, expected.MyEnum, res.MyEnum)
			count++
			return nil
		})
	require.NoError(t, err, "Failed to stream data")
	require.Equal(t, txnSize*numThreads, count, "Row count mismatch")
}
