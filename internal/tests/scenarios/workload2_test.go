// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc
//
// Workload2 tests KnoxDB's handling of parallel transactions across multiple threads.
// Ensures:
// - no data loss or corruption across threads.
// - total row count and content correctness are verified post-insertion.

package scenarios

import (
	"context"
	"sync"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/knox"
	"github.com/stretchr/testify/require"
)

func TestWorkload2(t *testing.T) {
	_, table, cleanup := SetupDatabase(t)
	defer cleanup()

	ctx := context.Background()
	const txnSize = 50
	const numThreads = 4

	var wg sync.WaitGroup
	insertedData := sync.Map{}

	// Concurrent inserts
	for threadID := 0; threadID < numThreads; threadID++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			for i := 0; i < txnSize; i++ {
				record := NewRandomTypes(threadID*txnSize + i)
				pk, err := table.Insert(ctx, []*Types{record})
				require.NoError(t, err, "Failed to insert data")
				record.Id = pk
				insertedData.Store(record.Id, record)
			}
		}(threadID)
	}

	// Wait for threads to finish
	require.Eventually(t, func() bool {
		wg.Wait()
		return true
	}, 10*time.Second, 100*time.Millisecond, "Deadlock detected: threads did not complete")

	// Validate all rows are inserted correctly
	count := 0
	err := knox.NewGenericQuery[Types]().
		WithTable(table).
		Stream(ctx, func(res *Types) error {
			val, ok := insertedData.Load(res.Id)
			require.True(t, ok, "Missing record for Id: %d", res.Id)
			expected := val.(*Types)
			require.Equal(t, expected.Int64, res.Int64)
			require.Equal(t, expected.MyEnum, res.MyEnum)
			count++
			return nil
		})
	require.NoError(t, err, "Failed to stream data")
	require.Equal(t, txnSize*numThreads, count, "Row count mismatch")
}
