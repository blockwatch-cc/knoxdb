// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc
//
// Workload4 tests interleaved operations for isolation and concurrency.
// Ensures:
// - thread safety and data isolation during concurrent access.
// - data consistency and correctness across all operations.
// - no deadlocks or livelocks occur.

package scenarios

import (
	"context"
	"sync"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/knox"
	"github.com/echa/log" // Fix: Added missing import
	"github.com/stretchr/testify/require"
)

func TestWorkload4(t *testing.T) {
	_, table, cleanup := SetupDatabase(t)
	defer cleanup()

	ctx := context.Background()
	const txnSize = 20
	const numThreads = 4

	var wg sync.WaitGroup
	insertedData := sync.Map{}

	// Multi-threaded interleaved operations
	for threadID := 0; threadID < numThreads; threadID++ {
		wg.Add(1)
		go func(threadID int) {
			start := time.Now() // Measure start time for the goroutine
			defer func() {
				log.Infof("Goroutine %d completed in %s", threadID, time.Since(start))
				wg.Done()
			}()
			for i := 0; i < txnSize; i++ {
				time.Sleep(1 * time.Millisecond) // Simulate delay for each operation
				record := NewRandomTypes(threadID*txnSize + i)
				pk, err := table.Insert(ctx, []*Types{record})
				require.NoError(t, err, "Failed to insert data")
				require.NotEmpty(t, record.MyEnum, "Enum field is empty for record")
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

	// Validate inserted records
	count := 0
	err := knox.NewGenericQuery[Types]().
		WithTable(table).
		Stream(ctx, func(res *Types) error {
			val, ok := insertedData.Load(res.Id)
			require.True(t, ok, "Missing record for Id: %d", res.Id)
			require.NotEmpty(t, res.MyEnum, "Streamed record has empty MyEnum field")
			log.Infof("Streamed record: ID=%d, MyEnum=%s", res.Id, res.MyEnum)
			expected := val.(*Types)
			require.Equal(t, expected.Int64, res.Int64)
			require.Equal(t, expected.MyEnum, res.MyEnum)
			count++
			return nil
		})
	require.NoError(t, err, "Failed to stream data")
	require.Equal(t, txnSize*numThreads, count, "Row count mismatch")
}
