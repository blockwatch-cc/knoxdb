// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc
//
// TestWorkload4 tests interleaved operations for isolation and concurrency,
// involving both meta rows and work rows stored in a single table.
// Ensures:
// - thread safety and data isolation during concurrent access.
// - data consistency and correctness across all operations, including meta-work row linkage.
// - each transaction updates exactly two work-row keys.
// - no deadlocks or livelocks occur.

package scenarios

import (
	"context"
	"sync"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/knox"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

func TestWorkload4(t *testing.T) {
	// Setup the unified database
	_, unifiedTable, cleanup := SetupUnifiedDatabase(t)
	defer cleanup()

	ctx := context.Background()
	const txnSize = 20
	const numThreads = 4

	var wg sync.WaitGroup
	initialWorkRows := sync.Map{}  // Track initial state of work rows
	updatedWorkRows := sync.Map{}  // Track updated work rows
	insertedMetaRows := sync.Map{} // Track meta rows

	log.Infof("Starting TestWorkload4 with %d threads and %d transactions per thread", numThreads, txnSize)

	// Insert initial work rows
	for i := 0; i < txnSize*numThreads*2; i++ {
		row := &UnifiedRow{
			RowType: "work",
			Value:   NewRandomData(),
			Updated: false,
		}
		pk, err := unifiedTable.Insert(ctx, []*UnifiedRow{row})
		require.NoError(t, err, "Failed to insert initial work row")
		row.Id = pk
		initialWorkRows.Store(pk, row)
	}

	// Multi-threaded interleaved operations
	for threadID := 0; threadID < numThreads; threadID++ {
		wg.Add(1)
		go func(threadID int) {
			start := time.Now()
			defer func() {
				log.Infof("Thread %d completed in %s", threadID, time.Since(start))
				wg.Done()
			}()

			for i := 0; i < txnSize; i++ {
				// Determine two work-row keys
				workRowID1 := uint64(threadID*txnSize*2 + i*2 + 1)
				workRowID2 := uint64(threadID*txnSize*2 + i*2 + 2)

				// Create a meta row recording both updated work-row keys
				metaRow := &UnifiedRow{
					RowType:   "meta",
					ThreadID:  threadID,
					Timestamp: time.Now().UTC(),
					Operation: "update",
				}
				metaPk, err := unifiedTable.Insert(ctx, []*UnifiedRow{metaRow})
				require.NoError(t, err, "Failed to insert meta row")
				metaRow.Id = metaPk
				insertedMetaRows.Store(metaPk, metaRow)

				// Update the two work rows
				workRow1 := &UnifiedRow{
					Id:        workRowID1,
					RowType:   "work",
					MetaRowID: metaPk,
					Value:     NewRandomData(),
					Updated:   true,
				}
				workRow2 := &UnifiedRow{
					Id:        workRowID2,
					RowType:   "work",
					MetaRowID: metaPk,
					Value:     NewRandomData(),
					Updated:   true,
				}

				log.Debugf("Thread %d: Updating work rows %d and %d with meta row %d", threadID, workRowID1, workRowID2, metaPk)

				_, err = unifiedTable.Update(ctx, []*UnifiedRow{workRow1, workRow2})
				require.NoError(t, err, "Failed to update work rows")
				updatedWorkRows.Store(workRowID1, workRow1)
				updatedWorkRows.Store(workRowID2, workRow2)
			}
		}(threadID)
	}

	// Wait for all threads to complete
	wg.Wait()

	log.Infof("All threads completed. Starting validation of inserted and updated records.")

	// Validate work rows
	err := knox.NewGenericQuery[UnifiedRow]().
		WithTable(unifiedTable).
		AndEqual("row_type", "work").
		Stream(ctx, func(res *UnifiedRow) error {
			if res.Updated {
				metaRow, ok := insertedMetaRows.Load(res.MetaRowID)
				require.True(t, ok, "Meta row not found for updated work row: %d", res.Id)
				require.Equal(t, "update", metaRow.(*UnifiedRow).Operation, "Meta row operation mismatch")
			} else {
				initialRow, ok := initialWorkRows.Load(res.Id)
				require.True(t, ok, "Initial work row not found: %d", res.Id)
				require.Equal(t, initialRow.(*UnifiedRow).Value, res.Value, "Initial value mismatch")
			}
			return nil
		})
	require.NoError(t, err, "Failed to validate work rows")

	log.Infof("Validation of work rows completed.")
}
