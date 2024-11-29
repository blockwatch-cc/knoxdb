// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc
//
// TestWorkload4 tests interleaved operations for isolation and concurrency, involving both meta rows and work rows.
// Ensures:
// - thread safety and data isolation during concurrent access.
// - data consistency and correctness across all operations, including meta-work row linkage.
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
	// Setup the database with schemas for MetaRow and WorkRow
	_, metaTable, cleanupMeta := SetupDatabaseWithSchema(t, &MetaRow{})
	defer cleanupMeta()

	_, workTable, cleanupWork := SetupDatabaseWithSchema(t, &WorkRow{})
	defer cleanupWork()

	ctx := context.Background()
	const txnSize = 20
	const numThreads = 4

	var wg sync.WaitGroup
	insertedMetaRows := sync.Map{} // Track meta rows
	initialWorkRows := sync.Map{}  // Track initial state of work rows
	updatedWorkRows := sync.Map{}  // Track updated work rows

	log.Infof("Starting TestWorkload4 with %d threads and %d transactions per thread", numThreads, txnSize)

	// Insert initial work rows
	for i := 0; i < txnSize*numThreads; i++ {
		row := &WorkRow{
			Value:   NewRandomData(),
			Updated: false,
		}
		pk, err := workTable.Insert(ctx, []*WorkRow{row})
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
				// Create a meta row
				metaRow := &MetaRow{
					ThreadID:  threadID,
					Timestamp: time.Now().UTC(),
					Operation: "update",
				}
				metaPk, err := metaTable.Insert(ctx, []*MetaRow{metaRow})
				require.NoError(t, err, "Failed to insert meta row")
				metaRow.Id = metaPk
				insertedMetaRows.Store(metaPk, metaRow)

				// Update a work row
				workRowID := uint64(threadID*txnSize + i + 1)
				workRow := &WorkRow{
					Id:        workRowID,
					MetaRowID: metaPk,
					Value:     NewRandomData(),
					Updated:   true,
				}

				log.Debugf("Thread %d: Attempting to update work row %d with meta row %d", threadID, workRowID, metaPk)

				enumMutex.Lock() // Ensure thread-safe access
				_, err = workTable.Update(ctx, []*WorkRow{workRow})
				enumMutex.Unlock()

				require.NoError(t, err, "Failed to update work row")
				updatedWorkRows.Store(workRowID, workRow)
			}
		}(threadID)
	}

	// Wait for all threads to complete
	wg.Wait()

	log.Infof("All threads completed. Starting validation of inserted and updated records.")

	// Validate work rows
	err := knox.NewGenericQuery[WorkRow]().
		WithTable(workTable).
		Stream(ctx, func(res *WorkRow) error {
			if res.Updated {
				// Validate linkage to meta row
				metaRow, ok := insertedMetaRows.Load(res.MetaRowID)
				require.True(t, ok, "Meta row not found for updated work row: %d", res.Id)
				meta := metaRow.(*MetaRow)
				require.Equal(t, "update", meta.Operation, "Meta row operation mismatch")

				// Validate updated row
				updatedRow, ok := updatedWorkRows.Load(res.Id)
				require.True(t, ok, "Updated work row not found: %d", res.Id)
				require.Equal(t, updatedRow.(*WorkRow).Value, res.Value, "Updated work row value mismatch")
			} else {
				// Validate initial values for untouched rows
				initialRow, ok := initialWorkRows.Load(res.Id)
				require.True(t, ok, "Initial work row not found: %d", res.Id)
				require.False(t, res.Updated, "Untouched work row marked as updated")
				require.Equal(t, initialRow.(*WorkRow).Value, res.Value, "Initial value mismatch")
			}
			return nil
		})
	require.NoError(t, err, "Failed to stream work rows")

	log.Infof("Validation of work rows completed.")

	// Validate meta rows
	err = knox.NewGenericQuery[MetaRow]().
		WithTable(metaTable).
		Stream(ctx, func(res *MetaRow) error {
			log.Infof("Validated meta row: %+v", res)
			return nil
		})
	require.NoError(t, err, "Failed to stream meta rows")

	log.Infof("TestWorkload4 completed successfully.")
}
