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

	tests "blockwatch.cc/knoxdb/internal/tests/engine"
	"blockwatch.cc/knoxdb/pkg/knox"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

type RowType byte

const (
	RowTypeWork RowType = iota
	RowTypeMeta
)

// UnifiedRow defines the schema for meta and work rows combined.
type UnifiedRow struct {
	Id        uint64    `knox:"id,pk"`
	RowType   RowType   `knox:"row_type"`   // "meta" or "work"
	ThreadID  int       `knox:"thread_id"`  // For meta rows + work rows
	TxId      int       `knox:"tx_id"`      // For meta rows + work rows
	Timestamp time.Time `knox:"timestamp"`  // For meta rows
	WorkRow1  uint64    `knox:"work_row_1"` // For meta rows
	WorkRow2  uint64    `knox:"work_row_2"` // For meta rows
}

func TestWorkload4(t *testing.T) {
	// setup determinism
	SetupDeterministicRand(t)

	// Setup the unified database
	eng, cleanup := tests.NewDatabase(t, &UnifiedRow{})
	t.Cleanup(func() {
		cleanup()
		tests.SaveDatabaseFiles(t, eng)
	})
	db := knox.WrapEngine(eng)
	table, err := db.FindTable("unified_row")
	require.NoError(t, err, "Missing table")

	ctx := context.Background()
	const txnSize = 20
	const numThreads = 4
	const numWorkRows = 16

	var wg sync.WaitGroup
	threadMaps := make([]sync.Map, numThreads)

	log.Infof("Starting TestWorkload4 with %d threads and %d transactions per thread", numThreads, txnSize)

	// Insert initial work rows
	initRows := make([]*UnifiedRow, 0)
	for i := 0; i < numWorkRows; i++ {
		row := &UnifiedRow{
			RowType:  RowTypeWork,
			ThreadID: 0,
			TxId:     0,
		}
		initRows = append(initRows, row)
	}
	_, _, err = table.Insert(ctx, initRows)
	require.NoError(t, err, "Failed to insert work rows")

	// Multi-threaded interleaved operations
	for threadID := 1; threadID <= numThreads; threadID++ {
		threadMap := &threadMaps[threadID-1]
		wg.Add(1)
		go func(threadID int, thMap *sync.Map) {
			start := time.Now()
			defer func() {
				log.Infof("Thread %d completed in %s", threadID, time.Since(start))
				wg.Done()
			}()

			for i := 1; i <= txnSize; i++ {
				func(txId int) {
					// Determine two work-row keys
					workRowID1 := util.RandUint64n(numWorkRows) + 1
					workRowID2 := util.RandUint64n(numWorkRows) + 1

					ctx, commit, abort, err := db.Begin(ctx)
					require.NoError(t, err, "Begin tx failed")
					defer abort()

					// Update the two work rows
					workRow1 := &UnifiedRow{
						Id:       workRowID1,
						RowType:  RowTypeWork,
						ThreadID: threadID,
						TxId:     txId,
					}
					workRow2 := &UnifiedRow{
						Id:       workRowID2,
						RowType:  RowTypeWork,
						ThreadID: threadID,
						TxId:     txId,
					}

					// log.Debugf("Thread %d: Updating work rows %d and %d in tx %d", threadID, workRowID1, workRowID2, txId)

					_, err = table.Update(ctx, []*UnifiedRow{workRow1, workRow2})
					require.NoError(t, err, "Failed to update work rows")

					// Create a meta row recording both updated work-row keys
					metaRow := &UnifiedRow{
						RowType:   RowTypeMeta,
						ThreadID:  threadID,
						TxId:      txId,
						Timestamp: time.Now().UTC(),
						WorkRow1:  workRowID1,
						WorkRow2:  workRowID2,
					}

					t.Logf("Writing meta row TH-%d-TXN-%d", metaRow.ThreadID, metaRow.TxId)
					_, _, err = table.Insert(ctx, []*UnifiedRow{metaRow})
					require.NoError(t, err, "Failed to insert meta row")

					require.NoError(t, commit(), "Commit failed")

					thMap.Store(metaRow.Id, time.Now().UTC())
				}(i)
			}
		}(threadID, threadMap)
	}

	// Wait for all threads to complete
	wg.Wait()

	log.Infof("All threads completed. Starting validation of inserted and updated records.")

	// 1 Validate number of work rows
	var workRows []*UnifiedRow
	_, err = knox.NewGenericQuery[UnifiedRow]().
		WithTable(table).
		AndEqual("row_type", RowTypeWork).
		Execute(ctx, &workRows)
	require.NoError(t, err, "Failed to validate work rows")
	require.Len(t, workRows, numWorkRows, "Work row count must match initial count")

	// 2 run point queries
	for _, r := range workRows {
		var row UnifiedRow
		_, err = knox.NewGenericQuery[UnifiedRow]().
			WithTable(table).
			AndEqual("id", r.Id).
			Execute(ctx, &row)
		require.NoError(t, err, "Failed to load work row")
		require.Equal(t, r.Id, row.Id, "Row id matches")
		if r.ThreadID == 0 {
			// initial state
			require.Equal(t, 0, r.TxId, "Non zero tx id for initial state")
		} else {
			// updated state (formatting rules)
			require.GreaterOrEqual(t, r.ThreadID, 1)
			require.LessOrEqual(t, r.ThreadID, numThreads)
			require.GreaterOrEqual(t, r.TxId, 1)
			require.LessOrEqual(t, r.TxId, txnSize)
		}
	}

	var metaRows []*UnifiedRow
	_, err = knox.NewGenericQuery[UnifiedRow]().
		WithTable(table).
		AndEqual("row_type", RowTypeMeta).
		WithLogger(log.Log).
		Execute(ctx, &metaRows)
	require.NoError(t, err, "Failed to validate work rows")
	// for _, r := range metaRows {
	// 	t.Logf("Found row id=%d TH-%d-TX-%d w1=%d w2=%d", r.Id, r.ThreadID, r.TxId, r.WorkRow1, r.WorkRow2)
	// }

	// 3 work rows match meta rows (i.e. the last update to a row was written by the correct
	// thread in the correct tx)
	for _, r := range workRows {
		// skip untouched work rows
		if r.ThreadID == 0 {
			continue
		}
		// t.Logf("Looking for meta row TH-%d-TXN-%d", r.ThreadID, r.TxId)
		var metarow UnifiedRow
		_, err = knox.NewGenericQuery[UnifiedRow]().
			WithTable(table).
			AndEqual("row_type", RowTypeMeta).
			AndEqual("thread_id", r.ThreadID).
			AndEqual("tx_id", r.TxId).
			Execute(ctx, &metarow)
		require.NoError(t, err, "Failed to load work row")
		require.NotEqual(t, uint64(0), metarow.Id, "Meta row was not found")
		require.Equal(t, r.ThreadID, metarow.ThreadID, "Thread id mismatch")
		require.Equal(t, r.TxId, metarow.TxId, "tx id mismatch")
		require.True(t, metarow.WorkRow1 == r.Id || metarow.WorkRow2 == r.Id, "work row was not last updated in this transaction by this thread")
	}

	// TODO
	// - remember fault injection timestamp and check tranactions that committed before
	//   are durable, txn that did not commit are not visible

	// 4 check multiple row with each transaction
	// eg. the work rows and meta rows
	for txId := 1; txId <= txnSize; txId++ {
		for thId := 1; thId <= numThreads; thId++ {
			var txMetaRow UnifiedRow
			_, err = knox.NewGenericQuery[UnifiedRow]().
				WithTable(table).
				AndEqual("row_type", RowTypeMeta).
				AndEqual("tx_id", txId).
				AndEqual("thread_id", thId).
				Execute(ctx, &txMetaRow)
			require.NoError(t, err)
			require.NotEqual(t, txMetaRow.Id, uint64(0))

			var txWorkRow1 UnifiedRow
			_, err = knox.NewGenericQuery[UnifiedRow]().
				WithTable(table).
				AndEqual("row_type", RowTypeWork).
				AndEqual("tx_id", txId).
				AndEqual("thread_id", thId).
				AndEqual("work_row_1", txMetaRow.WorkRow1).
				Execute(ctx, &txWorkRow1)
			require.NoError(t, err)
			require.NotEqual(t, uint(0), txWorkRow1.Id)

			var txWorkRow2 UnifiedRow
			_, err = knox.NewGenericQuery[UnifiedRow]().
				WithTable(table).
				AndEqual("row_type", RowTypeWork).
				AndEqual("tx_id", txId).
				AndEqual("thread_id", thId).
				AndEqual("work_row_2", txMetaRow.WorkRow2).
				Execute(ctx, &txWorkRow2)
			require.NoError(t, err)
			require.NotEqual(t, uint(0), txWorkRow2.Id)
		}
	}

	log.Infof("Validation of work rows completed.")
}
