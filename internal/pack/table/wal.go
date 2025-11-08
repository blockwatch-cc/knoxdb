// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"io"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
)

// WAL record formats
//
// Insert (rids are sequential)
// | wal rec header | rid1 | wire1 | wire2 | ... |
//
// Update (rids are sequential)
// | wal rec header | changeset | rid1 | ref1 | wire1 | ref2 | wire2 | ... |
//
// Delete
// | wal rec header | rid1 | rid2 | ... |
func (t *Table) ReplayWal(ctx context.Context) error {
	var xmax types.XID // highest xid seen
	start := time.Now()

	t.log.Debugf("recovering journals from wal lsn 0x%x", t.state.Checkpoint)
	r := t.engine.Wal().NewReader().WithEntity(t.id)
	defer r.Close()
	if err := r.Seek(t.state.Checkpoint); err != nil {
		return err
	}

	// process wal records (even on a clean shutdown we reconstruct
	// an active journal segment from WAL)
	var nProcessed int
	for {
		rec, err := r.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		nProcessed++

		// init a new table reader for updates
		var rd engine.TableReader
		if rec.Type == wal.RecordTypeUpdate {
			rd = t.NewReader()
		}

		// insert record into journal
		if err := t.journal.ReplayWalRecord(ctx, rec, rd); err != nil {
			return err
		}

		switch rec.Type {
		case wal.RecordTypeCommit, wal.RecordTypeAbort:
			xmax = max(xmax, rec.TxID)
		}

		if rd != nil {
			rd.Close()
		}
	}

	// abort all remaining pending tx
	nAborted, canMerge := t.journal.AbortActiveTx()
	t.log.Debugf("processed %d wal records, aborted %d txn in %s",
		nProcessed, nAborted, time.Since(start))

	// merge journal after crash recovery when segments are ready
	if canMerge {
		t.log.Trace("scheduling merge task")
		ok := t.engine.Schedule(engine.NewTask(t.Merge))
		if !ok {
			t.log.Trace("merge task queue full")
		}
	}

	// track max xid across all tables
	t.engine.UpdateTxHorizon(xmax)

	return nil
}
