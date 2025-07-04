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

	t.log.Debugf("table[%s]: recovering journals from wal lsn 0x%x", t.schema.Name(), t.state.Checkpoint)
	r := t.engine.Wal().NewReader().WithEntity(t.id)
	defer r.Close()
	if err := r.Seek(t.state.Checkpoint); err != nil {
		return err
	}

	// process wal records (even on a clean shutdown we reconstruct
	// an active journal segment from WAL)
	var n int
	for {
		rec, err := r.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		n++

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
	skipped, canMerge := t.journal.AbortActiveTx()
	t.log.Debugf("table[%s]: processed %d wal records, aborted %d txn in %s",
		t.schema.Name(), n, skipped, time.Since(start))

	// merge journal after crash recovery when segments are ready
	if canMerge {
		t.log.Debugf("table[%s]: scheduling merge task", t.schema.Name())
		ok := t.engine.Schedule(engine.NewTask(t.Merge))
		if !ok {
			t.log.Warnf("table[%s]: merge task queue full", t.schema.Name())
		}
	}

	// track max xid across all tables
	t.engine.UpdateTxHorizon(xmax)

	return nil
}
