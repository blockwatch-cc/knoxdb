// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
)

func (t *Table) UpdateRows(ctx context.Context, buf []byte) (uint64, error) {
	// check message
	if len(buf) == 0 {
		return 0, nil
	}
	if len(buf) < t.schema.WireSize() {
		return 0, engine.ErrShortMessage
	}

	// check table state
	if t.opts.ReadOnly {
		return 0, engine.ErrDatabaseReadOnly
	}
	atomic.AddInt64(&t.stats.UpdateCalls, 1)

	// protect journal write access
	t.mu.Lock()
	defer t.mu.Unlock()

	// upgrade tx for writing and register touched table for later commit
	engine.GetTransaction(ctx).Touch(t.tableId)

	// try write updated records to journal (may run full, so we must loop)
	var (
		count, n uint64
		err      error
	)
	for len(buf) > 0 {
		// insert messages into journal, may fail when pk = 0
		n, buf, err = t.journal.UpdateBatch(buf)
		if err != nil {
			break
		}
		count += n

		// write journal data to disk before we continue
		if t.journal.IsFull() {
			// check context cancelation
			err = ctx.Err()
			if err != nil {
				break
			}

			// flush pack data to storage, will open storage write transaction
			// TODO: write a new layer pack (fast) and merge in background
			err = t.mergeJournal(ctx)
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		// TOOD: will fail the tx and should reload journal from wal afterwards
		return 0, err
	}

	if count > 0 {
		atomic.AddInt64(&t.stats.UpdatedTuples, int64(count))
	}

	return uint64(count), nil
}
