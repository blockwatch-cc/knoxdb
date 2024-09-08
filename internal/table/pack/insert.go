// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"context"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
)

func (t *Table) InsertRows(ctx context.Context, buf []byte) (uint64, error) {
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
	atomic.AddInt64(&t.stats.InsertCalls, 1)

	// protect journal write access
	t.mu.Lock()
	defer t.mu.Unlock()

	// keep a copy of the state
	firstPk := t.state.Sequence
	state := t.state

	// try insert data into the journal (may run full, so we must loop)
	var (
		count, n uint64
		err      error
	)
	for len(buf) > 0 {
		// insert messages into journal
		n, buf = t.journal.InsertBatch(buf, state.Sequence)
		count += n

		// update state
		state.Rows += n
		state.Sequence += n

		// write journal data to disk before we continue
		if t.journal.IsFull() {
			// check context cancelation
			err = ctx.Err()
			if err != nil {
				break
			}

			// sync state with catalog
			t.engine.Catalog().SetState(t.tableId, state.Sequence, state.Rows)

			// flush pack data to storage, will open storage write transaction
			// TODO: write a new layer pack (fast) and merge in background
			err = t.mergeJournal(ctx)
			if err != nil {
				break
			}

			// sync state back to table
			t.state = state
		}
	}
	if err != nil {
		// TOOD: will fail the tx and should reload journal from wal afterwards
		return 0, err
	}

	atomic.AddInt64(&t.stats.InsertedTuples, int64(count))

	// sync state with catalog
	if count > 0 {
		// sync state with catalog
		t.engine.Catalog().SetState(t.tableId, state.Sequence, state.Rows)
		engine.GetTransaction(ctx).Touch(t.tableId)

		// sync state back to table
		t.state = state
	}

	return firstPk, nil
}
