// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/wal"
)

const (
	TODO_ROWID uint64 = 0
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
	atomic.AddInt64(&t.metrics.InsertCalls, 1)

	// protect journal write access
	t.mu.Lock()
	defer t.mu.Unlock()

	// keep a copy of the state
	firstPk := t.state.Sequence

	// try insert data into the journal (may run full, so we must loop)
	var (
		count, n uint64
		err      error
	)
	for len(buf) > 0 {
		// insert messages into journal
		n, buf = t.journal.InsertBatch(buf, t.state.Sequence)
		count += n

		// sync state with catalog
		engine.GetTransaction(ctx).Touch(t.tableId)

		// update state
		t.state.NRows += n
		t.state.Sequence += n

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

	atomic.AddInt64(&t.metrics.InsertedTuples, int64(count))

	if count > 0 {
		return firstPk, nil
	}

	return 0, nil
}

func (t *Table) ApplyWalRecord(ctx context.Context, rec *wal.Record) error {
	// TODO: refactor to use wal
	return engine.ErrNotImplemented
}
