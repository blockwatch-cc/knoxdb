// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// Inserts batch of user records in to the table journal and returns the first
// assigned primary key.
//
// Steps
// - single writer tx, only concurrent readers must be considered
// - generate PKs and patch into record layout before WAL write
// - generate RIDs and patch into jornal pack on append
// - split records across segments, rotate when full
// - write WAL records with batches or records (less wal headers)
// - loop until all records are written to WAL and journal segments
//
// Durability/Recovery
// - sync journal append batches with WAL record LSNs
//   - write only as many records as journal capacity can hold
//   - on first write to a new journal segment, store WAL LSN (for later checkpointing)
//   - keep rotated segments in memory (don't flush - simplified design, less write ampl)
// - start journal merge of full segments on tx commit
//   - compress/store segment
//   - write WAL checkpoint
//   - release merged segment (careful with concurrent readers)
//
// Durability
// - we don't know when the tx will commit, but if we hold segment flush until commit
//   and then merge instead of flush we can avoid write amplification
// - durability is still guaranteed because data is safe in WAL (don't need the journal
//   segs to flush)
// - a checkpoint is written after journal segment merge (note: LSN from next unmerged
//   segment)
// - the checkpoint must contain the LSN from which to start replay into journal
// - every segment must remember the first LSN for its data
//
// Determinism
// - howto manage state updates/rollbacks on commit/abort?
//   - reset state (pk, rid, nrows) in abort callback
// - howto manage state jumps on dag-journal segments when switching forks?
//   - each segment keeps pk/rid counter
//   - next segment references parent and inherits counters
//   - merge only ever merges one fork

func (t *Table) InsertRows(ctx context.Context, buf []byte) (uint64, error) {
	// reject invalid messages
	if len(buf) == 0 {
		return 0, nil
	}
	if len(buf) < t.schema.WireSize() {
		return 0, engine.ErrShortMessage
	}

	// check table state
	if t.opts.ReadOnly {
		return 0, engine.ErrTableReadOnly
	}
	atomic.AddInt64(&t.metrics.InsertCalls, 1)

	// obtain shared table lock
	tx := engine.GetTransaction(ctx)
	err := tx.RLock(ctx, t.id)
	if err != nil {
		return 0, err
	}

	// register state reset callback only once
	if !tx.Touched(t.id) {
		prevState := t.state
		tx.OnAbort(func(_ context.Context) error {
			t.state = prevState
			return nil
		})
	}

	// register table for commit/abort callbacks
	tx.Touch(t.id)

	// protect journal access
	t.mu.RLock()
	defer t.mu.RUnlock()

	// insert to journal, write WAL
	pk, n, err := t.journal.InsertRecords(ctx, buf)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(&t.metrics.InsertedTuples, int64(n))

	return pk, nil
}

// Insert into journal from query (pack or result, likely joined/computed columns,
// mix of materialized and not materialized, with or without selection vector)
func (t *Table) InsertInto(ctx context.Context, src *pack.Package) (uint64, error) {
	// ensure pack schemas match
	if !src.Schema().EqualHash(t.schema.Hash()) {
		return 0, schema.ErrSchemaMismatch
	}

	// check table state
	if t.opts.ReadOnly {
		return 0, engine.ErrTableReadOnly
	}
	atomic.AddInt64(&t.metrics.InsertCalls, 1)

	// obtain shared table lock
	tx := engine.GetTransaction(ctx)
	err := tx.RLock(ctx, t.id)
	if err != nil {
		return 0, err
	}

	// register state reset callback only once
	if !tx.Touched(t.id) {
		prevState := t.state
		tx.OnAbort(func(_ context.Context) error {
			t.state = prevState
			return nil
		})
	}

	// register table for commit/abort callbacks
	tx.Touch(t.id)

	// protect journal access
	t.mu.RLock()
	defer t.mu.RUnlock()

	// insert to journal, write WAL
	pk, n, err := t.journal.InsertPack(ctx, src)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(&t.metrics.InsertedTuples, int64(n))

	return pk, nil
}

func (t *Table) ImportInto(ctx context.Context, pkg *pack.Package) (uint64, error) {
	// TODO bulk import from external (CSV, parquet, network sync) or table copy
	// - src: full pkg without selection (importer or TableReader)
	// - dst: direct write to table storage without wal
	//
	// Steps
	// - ensure journal is flushed and merged
	// - assign metadata to pack (pk, rid, xmin)
	// - write wal (maybe not here, this should happen once per copy/import stmt)
	// - assign pack id, store as table pack
	//
	// Tx handling
	// - import/sync would block all write tx (maybe that is desired)
	// - abort would have to clear table storage again (truncate/delete)
	// - design wise import/copy may be limited to new tables only (created in same tx)
	//   then a tx rollback would clean up the table file and references if import/sync
	//   fails
	return 0, nil
}
