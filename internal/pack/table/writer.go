// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/store"
)

// TableWriter
//
// A physical data write operator that inserts or overwrites table data at
// pack granularity. Can be used to append data to a table or rewrite existing
// data packs with changed content (e.g. remove records on merge or GC).
//
// Write modes
// - append: inserts new data at the tail, grows table by adding new packs as needed
// - replace: overrides existing data packs with new version
//
// Merge modes
// - all: ignores pack selection vector and writes all rows
// - include: uses rows referenced by selection vector when writing
// - exclude: skips rows referenced by selection vector when writing

var _ engine.TableWriter = (*Writer)(nil)

type Writer struct {
	table   *Table        // table back-reference
	stats   *stats.Index  // private statistics index copy
	tail    *pack.Package // current tail package
	wasFull bool          // last known tail pack was full, create new tail on write
}

func (t *Table) NewWriter() engine.TableWriter {
	s := t.stats.Load().(*stats.Index).Clone()
	return &Writer{
		table:   t,
		stats:   s,
		wasFull: s.IsTailFull(),
	}
}

// Appends src data to the table and flushes packs as they get full.
// Write mode defines which records to copy based on selection vector.
func (w *Writer) Append(ctx context.Context, src *pack.Package, mode engine.WriteMode) error {
	var (
		state pack.AppendState
		err   error
	)
	for {
		// append next chunk of data to tail: max(cap(tail), len(src))
		state, err = w.appendTail(ctx, src, mode, state)
		if err != nil {
			return err
		}

		// store full packs
		if w.tail.IsFull() {
			if err = w.storePack(ctx, w.tail); err != nil {
				return err
			}
			w.tail.Release()
			w.tail = nil
			w.wasFull = true
		}

		// stop when src is exhausted
		if !state.More() {
			break
		}
	}
	return nil
}

// Replace rewrites src pack data on disk, optionally removing src data based
// on mode and pack selection vector.
func (w *Writer) Replace(ctx context.Context, src *pack.Package, mode engine.WriteMode) error {
	// backup tail to allow mixed Append/Replace mode
	tail := w.tail

	// append source data to new pack using selection mode
	w.tail = pack.New().
		WithKey(src.Key()).
		WithSchema(w.table.schema).
		WithMaxRows(w.table.opts.PackSize).
		Alloc()
	defer func() {
		w.tail.Release()
		w.tail = tail
	}()

	// single iteration is enough because src length <= capacity
	_, err := w.appendTail(ctx, src, mode, pack.AppendState{})
	if err != nil {
		return err
	}

	// store pack, update metadata
	return w.storePack(ctx, w.tail)
}

func (w *Writer) Finalize(ctx context.Context) error {
	// store tail pack
	if w.tail != nil {
		err := w.storePack(ctx, w.tail)
		if err != nil {
			return err
		}
		w.tail.Release()
		w.tail = nil
	}

	// publish new stats index to table
	w.table.stats.Store(w.stats)
	w.stats = nil

	return nil
}

func (w *Writer) Close() {
	if w.stats != nil {
		w.stats.Close()
		w.stats = nil
	}
	if w.tail != nil {
		w.tail.Release()
		w.tail = nil
	}
	w.table = nil
}

// appendTail appends records from src to tail according to mode until tail is full.
// Use offsets osrc (into src pack) and osel (into selection vector) to chain multiple
// calls. Returns the next source and selection offsets and a boolean indicating when
// more source data is available.
func (w *Writer) appendTail(ctx context.Context, src *pack.Package, mode pack.WriteMode, state pack.AppendState) (pack.AppendState, error) {
	// load or create a new tail pack when missing (this happens on first call and after store)
	if w.tail == nil {
		if w.wasFull {
			w.tail = pack.New().
				WithKey(w.stats.NextKey()).
				WithSchema(w.table.schema).
				WithMaxRows(w.table.opts.PackSize).
				Alloc()
			w.wasFull = false
		} else {
			// FIXME: materialize?
			var err error
			w.tail, err = w.table.NewReader().Read(ctx, w.stats.NextKey()-1)
			if err != nil {
				return state, err
			}
		}
	}

	return w.tail.AppendSelected(src, mode, state), nil
}

func (w *Writer) storePack(ctx context.Context, pkg *pack.Package) error {
	// remove zero length packs
	if pkg.Len() == 0 {
		// drop from stats
		if err := w.stats.DeletePack(ctx, pkg); err != nil {
			return err
		}

		// remove from storage
		err := w.table.db.Update(func(tx store.Tx) error {
			return pkg.RemoveFromDisk(ctx, w.table.dataBucket(tx))
		})

		// remove from cache
		pkg.DropFromCache(engine.GetEngine(ctx).BlockCache(w.table.id))

		return err
	}

	// init statistics
	pkg.WithStats()

	// analyze, optimize, compress and write to disk
	err := w.table.db.Update(func(tx store.Tx) error {
		n, err := pkg.StoreToDisk(ctx, w.table.dataBucket(tx))
		if err == nil {
			atomic.AddInt64(&w.table.metrics.PacksStored, 1)
			atomic.AddInt64(&w.table.metrics.BytesWritten, int64(n))
		}
		return err
	})
	if err != nil {
		return err
	}

	// update statistics
	if pkg.Key() < w.stats.NextKey() {
		err = w.stats.UpdatePack(ctx, pkg)
	} else {
		err = w.stats.AddPack(ctx, pkg)
	}

	// remove from cache
	pkg.DropFromCache(engine.GetEngine(ctx).BlockCache(w.table.id))

	// cleanup statistics
	pkg.CloseStats()

	return err
}
