// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"sync/atomic"
	"time"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/pkg/store"
	"github.com/echa/log"
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
	table    *Table                    // table back-reference
	stats    *stats.Index              // private statistics index copy
	tail     *pack.Package             // current tail package
	vtail    uint32                    // previous storage version for loaded tail packs
	bcache   block.BlockCachePartition // block cache reference
	wasFull  bool                      // last known tail pack was full (new on write)
	log      log.Logger
	nPacks   int
	nRecords int
	nBytes   int
	start    time.Time
}

func (t *Table) NewWriter(epoch uint32) engine.TableWriter {
	if epoch == 0 {
		epoch = uint32(t.state.Epoch)
	}
	// its safe to call Get here because we will be the onlt thread executing
	// merge on this table
	s := t.stats.Get().Clone().WithEpoch(epoch)
	return &Writer{
		table:   t,
		stats:   s,
		log:     t.log,
		vtail:   0,
		wasFull: s.IsTailFull(),
		start:   time.Now().UTC(),
	}
}

func (w *Writer) Epoch() uint32 {
	return w.stats.Epoch()
}

func (w *Writer) Close() {
	if w.stats != nil {
		w.stats.Free() // careful: use free! close drops shared snodes
		w.stats = nil
	}
	if w.tail != nil {
		w.tail.Release()
		w.tail = nil
	}
	w.vtail = 0
	w.table = nil
	w.bcache = nil
	w.log = nil
	w.vtail = 0
	w.wasFull = false
	w.nPacks = 0
	w.nRecords = 0
	w.nBytes = 0
	w.start = time.Time{}
}

// Runs garbage collection on the table dropping old versions of vector blocks
// and metadata. May be called before merge starts to free storage space that
// new merged blocks can occupy. Note after merge completes, GC will run
// automatically again, but only if the writer drops the last reference to
// the current stats index epoch.
func (w *Writer) GC() error {
	if !w.stats.IsClean() {
		return w.table.db.Update(func(tx store.Tx) error {
			return w.stats.RunGC(tx)
		})
	}
	return nil
}

// Appends src data to table and indexes. Writes new pack versions as they become full.
// Write mode defines which records to copy based on selection vector.
func (w *Writer) Append(ctx context.Context, src *pack.Package, mode engine.WriteMode) error {
	var (
		state pack.AppendState
		err   error
	)

	// w.log.Debugf("appending journal pack %08x", src.Key())

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
			// w.log.Debugf("no more data to append")
			break
		}
	}

	// append to indexes
	return w.AppendIndexes(ctx, src, mode)
}

// Replace rewrites src pack data on disk, optionally removing src data based
// on mode and pack selection vector.
func (w *Writer) Replace(ctx context.Context, src *pack.Package, mode engine.WriteMode) error {
	// backup tail to allow mixed Append/Replace mode
	tail := w.tail

	// w.log.Debugf("replace pack %08x[v%d]", src.Key(), src.Version())

	// append source data to new pack using selection mode
	w.tail = pack.New().
		WithKey(src.Key()).
		WithVersion(src.Version() + 1).
		WithSchema(w.table.schema).
		WithMaxRows(w.table.opts.PackSize).
		Alloc()
	w.vtail = src.Version()
	defer func() {
		w.tail.Release()
		w.tail = tail
		w.vtail = 0
	}()

	// single iteration is enough because src length <= capacity
	_, err := w.appendTail(ctx, src, mode, pack.AppendState{})
	if err != nil {
		return err
	}

	// store pack, update metadata
	return w.storePack(ctx, w.tail)
}

func (w *Writer) Finalize(ctx context.Context, state engine.ObjectState) error {
	// w.log.Debug("finalize")

	// store tail pack
	if w.tail != nil {
		err := w.storePack(ctx, w.tail)
		if err != nil {
			return err
		}
		w.tail.Release()
		w.tail = nil
	}

	// check for abort
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// finalize indexes (merge new and mark deleted entries)
	w.log.Debug("finalize indexes")
	if err := w.FinalizeIndexes(ctx); err != nil {
		return err
	}

	// check for abort
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// write stats update and table state (WAL checkpoint and LSN of next segment)
	// this will finalize the merge
	err := w.table.db.Update(func(tx store.Tx) error {
		// w.log.Debugf("storing metadata v%d", w.stats.Epoch())
		if err := w.stats.Store(ctx, tx); err != nil {
			return err
		}

		// write state snapshot (as of at end of the current merged journal segment)
		w.table.state.Epoch = uint64(w.stats.Epoch())
		w.table.state.Checkpoint = state.Checkpoint
		w.table.state.NRows = state.NRows
		w.table.state.NextPk = state.NextPk
		w.table.state.NextRid = state.NextRid

		// w.log.Debugf("table checkpoint v%d lsn=%d", w.stats.Epoch(), state.Checkpoint)
		return w.table.state.Store(ctx, tx)
	})
	if err != nil {
		return err
	}

	// sync table when running in no-sync mode
	if w.table.opts.NoSync {
		if err := w.table.Sync(ctx); err != nil {
			return err
		}
	}

	// swap new stats index, may GC previous version
	// w.log.Debugf("installing new metadata v%d", w.stats.Epoch())
	w.table.stats.Update(w.stats)
	w.stats = nil

	return nil
}

func (w *Writer) AppendIndexes(ctx context.Context, src *pack.Package, mode engine.WriteMode) error {
	for _, idx := range w.table.Indexes() {
		if err := idx.(engine.IndexEngine).AddPack(ctx, src, mode); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) DeleteIndexes(ctx context.Context, src *pack.Package, mode engine.WriteMode) error {
	for _, idx := range w.table.Indexes() {
		if err := idx.(engine.IndexEngine).DelPack(ctx, src, mode, w.stats.Epoch()); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) FinalizeIndexes(ctx context.Context) error {
	for _, v := range w.table.Indexes() {
		idx := v.(engine.IndexEngine)
		if err := idx.Finalize(ctx, w.stats.Epoch()); err != nil {
			return err
		}
	}
	return nil
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
				WithVersion(1).
				WithSchema(w.table.schema).
				WithMaxRows(w.table.opts.PackSize).
				Alloc()
			w.wasFull = false
			w.vtail = 0
		} else {
			pkg, err := w.loadTail(ctx)
			if err != nil {
				return state, err
			}
			w.tail = pkg.Materialize().WithVersion(pkg.Version() + 1)
		}
	}
	var n int
	n, state = w.tail.AppendSelected(src, mode, state)
	w.nRecords += n

	// w.log.Debugf("append %d records to pack %08x[v%d]",
	// n, w.tail.Key(), w.tail.Version())
	// if n == 0 {
	// 	sel := src.Selected()
	// 	panic(fmt.Errorf("selection error n=0, sel=%d %v", len(sel), sel[:min(8, len(sel))]))
	// }

	return state, nil
}

func (w *Writer) storePack(ctx context.Context, pkg *pack.Package) error {
	// remove zero length packs
	if pkg.Len() == 0 {
		// drop from stats
		if err := w.stats.DeletePack(ctx, pkg); err != nil {
			return err
		}

		// w.log.Debugf("mark empty pack %08x[v%d] vtail=%d",
		// 	pkg.Key(), pkg.Version(), w.vtail)

		// schedule garbage collection
		var err error
		if w.vtail > 0 {
			err = w.table.db.Update(func(tx store.Tx) error {
				// write a tombstone record for the previous version of this pack
				return w.stats.Tomb().AddDataPack(tx, pkg.Key(), w.vtail)
			})
		}
		w.vtail = 0
		return err
	}

	// init statistics
	pkg.WithStats()

	// w.log.Debugf("storing pack %08x[v%d]", pkg.Key(), pkg.Version())

	// analyze, optimize, compress and write to disk
	err := w.table.db.Update(func(tx store.Tx) error {
		n, err := pkg.StoreToDisk(ctx, w.table.dataBucket(tx))
		if err == nil {
			// write a tombstone record for the previous version of this pack
			// - append: may have a prev version, often not
			// - replace: has prev version
			if w.vtail > 0 {
				err = w.stats.Tomb().AddDataPack(tx, pkg.Key(), w.vtail)
			}
			atomic.AddInt64(&w.table.metrics.PacksStored, 1)
			atomic.AddInt64(&w.table.metrics.BytesWritten, int64(n))
			w.nBytes += n
		}
		return err
	})
	if err != nil {
		return err
	}
	w.nPacks++

	// update private metadata index
	if w.vtail > 0 {
		err = w.stats.UpdatePack(ctx, pkg)
	} else {
		err = w.stats.AddPack(ctx, pkg)
	}

	// cleanup pack statistics
	pkg.CloseStats()

	// reset previous version
	w.vtail = 0

	return err
}

func (w *Writer) loadTail(ctx context.Context) (*pack.Package, error) {
	// fetch tail pack info from stats index
	key, ver, nvals := w.stats.TailInfo()
	// w.log.Debugf("loading pack %08x[v%d]", key, ver)

	// prepare an empty pack without block storage
	pkg := pack.New().
		WithKey(key).
		WithVersion(ver).
		WithSchema(w.table.schema).
		WithMaxRows(w.table.opts.PackSize)

	// try load from cache using tableid as cache tag
	// count number of expected blocks
	nBlocks := w.table.schema.NumActive()

	// init cache on first call
	if w.bcache == nil {
		w.bcache = engine.GetEngine(ctx).BlockCache(w.table.id)
	}

	// stop early when all requested blocks are found
	if pkg.LoadFromCache(w.bcache, nil) == nBlocks {
		return pkg, nil
	}

	// load from table data bucket in short-lived read tx
	err := w.table.db.View(func(tx store.Tx) error {
		n, err := pkg.LoadFromDisk(ctx, w.table.dataBucket(tx), nil, nvals)
		if err == nil {
			// count stats
			atomic.AddInt64(&w.table.metrics.PacksLoaded, 1)
			atomic.AddInt64(&w.table.metrics.BytesRead, int64(n))
		}
		return err
	})
	if err != nil {
		pkg.Release()
		return nil, err
	}
	w.vtail = ver

	// ld := make([]int, 0)
	// for i, b := range pkg.Blocks() {
	// 	if b == nil {
	// 		continue
	// 	}
	// 	ld = append(ld, i)
	// }
	// w.log.Debugf("writer loaded blocks %v", ld)

	return pkg, nil
}
