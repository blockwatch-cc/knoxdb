// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"sync/atomic"
	"time"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/journal"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

// Merge copies all changes in a journal segment into compact table vectors.
// Merging is an idempotent process. It can crash, get interrupted and restarted.
// It writes new versions of backend table storage blocks and index blocks and
// only discards a journal segment when all data has been successfully written.
//
// Merge works in multiple stages
// 1. update journal segment state to merging
// 2. process deleted records
//    - append to history (optional, patch xmax metadata from tombstones)
//    - add to history indexes
//    - drop from main indexes
//    - rewrite main table packs
// 3. process new record versions
//    - append to main table
//    - add to main indexes
//
// Storage writes are atomic for all vector blocks in a pack. Concurrent readers
// use the on-disk versions of blocks that were active at transaction start.
// New block versions become only visible once merge completes and only for
// future transactions. We achieve this by producing a COW copy of block metadata
// and atomically installing the new metadata version on merge completion.
// While merge progresses, old readers observe the previous on-disk versions of
// blocks overlayed by changes in the journal. After potential crash incomplete written
// blocks are removed and written again by the next merge process.
//
// Merge appends new row versions from insert/update operations at the end of
// a table's data vectors and removes old rows replaced by update/delete.
// When history is enabled, pre-images of old rows are moved to a history table.
//
// Merge does not block readers or writers (other than by very short-lived exclusive
// backend write transactions) but it is not concurrency safe itself. Callers to merge
// must ensure only a single thread executes a single merge operation at a time and
// journal segments are merged in order.
//
// Invariants
// - unique pk: main table contains at most one record per primary key
// - unique rid: main and history tables contain at most one record with the same row id
// - sorted rid: main table is sorted by rid (append only)
//
// History Table Merge Strategy (unique rid, sorted by xmax)
// - update -> append pre-image to last pack, set xmax = xid, is_deleted = false
// - delete -> append pre-image to last pack, set xmax = xid, is_deleted = true
//
// Main Table Merge Strategy (unique pk, sorted by rid)
//   - insert -> append new record from journal to last pack
//   - update -> remove pre-image rid or mark as deleted (set xmax, is_deleted)
//     append post-image from journal to last pack
//   - delete -> remove pre-image rid or mark as deleted (set xmax, is_deleted)
//
// No visible side effects for concurrent read/write transactions
// - table statistics updates are COW using a private copy
// - new table statistics are installed via atomic update on completion (see TableWriter.Finalize())
// - on-disk table and index blocks are versioned and invisible until statistics update
//
// Design considerations for background merge
//
// - called by background job
// - merges one journal segment at a time
// - no database engine (user) transaction exists
// - uses short-lived backend write transactions to atomically write pack blocks
// - readers are only blocked during short backend writes (with boltdb storage engine)
// - may briefly stall on I/O while holding backend store locks
// - merge can get interrupted without side-effects (by error, context cancel or crash)
// - journal data and tombstone remain authoritative source (overlay pack data)
//   until merge is confirmed
// - MVCC still works for merged data (record metadata remains available to queries)
//
// Protocol
//   1. write new table data, update indexes, register tombstones
//   2. write epoch + LSN to table state (normative end of merge)
//   3. swap meta index ptr (only afterwards its safe to drop merged segment)
//   4. drop merged segment from journal
//   5. GC wal segments, gc unused block versions
// - notes
//   - crash recovery after step 2 skips already merged journal segment
//   - must filter journal query by meta epoch to prevent duplicates

// Merge is called as background task and operates concurrent to journal
// writes and query readers. If another segment from the same journal
// is currently in merging state, the task yields and retries later.
func (t *Table) Merge(ctx context.Context) error {
	var (
		seg      *journal.Segment
		err      error
		nRetries = 3
	)

	t.log.Trace("starting merge task")

	// reset merge task handle on completion
	defer t.task.Store(nil)

	if t.IsReadOnly() {
		return engine.ErrTableReadOnly
	}

	for {
		// get next mergable segment, will atomically mark as merge in progress
		t.mu.Lock()
		seg, err = t.journal.NextMergable()
		t.mu.Unlock()
		nRetries--
		if err == nil || nRetries <= 0 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// wait a bit
		}
	}

	// nothing to do (unlikely)
	if seg == nil {
		return nil
	}

	// run merge
	t.log.Tracef("merging journal segment %d", seg.Id())
	err = t.mergeJournal(ctx, seg)
	if err != nil {
		// notify journal, will keep segment in memory and retry
		t.log.Errorf("merge segment %d: %v", seg.Id(), err)
		t.mu.Lock()
		t.journal.AbortMerged(seg)
		t.mu.Unlock()
	} else {
		// cross-check cancel state
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// gc journal segment
		t.mu.Lock()
		t.journal.ConfirmMerged(ctx, seg)
		t.mu.Unlock()

		// gc wal after merge. this ensures that we don't keep a large amount
		// of wal files at high write volume. internally TryGC() will schedule
		// a task that rotates and checkpoints all table journals.
		if err := t.engine.TryGC(ctx); err != nil {
			t.log.Warnf("wal gc: %v", err)
		}
	}

	return err
}

func (t *Table) mergeJournal(ctx context.Context, seg *journal.Segment) error {
	// metrics
	var (
		nStones    int
		nPacks     int
		nAdd, nDel int
		nHeap      = seg.Size()
		nBytes     = atomic.LoadInt64(&t.metrics.BytesWritten)
		start      = time.Now()
	)

	// init table writer
	table := t.NewWriter(seg.Id())
	defer table.Close()

	// run table GC to free up unused space
	if err := table.GC(); err != nil {
		t.log.Error(err)
	}

	// t.log.Debugf("merge epoch %d", seg.Id())

	// init history writer
	var hist engine.TableWriter
	if ht, err := engine.GetEngine(ctx).FindTable(t.schema.Name + "_history"); err == nil {
		hist = ht.NewWriter(seg.Id())
		defer hist.Close()
	}

	// Phase 1 - move deleted rows to history, rewrite table packs
	stones := seg.Tomb().Stones() // non-aborted deletes (within and outside the segment)
	mask := seg.Tomb().RowIds()   // row id bitmap of all deletes, nil when empty
	replaced := seg.Replaced()    // bitset of updated/deleted records, nil when empty
	nStones = len(stones)

	if mask != nil && mask.Any() && mask.Min() < t.stats.Get().GlobalMaxRid() {
		t.log.Tracef("merge phase 1: %d/%d tombstones", mask.Count(), len(stones))
		src := t.NewReader().WithMask(mask, engine.ReadModeIncludeMask)
		defer src.Close()
		for {
			pkg, err := src.Next(ctx)
			if err != nil {
				return err
			}
			if pkg == nil {
				break
			}
			nPacks++
			nDel++
			// t.log.Debugf("merge pack 0x%08x[v%d] with %d tombs",
			// 	pkg.Key(), pkg.Version(), len(pkg.Selected()))

			if hist != nil {
				// TODO: patch xmax in history pack (which is writable)

				// set xmax for deleted/replaced rows, set del flag for deleted rows
				xmaxId, ok := pkg.Schema().IndexId(schema.MetaXmax)
				delId, ok2 := pkg.Schema().IndexId(schema.MetaDel)
				if ok && ok2 {
					pkg.MaterializeBlock(xmaxId)
					pkg.MaterializeBlock(delId)
					xmaxs := pkg.Xmaxs().Slice()
					dels := pkg.Dels().Writer()
					for _, v := range pkg.Selected() {
						xmaxs[int(v)] = uint64(stones[0].Xid)
						if stones[0].IsDel {
							dels.Set(int(v))
						}
						stones = stones[1:]
					}
					pkg.Block(xmaxId).SetDirty()
					pkg.Block(delId).SetDirty()
				}

				// insert deleted rows into history
				if err := hist.Append(ctx, pkg, pack.WriteModeIncludeSelected); err != nil {
					return err
				}
			}

			// update indexes, mark deleted rows for deletion
			if err := table.DeleteIndexes(ctx, pkg, pack.WriteModeIncludeSelected); err != nil {
				return err
			}

			// rewrite table pack excluding deleted rows
			sel := pkg.Selected()
			neg := types.NegateSelection(sel, pkg.Len())
			// t.log.Debugf("merge neg sel %d+%d=%d(%d) body=%v",
			// 	len(sel), len(neg), len(sel)+len(neg), pkg.Len(),
			// 	neg[:min(32, len(neg))],
			// )
			pkg.WithSelection(neg)
			if err := table.Replace(ctx, pkg, pack.WriteModeIncludeSelected); err != nil {
				return err
			}
			pkg.WithSelection(sel)
			arena.Free(neg)
		}

		// close source reader
		src.Close()

		// write in-segment replaced records to history
		if hist != nil && replaced != nil {
			// copy journal segment pack and attach private selection vector
			pkg := seg.Data().Copy()

			sel := replaced.Indexes(arena.AllocUint32(replaced.Count()))
			pkg.WithSelection(sel)

			// append to history and indexes
			if err := hist.Append(ctx, pkg, pack.WriteModeIncludeSelected); err != nil {
				return err
			}

			// free copy
			pkg.Release()
		}
	}

	// Phase 2 - move journal data to table, exclude aborted and replaced records
	if seg.Data().Len() > 0 {
		aborted := seg.Aborted() // bitset of aborted records, nil when empty
		if aborted != nil || replaced != nil {
			// copy journal segment pack and attach private selection vector
			pkg := seg.Data().Copy()

			// create selection vector for all non-replaced & non-aborted records
			var live *bitset.Bitset
			switch {
			case aborted != nil && replaced != nil:
				live = aborted.Clone().Or(replaced).Neg()
			case aborted != nil:
				live = aborted.Clone().Neg()
			case replaced != nil:
				live = replaced.Clone().Neg()
			}
			n := live.Count()
			pkg.WithSelection(live.Indexes(arena.AllocUint32(n)))
			nAdd += n
			nPacks += (n + t.opts.PackSize - 1) / t.opts.PackSize
			live.Close()
			t.log.Tracef("merge phase 2: %d/%d records", pkg.NumSelected(), seg.Data().Len())

			// append active records to table and indexes
			if err := table.Append(ctx, pkg, pack.WriteModeIncludeSelected); err != nil {
				return err
			}

			// free copy
			arena.Free(pkg.Selected())
			pkg.Release()

		} else {
			// no deletes, no aborts, all records are valid post-images
			pkg := seg.Data()
			nAdd += pkg.Len()
			nPacks += (pkg.Len() + t.opts.PackSize - 1) / t.opts.PackSize
			t.log.Tracef("merge phase 2: %d records", pkg.Len())

			// fast-path (journal contains only valid post-images)
			if err := table.Append(ctx, pkg, pack.WriteModeAll); err != nil {
				return err
			}
		}
	}

	if hist != nil {
		// FIXME: howto track history table state?
		// t.log.Debugf("finalize history")
		if err := hist.Finalize(ctx, seg.State()); err != nil {
			return err
		}
	}

	// finalize will flush remaining writer packs to disk, update table state
	// and make new epoch visible by atomically replacing the table stats index
	// with the new version produced during merge
	// t.log.Debugf("finalize merge")
	if err := table.Finalize(ctx, seg.State()); err != nil {
		return err
	}

	// collect metrics
	dur := time.Since(start)
	atomic.AddInt64(&t.metrics.MergeCalls, 1)
	atomic.AddInt64(&t.metrics.MergedTuples, int64(seg.Len()))
	atomic.StoreInt64(&t.metrics.LastMergeTime, start.UnixNano())
	atomic.StoreInt64(&t.metrics.LastMergeDuration, int64(dur))
	nBytes = atomic.LoadInt64(&t.metrics.BytesWritten) - nBytes

	t.log.Debugf("merged segment %d packs=%d records=%d tombs=%d heap=%s stored=%s comp=%.2f%% in %s",
		seg.Id(), nPacks, nAdd, nStones, util.ByteSize(nHeap), util.ByteSize(nBytes),
		float64(nBytes)*100/float64(nHeap), dur)

	return nil
}
