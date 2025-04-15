// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/journal"
)

// Merge processes updates from a journal segment writing them back into table backend
// storage and indexes. Merge is idempotent, it can crash, get interrupted and restarted.
// The merged journal segment is only discarded when all data has been successfully written
// eventually. Due to journal contents masking table data it is save to merge a segment
// into on disk data vectors step wise without explicit syncronization or logging.
//
// To prevent inconsistent backend data on crash, Merge uses short-lived backend
// transactions for atomically updating all related column vectors for a particlar pack.
//
// Merge appends new record versions from insert/updated operations at the end of
// a table's data vectors and removes old row versions replaced by update/delete.
// When a history table is available, pre-images of old record versions are moved there.
//
// Merge does not block readers or writers (other than by very short-lived exclusive
// backend write transactions) but it is not concurrency safe itself. Callers to merge
// must ensure only a single thread executes a single merge operation at a time and
// journal segments are merged in order.
//
// Invariants
// - unique pk: main table contains at most one record per pk
// - unique rid: main and history table tables contain at most one record with the same rid
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
// Notable Side Effects
// - updates table statistics (single-writer private copy, copy-on-write updates)
//   - writes to a private stats index clone during merge
//   - all stats index changes are copy-on-write (re-uses tree and pack structure)
//   - when done, atomically replaces stats index ptr in table
// - updates indexes
// - replaces stored data blocks on disk
//   - atomic backend write of all blocks in a single pack
//   - followed by cache flush of all blocks (under lock)
//   - concurrent readers always see consistent version
// - invalidates cached blocks
//
// Design considerations for background merge
//
// - called by background job
// - merges one journal segment at a time
// - no database engine (user) transaction exists
// - uses short-lived backend write transactions to atomically write pack blocks
// - readers are only blocked while a single pack is written at a time
// - may stall on I/O while holding backend tx locks
// - merge may get interrupted (context cancel, crash)
// - journal data and tombstone remain authoritative source (overlay pack data)
// - MVCC still works for merged data (record metadata remains available to queries)

func (t *Table) Merge(ctx context.Context) error {
	t.mu.RLock()
	// get next segment and mark as merge in progress
	seg := t.journal.MergeNext()
	if seg != nil {
		seg.SetState(journal.SegmentStateMerging)
	}
	t.mu.RUnlock()

	for seg != nil {
		err := t.mergeJournal(ctx, seg)
		if err != nil {
			t.mu.Lock()
			seg.SetState(journal.SegmentStateComplete)
			t.mu.Unlock()
			return err
		} else {
			// TODO: find a way to remove merged segments earlier (or ignore them for queries)
			t.mu.Lock()
			// set segment done (can be garbage collected later)
			seg.SetState(journal.SegmentStateMerged)
			seg = t.journal.MergeNext()
			t.mu.Unlock()
		}
	}

	return nil
}

func (t *Table) mergeJournal(ctx context.Context, seg *journal.Segment) error {
	// init table writer
	tab := t.NewWriter()
	defer tab.Close()

	// init history writer
	var hist engine.TableWriter
	if ht, err := engine.GetEngine(ctx).UseTable(t.schema.Name() + "_history"); err == nil {
		hist = ht.NewWriter()
		defer hist.Close()
	}

	// Phase 1 - move deleted table rows to history, rewrite table packs
	tomb, mask, inJournalDeletes := seg.PrepareMerge()
	if len(mask) > 0 {
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

			if hist != nil {
				// set xmax for deleted rows
				pkg.Xmaxs().Materialize()
				xmaxs := pkg.Xmaxs().Uint64().Slice()
				rids := pkg.RowIds().Uint64().Slice()
				for i, l := 0, pkg.Len(); i < l; i++ {
					if xid, ok := tomb[rids[i]]; ok {
						xmaxs[i] = xid
					}
				}

				// insert deleted rows into history
				if err := hist.Append(ctx, pkg, pack.WriteModeIncludeSelected); err != nil {
					return err
				}

				// TODO: update history indexes
			}

			// rewrite table pack excluding deleted rows
			if err := tab.Replace(ctx, pkg, pack.WriteModeExcludeSelected); err != nil {
				return err
			}

			// update indexes, drop deleted rows
			if err := t.delFromIndexes(ctx, pkg, pack.WriteModeIncludeSelected); err != nil {
				return err
			}
		}

		// close source reader
		src.Close()
		src = nil
	}

	// Phase 2 - move journal data to table and history
	if seg.Data().Len() > 0 {
		if inJournalDeletes {
			// create selection vector for split based on xmax
			sel := arena.AllocUint32(seg.Data().Len())
			for i, v := range seg.Data().Xmaxs().Uint64().Slice() {
				if v == 0 {
					sel = append(sel, uint32(i))
				}
			}

			// copy journal segment pack and attach selection vector
			// (required because segment is shared with readers, reference blocks only)
			pkg := seg.Data().Copy().WithSelection(sel)

			// append deleted records to history
			if hist != nil {
				if err := hist.Append(ctx, pkg, pack.WriteModeExcludeSelected); err != nil {
					return err
				}

				// TODO: update history indexes
			}

			// append active records to table
			if err := tab.Append(ctx, pkg, pack.WriteModeIncludeSelected); err != nil {
				return err
			}

			// update indexes, insert new records
			if err := t.addToIndexes(ctx, pkg, pack.WriteModeIncludeSelected); err != nil {
				return err
			}

			// free resources
			pkg.Release()
			arena.Free(sel)

		} else {
			pkg := seg.Data()

			// fast-path (journal contains only post-images)
			if err := tab.Append(ctx, pkg, pack.WriteModeAll); err != nil {
				return err
			}

			// update indexes, insert new records
			if err := t.addToIndexes(ctx, pkg, pack.WriteModeAll); err != nil {
				return err
			}
		}
	}

	// finalize writers will flush remaining data to disk and update table stats
	if err := tab.Finalize(ctx); err != nil {
		return err
	}

	// TODO: maybe merge index writes into table writer, then sync on finalize

	// flush indexes
	if err := t.syncIndexes(ctx); err != nil {
		return err
	}

	if hist != nil {
		if err := hist.Finalize(ctx); err != nil {
			return err
		}

		// TODO: flush history indexes
	}

	// collect metrics
	// atomic.AddInt64(&t.metrics.FlushCalls, 1)
	// atomic.AddInt64(&t.metrics.FlushedTuples, int64(t.journal.Len()))
	// atomic.StoreInt64(&t.metrics.LastFlushTime, start.UnixNano())
	// dur := time.Since(start)
	// atomic.StoreInt64(&t.stats.LastFlushDuration, int64(dur))
	// t.log.Debugf("flush: %s table %d packs add=%d del=%d heap=%s stored=%s comp=%.2f%% in %s",
	// 	t.schema.Name(), nParts, nAdd, nDel, util.ByteSize(nHeap), util.ByteSize(nBytes),
	// 	float64(nBytes)*100/float64(nHeap), dur)

	return nil
}

func (t *Table) delFromIndexes(ctx context.Context, pkg *pack.Package, mode engine.WriteMode) error {
	for _, idx := range t.indexes {
		if err := idx.(engine.IndexEngine).DelPack(ctx, pkg, mode); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) addToIndexes(ctx context.Context, pkg *pack.Package, mode engine.WriteMode) error {
	for _, idx := range t.indexes {
		if err := idx.(engine.IndexEngine).AddPack(ctx, pkg, mode); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) syncIndexes(ctx context.Context) error {
	for _, idx := range t.indexes {
		if err := idx.(engine.IndexEngine).Sync(ctx); err != nil {
			return err
		}
	}
	return nil
}
