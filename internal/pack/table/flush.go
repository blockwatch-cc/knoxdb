// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"fmt"
	"runtime/debug"
	"sort"
	"sync/atomic"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/util"
	logpkg "github.com/echa/log"
)

func (t *Table) Flush(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mergeJournal(ctx)
}

// TODO background merge
// - make concurrency safe to be called from background writer
// - allow multiple flushed journals before merge as L0 (packs are L1)
// - allow step-wise execution (merge some number of records only)
// - support context cancellation
//
// merge journal entries into data partitions, repack, update indexes and  store
func (t *Table) mergeJournal(ctx context.Context) error {
	var (
		nParts, nBytes, nHeap, nUpd, nAdd, nDel, pending, n int                          // total stats counters
		pUpd, pAdd, pDel                                    int                          // per-pack stats counters
		start                                               time.Time = time.Now().UTC() // logging
		err                                                 error
	)

	atomic.AddInt64(&t.metrics.FlushCalls, 1)
	atomic.AddInt64(&t.metrics.FlushedTuples, int64(t.journal.Len()+t.journal.TombLen()))
	atomic.StoreInt64(&t.metrics.LastFlushTime, start.UnixNano())

	// use internal journal data slices for faster lookups
	live := t.journal.Keys
	dead := t.journal.Tomb.Bitmap.ToArray()
	jpack := t.journal.Data
	dbits := t.journal.Deleted

	// walk journal/tombstone updates and group updates by pack
	var (
		pkg                                  *pack.Package // current target pack
		jpos, tpos, jlen, tlen               int           // journal/tomb slice offsets & lengths
		nextpack, lastpack                   int           // pack list positions (not keys)
		packmin, packmax, nextmin, globalmax uint64        // data placement hints
		needsort                             bool          // true if current pack needs sort before store
		loop, maxloop                        int           // circuit breaker
	)

	// FIXME: background flush will not run inside a tx
	// open write transaction (or reuse existing tx)
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return err
	}

	// on error roll back table statistics to last valid value on storage
	defer func() {
		if e := recover(); e != nil || err != nil {
			if err != nil {
				t.log.Errorf("table %s: catching error: %v", t.schema.Name(), err)
			} else {
				t.log.Errorf("table %s: catching panic: %v", t.schema.Name(), e)
				t.log.Error(string(debug.Stack()))
			}
			t.log.Debugf("table %s: restoring statistics", t.schema.Name())
			if _, err := t.stats.Load(ctx, tx, t.schema.Name()); err != nil {
				t.log.Errorf("table %s statistics rollback failed: %v", t.schema.Name(), err)
			}
			if err != nil {
				panic(err)
			}
			if e != nil {
				panic(fmt.Errorf("Database likely corrupt."))
			}
		}
	}()

	// init global max
	jlen, tlen = len(live), len(dead)
	_, globalmax = t.stats.GlobalMinMax()
	maxloop = 2*t.stats.Len() + 2*(tlen+jlen)/t.opts.PackSize + 2

	// This algorithm works like a merge-sort over a sequence of sorted packs.
	for {
		// stop when all journal and tombstone entries have been processed
		if jpos >= jlen && tpos >= tlen {
			break
		}

		// skip deleted journal entries
		for ; jpos < jlen && dbits.IsSet(live[jpos].Idx); jpos++ {
			// t.log.Debugf("%s: skipping deleted journal entry %d/%d gmax=%d", t.schema.Name(), jpos, jlen, globalmax)
		}

		// skip processed tombstone entries
		for ; tpos < tlen && dead[tpos] == 0; tpos++ {
			// t.log.Debugf("%s: skipping processed tomb entry %d/%d gmax=%d", t.schema.Name(), tpos, tlen, globalmax)
		}

		// skip trailing tombstone entries (for unwritten journal entries)
		for ; tpos < tlen && dead[tpos] > globalmax; tpos++ {
			// t.log.Debugf("%s: skipping trailing tomb entry %d at %d/%d gmax=%d", t.schema.Name(), dead[tpos], tpos, tlen, globalmax)
		}

		// init on each iteration, either from journal or tombstone
		var nextid uint64
		switch true {
		case jpos < jlen && tpos < tlen:
			nextid = min(live[jpos].Pk, dead[tpos])
			// if nextid == live[jpos].pk {
			// 	log.Debugf("%s: next id %d from journal %d/%d, gmax=%d", t.schema.Name(), nextid, jpos, jlen, globalmax)
			// } else {
			// 	log.Debugf("%s: next id %d from tomb %d/%d, gmax=%d", t.schema.Name(), nextid, tpos, tlen, globalmax)
			// }
		case jpos < jlen && tpos >= tlen:
			nextid = live[jpos].Pk
			// t.log.Debugf("%s: next id %d from journal %d/%d, gmax=%d", t.schema.Name(), nextid, jpos, jlen, globalmax)
		case jpos >= jlen && tpos < tlen:
			nextid = dead[tpos]
			// t.log.Debugf("%s: next id %d from tomb %d/%d, gmax=%d", t.schema.Name(), nextid, tpos, tlen, globalmax)
		default:
			// stop in case remaining journal/tombstone entries were skipped
			break
		}

		// find best pack for insert/update/delete
		// skip when we're already appending to a new pack
		if lastpack < t.stats.Len() {
			nextpack, packmin, packmax, nextmin = t.findBestPack(nextid)
			// t.log.Debugf("%s: selecting next pack %d with range [%d:%d] for next pkid=%d last-pack=%d/%d next-min=%d",
			// 	t.schema.Name(), nextpack, packmin, packmax, nextid, lastpack, t.stats.Len(), nextmin)
		}

		// store last pack when nextpack changes
		if lastpack != nextpack && pkg != nil {
			// saving a pack also deletes empty packs from storage!
			if pkg.IsDirty() {
				if needsort {
					pkg.PkSort()
				}
				// t.log.Debugf("Storing pack %d with key %d with %d records", lastpack, pkg.key, pkg.Len())
				n, err = t.storePack(ctx, pkg)
				if err != nil {
					return err
				}
				nParts++
				nBytes += n
				nHeap += pkg.HeapSize()
				pending += n
				// commit storage tx after each N written packs
				// if pending >= t.opts.TxMaxSize {
				// 	// TODO: for a safe return we must also
				// 	// - clear written journal/tombstone entries
				// 	// - flush index (or implement index journal lookup)
				// 	// - write table metadata and pack headers
				// 	if tx, err = store.CommitAndContinue(tx); err != nil {
				// 		return err
				// 	}
				// 	pending = 0
				// }
				// update next values after pack index has changed
				nextpack, _, packmax, nextmin = t.findBestPack(nextid)
				// t.log.Debugf("%s: post-store next pack %d max=%d nextmin=%d", t.schema.Name(), nextpack, packmax, nextmin)
			}
			// prepare for next pack
			pkg.Release()
			pkg = nil
			needsort = false
		}

		// load or create the next pack
		if pkg == nil {
			if nextpack < t.stats.Len() {
				// t.log.Debugf("%s: loading pack %d/%d key=%d len=%d", t.schema.Name(), nextpack, t.stats.Len(), t.stats.packs[nextpack].Key, t.stats.packs[nextpack].NValues)
				info, ok := t.stats.GetPos(nextpack)
				if ok {
					pkg, err = t.loadWritablePack(ctx, info.Key, info.NValues)
					if err != nil {
						return err
					}
				}
				// when no block data is found, pack contains nil pointers only
				if pkg.IsNil() {
					pkg.Release()
					pkg = nil
				}
			}
			// start new pack
			if pkg == nil {
				nextpack = t.stats.Len()
				packmin = 0
				packmax = 0
				nextmin = 0
				pkg = pack.New().
					WithKey(t.stats.NextKey()).
					WithSchema(t.schema).
					WithMaxRows(t.opts.PackSize).
					Alloc()
				// t.log.Debugf("%s: starting new pack %d/%d with key %d", t.schema.Name(), nextpack, t.stats.Len(), pkg.key)
			}
			lastpack = nextpack
			pAdd = 0
			pDel = 0
			pUpd = 0
		}

		// t.log.Debugf("Loop %d: tomb=%d/%d journal=%d/%d", loop, tpos, tlen, jpos, jlen)
		loop++
		if loop > 2*maxloop {
			t.log.Errorf("pack: %s stopping infinite flush loop %d: tomb-flush-pos=%d/%d journal-flush-pos=%d/%d pack=%d/%d nextid=%d nextpack=%d",
				t.schema.Name(), loop, tpos, tlen, jpos, jlen, lastpack, t.stats.Len(), nextid, nextpack,
			)
			return fmt.Errorf("pack: %s infinite flush loop. Database likely corrupt.", t.schema.Name())
		} else if loop == maxloop {
			lvl := t.log.Level()
			t.log.SetLevel(logpkg.LevelDebug)
			defer t.log.SetLevel(lvl)
			t.log.Debugf("pack: %s circuit breaker activated: tomb-flush-pos=%d/%d journal-flush-pos=%d/%d pack=%d/%d nextid=%d nextpack=%d",
				t.schema.Name(), tpos, tlen, jpos, jlen, lastpack, t.stats.Len(), nextid, nextpack,
			)
		}

		// process tombstone records for this pack (skip for empty packs)
		if tpos < tlen && packmax > 0 && dead[tpos] <= packmax {
			// load current state of pack slices (will change after delete)
			pkcol := pkg.PkColumn()

			for ppos := 0; tpos < tlen; tpos++ {
				// next pk to delete
				pkid := dead[tpos]

				// skip already processed tombstone entries
				if pkid == 0 {
					continue
				}

				// stop on pack boundary
				if pkid > packmax {
					// t.log.Debugf("Tomb key %d does not match pack %d [%d:%d]", pkid, lastpack, packmin, packmax)
					break
				}

				// find the next matching pkid to clear
				ppos += sort.Search(len(pkcol)-ppos, func(i int) bool {
					return pkcol[i+ppos] >= pkid
				})
				if ppos == len(pkcol) || pkcol[ppos] != pkid {
					// clear from tombstone if not found
					dead[tpos] = 0
					continue
				}

				// count consecutive matches
				n := 1
				for tpos+n < tlen &&
					ppos+n < len(pkcol) &&
					pkcol[ppos+n] == dead[tpos+n] {
					n++
				}

				// remove records from all indexes
				if len(t.indexes) > 0 {
					for i := 0; i < n; i++ {
						prev, _ := pkg.ReadWire(ppos + i)
						for _, idx := range t.indexes {
							if err = idx.Del(ctx, prev); err != nil {
								return err
							}
						}
					}
				}

				// remove records from pack, changes pkcol (!)
				pkg.Delete(ppos, n)

				// mark as processed
				for i := 0; i < n; i++ {
					dead[tpos+i] = 0
				}
				nDel += n
				pDel += n

				// reload current state of pack slices
				pkcol = pkg.PkColumn()

				// update pack min/max
				packmin, packmax = 0, 0
				if l := len(pkcol); l > 0 {
					packmin, packmax = pkcol[0], pkcol[l-1]
				}

				// advance tomb pointer by one less (for-loop adds +1)
				tpos += n - 1
				// t.log.Debugf("Deleted %d tombstones from pack %d/%d with key %d", n, lastpack, t.stats.Len(), pkg.key)
			}
		} else {
			// process journal entries for this pack

			// TODO: can we optimize for bulk-insert/append, e.g. when pk > packmax?
			// journal order matters since we walk indirect
			//
			// implement a reverse-merge-sort like algorithm similar
			// to how we handle journal data, bulk update/insert/append
			// when journal data is consecutive

			for last, offs := 0, 0; jpos < jlen; jpos++ {
				// next journal key for insert/update
				key := live[jpos]

				// skip deleted journal records
				if dbits.IsSet(key.Idx) {
					continue
				}

				// stop on pack boundary
				if nextmin > 0 && key.Pk >= nextmin {
					// best, min, max, _ := t.findBestPack(key.pk)
					// t.log.Debugf("Key %d does not fit into pack %d [%d:%d], suggested %d/%d [%d:%d] nextmin=%d",
					// 	key.pk, lastpack, packmin, packmax, best, t.stats.Len(), min, max, nextmin)
					break
				}

				// check if record exists: packs are sorted by pk, so we can
				// safely skip ahead using the last offset, if the pk does
				// not exist we know the insert position right away; insert
				// will have to move all block slices by +1 so it is highly
				// inefficient for massive amounts of out-of-order inserts
				offs, last = pkg.FindPk(key.Pk, last)
				var isOOInsert bool

				if offs > -1 {
					// update existing record

					// replace index records when data has changed
					if len(t.indexes) > 0 {
						prev, _ := pkg.ReadWire(offs)
						next, _ := jpack.ReadWire(key.Idx)
						for _, idx := range t.indexes {
							if err = idx.Add(ctx, prev, next); err != nil {
								return err
							}
						}
					}

					// overwrite original
					if err = pkg.ReplacePack(jpack, offs, key.Idx, 1); err != nil {
						return err
					}
					nUpd++
					pUpd++

					// next journal record
					continue

				} else {
					// detect out of order inserts
					isOOInsert = key.Pk < packmax

					// split on out-of-order inserts into a full pack
					if isOOInsert && pkg.IsFull() {
						t.log.Warnf("flush: split %s table pack %d [%d:%d] at out-of-order insert key %d ",
							t.schema.Name(), pkg.Key(), packmin, packmax, key.Pk)

						// keep sorted
						if needsort {
							pkg.PkSort()
							needsort = false
						}
						// split pack
						n, err = t.splitPack(ctx, pkg)
						if err != nil {
							return err
						}
						nParts++
						nBytes += n
						nHeap += pkg.HeapSize()

						// leave journal for-loop to trigger new pack selection
						loop = 0      // reset circuit breaker check
						lastpack = -1 // force pack load in next round
						pkg.Release()
						pkg = nil
						break
					}

					// Don't insert when pack is full to prevent buffer overflows. This may
					// happen when the current full pack was selected for a prior update,
					// but no re-selection happened before this insert.
					//
					// Reason is that the above boundary check does not always work, in
					// particular for the edge case of the very last pack because
					// nextmin = 0 in this case.
					//
					if pkg.IsFull() {
						break
					}

					// insert new record
					if isOOInsert {
						// insert in-place (EXPENSIVE!)
						// t.log.Debugf("Insert key %d to pack %d", key.pk, lastpack)
						if err = pkg.InsertPack(jpack, last, key.Idx, 1); err != nil {
							return err
						}
						packmin = util.NonZeroMin(packmin, key.Pk)
					} else {
						// append new records
						// t.log.Debugf("Append key %d to pack %d", key.pk, lastpack)
						if err = pkg.AppendPack(jpack, key.Idx, 1); err != nil {
							return err
						}
						packmax = max(packmax, key.Pk)
						globalmax = max(globalmax, key.Pk)
					}

					// add to indexes
					if len(t.indexes) > 0 {
						next, _ := jpack.ReadWire(key.Idx)
						for _, idx := range t.indexes {
							if err = idx.Add(ctx, nil, next); err != nil {
								return err
							}
						}
					}
				}
				nAdd++
				pAdd++

				// save when full
				if pkg.IsFull() {
					// keep sorted
					if needsort {
						pkg.PkSort()
						needsort = false
					}

					// store pack, will update t.stats
					// t.log.Debugf("%s: storing pack %d with %d records at key %d", t.schema.Name(), lastpack, pkg.Len(), pkg.key)
					n, err = t.storePack(ctx, pkg)
					if err != nil {
						return err
					}
					nParts++
					nBytes += n
					pending += n
					nHeap += pkg.HeapSize()

					// commit tx after each N written packs
					// if pending >= t.opts.TxMaxSize {
					// 	// TODO: for a safe return we must also
					// 	// - clear written journal/tombstone entries
					// 	// - flush index (or implement index journal lookup)
					// 	// - write table metadata and pack headers
					// 	//
					// 	if tx, err = store.CommitAndContinue(tx); err != nil {
					// 		return err
					// 	}
					// pending = 0
					// }

					// after store, leave journal for-loop to trigger pack selection
					jpos++
					lastpack = -1 // force pack load in next round
					pkg.Release()
					pkg = nil
					break
				}
			}
		}
	}

	// store last processed pack
	if pkg != nil && pkg.IsDirty() {
		if needsort {
			pkg.PkSort()
		}
		// t.log.Debugf("Storing final pack %d with %d records at key %d", lastpack, pkg.Len(), pkg.key)
		n, err = t.storePack(ctx, pkg)
		if err != nil {
			return err
		}
		nParts++
		nBytes += n
		nHeap += pkg.HeapSize()
		pkg.Release()
		pkg = nil
	}

	dur := time.Since(start)
	atomic.StoreInt64(&t.metrics.LastFlushDuration, int64(dur))
	t.log.Debugf("flush: %s table %d packs add=%d del=%d heap=%s stored=%s comp=%.2f%% in %s",
		t.schema.Name(), nParts, nAdd, nDel, util.ByteSize(nHeap), util.ByteSize(nBytes),
		float64(nBytes)*100/float64(nHeap), dur)

	// flush indexes
	for _, idx := range t.indexes {
		if err = idx.Sync(ctx); err != nil {
			return err
		}
	}

	// fix row count which becomes wrong after delete
	if c := t.stats.Count(); uint64(c) != t.state.NRows {
		atomic.StoreInt64(&t.metrics.TupleCount, int64(c))
		t.state.NRows = uint64(c)
		t.engine.Catalog().SetState(t.tableId, t.state.ToObjectState())

		// FIXME: background flush will not run inside a tx
		engine.GetTransaction(ctx).Touch(t.tableId)
	}

	// clear journal and tombstone
	t.journal.Reset()

	// save (now empty) journal and tombstone
	return t.storeJournal(ctx)
}

func (t *Table) storeJournal(ctx context.Context) error {
	// use write transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return err
	}

	nTuples, nTomb := t.journal.Len(), t.journal.TombLen()
	nJournalBytes, nTombBytes, err := t.journal.StoreLegacy(ctx, tx, t.schema.Name())
	if err != nil {
		return err
	}
	atomic.AddInt64(&t.metrics.JournalTuplesFlushed, int64(nTuples))
	atomic.AddInt64(&t.metrics.JournalPacksStored, 1)
	atomic.AddInt64(&t.metrics.JournalBytesWritten, int64(nJournalBytes))
	atomic.AddInt64(&t.metrics.TombstoneTuplesFlushed, int64(nTomb))
	atomic.AddInt64(&t.metrics.TombstonePacksStored, 1)
	atomic.AddInt64(&t.metrics.TombstoneBytesWritten, int64(nTombBytes))
	atomic.StoreInt64(&t.metrics.JournalDiskSize, int64(nJournalBytes))
	atomic.StoreInt64(&t.metrics.TombstoneDiskSize, int64(nTombBytes))

	return nil
}

// Use pack index to find closest match for placing pk based on min/max of the
// pk column. Handles gaps in the pk sequence inside packs and gaps between packs.
// This may happen after delete or when pk values are user-defined.
//
// Attention!
//
// Out-of-order pk inserts or delete+reinsert of the same keys will lead to
// fragmentation. See mergeJournal() for details.
//
// The placement algorithm works as follows:
// - keep lastpack when no pack exists (effectively == 0)
// - choose pack with pack.min <= val <= pack.max
// - choose pack with closest max < val
// - when val < min of first pack, choose first pack
func (t *Table) findBestPack(pk uint64) (int, uint64, uint64, uint64) {
	// returns 0 when list is empty, this ensures we initially stick
	// to the first pack until it's full; returns last pack for values
	// > global max
	bestpack, min, max, nextmin, isFull := t.stats.Best(pk)
	// t.log.Debugf("find: best=%d min=%d max=%d nextmin=%d, isFull=%t opts=%v",
	// 	bestpack, min, max, nextmin, isFull, t.opts)

	// insert/update placement into an exsting pack's range always stays with this pack

	// hacker's delight trick for unsigned range checks
	// see https://stackoverflow.com/questions/17095324/fastest-way-to-determine-if-an-integer-is-between-two-integers-inclusive-with
	// pk >= min && pk <= max
	if !isFull || pk-min <= max-min {
		// t.log.Debugf("%s: %d is full=%t or pk %d is in range [%d:%d]", t.schema.Name(), bestpack, isFull, pk, min, max)
		return bestpack, min, max, nextmin
	}

	// if pack is full check if there is room in the next pack, but protect
	// invariant by checking pk against next pack's min value
	if isFull && nextmin > 0 && pk < nextmin {
		nextbest, min, max, nextmin, isFull := t.stats.Next(bestpack)
		if min+max > 0 && !isFull {
			// t.log.Debugf("%s: %d is full, but next pack %d exists and is not", t.schema.Name(), bestpack, nextbest)
			return nextbest, min, max, nextmin
		}
	}

	// trigger new pack creation
	// t.log.Debugf("%s: Should create new pack for key=%d: isfull=%t min=%d, max=%d nextmin=%d", t.schema.Name(), pk, isFull, min, max, nextmin)
	return t.stats.Len(), 0, 0, 0
}
