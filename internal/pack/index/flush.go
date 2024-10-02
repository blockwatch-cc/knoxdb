// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
)

// merge journal entries into data partitions, repack, store, and update metadata
func (idx *Index) flush(ctx context.Context) error {
	// NEW ALGO (WIP) <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	// TODO: replace append+quicksort with reverse mergesort
	//
	// requires sorted journal and tomb
	// idx.journal.PkSort()
	// idx.tomb.PkSort()

	// direct access to pk columns of both journal parts
	// jkeys, tkeys := idx.journal.PkColumn(), idx.tomb.PkColumn()
	// jlen, tlen := len(jkeys), len(tkeys)
	// var jpos, tpos int

	// // iterator to load the next usable package in pk order for writing
	// it := NewIndexFlushIterator(idx)
	// defer it.Close()

	// // init global max
	// _, maxPk := idx.stats.GlobalMinMax()
	// if jlen > 0 {
	//  maxPk = max(maxPk, jkeys[jlen-1])
	// }

	// for {
	//  // stop when all journal and tomb records are processed
	//  if jpos >= jlen && tpos >= tlen {
	//      break
	//  }

	//  // skip trailing tomb records (for unwritten journal records)
	//  // TODO: most likely not relevant for index packs
	//  for tpos < tlen && tkeys[tpos] > maxPk {
	//      tpos++
	//  }

	//  // skip equal records in journal and tomb (i.e. inserted + deleted)

	//  var nextPk uint64
	//  // init on each iteration, either from journal or tombstone
	//  switch true {
	//  case jpos < jlen && tpos < tlen:
	//      nextPk = min(jkeys[jpos], tkeys[tpos])
	//  case jpos < jlen && tpos >= tlen:
	//      nextPk = jkeys[jpos]
	//  case jpos >= jlen && tpos < tlen:
	//      nextPk = tkeys[tpos]
	//  default:
	//      // stop in case remaining journal/tombstone entries were skipped
	//      break
	//  }

	//  // Algo design
	//  // - find next pack with matching pk (journal or tomb)
	//  // - create fresh writable pack
	//  // - merge prev pack & journal (up to next min) while skipping tomb keys
	//  // - when full, store pack under same

	//  // get package pk range
	//  // get all tomb records matching this range
	//  // get all journal records matching this range (up until nextMin if not last)
	//  // delete tomb records from pack
	//  // insert journal records until pack is full
	//  // if pack is full, split

	// }
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	start := time.Now().UTC()
	lvl := idx.log.Level()

	idx.journal.PkSort()
	idx.tomb.PkSort()
	pk, dead := idx.journal.PkColumn(), idx.tomb.PkColumn()
	pkval := idx.journal.Block(1).Uint64().Slice()
	deadval := idx.tomb.Block(1).Uint64().Slice()

	var nAdd, nDel, nParts, nBytes int

	// Mark deleted journal records first (set value to zero; zero keys have
	// meaning for hash indexes)
	if len(pk) > 0 && len(dead) > 0 {
		// start at the first tombstone record that may be in journal
		var d1, j1 int
		d1 = sort.Search(len(dead), func(x int) bool { return dead[x] >= pk[0] })

		// start at the first journal record that may be in tombstone
		if d1 < len(dead) {
			j1 = sort.Search(len(pk), func(x int) bool { return pk[x] >= dead[d1] })
		}

		for j, d, jl, dl := j1, d1, len(pk), len(dead); j < jl && d < dl; {
			// find the next matching journal pos where key >= tomb record
			j += sort.Search(jl-j, func(x int) bool { return pk[x+j] >= dead[d] })

			// stop at pack end
			if j == jl {
				break
			}

			// if no match was found, advance tomb pointer
			for d < dl && pk[j] > dead[d] {
				d++
			}

			// stop at tomb end
			if d == dl {
				break
			}

			// ensure we only delete real matches by checking key AND value
			for dead[d] == pk[j] && j < jl {
				// we expect at most one match in value
				if deadval[d] == pkval[j] {
					// mark journal records as processed
					pkval[j] = 0

					// mark tomb records as processed
					deadval[d] = 0

					// advance pointers
					nDel++
					j++
					break
				}
				j++
			}
			d++
		}
		// log.Debugf("pack: %s flush marked %d dead journal records", idx.name(), nDel)
	}

	// walk journal/tombstone and group updates by pack
	var (
		pkg                         *pack.Package // current target pack
		packsz                      int           // target pack size
		jpos, tpos, jlen, tlen      int           // journal/tomb slice offsets & lengths
		lastpack, nextpack          int           // pack list positions (not keys)
		nextid                      uint64        // next index key to process (tomb or journal)
		packmax, nextmin, globalmax uint64        // data placement hints
		needsort                    bool          // true if current pack needs sort before store
		loop, maxloop               int           // circuit breaker
		pending                     int           // tx size counter
	)

	// init
	// packsz = idx.opts.PackSize()
	// jlen, tlen = len(pk), len(dead)
	// _, globalmax = idx.packidx.GlobalMinMax()
	// maxloop = 2*idx.packidx.Len() + 2*jlen/packsz + 2 // 2x to consider splits

	// create an initial pack on first insert
	if idx.stats.Len() == 0 {
		pkg = pack.New().
			WithKey(idx.stats.NextKey()).
			WithMaxRows(idx.opts.PackSize).
			WithSchema(idx.schema).
			Alloc()
	}

	// open write transaction (or reuse existing tx)
	// tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, true)
	// if err != nil {
	// 	return err
	// }

	// This algorithm works like a merge-sort over a sequence of sorted packs.
	for {
		// stop when all journal and tombstone entries have been processed
		if jpos >= jlen && tpos >= tlen {
			break
		}

		// skip deleted journal entries
		for ; jpos < jlen && pkval[jpos] == 0; jpos++ {
		}

		// skip processed tombstone entries
		for ; tpos < tlen && deadval[tpos] == 0; tpos++ {
		}

		// skip trailing tombstone entries (for unwritten journal entries)
		// TODO: most likely not relevant for index packs
		for ; tpos < tlen && dead[tpos] > globalmax; tpos++ {
		}

		// init on each iteration, either from journal or tombstone
		switch true {
		case jpos < jlen && tpos < tlen:
			nextid = min(pk[jpos], dead[tpos])
		case jpos < jlen && tpos >= tlen:
			nextid = pk[jpos]
		case jpos >= jlen && tpos < tlen:
			nextid = dead[tpos]
		default:
			// stop in case remaining journal/tombstone entries were skipped
			break
		}

		// find best pack for inserting/deleting next record
		nextpack, _, packmax, nextmin, _ = idx.stats.Best(nextid)
		// log.Debugf("Next pack %d max=%d nextmin=%d", nextpack, packmax, nextmin)

		// store last pack when nextpack changes
		if lastpack != nextpack && pkg != nil {
			if pkg.IsDirty() {
				// keep pack sorted
				if needsort {
					pkg.PkSort()
				}
				// log.Debugf("%s: storing pack %d with %d records", idx.name(), pkg.key, pkg.Len())
				n, err := idx.storePack(ctx, pkg)
				if err != nil {
					return err
				}
				nParts++
				nBytes += n
				pending += n
				// commit storage tx after each N written packs
				// if pending >= idx.opts.TxMaxSize {
				// 	tx, err = store.CommitAndContinue(tx)
				// 	if err != nil {
				// 		return err
				// 	}
				// 	pending = 0
				// }
				// update next values after pack index has changed
				nextpack, _, packmax, nextmin, _ = idx.stats.Best(nextid)
				// log.Debugf("%s: post-store next pack %d max=%d nextmin=%d",
				//  idx.name(), nextpack, packmax, nextmin)
			}
			// prepare for next pack
			pkg = nil
			needsort = false
		}

		// load the next pack
		if pkg == nil {
			var err error
			info, _ := idx.stats.GetPos(nextpack)
			pkg, err = idx.loadWritablePack(ctx, info.Key, info.NValues)
			if err != nil {
				return err
			}
			lastpack = nextpack
			// log.Debugf("%s: loaded pack %d with %d records", idx.name(), pkg.key, pkg.Len())
		}

		// circuit breaker
		loop++
		if loop > 2*maxloop {
			idx.log.Errorf("knox: %s stopping infinite flush loop %d: tomb-flush-pos=%d/%d journal-flush-pos=%d/%d pack=%d/%d nextid=%d",
				idx.schema.Name(), loop, tpos, tlen, jpos, jlen, lastpack, idx.stats.Len(), nextid,
			)
			return fmt.Errorf("pack: %s infinite flush loop detected. Database is likely corrupted.", idx.schema.Name())
		} else if loop > maxloop {
			idx.log.SetLevel(log.LevelDebug)
			idx.log.Debugf("knox: %s circuit breaker activated at loop %d tomb-flush-pos=%d/%d journal-flush-pos=%d/%d pack=%d/%d nextid=%d",
				idx.schema.Name(), loop, tpos, tlen, jpos, jlen, lastpack, idx.stats.Len(), nextid,
			)
		}

		// process tombstone records for this pack (skip for empty packs)
		if tpos < tlen && packmax > 0 && dead[tpos] <= packmax {
			// load current state of pack slices (will change after delete)
			keycol := pkg.PkColumn()
			valcol := pkg.Block(1).Uint64().Slice()

			for ppos := 0; tpos < tlen; tpos++ {
				// skip already processed tombstone records
				if deadval[tpos] == 0 {
					continue
				}

				// next pk to delete
				key := dead[tpos]

				// stop on pack boundary
				if key > packmax {
					break
				}

				// find the next matching key to clear
				ppos += sort.Search(len(keycol)-ppos, func(i int) bool { return keycol[i+ppos] >= key })
				if ppos == len(keycol) || keycol[ppos] != key {
					// clear from tombstone if not found
					deadval[tpos] = 0
					continue
				}

				// count consecutive matches
				n := 1
				for tpos+n < tlen && // until tomb end
					ppos+n < len(keycol) && // until pack end
					keycol[ppos+n] == dead[tpos+n] && // key must match
					valcol[ppos+n] == deadval[tpos+n] { // value must match
					n++
				}

				// remove n records from pack, changes keycol & valcol (!)
				pkg.Delete(ppos, n)

				// mark as processed
				for i := 0; i < n; i++ {
					deadval[tpos+i] = 0
				}
				nDel += n

				// reload current state of pack slices
				keycol = pkg.PkColumn()
				valcol = pkg.Block(1).Uint64().Slice()

				// update pack max
				packmax = 0
				if l := len(keycol); l > 0 {
					packmax = keycol[l-1]
				}

				// advance tomb pointer by one less (for-loop adds +1)
				tpos += n - 1
			}
		}

		// process journal records for this pack (insert only, no update)
		for jpos < jlen {
			// skip deleted journal records
			if pkval[jpos] == 0 {
				jpos++
				continue
			}

			// stop on pack boundary
			if nextmin > 0 && pk[jpos] >= nextmin {
				break
			}

			// count consecutive matches, stop at removed records
			// and when crossing the next pack's boundary
			n, l := 1, pkg.Len()
			for jpos+n < jlen && // until journal end
				l+n < packsz && // until pack is full
				(nextmin == 0 || pk[jpos+n] < nextmin) && // until next pack's min boundary (!invariant)
				pkval[jpos+n] > 0 { // only non-deleted records
				n++
			}

			// append journal records
			if err := pkg.AppendPack(idx.journal, jpos, n); err != nil {
				return err
			}

			// update state
			needsort = needsort || pk[jpos] < packmax
			packmax = util.Max(packmax, pk[jpos])
			globalmax = util.Max(globalmax, packmax)
			nAdd += n
			jpos += n

			// split when full
			if pkg.Len() == packsz {
				if needsort {
					pkg.PkSort()
					needsort = false
				}
				// log.Debugf("%s: split pack %d with %d records", idx.name(), pkg.key, pkg.Len())
				n, err := idx.splitPack(ctx, pkg)
				if err != nil {
					return err
				}
				nParts++
				nBytes += n
				pending += n
				lastpack = -1 // force pack load in next round
				pkg = nil

				// commit tx after each N written packs
				// if pending >= idx.opts.TxMaxSize {
				// 	// TODO: for a safe return we must also
				// 	// - mark or clear written journal records
				// 	// - save journal
				// 	// - commit tx
				// 	tx, err = store.CommitAndContinue(tx)
				// 	if err != nil {
				// 		return err
				// 	}
				// 	pending = 0
				// }

				// leave journal for-loop and trigger new pack selection
				break
			}
		}
	}

	// store last processed pack
	if pkg != nil && pkg.IsDirty() {
		if needsort {
			pkg.PkSort()
		}
		// log.Debugf("%s: storing final pack %d with %d records", idx.name(), pkg.key, pkg.Len())
		n, err := idx.storePack(ctx, pkg)
		if err != nil {
			return err
		}
		pkg = nil
		nParts++
		nBytes += n
	}

	// update counters
	atomic.StoreInt64(&idx.metrics.TupleCount, int64(idx.stats.Count()))
	atomic.StoreInt64(&idx.metrics.MetaSize, int64(idx.stats.HeapSize()))
	atomic.StoreInt64(&idx.metrics.TotalSize, int64(idx.stats.TableSize()))
	atomic.StoreInt64(&idx.metrics.LastFlushTime, start.UnixNano())
	atomic.StoreInt64(&idx.metrics.LastFlushDuration, int64(time.Since(start)))

	idx.log.Debugf("pack: %s flushed %d packs add=%d/%d del=%d/%d total_size=%s in %s",
		idx.schema.Name(), nParts, nAdd, idx.journal.Len(), nDel, idx.tomb.Len(),
		util.ByteSize(nBytes), time.Duration(idx.metrics.LastFlushDuration))

	// ignore any remaining records
	idx.tomb.Clear()
	idx.journal.Clear()
	idx.log.SetLevel(lvl)

	return nil
}
