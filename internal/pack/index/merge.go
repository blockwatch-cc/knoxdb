// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/util"
)

// MergeValue is a helper to compare multi-key records during merge.
type MergeValue struct {
	Ik uint64 // indexed key
	Pk uint64 // primary key
	Ok bool   // flag indicating this value is initialized
}

func NewMergeValue(ik, pk uint64) MergeValue {
	return MergeValue{
		Ik: ik,
		Pk: pk,
		Ok: true,
	}
}

func (v *MergeValue) Reset() {
	v.Ik = 0
	v.Pk = 0
	v.Ok = false
}

func (v MergeValue) IsValid() bool {
	return v.Ok
}

func (v MergeValue) Equal(w MergeValue) bool {
	return v.Ok && w.Ok && v.Ik == w.Ik && v.Pk == w.Pk
}

func (v MergeValue) Less(w MergeValue) bool {
	switch {
	case v.Ok && w.Ok:
		return v.Ik < w.Ik || (v.Ik == w.Ik && v.Pk < w.Pk)
	case v.Ok:
		return true
	default:
		return false
	}
}

// MergeIterator is a helper to locate, load and store index packs.
type MergeIterator struct {
	idx    *Index                // ref to index
	tx     store.Tx              // current write tx
	cur    store.Cursor          // current read cursor
	pack   *pack.Package         // current read-only pack
	bcache engine.BlockCacheType // cache reference
	last   MergeValue            // current pack keys (used on store/delete)
	lastSz int                   // current pack data size (to calculate diff)

	// stats
	nTxBytes      int // pending tx bytes to write
	nDel          int // number of records deleted from index
	nIns          int // number of records inserted into index
	nBytesRead    int // total bytes read by merge
	nBytesWritten int // total bytes written by merge
	nBytesDiff    int // storage size diff after merge
	nPacksLoaded  int // number of packs read during merge
	nPacksStored  int // number of packs written during merge
	nPacksDiff    int // pack count diff after merge
}

func NewMergeIterator(idx *Index) *MergeIterator {
	return &MergeIterator{
		idx: idx,
	}
}

func (it *MergeIterator) Close() {
	if it.pack != nil {
		it.pack.Release()
		it.pack = nil
	}
	if it.cur != nil {
		it.cur.Close()
		it.cur = nil
	}
	if it.tx != nil {
		it.tx.Rollback()
		it.tx = nil
	}
	it.idx = nil
	it.bcache = nil
	it.nTxBytes = 0
	it.last.Reset()
}

func (it *MergeIterator) NewPack() *pack.Package {
	return pack.New().
		WithSchema(it.idx.schema).
		WithMaxRows(it.idx.opts.PackSize).
		Alloc()
}

func (it *MergeIterator) Complete(ctx context.Context) error {
	// commit storage tx when data is pending
	if it.nTxBytes == 0 {
		return nil
	}
	if it.cur != nil {
		it.cur.Close()
		it.cur = nil
	}
	if it.tx != nil {
		if err := it.tx.Commit(); err != nil {
			return err
		}
		it.tx = nil
		it.nTxBytes = 0
	}
	return nil
}

func (it *MergeIterator) UpdateIndexState(ctx context.Context) error {
	// no change
	if it.nIns+it.nDel == 0 {
		return nil
	}

	// require store tx
	if it.tx == nil {
		return engine.ErrNoTx
	}

	// row count
	if it.nIns != it.nDel {
		n := max(0, int(it.idx.state.NRows)+it.nIns-it.nDel)
		it.idx.state.NRows = uint64(n)
	}

	// byte size
	if it.nBytesDiff != 0 {
		n := max(0, int(it.idx.state.Size)+it.nBytesDiff)
		it.idx.state.Size = uint64(n)
	}

	// pack count
	if it.nPacksDiff != 0 {
		n := max(0, int(it.idx.state.Count)+it.nPacksDiff)
		it.idx.state.Size = uint64(n)
	}

	return it.idx.state.Store(ctx, it.tx, it.idx.schema.Name())
}

func (it *MergeIterator) Store(pkg *pack.Package) error {
	// init data bucket
	bucket := it.idx.dataBucket(it.tx)
	if bucket == nil {
		return engine.ErrNoBucket
	}

	// keep loaded keys or compute new block keys from first record when zero
	id := it.last
	if !id.IsValid() && pkg.Len() > 0 {
		id = NewMergeValue(pkg.Uint64(0, 0), pkg.Uint64(1, 0))
		it.nPacksDiff++
	}

	// encode and store blocks
	var n int
	for i := range []int{0, 1} {
		var err error
		key := it.idx.encodePackKey(id.Ik, id.Pk, i)
		if pkg.Len() == 0 {
			err = bucket.Delete(key)
			it.nTxBytes++
		} else {
			// it.idx.log.Infof("Storing block 0x%016x:%016x:%d", id.Ik, id.Pk, i)
			b := pkg.Block(i)
			buf := bytes.NewBuffer(make([]byte, 0, b.MaxStoredSize()))
			_, err = b.WriteTo(buf)
			if err == nil {
				err = bucket.Put(key, buf.Bytes())
			}
			n += buf.Len()
			b.SetClean()
		}
		if err != nil {
			return err
		}
		// drop cache keys
		if it.bcache != nil {
			it.bcache.Remove(it.idx.encodeCacheKey(id.Ik, id.Pk, i))
		}
	}

	// update counters
	it.nTxBytes += n
	it.nBytesWritten += n
	it.nBytesDiff += n - it.lastSz
	it.lastSz = 0

	// reset last pack reference, this ensures the next pack will use new keys
	it.last.Reset()
	it.nPacksStored++

	return nil
}

func (it *MergeIterator) Next(ctx context.Context, id MergeValue) (*pack.Package, MergeValue, error) {
	// release last source pack
	if it.pack != nil {
		it.pack.Release()
		it.pack = nil
	}

	// commit/continue storage tx (must close and reinit cursor)
	// we do this here because in case multiple 'split' packs are
	// written during merge from the same src pack we must commit
	// them atomically. The max size of such a write is bounded
	// by journal size (we never insert more data than in journal).
	if it.nTxBytes >= it.idx.opts.TxMaxSize {
		var err error
		it.cur.Close()
		it.cur = nil
		it.tx, err = store.CommitAndContinue(it.tx)
		if err != nil {
			return nil, MergeValue{}, err
		}
		it.cur = it.idx.dataBucket(it.tx).Cursor()
		it.nTxBytes = 0
	}

	// init cursor
	if it.cur == nil {
		var err error
		it.tx, err = it.idx.db.Begin(true)
		if err != nil {
			return nil, MergeValue{}, err
		}
		if bucket := it.idx.dataBucket(it.tx); bucket != nil {
			it.cur = bucket.Cursor()
		} else {
			return nil, MergeValue{}, engine.ErrNoBucket
		}
		if e := engine.GetEngine(ctx); e != nil {
			it.bcache = e.BlockCache()
		}
	}

	// load and materialize pack that matches ik+pk combi or create new pack
	if err := it.loadNextPack(id); err != nil {
		return nil, MergeValue{}, err
	}

	// cursor either points one key behind the loaded pack's blocks now or
	// is invalid in which case we created a new pack at the end. we try
	// decoding the next key either way which yields zero or correct ids
	// which we'll use as boundary during merge.
	if it.cur.Key() != nil {
		id.Ik, id.Pk, _, _ = it.idx.decodePackKey(it.cur.Key())
		id.Ok = true
		// it.idx.log.Infof("Merge: next id 0x%016x:%016x", id.Ik, id.Pk)
	} else {
		id.Reset()
		// it.idx.log.Infof("Merge: invalid next id")
	}

	// return current pack and boundary of next pack
	return it.pack, id, nil
}

func (it *MergeIterator) loadNextPack(search MergeValue) error {
	// seek to search position (this likely does not exist and we will find the next
	// higher pack in which case we rewind)
	ok := it.cur.Seek(it.idx.encodePackKey(search.Ik, search.Pk, 0))
	// it.idx.log.Infof("Merge: Seek 0x%016x:%016x:%d ok=%t", search.Ik, search.Pk, 0, ok)

	// seek to last pack when not found (our search key is likely in this pack)
	if !ok {
		// if exists this will set cur to the first block in the last pair
		it.cur.Last()
		ok = it.cur.Prev()
		// it.idx.log.Infof("Merge: Seek last>prev %t, key=%x", ok, it.cur.Key())
	}

	// no last pack? this must be an empty bucket, we'll create our first pack
	if !ok {
		// it.idx.log.Infof("Merge: no pack found")
		return nil
	}

	// try decode the key
	ik, pk, id, err := it.idx.decodePackKey(it.cur.Key())
	if err != nil {
		return err
	}
	// it.idx.log.Infof("Merge: Found 0x%016x:%016x:%d", ik, pk, id)

	// rewind if we're behind the search key
	if ik > search.Ik || pk > search.Pk {
		// if exists double prev will set cur the first block of the previous pair
		it.cur.Prev()
		ok = it.cur.Prev()
		// it.idx.log.Infof("Merge: Rewind prev>prev %t", ok)

		// decode the previous key
		if ok {
			ik, pk, id, err = it.idx.decodePackKey(it.cur.Key())
			if err != nil {
				return err
			}
			// it.idx.log.Infof("Merge: Now 0x%016x:%016x:%d", ik, pk, id)
			// assert we're actually at the first block
			assert.Always(id == 0, "must be at first block in index pair")
		}
	}

	// looks like this was the first pack and it did not contain
	// our search key, we're going to create a new pack and place it in front
	if !ok {
		// it.idx.log.Infof("Merge: must create new pack in front")
		return nil
	}

	// we're at the correct pack now, load blocks into package
	it.pack = pack.New().
		WithSchema(it.idx.schema).
		WithMaxRows(it.idx.opts.PackSize)

	// load block pair from cursor
	var n int
	for i := range []int{0, 1} {
		// assert block is correct
		bik, bpk, bid, err := it.idx.decodePackKey(it.cur.Key())
		if err != nil {
			return fmt.Errorf("loading block 0x%08x:%08x:%d: %v", bik, bpk, bid, err)
		}
		assert.Always(bid == i, "unexpected block id", "ik", bik, "pk", bpk, "id", bid)

		// it.idx.log.Infof("Merge: Loading block 0x%016x:%016x:%d", bik, bpk, i)

		// create and decode block
		b := block.New(
			types.BlockTypes[it.idx.schema.Exported()[i].Type],
			it.idx.opts.PackSize,
		)
		if err := b.Decode(it.cur.Value()); err != nil {
			return fmt.Errorf("loading block 0x%08x:%08x:%d: %v", bik, bpk, bid, err)
		}
		it.pack.WithBlock(i, b)
		n += len(it.cur.Value())

		// go to next storage key (may be end of bucket)
		it.cur.Next()
	}
	it.nPacksLoaded++
	it.last = NewMergeValue(ik, pk)
	it.lastSz = n
	it.nBytesRead += n

	return nil
}

// Merge journal and tomb entries into data partitions, repack and store.
//
// Algo design
//   - find source pack with where next to be merged pk (journal or tomb) is in range
//   - create a new writable output pack, inherit the source pack's key on first output
//   - merge src & journal records into output while skipping tomb entries
//   - note tomb entries cancel out both source and journal entries (both may exist)
//   - stop when journal or tomb records cross the next pack's min boundary
//   - when output pack is full (this happens when inserting more records than deleting)
//     -- save the current output pack
//     -- start a new output pack
//
// Data placement
//   - block vector KV keys are generated from the first record in each pack
//     with format `index key + primary key + block id`
//   - merge iterator peeks into next KV store key to identify next pack's start ids
//
// Edge cases
//   - nextMinIk & nextMinPk can be 0 (empty index, behind last index pack)
//   - entire pack runs empty -> drop from storage (iterator remembers last read KV keys)
//   - first record in a pack is deleted (on store we keep the original KV keys)
//   - first append (empty src, empty tomb, full journal)
//   - delete only (empty journal, tomb deletes all src records)
//   - stale tombstones without match in journal/source may exist
//     when merge restarts after error while packs have been committed before
func (idx *Index) merge(ctx context.Context) error {
	// measure stats
	start := time.Now()

	// direct access to pk columns of both journal parts
	jkeys := idx.journal.Block(0).Uint64().Slice()
	jpks := idx.journal.Block(1).Uint64().Slice()
	tkeys := idx.tomb.Block(0).Uint64().Slice()
	tpks := idx.tomb.Block(1).Uint64().Slice()
	jlen, tlen := len(jkeys), len(tkeys)
	var jpos, tpos int

	// co-sort journal and tomb columns in-place
	util.Sort2(jkeys, jpks)
	util.Sort2(tkeys, tpks)

	// iterator to lookup matching packages
	it := NewMergeIterator(idx)
	defer it.Close()

	// 3-way merge into packs
	for {
		// check context
		if err := ctx.Err(); err != nil {
			return err
		}

		// stop when all journal and tomb records are processed
		if jpos == jlen && tpos == tlen {
			break
		}

		// init next value from journal or tombstone
		var search MergeValue
		switch {
		case jpos < jlen && tpos < tlen:
			// select the smalles next value to load a source pack for
			if jkeys[jpos] < tkeys[tpos] || (jkeys[jpos] == tkeys[tpos] && jpks[jpos] < tpks[tpos]) {
				search = NewMergeValue(jkeys[jpos], jpks[jpos])
			} else {
				search = NewMergeValue(tkeys[tpos], tpks[tpos])
			}
		case tpos == tlen:
			// no more tomb records
			search = NewMergeValue(jkeys[jpos], jpks[jpos])
		case jpos == jlen:
			// no more journal records
			search = NewMergeValue(tkeys[tpos], tpks[tpos])
		default:
			// should never reach here
			break
		}

		// load next source pack (may be nil when ik/pk is supposed to be inserted
		// behind the last pack or when no pack exists yet)
		src, next, err := it.Next(ctx, search)
		if err != nil {
			return err
		}
		var spos, slen int
		if src != nil {
			slen = src.Len()
		}
		// idx.log.Infof("Merge src=%d j=%d t=%d next=%016x:%016x:%t", slen, jlen, tlen, next.Ik, next.Pk, next.Ok)

		// 3-way merge: src, journal, tomb -> out
		var (
			pkg        *pack.Package
			col1, col2 block.BlockAccessor[uint64]
		)
		for {
			// stop when all inputs are exhausted
			if jpos == jlen && spos == slen {
				break
			}

			// create a new writable pack on first iteration or after a previous
			// full pack was stored
			if pkg == nil {
				// idx.log.Infof("Merge init new out pack")
				pkg = it.NewPack()
				col1 = pkg.Block(0).Uint64()
				col2 = pkg.Block(1).Uint64()
			}

			// init values
			var jval, tval, sval MergeValue
			if jpos < jlen {
				jval = NewMergeValue(jkeys[jpos], jpks[jpos])
				// idx.log.Infof("Merge jval=%016x:%016x", jkeys[jpos], jpks[jpos])
			}

			// stop when this journal value crosses next src pack's min
			if jval.IsValid() && next.IsValid() && !jval.Less(next) {
				// idx.log.Infof("Merge break to next pack")
				break
			}

			// init next source val when available
			if spos < slen {
				sval = NewMergeValue(src.Uint64(0, spos), src.Uint64(1, spos))
				// idx.log.Infof("Merge sval=%016x:%016x", sval.Ik, sval.Pk)
			}

			// load next tomb val, skip stale tomb records
			for tpos < tlen {
				val := NewMergeValue(tkeys[tpos], tpks[tpos])
				if sval.IsValid() && val.Less(sval) {
					// idx.log.Infof("Merge skip unused tomb val=%016x:%016x", val.Ik, val.Pk)
					tpos++
					continue
				}
				if jval.IsValid() && val.Less(jval) {
					// idx.log.Infof("Merge skip unused tomb val=%016x:%016x", val.Ik, val.Pk)
					tpos++
					continue
				}
				tval = val
				break
			}

			// skip deleted values and advance pointers
			if tval.Equal(sval) {
				spos++
				tpos++
				if tval.Equal(jval) {
					jpos++
				}
				it.nDel++
				// it.idx.log.Infof("Merge: skip src 0x%016x:%016x", sval.Ik, sval.Pk)
				continue
			}
			if tval.Equal(jval) {
				jpos++
				tpos++
				// it.idx.log.Infof("Merge: skip new 0x%016x:%016x", jval.Ik, jval.Pk)
				continue
			}

			// output the smaller of two values, at least one of both values
			// is valid here
			switch {
			case sval.IsValid() && sval.Less(jval):
				// merge src row
				spos++
				col1.Append(sval.Ik)
				col2.Append(sval.Pk)
				pkg.UpdateLen()
				// it.idx.log.Infof("Merge: append src 0x%016x:%016x", sval.Ik, sval.Pk)
			case jval.IsValid():
				// merge journal row
				jpos++
				col1.Append(jval.Ik)
				col2.Append(jval.Pk)
				pkg.UpdateLen()
				it.nIns++
				// it.idx.log.Infof("Merge: append new 0x%016x:%016x", jval.Ik, jval.Pk)
			}

			// store output pack when full
			if pkg.IsFull() {
				if err := it.Store(pkg); err != nil {
					return err
				}
				col1.Close()
				col2.Close()
				pkg.Release()
				pkg = nil
			}
		}

		// store pending output pack
		if pkg != nil {
			if err := it.Store(pkg); err != nil {
				return err
			}
			col1.Close()
			col2.Close()
			pkg.Release()
			pkg = nil
		}

		// skip unused trailing tomb records
		if next.IsValid() {
			for tpos < tlen {
				if NewMergeValue(tkeys[tpos], tpks[tpos]).Less(next) {
					tpos++
				} else {
					break
				}
			}
		} else {
			tpos = tlen
		}
	}

	// update index state
	if err := it.UpdateIndexState(ctx); err != nil {
		return err
	}

	// commit backend transaction
	if err := it.Complete(ctx); err != nil {
		return err
	}

	// reset journal
	idx.tomb.Clear()
	idx.journal.Clear()

	// update counters
	atomic.StoreInt64(&idx.metrics.LastFlushTime, start.UnixNano())
	atomic.StoreInt64(&idx.metrics.LastFlushDuration, int64(time.Since(start)))
	atomic.StoreInt64(&idx.metrics.TotalSize, int64(idx.state.Size))
	atomic.AddInt64(&idx.metrics.NumCalls, 1)
	atomic.AddInt64(&idx.metrics.InsertedTuples, int64(it.nIns))
	atomic.AddInt64(&idx.metrics.DeletedTuples, int64(it.nDel))
	atomic.AddInt64(&idx.metrics.PacksLoaded, int64(it.nPacksLoaded))
	atomic.AddInt64(&idx.metrics.PacksStored, int64(it.nPacksStored))
	atomic.AddInt64(&idx.metrics.BlocksLoaded, int64(it.nPacksLoaded*2))
	atomic.AddInt64(&idx.metrics.BlocksStored, int64(it.nPacksStored*2))
	atomic.AddInt64(&idx.metrics.BytesRead, int64(it.nBytesRead))
	atomic.AddInt64(&idx.metrics.BytesWritten, int64(it.nBytesWritten))

	idx.log.Debugf("pack: %s flushed %d packs add=%d/%d del=%d/%d total_size=%s in %s",
		idx.schema.Name(),
		it.nPacksStored,
		it.nIns,
		jlen,
		it.nDel,
		tlen,
		util.ByteSize(it.nBytesWritten),
		time.Since(start),
	)

	return nil
}
