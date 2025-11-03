// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/encode"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/store"
	"blockwatch.cc/knoxdb/pkg/util"
)

// MergeValue is a helper to compare multi-key records during merge.
type MergeValue struct {
	Key uint64 // indexed key
	Rid uint64 // row id
	Ok  bool   // flag indicating this value is initialized
}

func NewMergeValue(k, r uint64) MergeValue {
	return MergeValue{
		Key: k,
		Rid: r,
		Ok:  true,
	}
}

func (v *MergeValue) Reset() {
	v.Key = 0
	v.Rid = 0
	v.Ok = false
}

func (v MergeValue) IsValid() bool {
	return v.Ok
}

func (v MergeValue) Equal(w MergeValue) bool {
	return v.Ok && w.Ok && v.Key == w.Key && v.Rid == w.Rid
}

func (v MergeValue) Less(w MergeValue) bool {
	switch {
	case v.Ok && w.Ok:
		return v.Key < w.Key || (v.Key == w.Key && v.Rid < w.Rid)
	case v.Ok:
		return true
	default:
		return false
	}
}

// MergeIterator is a helper to locate, load and store index packs.
type MergeIterator struct {
	idx     *Index                    // ref to index
	tx      store.Tx                  // current write tx
	cur     store.Cursor              // current read cursor
	pack    *pack.Package             // current read-only pack
	bcache  block.BlockCachePartition // cache reference
	last    MergeValue                // current pack keys (used on store/delete)
	lastSz  int                       // current pack data size (to calculate diff)
	halfSel []uint32                  // selector for second half of a pack for splitting

	// stats
	nTxBytes      int // pending tx bytes to write
	nDel          int // number of records deleted from index
	nIns          int // number of records inserted into index
	nDups         int // number of duplicate records from earlier aborted merge
	nBytesRead    int // total bytes read by merge
	nBytesWritten int // total bytes written by merge
	nBytesDiff    int // storage size diff after merge
	nPacksLoaded  int // number of packs read during merge
	nPacksStored  int // number of packs written during merge
	nPacksDiff    int // pack count diff after merge
}

func NewMergeIterator(idx *Index) *MergeIterator {
	return &MergeIterator{
		idx:     idx,
		halfSel: types.NewRange(idx.opts.PackSize/2, idx.opts.PackSize).AsSelection(),
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
	arena.Free(it.halfSel)
	it.idx = nil
	it.bcache = nil
	it.nTxBytes = 0
	it.last.Reset()
}

func (it *MergeIterator) NewPack() *pack.Package {
	return pack.New().
		WithSchema(it.idx.sstore).
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

	// FIXME: with aborted merges (both for append and drop passes)
	// the stats below will drift as packs are written and committed
	// in multiple storage transactions, the index statistics are only
	// committed at a successful end

	// require store tx
	if it.tx == nil {
		return engine.ErrNoTx
	}

	// row count (remove duplicates from earlier aborted merge)
	if it.nIns != it.nDel {
		n := max(0, int(it.idx.state.NRows)+it.nIns-it.nDel-it.nDups)
		it.idx.state.NRows = uint64(n)
	}

	// byte size
	if it.nBytesDiff != 0 {
		n := max(0, int(it.idx.state.NextPk)+it.nBytesDiff)
		it.idx.state.NextPk = uint64(n)
	}

	// pack count
	if it.nPacksDiff != 0 {
		n := max(0, int(it.idx.state.NextRid)+it.nPacksDiff)
		it.idx.state.NextRid = uint64(n)
	}

	return it.idx.state.Store(ctx, it.tx)
}

func (it *MergeIterator) SplitAndStore(pkg *pack.Package) (*pack.Package, error) {
	// move the second half of the pack's contents to a new pack
	half := it.NewPack()
	pkg.AppendTo(half, it.halfSel)

	// drop second half from pack
	pkg.Delete(it.idx.opts.PackSize/2, it.idx.opts.PackSize)

	// store first half
	if err := it.Store(pkg); err != nil {
		return nil, err
	}

	// free first half
	pkg.Release()

	// return second half
	return half, nil
}

func (it *MergeIterator) Store(pkg *pack.Package) error {
	// init data bucket
	bucket := it.idx.dataBucket(it.tx)
	if bucket == nil {
		return store.ErrBucketNotFound
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
		key := it.idx.encodePackKey(id.Key, id.Rid, i)
		if pkg.Len() == 0 {
			err = bucket.Delete(key)
			it.nTxBytes++
		} else {
			var (
				buf   []byte
				stats encode.ContextExporter
			)
			buf, stats, err = pkg.Block(i).Encode(types.BlockCompressNone)
			if err == nil {
				err = bucket.Put(key, buf)
				// it.idx.log.Tracef("merge storing block 0x%016x:%016x:%d len=%d size=%d",
				// 	id.Key, id.Rid, i, pkg.Len(), len(buf))
				stats.Close()
				n += len(buf)
				pkg.Block(i).SetClean()
			}
		}
		if err != nil {
			return err
		}
		// drop cache keys
		if it.bcache != nil {
			it.bcache.Remove(it.idx.encodeCacheKey(id.Key, id.Rid, i))
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
			return nil, MergeValue{}, store.ErrBucketNotFound
		}
		if e := engine.GetEngine(ctx); e != nil {
			it.bcache = e.BlockCache(it.idx.id)
		}
	}

	// load source pack pack that matches key+rid combi, when nil create new pack
	if err := it.loadNextPack(id); err != nil {
		return nil, MergeValue{}, err
	}

	// cursor either points one key behind the loaded pack's blocks now or
	// is invalid in which case we will create a new pack at the end. we try
	// decoding the next key either way which yields zero or correct ids
	// which we'll use as boundary during merge.
	if it.cur.Key() != nil {
		id.Key, id.Rid, _ = it.idx.decodePackKey(it.cur.Key())
		id.Ok = true
		// it.idx.log.Tracef("merge: next id 0x%016x:%016x", id.Key, id.Rid)
	} else {
		id.Reset()
		// it.idx.log.Tracef("merge: next id not yet on storage, need new pack")
	}

	// return current pack and boundary of next pack
	return it.pack, id, nil
}

func (it *MergeIterator) loadNextPack(search MergeValue) error {
	// seek to search MergeValue's position, most likely we will find the next
	// higher pack in which case we rewind one pack
	ok := it.cur.Seek(it.idx.encodePackKey(search.Key, search.Rid, 0))
	// it.idx.log.Tracef("merge: seek 0x%016x:%016x:%d key=%x ok=%t", search.Key, search.Rid, 0,
	// 	it.idx.encodePackKey(search.Key, search.Rid, 0), ok)

	// seek to last pack when not found (our search key is within this pack's range)
	if !ok {
		// if not found, set cur to the first block of the last pack
		it.cur.Last()
		ok = it.cur.Prev()
		// it.idx.log.Tracef("merge: seek last>prev %t, key=%x", ok, it.cur.Key())
	}

	// still not found? this must be an empty bucket, we'll create our first pack
	if !ok {
		// it.idx.log.Tracef("merge: no pack found")
		return nil
	}

	// decode the block's key
	key, rid, id := it.idx.decodePackKey(it.cur.Key())
	// it.idx.log.Tracef("merge: found 0x%016x:%016x:%d", key, rid, id)

	// rewind if we're behind the search key
	if key > search.Key || (key == search.Key && rid > search.Rid) {
		// if exists, calling `prev` again will set cur the first block of the previous pair
		it.cur.Prev()
		ok = it.cur.Prev()
		// it.idx.log.Tracef("merge: rewind prev>prev %t", ok)

		// decode the previous key
		if ok {
			key, rid, id = it.idx.decodePackKey(it.cur.Key())
			// it.idx.log.Tracef("merge: now 0x%016x:%016x:%d", key, rid, id)
			// assert we're actually at the first block
		}
	}
	assert.Always(id == 0, "must be at first block in index pair")

	// looks like this was the first pack and it did not contain
	// our search key, we're going to create a new pack to place in front
	if !ok {
		// it.idx.log.Tracef("merge: must create new pack in front")
		return nil
	}

	// we're at the correct pack now, load blocks into package
	it.pack = pack.New().
		WithSchema(it.idx.sstore).
		WithMaxRows(it.idx.opts.PackSize)

	// load block pair from cursor
	var n int
	for i := range []int{0, 1} {
		// assert block is correct
		bkey, brid, bid := it.idx.decodePackKey(it.cur.Key())
		assert.Always(bid == i, "unexpected block id", "key", bkey, "rid", brid, "id", bid)

		// create and decode block
		b, err := block.Decode(
			it.idx.sstore.Fields[i].Type.BlockType(),
			it.cur.Value(),
		)
		if err != nil {
			return fmt.Errorf("loading block 0x%08x:%08x:%d: %v", bkey, brid, bid, err)
		}

		// it.idx.log.Tracef("merge loading block 0x%016x:%016x:%d len=%d",
		// 	bkey, brid, i, b.Len())

		it.pack.WithBlock(i, b)
		n += len(it.cur.Value())

		// go to next storage key (may be end of bucket)
		it.cur.Next()
	}
	it.nPacksLoaded++
	it.last = NewMergeValue(key, rid)
	it.lastSz = n
	it.nBytesRead += n

	return nil
}

// Merge journal and tomb entries into data partitions, repack and store.
//
// Algo design
//   - append happens direct, delete is deferred to GC
//   - on-disk packs are read-only, merge appends to new packs
//   - pack keys are shared keys so that merge effectively overwrites/replaces versions
//   - append pass: merge src & journal records into output
//   - delete pass: copy src records while skipping tomb entries
//
// Details
//   - block keys encode the key/rid combination of the first record in a pack
//   - keys remain unchanged even if the first record is removed later
//   - merge find source packs where new/old rids are in range
//   - writable output packs inherit the source pack's key (if a src exist)
//   - data is merged in sorted order (sort by first key then rid)
//   - merge allocates records to a pack until key/rid crosses the next packs start boundary
//   - when an output pack is full (happens during append only)
//     - the pack is split in half
//     - the first half is stored
//     - the second half continues to be used as output
//   - storing packs may commit the backend tx when the configured max tx
//     size limit is crossed (large merges progress in multiple atomic steps
//     which is safe for both append and garbage collection)
//
// Data placement
//   - block vector KV keys are generated from the first record in a pack
//     with format `index key + rowid + block id`
//   - merge iterator peeks into next KV store keys to identify next pack's start ids
//     which serves as boundary for insertion
//
// Edge cases
//   - nextMinKey & nextMinRid can be 0 (empty index, behind last index pack)
//   - a pack becomes empty -> dropped from storage (iterator remembers last read KV keys)
//   - first record in a pack is deleted (keep the original pack storage key)
//   - stale tombstones without match in source packs may exist
//     when GC restarts after error/crash as some packs may have committed
//     before

// writes journal records to index packs. this is called during table merge when
// index journal runs full and when finalizing index updates.
func (idx *Index) mergeAppend(ctx context.Context) error {
	idx.log.Debugf("merging journal[%d]", idx.journal.Len())

	var (
		start = time.Now()
		jpos  int
	)

	// direct access to journal vectors
	j0 := idx.journal.Block(0).Uint64().Slice()
	j1 := idx.journal.Block(1).Uint64().Slice()
	jlen := len(j0)

	// co-sort journal vectors in-place
	util.Sort2(j0, j1)

	// iterator to lookup matching packages
	it := NewMergeIterator(idx)
	defer it.Close()

	// 2-way merge into packs
	for {
		// check context
		if err := ctx.Err(); err != nil {
			return err
		}

		// stop when all journal records are processed
		if jpos == jlen {
			break
		}

		// load next source pack (may be nil when key/rid is not found, ie.
		// we're behind the last pack or no pack exists yet) in which case
		// we allocate a new output pack
		var (
			spos, slen int
			s0, s1     types.NumberAccessor[uint64]
		)
		src, next, err := it.Next(ctx, NewMergeValue(j0[jpos], j1[jpos]))
		if err != nil {
			return err
		}

		if src != nil {
			slen = src.Len()
			s0 = src.Block(0).Uint64()
			s1 = src.Block(1).Uint64()
		}
		// idx.log.Infof("merge next src=%d/%d j=%d/%d next=%016x:%016x:%t",
		// 	spos, slen, jpos, jlen, next.Key, next.Rid, next.Ok)

		// 2-way merge: src & journal

		// create a new output pack
		out := it.NewPack()
		o0 := out.Block(0).Uint64()
		o1 := out.Block(1).Uint64()

		// assign src/journal -> out
		var sval, jval MergeValue
	mergeloop:
		for {
			// load values
			if spos < slen && !sval.IsValid() {
				sval = NewMergeValue(s0.Get(spos), s1.Get(spos))
			}
			if jpos < jlen && !jval.IsValid() {
				jval = NewMergeValue(j0[jpos], j1[jpos])

				// stop when this journal value crosses next src pack's min
				if next.IsValid() && !jval.Less(next) {
					// idx.log.Infof("merge: break to next pack at jval %d/%d", jval.Key, jval.Rid)
					break mergeloop
				}
			}

			// merge
			switch {
			case sval.IsValid() && jval.IsValid():
				// merge lesser value first
				switch {
				case sval.Less(jval):
					o0.Append(sval.Key)
					o1.Append(sval.Rid)
					out.UpdateLen()
					spos++
					sval.Reset()
				case sval.Equal(jval):
					// exact same value, must be from an aborted earlier merge,
					// skip but still count
					jpos++
					it.nIns++
					it.nDups++
					jval.Reset()
				default:
					// write jval
					o0.Append(jval.Key)
					o1.Append(jval.Rid)
					out.UpdateLen()
					jpos++
					it.nIns++
					jval.Reset()
				}
			case sval.IsValid():
				// no more jvals, merge sval
				o0.Append(sval.Key)
				o1.Append(sval.Rid)
				out.UpdateLen()
				spos++
				sval.Reset()

			case jval.IsValid():
				// no more svals, merge jval
				o0.Append(jval.Key)
				o1.Append(jval.Rid)
				out.UpdateLen()
				jpos++
				it.nIns++
				jval.Reset()

			default:
				// no more values, we're done
				// idx.log.Infof("merge: no more values")
				break mergeloop
			}

			// split pack when full
			if out.IsFull() {
				half, err := it.SplitAndStore(out)
				if err != nil {
					return err
				}
				// continue appending to the second half (a new pack)
				out = half
				o0 = out.Block(0).Uint64()
				o1 = out.Block(1).Uint64()
			}
		}

		// store non-empty output pack
		if out.Len() > 0 {
			if err = it.Store(out); err != nil {
				return err
			}
		}
		out.Release()
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
	idx.journal.Clear()

	// update counters
	atomic.StoreInt64(&idx.metrics.LastMergeTime, start.UnixNano())
	atomic.StoreInt64(&idx.metrics.LastMergeDuration, int64(time.Since(start)))
	atomic.StoreInt64(&idx.metrics.TotalSize, int64(idx.state.NextPk))
	atomic.StoreInt64(&idx.metrics.PacksCount, int64(idx.state.NextRid))
	atomic.AddInt64(&idx.metrics.NumCalls, 1)
	atomic.AddInt64(&idx.metrics.InsertedTuples, int64(it.nIns))
	atomic.AddInt64(&idx.metrics.PacksLoaded, int64(it.nPacksLoaded))
	atomic.AddInt64(&idx.metrics.PacksStored, int64(it.nPacksStored))
	atomic.AddInt64(&idx.metrics.BlocksLoaded, int64(it.nPacksLoaded*2))
	atomic.AddInt64(&idx.metrics.BlocksStored, int64(it.nPacksStored*2))
	atomic.AddInt64(&idx.metrics.BytesRead, int64(it.nBytesRead))
	atomic.AddInt64(&idx.metrics.BytesWritten, int64(it.nBytesWritten))

	idx.log.Debugf("merged journal packs=%d add=%d/%d dups=%d total_size=%s in %s",
		it.nPacksStored,
		it.nIns,
		jlen,
		it.nDups,
		util.ByteSize(it.nBytesWritten),
		time.Since(start),
	)

	return nil
}

// removes tombstoned records from journal packs by rewriting packs.
func (idx *Index) mergeTomb(ctx context.Context, tomb *pack.Package) error {
	idx.log.Debugf("merging tomb[%d]", tomb.Len())

	var (
		start = time.Now()
		tpos  int
	)

	// direct access to both tomb vectors (pre-sorted on store)
	t0 := tomb.Block(0).Uint64() // keys
	t1 := tomb.Block(1).Uint64() // rowids
	tlen := t0.Len()

	// iterator to lookup matching packages
	it := NewMergeIterator(idx)
	defer it.Close()

	// 2-way merge into packs
	for {
		// check context
		if err := ctx.Err(); err != nil {
			return err
		}

		// stop when all tomb records are processed
		if tpos == tlen {
			break
		}

		// load next source pack (may be nil when key/rid does not match any pack on disk)
		src, next, err := it.Next(ctx, NewMergeValue(t0.Get(tpos), t1.Get(tpos)))
		if err != nil {
			return err
		}

		// skip trailing tombstones (outside any packs range)
		if src == nil {
			// idx.log.Tracef("merge no more src, skip remaining %d tombstones", tlen-tpos)
			break
		}

		// init source accessors
		spos := 0
		slen := src.Len()
		s0 := src.Block(0).Uint64()
		s1 := src.Block(1).Uint64()

		// create a new output pack
		out := it.NewPack()
		o0 := out.Block(0).Uint64()
		o1 := out.Block(1).Uint64()

		// idx.log.Tracef("merge src=%d/%d tomb=%d/%d next=%016x:%016x:%t",
		// 	spos, slen, tpos, tlen, next.Key, next.Rid, next.Ok)

		var sval, tval MergeValue
		for spos < slen {
			// load values
			if spos < slen && !sval.IsValid() {
				sval = NewMergeValue(s0.Get(spos), s1.Get(spos))
			}
			if tpos < tlen && !tval.IsValid() {
				tval = NewMergeValue(t0.Get(tpos), t1.Get(tpos))
			}

			// merge
			switch {
			case sval.IsValid() && tval.IsValid():
				switch {
				case sval.Equal(tval):
					// skip svals with tombstones
					spos++
					tpos++
					it.nDel++
					sval.Reset()
					tval.Reset()
				case sval.Less(tval):
					// keep svals < next tombstone
					o0.Append(sval.Key)
					o1.Append(sval.Rid)
					out.UpdateLen()
					spos++
					sval.Reset()
				default:
					// skip stray tombstone, don't output sval yet, it may
					// match another tombstone
					tpos++
					tval.Reset()
				}
			case sval.IsValid():
				// no more tombstones, merge sval
				o0.Append(sval.Key)
				o1.Append(sval.Rid)
				out.UpdateLen()
				spos++
				sval.Reset()

				// impossible: tval without sval (for loop checks end of src)
				// impossible: both vals invalid
			}
		}

		// store output pack (note: in case all entries were deleted, store
		// will drop block vectors from storage)
		// it.idx.log.Tracef("store pack 0x%016x:%016x len=%d",
		// 	it.last.Key, it.last.Rid, out.Len())
		if err = it.Store(out); err != nil {
			return err
		}
		out.Release()
		out = nil
		o0 = nil
		o1 = nil

		// skip stray trailing tombstones lower than `next` index pack's start
		if next.IsValid() {
			for tpos < tlen {
				if NewMergeValue(t0.Get(tpos), t1.Get(tpos)).Less(next) {
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

	// update counters
	atomic.StoreInt64(&idx.metrics.LastMergeTime, start.UnixNano())
	atomic.StoreInt64(&idx.metrics.LastMergeDuration, int64(time.Since(start)))
	atomic.StoreInt64(&idx.metrics.TotalSize, int64(idx.state.NextRid))
	atomic.StoreInt64(&idx.metrics.PacksCount, int64(idx.state.NextPk))
	atomic.AddInt64(&idx.metrics.NumCalls, 1)
	atomic.AddInt64(&idx.metrics.DeletedTuples, int64(it.nDel))
	atomic.AddInt64(&idx.metrics.PacksLoaded, int64(it.nPacksLoaded))
	atomic.AddInt64(&idx.metrics.PacksStored, int64(it.nPacksStored))
	atomic.AddInt64(&idx.metrics.BlocksLoaded, int64(it.nPacksLoaded*2))
	atomic.AddInt64(&idx.metrics.BlocksStored, int64(it.nPacksStored*2))
	atomic.AddInt64(&idx.metrics.BytesRead, int64(it.nBytesRead))
	atomic.AddInt64(&idx.metrics.BytesWritten, int64(it.nBytesWritten))

	idx.log.Debugf("merged tomb packs=%d del=%d/%d total_size=%s in %s",
		it.nPacksStored,
		it.nDel,
		tlen,
		util.ByteSize(it.nBytesWritten),
		time.Since(start),
	)

	return nil
}
