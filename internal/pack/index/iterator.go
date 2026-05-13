// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/store"
	"blockwatch.cc/knoxdb/pkg/util"
)

// LookupIterator is used during query execution to find interesting
// packs for indexes with indirect keys (hash and composite hash
// indexes). It performs a direct lookup from a list of keys,
// identifies and loads the index packs that likely contain those
// keys. The challenge in finding the right index pack is that
// pack keys are created from the first record. Hence we must
// search for the next lower or equal pack key for each given
// search key. Pack keys have form <ikey>:<rid>:<idx>, so the
// search is a simple prefix scan for the next lower ikey.
type LookupIterator struct {
	keys     []uint64           // idx keys (sorted)
	tx       store.Tx           // backend tx
	bucket   store.Bucket       // bucket reference
	nextRid  uint64             // next higher row id (for duplicate handling)
	idx      *Index             // back-ref to idx
	pack     *pack.Package      // current idx package
	btypes   [2]types.BlockType // expected block types for decode
	useCache bool
}

func NewLookupIterator(idx *Index, keys []uint64, useCache bool) *LookupIterator {
	util.Sort(keys, 0)
	return &LookupIterator{
		keys:     keys,
		idx:      idx,
		useCache: useCache,
		btypes: [2]types.BlockType{
			idx.sstore.Fields[0].Type.BlockType(),
			idx.sstore.Fields[1].Type.BlockType(),
		},
	}
}

func (it *LookupIterator) Close() {
	if it.pack != nil {
		it.pack.Release()
		it.pack = nil
	}
	if it.tx != nil {
		it.tx.Rollback()
		it.tx = nil
	}
	it.bucket = nil
	it.keys = nil
	it.idx = nil
	it.nextRid = 0
	it.useCache = false
}

// Next emits the next index pack that likely contains some of the search
// keys and a boundary value which is the last index key found in this pack
// (mostly for convenience) or an error if anything goes wrong. When exchausted
// Next will return a nil pack and nil error.
func (it *LookupIterator) Next(ctx context.Context) (*pack.Package, uint64, error) {
	// release last pack
	if it.pack != nil {
		it.pack.Release()
		it.pack = nil
	}

	// stop when lookup list is exhausted
	if len(it.keys) == 0 {
		return nil, 0, nil
	}

	// try find the next idx pack with matches
	ok, err := it.loadNextPack(ctx)
	if err != nil {
		return nil, 0, err
	}

	// nothing found, wrap up
	if !ok {
		return nil, 0, nil
	}

	// Skip all search keys that may be in this pack. As the last
	// index key may continue in the following pack, do not yet remove it!
	last := it.pack.Uint64(0, it.pack.Len()-1)
	for len(it.keys) > 0 && it.keys[0] < last {
		it.keys = it.keys[1:]
	}

	// update our expectation about the follower pack to handle
	// non-unique indexes (from duplicate hashes or intentional
	// duplicates). If the last pack key is a direct hit, we track
	// the next expected row id for continuing next time. Otherwise
	// loadNextPack would find the same pack again and loop.
	if len(it.keys) > 0 {
		if last == it.keys[0] {
			it.nextRid = it.pack.Uint64(1, it.pack.Len()-1) + 1
		} else {
			it.nextRid = 0
			it.keys = it.keys[1:]
		}
	}

	// return pack and last as boundary value (inclusive)
	return it.pack, last, nil
}

func (it *LookupIterator) loadNextPack(ctx context.Context) (bool, error) {
	// init read tx on first call
	if it.tx == nil {
		tx, err := it.idx.db.Begin()
		if err != nil {
			return false, err
		}
		if it.bucket = it.idx.dataBucket(tx); it.bucket == nil {
			tx.Rollback()
			return false, store.ErrBucketNotFound
		}
		it.tx = tx

		// check if bucket is empty by loading the first pack
		key, _, err := it.bucket.SearchGE(nil)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				// this must be an empty bucket, drop all search keys and exit
				// it.idx.log.Debug("lookup: empty bucket, skipping all searches")
				it.keys = it.keys[:0]
				return false, nil
			}
			return false, err
		} else {
			// decode the first pack's key
			firstIk, _, _ := it.idx.decodePackKey(key)
			// it.idx.log.Debugf("lookup: first key 0x%016x", firstIk)

			// skip all search keys smaller than the first key as they
			// cannot possibly be in the index
			for len(it.keys) > 0 && it.keys[0] < firstIk {
				// it.idx.log.Debugf("lookup: ignoring below min key 0x%016x", it.keys[0])
				it.keys = it.keys[1:]
			}
		}
	}

	// stop when search keys are exhausted
	if len(it.keys) == 0 {
		return false, nil
	}

	// seek to the next search key using LE (reverse binary search)
	// to find the exact pack that should contain the key; there is
	// a possibility this pack does not contain the search key if
	// the key does not exist in the index, then we load an unrelated
	// pack; use the expected next row id as hint to make progress
	// in case the same index key spreads across multiple index packs
	var search []byte
	if it.nextRid > 0 {
		search = it.idx.encodePackKey(it.keys[0], it.nextRid, 0)
		// it.idx.log.Debugf("lookup: searchLE 0x%016x:%016x:%d", it.keys[0], it.nextRid, 0)
	} else {
		search = num.EncodeUvarint(it.keys[0])
		// it.idx.log.Debugf("lookup: searchLE 0x%016x", it.keys[0])
	}
	key, val, err := it.bucket.SearchLE(search)
	if err != nil {
		// no hit? happens on prefix search when the search key is
		// an exact match to the index pack key;
		key, val, err = it.bucket.SearchGE(search)
		if err != nil {
			// it.idx.log.Debugf("lookup: unexpected, did not find any block")
			return false, err
		}
	}

	// decode the key, it either points to block idx = 0 (EQUAL atch)
	// or 1 (LESS match)
	ikey, rid, idx := it.idx.decodePackKey(key)
	// it.idx.log.Debugf("lookup: found 0x%016x:%016x:%d", ikey, rid, idx)

	// load blocks into package
	it.pack = pack.New().
		WithSchema(it.idx.sstore).
		WithMaxRows(it.idx.opts.PackSize)

	// try load block pair from cache
	if it.useCache {
		bcache := engine.GetEngine(ctx).BlockCache(it.idx.id)
		if b, ok := bcache.Get(it.idx.encodeCacheKey(ikey, rid, 0)); ok {
			it.pack.WithBlock(0, b)
		}
		if b, ok := bcache.Get(it.idx.encodeCacheKey(ikey, rid, 1)); ok {
			it.pack.WithBlock(1, b)
		}
	}

	// load missing blocks in pair
	for i := range []int{0, 1} {
		if it.pack.Block(i) != nil {
			continue
		}

		// decode when not already found in cache
		var buf []byte
		if idx == i {
			// we found this block during search above
			buf = val
		} else {
			// we must load this block
			buf, err = it.bucket.Get(it.idx.encodePackKey(ikey, rid, i))
			if err != nil {
				return false, fmt.Errorf("loading block 0x%016x:%016x:%d: %v", ikey, rid, i, err)
			}
		}

		// decode block
		// it.idx.log.Debugf("loading block 0x%016x:%016x:%d", ikey, rid, i)
		b, err := block.Decode(it.btypes[i], buf)
		if err != nil {
			return false, fmt.Errorf("decoding block 0x%016x:%016x:%d: %v", ikey, rid, i, err)
		}
		it.pack.WithBlock(i, b)
	}

	return true, nil
}

// ScanIterator is used during query execution for indexes that use direct keys
// (like integers) as indexed value. Here we can run a range scan for common
// query conditions (LE, LT, GE, GT, RG) directly on the sorted index to
// find their row ids. ScanIterator identifies and loads index packs with
// likely matches. For this it evaluates the query condition and translates
// the query into an index range scan. The challenge here is that index packs
// have arbitrary start keys (recall, the first index record produces the
// block keys use on disk). The strategy used is to first identify a good
// range start for index block keys and then scan in ascending order until
// we reach a stop criteria or reach the end of the index.
type ScanIterator struct {
	idx      *Index
	tx       store.Tx
	bucket   store.Bucket
	from     []byte
	to       []byte
	next     func() ([]byte, []byte, bool)
	stop     func()
	node     *filter.Node
	hits     []uint32
	pack     *pack.Package
	bits     *bitset.Bitset
	btypes   [2]types.BlockType
	useCache bool
}

func NewScanIterator(idx *Index, node *filter.Node, useCache bool) *ScanIterator {
	return &ScanIterator{
		node:     node,
		idx:      idx,
		hits:     arena.AllocUint32(idx.opts.PackSize),
		bits:     bitset.New(idx.opts.PackSize),
		useCache: useCache,
		btypes: [2]types.BlockType{
			idx.sstore.Fields[0].Type.BlockType(),
			idx.sstore.Fields[1].Type.BlockType(),
		},
	}
}

func (it *ScanIterator) Close() {
	if it.pack != nil {
		it.pack.Release()
	}
	if it.stop != nil {
		it.stop()
		it.stop = nil
		it.next = nil
	}
	if it.tx != nil {
		it.tx.Rollback()
		it.tx = nil
	}
	arena.Free(it.hits[:0])
	it.bits.Close()
	it.bits = nil
	it.pack = nil
	it.bucket = nil
	it.hits = nil
	it.idx = nil
	it.node = nil
	it.useCache = false
	it.from = nil
	it.to = nil
}

// Next emits the next index pack that actually contains query matches,
// a selector list of where those matches are or an error if anything
// goes wrong. When exhausted, Next will return a nil pack and nil selector
// without error.
func (it *ScanIterator) Next(ctx context.Context) (*pack.Package, []uint32, error) {
	// release last pack
	if it.pack != nil {
		it.pack.Release()
		it.pack = nil
	}

	// init transaction and range scan
	if it.tx == nil {
		tx, err := it.idx.db.Begin()
		if err != nil {
			return nil, nil, err
		}
		if it.bucket = it.idx.dataBucket(tx); it.bucket == nil {
			tx.Rollback()
			return nil, nil, store.ErrBucketNotFound
		}
		it.tx = tx

		// identify [from,to) key range
		it.initRange()

		// find the first pack with likely matches
		if it.from != nil {
			key, _, err := it.bucket.SearchLE(it.from)
			if err != nil {
				if !errors.Is(err, store.ErrKeyNotFound) {
					return nil, nil, err
				}
				// there is no match at all
				return nil, nil, nil
			}

			// use the pack's key as actual range start, but re-encode
			// to make sure we start at block 0
			ik, rid, _ := it.idx.decodePackKey(key)
			it.from = it.idx.encodePackKey(ik, rid, 0)
		}

		it.idx.log.Tracef("Scan %s => range %#v .. %#v", it.node, it.from, it.to)

		// init a scan iterator and wrap in a pull
		it.next, it.stop = iter.Pull2(it.bucket.ScanRange(it.from, it.to))
	}

	for {
		// load pack (will also advance cursor to the next block pair)
		ok, err := it.loadPack(ctx)
		if err != nil {
			return nil, nil, err
		}

		// finish when no more pack keys match
		if !ok {
			return nil, nil, nil
		}

		// find actual matches, zero bits before
		// it.idx.log.Infof("Run filter %s %T %v idx=%d",
		// 	it.node, it.node.Filter.Matcher, it.node.Filter.Matcher.Value(), it.node.Filter.Index)
		it.bits = filter.Match(it.node, it.pack, nil, it.bits.Zero().Resize(it.pack.Len()))

		// skip this pack when no matches were found
		if it.bits.None() {
			// it.idx.log.Infof("No match in this pack")
			it.pack.Release()
			it.pack = nil
			continue
		}

		// handle real matches
		it.hits = it.bits.Indexes(it.hits)
		// it.idx.log.Infof("Found %d hits", len(it.hits))

		return it.pack, it.hits, nil
	}
}

var (
	FF  = []byte{0xFF}
	END = num.EncodeUvarint(uint64(0xFFFFFFFFFFFFFFFF))
)

// convert filter value to uvarint key
func makePrefix(typ block.BlockType, val any) []byte {
	switch typ {
	case block.BlockInt64:
		return num.EncodeUvarint(uint64(val.(int64)))
	case block.BlockInt32:
		return num.EncodeUvarint(uint64(val.(int32)))
	case block.BlockInt16:
		return num.EncodeUvarint(uint64(val.(int16)))
	case block.BlockInt8:
		return num.EncodeUvarint(uint64(val.(int8)))
	case block.BlockUint64:
		return num.EncodeUvarint(val.(uint64))
	case block.BlockUint32:
		return num.EncodeUvarint(uint64(val.(uint32)))
	case block.BlockUint16:
		return num.EncodeUvarint(uint64(val.(uint16)))
	case block.BlockUint8:
		return num.EncodeUvarint(uint64(val.(uint8)))
	}
	return []byte{0}
}

// initRange unpacks the query condition and initializes a [from,to) range
// of index block key prefixes. The from part serves as an initial seek point
// for the bucket.SearchLE method which performs a revers binary search.
// If it succeeds it finds the last pack with a key smaller or equal to
// the from key. This pack must contain our first match and following
// packs may contain additional matches. We're going to scan all packs
// up until the to (stop) condition triggers.
func (it *ScanIterator) initRange() {
	// create from / to range
	switch it.node.Filter.Mode {
	case types.FilterModeEqual:
		// EQ => scan(LE(prefix), prefix+1)
		it.from = makePrefix(it.node.Filter.Type, it.node.Filter.Value)
		it.to = store.NextKey(it.from)

	case types.FilterModeLt:
		// LT => scan(nil, prefix)
		it.from = nil
		it.to = makePrefix(it.node.Filter.Type, it.node.Filter.Value)

	case types.FilterModeLe:
		// LE => scan(nil, prefix+1)
		it.from = nil
		it.to = store.NextKey(makePrefix(it.node.Filter.Type, it.node.Filter.Value))

	case types.FilterModeGt:
		// GT => scan(LE(prefix+1), nil)
		it.from = store.NextKey(makePrefix(it.node.Filter.Type, it.node.Filter.Value))
		it.to = nil // END

	case types.FilterModeGe:
		// GE => scan(LE(prefix), nil)
		it.from = makePrefix(it.node.Filter.Type, it.node.Filter.Value)
		it.to = nil // END

	case types.FilterModeRange:
		// RG => scan(LE(prefix), to+1)
		// note: range query semantic is [from,to] (inclusive boundary)
		it.from = makePrefix(it.node.Filter.Type, it.node.Filter.Value.(filter.RangeValue)[0])
		it.to = store.NextKey(makePrefix(it.node.Filter.Type, it.node.Filter.Value.(filter.RangeValue)[1]))
	}
}

func (it *ScanIterator) loadPack(ctx context.Context) (bool, error) {
	// fetch next block (should be first of index pair)
	key, val, ok := it.next()
	if !ok {
		it.idx.log.Tracef("scan: no more results")
		return ok, nil
	}

	// decode key and ensure we are at block 0
	ikey, rid, idx := it.idx.decodePackKey(key)
	assert.Always(idx == 0, "ScanIterator should point at first element in block pair")
	it.idx.log.Tracef("scan: found key 0x%016x:%016x:%d", ikey, rid, idx)

	// init pack
	it.pack = pack.New().
		WithSchema(it.idx.sstore).
		WithMaxRows(it.idx.opts.PackSize)

	// try load block pair from cache
	if it.useCache {
		bcache := engine.GetEngine(ctx).BlockCache(it.idx.id)
		if b, ok := bcache.Get(it.idx.encodeCacheKey(ikey, rid, 0)); ok {
			it.pack.WithBlock(0, b)
		}
		if b, ok := bcache.Get(it.idx.encodeCacheKey(ikey, rid, 1)); ok {
			it.pack.WithBlock(1, b)
		}
	}

	// load missing blocks and advance iterator
	for i := range []int{0, 1} {
		// decode block when not found in cache
		if it.pack.Block(i) == nil {
			// it.idx.log.Infof("Decoding block 0x%016x:%016x:%d", ik, pk, i)
			b, err := block.Decode(it.btypes[i], val)
			if err != nil {
				return false, fmt.Errorf("decode block 0x%016x:%016x:%d: %v", ikey, rid, idx, err)
			}
			it.pack.WithBlock(i, b)
		}

		// advance iterator to second block (we must do this regardless
		// of having a block found in cache because we expect more data
		// from the iterator in the next round and advancing it here
		// is better for state handling)
		if i == 0 {
			if _, val, ok = it.next(); !ok {
				// unlikely (missing block 1)
				return ok, fmt.Errorf("scan: missing block 0x%016x:%016x:%d", ikey, rid, 1)
			}
		}
	}
	return true, nil
}
