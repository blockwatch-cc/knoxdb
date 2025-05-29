// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/match"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
)

type LookupIterator struct {
	iks      []uint64      // idx keys (sorted)
	tx       store.Tx      // backend tx
	cur      store.Cursor  // backend cursor
	nextPk   uint64        // last matching idx key (for duplicate handling)
	idx      *Index        // back-ref to idx
	pack     *pack.Package // current idx package
	useCache bool
}

func NewLookupIterator(idx *Index, iks []uint64, useCache bool) *LookupIterator {
	slices.Sort(iks)
	return &LookupIterator{
		iks:      iks,
		idx:      idx,
		useCache: useCache,
	}
}

func (it *LookupIterator) Next(ctx context.Context) (*pack.Package, uint64, error) {
	// release last pack
	if it.pack != nil {
		it.pack.Release()
		it.pack = nil
	}

	// stop when lookup list is exhausted
	if len(it.iks) == 0 {
		return nil, 0, nil
	}

	// init cursor on first call
	if it.cur == nil {
		tx, err := it.idx.db.Begin(false)
		if err != nil {
			return nil, 0, err
		}
		if bucket := it.idx.dataBucket(tx); bucket != nil {
			it.cur = bucket.Cursor()
		} else {
			tx.Rollback()
			return nil, 0, store.ErrNoBucket
		}
		it.tx = tx
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

	// Skip all search keys that are in this pack. The last ik may continue
	// in the next pack, so we do not yet remove it
	lastIk := it.pack.Uint64(0, it.pack.Len()-1)
	for len(it.iks) > 0 && it.iks[0] < lastIk {
		it.iks = it.iks[1:]
	}

	// to handle non-unique indexes (duplicate hashes, intentional duplicates)
	// we use a cursor approach taking the next pk we're looking for into account
	// when scanning for the next pack. Our pack keys for this reason contain
	// the ik+pk combination of the first record in a pack as prefix.
	if len(it.iks) > 0 {
		if lastIk == it.iks[0] {
			it.nextPk = it.pack.Uint64(1, it.pack.Len()-1) + 1
		} else {
			it.nextPk = 0
			it.iks = it.iks[1:]
		}
	}

	return it.pack, lastIk, nil
}

func (it *LookupIterator) loadNextPack(ctx context.Context) (bool, error) {
	for {
		// stop when search keys are exhausted
		if len(it.iks) == 0 {
			return false, nil
		}

		// seek to the next search key, this will either point cur to the search
		// key directly or to the next larger key (in both cases the first
		// of a block pair, id = 0)
		ok := it.cur.Seek(it.idx.encodePackKey(it.iks[0], it.nextPk, 0))
		// it.idx.log.Infof("Seek 0x%016x:%016x:%d ok=%t", it.iks[0], it.nextPk, 0, ok)

		// seek to last pack when not found (our search key is likely in this pack)
		if !ok {
			// if exists, set cur to the first block in the last pair
			it.cur.Last()
			ok = it.cur.Prev()
			// it.idx.log.Infof("Seek last>prev %t", ok)
		}

		// no last pack? this must be an empty bucket
		if !ok {
			// it.idx.log.Infof("Empty bucket, skippikng all searches")
			it.iks = it.iks[:0]
			return false, nil
		}

		// decode the key
		ik, pk, id := it.idx.decodePackKey(it.cur.Key())
		// it.idx.log.Infof("Found 0x%016x:%016x:%d", ik, pk, id)

		// rewind if we're behind the search key
		if ik > it.iks[0] {
			// set cur to the first block in the previous pair
			it.cur.Prev()
			ok = it.cur.Prev()
			// it.idx.log.Infof("Rewind prev>prev %t", ok)

			// decode the previous key
			if ok {
				ik, pk, id = it.idx.decodePackKey(it.cur.Key())
				// it.idx.log.Infof("Now 0x%016x:%016x:%d", ik, pk, id)
				// assert we're actually at the first block
			}
		}
		assert.Always(id == 0, "must be at first block in index pair")

		// looks like this was the first pack and it did not contain
		// our search key, skip all search keys smaller than the found
		// key and retry
		if !ok {
			for len(it.iks) > 0 && it.iks[0] < ik {
				// it.idx.log.Infof("Ignoring ik 0x%016x", it.iks[0])
				it.iks = it.iks[1:]
			}
			continue
		}

		// we're at the correct pack now, load blocks into package
		it.pack = pack.New().
			WithSchema(it.idx.schema).
			WithMaxRows(it.idx.opts.PackSize)

		// try load block pair from cache
		if it.useCache {
			bcache := engine.GetEngine(ctx).BlockCache(it.idx.id)
			if b, ok := bcache.Get(it.idx.encodeCacheKey(ik, pk, 0)); ok {
				it.pack.WithBlock(0, b)
			}
			if b, ok := bcache.Get(it.idx.encodeCacheKey(ik, pk, 1)); ok {
				it.pack.WithBlock(1, b)
			}
		}

		// load missing blocks in pair from cursor
		for i := range []int{0, 1} {
			// assert block is correct
			bik, bpk, bid := it.idx.decodePackKey(it.cur.Key())
			assert.Always(bid == i, "unexpected block id", "ik", bik, "pk", bpk, "id", bid, "i", i)

			// decode when not already found in cache
			if it.pack.Block(i) == nil {
				// it.idx.log.Infof("Loading block 0x%016x:%016x:%d", bik, bpk, i)
				f, ok := it.idx.schema.FieldByIndex(i)
				assert.Always(ok, "missing schema field", "idx", i)
				b, err := block.Decode(f.Type().BlockType(), it.cur.Value())
				if err != nil {
					return false, fmt.Errorf("decoding block 0x%016x:%016x:%d: %v", bik, bpk, bid, err)
				}
				it.pack.WithBlock(i, b)
			}

			// go to next storage key (may be end of bucket)
			it.cur.Next()
		}

		return true, nil
	}
}

func (it *LookupIterator) Close() {
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
	it.iks = nil
	it.idx = nil
	it.nextPk = 0
	it.useCache = false
}

type ScanIterator struct {
	idx      *Index
	tx       store.Tx
	cur      store.Cursor
	from     []byte
	to       []byte
	node     *query.FilterTreeNode
	hits     []uint32
	pack     *pack.Package
	bits     *bitset.Bitset
	useCache bool
}

func NewScanIterator(idx *Index, node *query.FilterTreeNode, useCache bool) *ScanIterator {
	return &ScanIterator{
		node:     node,
		idx:      idx,
		hits:     arena.AllocUint32(idx.opts.PackSize),
		bits:     bitset.New(idx.opts.PackSize),
		useCache: useCache,
	}
}

func (it *ScanIterator) Next(ctx context.Context) (*pack.Package, []uint32, error) {
	// release last pack
	if it.pack != nil {
		it.pack.Release()
		it.pack = nil
	}

	// init range cursor
	if it.cur == nil {
		it.initRange()
		// it.idx.log.Infof("Scan range %x .. %x", it.from, it.to)
		tx, err := it.idx.db.Begin(false)
		if err != nil {
			return nil, nil, err
		}
		if bucket := it.idx.dataBucket(tx); bucket != nil {
			it.cur = bucket.Cursor()
		} else {
			tx.Rollback()
			return nil, nil, store.ErrNoBucket
		}
		it.tx = tx
		it.cur.Seek(it.from)

		// rewind from end if we have a single pack only
		if it.cur.Key() == nil {
			it.cur.Last()
			it.cur.Prev()
		}
	}

	for {
		// finish when no more pack keys match
		if it.cur.Key() == nil || bytes.Compare(it.cur.Key(), it.to) >= 0 {
			// it.idx.log.Infof("No more keys")
			return nil, nil, nil
		}

		// load pack (will also advance cursor to the next block pair)
		if err := it.loadPack(ctx); err != nil {
			return nil, nil, err
		}

		// find actual matches, zero bits before
		// it.idx.log.Infof("Run filter %s %T %v idx=%d",
		// it.node, it.node.Filter.Matcher, it.node.Filter.Matcher.Value(), it.node.Filter.Index)
		it.bits = match.MatchTree(it.node, it.pack, nil, it.bits.Zero())

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

func (it *ScanIterator) Close() {
	if it.pack != nil {
		it.pack.Release()
	}
	if it.cur != nil {
		it.cur.Close()
	}
	if it.tx != nil {
		it.tx.Rollback()
		it.tx = nil
	}
	arena.Free(it.hits[:0])
	it.bits.Close()
	it.bits = nil
	it.pack = nil
	it.cur = nil
	it.hits = nil
	it.idx = nil
	it.node = nil
	it.useCache = false
	it.from = nil
	it.to = nil
}

var (
	FF  = []byte{0xFF}
	END = num.EncodeUvarint(0xFFFFFFFFFFFFFFFF)
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

func (it *ScanIterator) initRange() {
	// create from / to range
	switch it.node.Filter.Mode {
	case types.FilterModeEqual:
		// EQ => scan(prefix, prefix+FF)
		it.from = makePrefix(it.node.Filter.Type, it.node.Filter.Value)
		it.to = store.BytesPrefix(it.from).Limit

	case types.FilterModeLt:
		// LT    => scan(0x, to)
		it.from = []byte{}
		it.to = makePrefix(it.node.Filter.Type, it.node.Filter.Value)

	case types.FilterModeLe:
		// LE    => scan(0x, to)
		it.from = []byte{}
		it.to = store.BytesPrefix(makePrefix(it.node.Filter.Type, it.node.Filter.Value)).Limit

	case types.FilterModeGt:
		// GT    => scan(from, FF)
		it.from = store.BytesPrefix(makePrefix(it.node.Filter.Type, it.node.Filter.Value)).Limit
		it.to = END

	case types.FilterModeGe:
		// GE    => scan(from, FF)
		it.from = makePrefix(it.node.Filter.Type, it.node.Filter.Value)
		it.to = END

	case types.FilterModeRange:
		// RG    => scan(from, to)
		it.from = makePrefix(it.node.Filter.Type, it.node.Filter.Value.(query.RangeValue)[0])
		it.to = makePrefix(it.node.Filter.Type, it.node.Filter.Value.(query.RangeValue)[1])
	}
}

func (it *ScanIterator) loadPack(ctx context.Context) error {
	// decode key
	ik, pk, id := it.idx.decodePackKey(it.cur.Key())
	assert.Always(id == 0, "ScanIterator cursor should point at first elemeng in block pair")

	// load pack
	it.pack = pack.New().
		WithSchema(it.idx.schema).
		WithMaxRows(it.idx.opts.PackSize)

	// try load block pair from cache
	if it.useCache {
		bcache := engine.GetEngine(ctx).BlockCache(it.idx.id)
		if b, ok := bcache.Get(it.idx.encodeCacheKey(ik, pk, 0)); ok {
			it.pack.WithBlock(0, b)
		}
		if b, ok := bcache.Get(it.idx.encodeCacheKey(ik, pk, 1)); ok {
			it.pack.WithBlock(1, b)
		}
	}

	// load missing blocks from cursor
	for i := range []int{0, 1} {
		if it.pack.Block(i) == nil {
			// it.idx.log.Infof("Loading block 0x%016x:%016x:%d", ik, pk, i)
			f, ok := it.idx.schema.FieldByIndex(i)
			assert.Always(ok, "missing schema field", "idx", i)
			b, err := block.Decode(f.Type().BlockType(), it.cur.Value())
			if err != nil {
				return fmt.Errorf("loading block 0x%016x:%016x:%d: %v", ik, pk, i, err)
			}
			it.pack.WithBlock(i, b)
		}
		it.cur.Next()
	}
	return nil
}
