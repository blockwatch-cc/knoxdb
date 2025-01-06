// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"slices"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/match"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/pkg/slicex"
)

type IndexLookupIterator struct {
	pks      *slicex.OrderedNumbers[uint64]
	idx      int
	index    *Index
	pack     *pack.Package
	useCache bool
}

func NewIndexLookupIterator(idx *Index, pks []uint64, useCache bool) *IndexLookupIterator {
	return &IndexLookupIterator{
		pks:      slicex.NewOrderedNumbers(pks),
		idx:      -1,
		index:    idx,
		useCache: useCache,
	}
}

func (it *IndexLookupIterator) Next(ctx context.Context) (*pack.Package, uint64, error) {
	// release last pack
	if it.pack != nil {
		it.pack.Release()
		it.pack = nil
	}

	// stop when lookup list is exhausted
	if it.pks.Len() == 0 {
		return nil, 0, nil
	}

	for {
		// find next potential pack match, scan in pk order low to high
		// (min/max pairs are NOT sorted by min pk, but  indirection is)
		var (
			info         *stats.PackStats
			ok           bool
			minPk, maxPk uint64
		)
		it.idx++
		for l := it.index.stats.Len(); it.idx < l && it.pks.Len() > 0; it.idx++ {
			// map index to pack position
			minPk, maxPk = it.index.stats.MinMaxSorted(it.idx)

			// check if we need to visit this pack
			if !it.pks.ContainsRange(minPk, maxPk) {
				continue
			}

			// trim lookup pks, find first pk larger than current pack max
			next, found := slices.BinarySearch(it.pks.Values, maxPk)
			if found {
				next++
			}
			it.pks.Values = it.pks.Values[next:]

			// fetch pack stats from index
			info, ok = it.index.stats.GetSorted(it.idx)
			break
		}

		// no more match, return end condition (nil pack and nil error)
		if !ok {
			return nil, 0, nil
		}

		// load pack (result schema)
		var err error
		it.pack, err = it.index.loadSharedPack(ctx, info.Key, info.NValues, it.useCache)
		if err != nil {
			return nil, 0, err
		}

		return it.pack, maxPk, nil
	}
}

func (it *IndexLookupIterator) Close() {
	if it.pack != nil {
		it.pack.Release()
	}
	it.pks = nil
	it.pack = nil
	it.index = nil
	it.idx = 0
	it.useCache = false
}

type IndexScanIterator struct {
	idx      int
	node     *query.FilterTreeNode
	index    *Index
	hits     []uint32
	pack     *pack.Package
	useCache bool
}

func NewIndexScanIterator(idx *Index, node *query.FilterTreeNode, useCache bool) *IndexScanIterator {
	return &IndexScanIterator{
		idx:      -1,
		node:     node,
		index:    idx,
		hits:     arena.Alloc(arena.AllocUint32, idx.opts.PackSize).([]uint32),
		useCache: useCache,
	}
}

func (it *IndexScanIterator) Next(ctx context.Context) (*pack.Package, []uint32, error) {
	// release last pack
	if it.pack != nil {
		it.pack.Release()
		it.pack = nil
	}

	for {
		// find next potential pack match, scan in pk order
		// (pairs in pack index are sorted by min pk)
		it.idx++
		info, ok := it.index.stats.GetSorted(it.idx)
		for ok {
			if match.MaybeMatchTree(it.node, info) {
				break
			}
			it.idx++
			info, ok = it.index.stats.GetSorted(it.idx)
		}

		// no more match, return end condition (nil pack and nil error)
		if !ok {
			return nil, nil, nil
		}

		// load match columns only
		var err error
		it.pack, err = it.index.loadSharedPack(ctx, info.Key, info.NValues, it.useCache)
		if err != nil {
			return nil, nil, err
		}

		// find actual matches
		bits := match.MatchTree(it.node, it.pack, info)

		// handle false positive stats matches
		if bits.Count() == 0 {
			bits.Close()
			it.pack.Release()
			it.pack = nil
			continue
		}

		// handle real matches
		it.hits = bits.Indexes(it.hits)
		bits.Close()

		// load remaining columns here
		it.pack, err = it.index.loadSharedPack(ctx, info.Key, info.NValues, it.useCache)
		if err != nil {
			return nil, nil, err
		}

		return it.pack, it.hits, nil
	}
}

func (it *IndexScanIterator) Close() {
	if it.pack != nil {
		it.pack.Release()
	}
	arena.Free(arena.AllocUint32, it.hits[:0])
	it.pack = nil
	it.hits = nil
	it.index = nil
	it.node = nil
	it.idx = 0
	it.useCache = false
}

// type IndexFlushIterator struct {
//  index *Index
//  pack  *pack.Package
//  last  int
// }

// func NewIndexFlushIterator(idx *Index) *IndexFlushIterator {
//  return &IndexFlushIterator{
//      index: idx,
//  }
// }

// func (it *IndexFlushIterator) Next(ctx context.Context, pk uint64) (*pack.Package, error) {
//  // release last pack
//  if it.pack != nil {
//      it.pack.Release()
//      it.pack = nil
//  }
//  key, pmin, pmax, nextmin, _ = it.index.stats.Best(pk)

// }
