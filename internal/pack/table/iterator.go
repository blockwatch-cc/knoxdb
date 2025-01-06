// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

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

var (
	// statistics keys
	PACKS_SCANNED_KEY   = "packs_scanned"
	PACKS_SCHEDULED_KEY = "packs_scheduled"
	JOURNAL_TIME_KEY    = "journal_time"
)

// Iterator is a common interface for walking packs
type Iterator interface {
	Next(context.Context) (*pack.Package, []uint32, error)
}

type ForwardIterator struct {
	idx      int
	query    *query.QueryPlan
	table    *Table
	hits     []uint32
	pack     *pack.Package
	useCache bool
}

func NewForwardIterator(q *query.QueryPlan) *ForwardIterator {
	t := q.Table.(*Table)
	return &ForwardIterator{
		idx:      -1,
		query:    q,
		table:    t,
		hits:     arena.Alloc(arena.AllocUint32, t.opts.PackSize).([]uint32),
		useCache: !q.Flags.IsNoCache(),
	}
}

func (it *ForwardIterator) Next(ctx context.Context) (*pack.Package, []uint32, error) {
	// release last pack
	if it.pack != nil {
		it.pack.Release()
		it.pack = nil
	}

	for {
		// find next potential pack match, scan in pk order
		// (pairs in pack index are sorted by min pk)
		it.idx++
		info, ok := it.table.stats.GetSorted(it.idx)
		for ok {
			it.query.Log.Debugf("IT-fwd checking stats for pack=%08x size=%d", info.Key, info.NValues)
			if match.MaybeMatchTree(it.query.Filters, info) {
				break
			}
			it.idx++
			info, ok = it.table.stats.GetSorted(it.idx)
		}

		// no more match, return end condition (nil pack and nil error)
		if !ok {
			return nil, nil, nil
		}

		// load match columns only
		var err error
		it.pack, err = it.table.loadSharedPack(ctx, info.Key, info.NValues, it.useCache, it.query.RequestSchema)
		if err != nil {
			return nil, nil, err
		}
		it.query.Stats.Count(PACKS_SCHEDULED_KEY, 1)

		it.query.Log.Debugf("IT-fwd checking pack=%08x size=%d", info.Key, info.NValues)

		// find actual matches
		bits := match.MatchTree(it.query.Filters, it.pack, info)

		// handle false positive metadata matches
		if bits.Count() == 0 {
			bits.Close()
			it.pack.Release()
			it.pack = nil
			continue
		}

		// handle real matches
		it.hits = bits.Indexes(it.hits)
		bits.Close()
		it.query.Stats.Count(PACKS_SCANNED_KEY, 1)

		// load remaining columns here
		it.pack, err = it.table.loadSharedPack(ctx, info.Key, info.NValues, it.useCache, it.query.ResultSchema)
		if err != nil {
			return nil, nil, err
		}

		it.query.Log.Debugf("IT-fwd pack=%08x matches=%d", info.Key, len(it.hits))

		return it.pack, it.hits, nil
	}
}

func (it *ForwardIterator) Close() {
	if it.pack != nil {
		it.pack.Release()
	}
	arena.Free(arena.AllocUint32, it.hits[:0])
	it.pack = nil
	it.hits = nil
	it.table = nil
	it.query = nil
	it.idx = 0
	it.useCache = false
}

type ReverseIterator struct {
	idx      int
	query    *query.QueryPlan
	table    *Table
	hits     []uint32
	pack     *pack.Package
	useCache bool
}

func NewReverseIterator(q *query.QueryPlan) *ReverseIterator {
	t := q.Table.(*Table)
	return &ReverseIterator{
		idx:      t.stats.Len(),
		query:    q,
		table:    t,
		hits:     arena.Alloc(arena.AllocUint32, t.opts.PackSize).([]uint32),
		useCache: !q.Flags.IsNoCache(),
	}
}

func (it *ReverseIterator) Next(ctx context.Context) (*pack.Package, []uint32, error) {
	// release last pack
	if it.pack != nil {
		it.pack.Release()
		it.pack = nil
	}

	for {
		// find next potential pack match, scan in pk order
		// (pairs in pack index are sorted by min pk)
		it.idx--
		info, ok := it.table.stats.GetSorted(it.idx)
		for ok {
			if match.MaybeMatchTree(it.query.Filters, info) {
				break
			}
			it.idx--
			info, ok = it.table.stats.GetSorted(it.idx)
		}

		// no more match, return end condition (nil pack and nil error)
		if !ok {
			return nil, nil, nil
		}

		// load match columns only
		var err error
		it.pack, err = it.table.loadSharedPack(ctx, info.Key, info.NValues, it.useCache, it.query.RequestSchema)
		if err != nil {
			return nil, nil, err
		}
		it.query.Stats.Count(PACKS_SCHEDULED_KEY, 1)

		it.query.Log.Debugf("IT-rev checking pack=%08x size=%d", info.Key, info.NValues)

		// find actual matches
		bits := match.MatchTree(it.query.Filters, it.pack, info)

		// handle false positive metadata matches
		if bits.Count() == 0 {
			bits.Close()
			it.pack.Release()
			it.pack = nil
			continue
		}

		// handle real matches
		it.hits = bits.Indexes(it.hits)
		bits.Close()
		it.query.Stats.Count(PACKS_SCANNED_KEY, 1)

		// load remaining columns here
		it.pack, err = it.table.loadSharedPack(ctx, info.Key, info.NValues, it.useCache, it.query.ResultSchema)
		if err != nil {
			return nil, nil, err
		}

		it.query.Log.Debugf("IT-rev pack=%08x matches=%d", info.Key, len(it.hits))

		return it.pack, it.hits, nil
	}
}

func (it *ReverseIterator) Close() {
	if it.pack != nil {
		it.pack.Release()
	}
	arena.Free(arena.AllocUint32, it.hits[:0])
	it.pack = nil
	it.hits = nil
	it.table = nil
	it.query = nil
	it.idx = 0
	it.useCache = false
}

type LookupIterator struct {
	pks      *slicex.OrderedNumbers[uint64]
	idx      int
	query    *query.QueryPlan
	table    *Table
	pack     *pack.Package
	useCache bool
}

func NewLookupIterator(q *query.QueryPlan, pks []uint64) *LookupIterator {
	return &LookupIterator{
		pks:      slicex.NewOrderedNumbers(pks),
		idx:      -1,
		query:    q,
		table:    q.Table.(*Table),
		useCache: !q.Flags.IsNoCache(),
	}
}

func (it *LookupIterator) Next(ctx context.Context) (*pack.Package, uint64, error) {
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
		for l := it.table.stats.Len(); it.idx < l && it.pks.Len() > 0; it.idx++ {
			// map index to pack position
			minPk, maxPk = it.table.stats.MinMaxSorted(it.idx)

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

			// fetch pack metadata from index
			info, ok = it.table.stats.GetSorted(it.idx)
			break
		}

		// no more match, return end condition (nil pack and nil error)
		if !ok {
			return nil, 0, nil
		}

		// load pack (result schema)
		var err error
		it.pack, err = it.table.loadSharedPack(ctx, info.Key, info.NValues, it.useCache, it.query.ResultSchema)
		if err != nil {
			return nil, 0, err
		}
		it.query.Stats.Count(PACKS_SCHEDULED_KEY, 1)
		it.query.Stats.Count(PACKS_SCANNED_KEY, 1)

		return it.pack, maxPk, nil
	}
}

func (it *LookupIterator) Close() {
	if it.pack != nil {
		it.pack.Release()
	}
	it.pks = nil
	it.pack = nil
	it.table = nil
	it.query = nil
	it.idx = 0
	it.useCache = false
}
