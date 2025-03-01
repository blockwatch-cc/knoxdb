// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/match"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/query"
)

var (
	// statistics keys
	PACKS_SCANNED_KEY   = "packs_scanned"
	PACKS_SCHEDULED_KEY = "packs_scheduled"
	JOURNAL_TIME_KEY    = "journal_time"
)

type Iterator struct {
	it       *stats.Iterator
	query    *query.QueryPlan
	table    *Table
	hits     []uint32
	pack     *pack.Package
	bits     *bitset.Bitset
	useCache bool
}

func NewIterator(q *query.QueryPlan) *Iterator {
	t := q.Table.(*Table)
	return &Iterator{
		query:    q,
		table:    t,
		hits:     arena.Alloc(arena.AllocUint32, t.opts.PackSize).([]uint32),
		bits:     bitset.NewBitset(t.opts.PackSize),
		useCache: !q.Flags.IsNoCache(),
	}
}

func (it *Iterator) Next(ctx context.Context) (*pack.Package, []uint32, error) {
	// release last pack
	if it.pack != nil {
		it.pack.Release()
		it.pack = nil
	}

	for {
		// find next potential pack match in statistics index, scan in pk order,
		// init on first use
		var ok bool
		if it.it == nil {
			it.it, ok = it.table.stats.Query(ctx, it.query.Filters, it.query.Order)
		} else {
			ok = it.it.Next()
		}

		// no more match, return end condition (nil pack and nil error)
		if !ok {
			return nil, nil, nil
		}

		// load match columns only
		key, nval := it.it.Key(), it.it.NValues()
		var err error
		it.pack, err = it.table.loadSharedPack(ctx, key, nval, it.useCache, it.query.RequestSchema)
		if err != nil {
			return nil, nil, err
		}
		it.query.Stats.Count(PACKS_SCHEDULED_KEY, 1)

		it.query.Log.Debugf("IT-fwd checking pack=%08x size=%d", key, nval)

		// find actual matches
		it.bits = match.MatchTree(it.query.Filters, it.pack, it.it, it.bits)

		// handle false positive metadata matches
		if it.bits.None() {
			it.pack.Release()
			it.pack = nil
			continue
		}

		// handle real matches
		it.hits = it.bits.Indexes(it.hits)
		it.query.Stats.Count(PACKS_SCANNED_KEY, 1)

		// load remaining columns here
		it.pack, err = it.table.loadSharedPack(ctx, key, nval, it.useCache, it.query.ResultSchema)
		if err != nil {
			return nil, nil, err
		}

		it.query.Log.Debugf("IT-fwd pack=%08x matches=%d", key, len(it.hits))

		return it.pack, it.hits, nil
	}
}

func (it *Iterator) Close() {
	if it.pack != nil {
		it.pack.Release()
	}
	arena.Free(arena.AllocUint32, it.hits[:0])
	it.bits.Close()
	it.pack = nil
	it.hits = nil
	it.table = nil
	it.query = nil
	it.it.Close()
	it.it = nil
	it.useCache = false
}
