// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"slices"

	"blockwatch.cc/knoxdb/vec"
)

type ForwardIterator struct {
	idx   int
	query *Query
	table *PackTable
	hits  []uint32
	pack  *Package
}

func NewForwardIterator(q *Query) *ForwardIterator {
	t := q.table.(*PackTable)
	return &ForwardIterator{
		idx:   -1,
		query: q,
		table: t,
		hits:  t.u32Pool.Get().([]uint32),
	}
}

func (it *ForwardIterator) Next(tx *Tx) (*Package, []uint32, error) {
	// release last pack
	if it.pack != nil {
		it.pack.Release()
		it.pack = nil
	}

	for {
		// find next potential pack match, scan in pk order
		// (pairs in pack index are sorted by min pk)
		var (
			info PackInfo
			ok   bool
		)
		it.idx++
		for l := len(it.table.packidx.pos); it.idx < l; it.idx++ {
			pos := it.table.packidx.pos[it.idx]
			info = it.table.packidx.packs[pos]
			if !it.query.conds.MaybeMatchPack(info) {
				continue
			}
			ok = true
			break
		}

		// no more match, return end condition (nil pack and nil error)
		if !ok {
			return nil, nil, nil
		}

		// load pack
		// TODO: only load match columns and load remaining columns later
		var err error
		it.pack, err = it.table.loadSharedPack(tx, info.Key, !it.query.NoCache, it.query.freq)
		if err != nil {
			return nil, nil, err
		}
		it.query.stats.PacksScheduled++

		// find actual matches
		bits := it.query.conds.MatchPack(it.pack, info)

		// handle false positive metadata matches
		if bits.Count() == 0 {
			bits.Close()
			it.pack.Release()
			it.pack = nil
			continue
		}

		// handle real matches
		it.hits = bits.IndexesU32(it.hits)
		bits.Close()
		it.query.stats.PacksScanned++

		// TODO: load remaining columns here

		return it.pack, it.hits, nil
	}
}

func (it *ForwardIterator) Close() {
	if it.pack != nil {
		it.pack.Release()
	}
	it.table.u32Pool.Put(it.hits[:0])
	it.pack = nil
	it.hits = nil
	it.table = nil
	it.query = nil
	it.idx = 0
}

type ReverseIterator struct {
	idx   int
	query *Query
	table *PackTable
	hits  []uint32
	pack  *Package
}

func NewReverseIterator(q *Query) *ReverseIterator {
	t := q.table.(*PackTable)
	return &ReverseIterator{
		idx:   len(t.packidx.pos),
		query: q,
		table: t,
		hits:  t.u32Pool.Get().([]uint32),
	}
}

func (it *ReverseIterator) Next(tx *Tx) (*Package, []uint32, error) {
	// release last pack
	if it.pack != nil {
		it.pack.Release()
		it.pack = nil
	}

	for {
		// find next potential pack match, scan in pk order
		// (pairs in pack index are sorted by min pk)
		var (
			info PackInfo
			ok   bool
		)
		it.idx--
		for ; it.idx >= 0; it.idx-- {
			pos := it.table.packidx.pos[it.idx]
			info = it.table.packidx.packs[pos]
			if !it.query.conds.MaybeMatchPack(info) {
				continue
			}
			ok = true
			break
		}

		// no more match, return end condition (nil pack and nil error)
		if !ok {
			return nil, nil, nil
		}

		// load pack
		// TODO: only load match columns and load remaining columns later
		var err error
		it.pack, err = it.table.loadSharedPack(tx, info.Key, !it.query.NoCache, it.query.freq)
		if err != nil {
			return nil, nil, err
		}
		it.query.stats.PacksScheduled++

		// find actual matches
		bits := it.query.conds.MatchPack(it.pack, info)

		// handle false positive metadata matches
		if bits.Count() == 0 {
			bits.Close()
			it.pack.Release()
			it.pack = nil
			continue
		}

		// handle real matches
		it.hits = bits.IndexesU32(it.hits)
		bits.Close()
		it.query.stats.PacksScanned++

		// TODO: load remaining columns here

		return it.pack, it.hits, nil
	}
}

func (it *ReverseIterator) Close() {
	if it.pack != nil {
		it.pack.Release()
	}
	it.table.u32Pool.Put(it.hits[:0])
	it.pack = nil
	it.hits = nil
	it.table = nil
	it.query = nil
	it.idx = 0
}

type LookupIterator struct {
	pks    []uint64
	idx    int
	query  *Query
	table  *PackTable
	pack   *Package
	minPks []uint64
	maxPks []uint64
}

func NewLookupIterator(q *Query, pks []uint64) *LookupIterator {
	t := q.table.(*PackTable)
	mins, maxs := t.packidx.MinMaxSlices()
	return &LookupIterator{
		pks:    pks,
		idx:    -1,
		query:  q,
		table:  t,
		minPks: mins,
		maxPks: maxs,
	}
}

func (it *LookupIterator) Next(tx *Tx) (*Package, uint64, error) {
	// release last pack
	if it.pack != nil {
		it.pack.Release()
		it.pack = nil
	}

	// stop when lookup list is exhausted
	if len(it.pks) == 0 {
		return nil, 0, nil
	}

	for {
		// find next potential pack match, scan in pk order
		// (pairs in pack index are sorted by min pk)
		var (
			info PackInfo
			ok   bool
		)
		it.idx++
		for l := len(it.minPks); it.idx < l; it.idx++ {
			if !vec.Uint64.ContainsRange(it.pks, it.minPks[it.idx], it.maxPks[it.idx]) {
				continue
			}
			pos := it.table.packidx.pos[it.idx]
			info = it.table.packidx.packs[pos]

			// trim lookup pks, find first pk larger than current pack max
			next, found := slices.BinarySearch(it.pks, it.maxPks[it.idx])
			if found {
				next++
			}
			it.pks = it.pks[next:]

			ok = true
			break
		}

		// no more match, return end condition (nil pack and nil error)
		if !ok {
			return nil, 0, nil
		}

		// load pack
		var err error
		it.pack, err = it.table.loadSharedPack(tx, info.Key, !it.query.NoCache, it.query.freq)
		if err != nil {
			return nil, 0, err
		}
		it.query.stats.PacksScheduled++
		it.query.stats.PacksScanned++

		return it.pack, it.maxPks[it.idx], nil
	}
}

func (it *LookupIterator) Close() {
	if it.pack != nil {
		it.pack.Release()
	}
	it.pks = nil
	it.minPks = nil
	it.maxPks = nil
	it.pack = nil
	it.table = nil
	it.query = nil
	it.idx = 0
}
