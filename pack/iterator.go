// Copyright (c) 2018-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

type ForwardIterator struct {
	idx   int
	query *Query
	table *Table
	hits  []uint32
	pack  *Package
}

func NewForwardIterator(q *Query) *ForwardIterator {
	return &ForwardIterator{
		idx:   -1,
		query: q,
		table: q.table,
		hits:  q.table.u32Pool.Get().([]uint32),
	}
}

func (it *ForwardIterator) Next(tx *Tx) (*Package, []uint32, error) {
	// release last pack
	if it.pack != nil {
		it.table.releaseSharedPack(it.pack)
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
			it.table.releaseSharedPack(it.pack)
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
		it.table.releaseSharedPack(it.pack)
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
	table *Table
	hits  []uint32
	pack  *Package
}

func NewReverseIterator(q *Query) *ReverseIterator {
	return &ReverseIterator{
		idx:   len(q.table.packidx.pos),
		query: q,
		table: q.table,
		hits:  q.table.u32Pool.Get().([]uint32),
	}
}

func (it *ReverseIterator) Next(tx *Tx) (*Package, []uint32, error) {
	// release last pack
	if it.pack != nil {
		it.table.releaseSharedPack(it.pack)
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
			it.table.releaseSharedPack(it.pack)
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
		it.table.releaseSharedPack(it.pack)
	}
	it.table.u32Pool.Put(it.hits[:0])
	it.pack = nil
	it.hits = nil
	it.table = nil
	it.query = nil
	it.idx = 0
}
