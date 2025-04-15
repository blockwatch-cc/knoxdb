// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"context"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
)

// stats iterator
type Iterator struct {
	ctx     context.Context
	idx     *Index                // back reference to index
	flt     *query.FilterTreeNode // ptr to current query conditions
	use     Features              // flags indicating which filter to use
	smatch  *bitset.Bitset        // snode matches
	vmatch  *bitset.Bitset        // spack matches
	sx      int                   // current snode index
	snode   *SNode                // current matching snode
	match   []uint32              // row matches in current stats pack
	n       int                   // current offset inside match rows
	reverse bool                  // iteration order
}

var _ Reader = (*Iterator)(nil)

func (it *Iterator) Close() {
	if it == nil {
		return
	}
	it.idx = nil
	it.flt = nil
	it.use = 0
	it.smatch.Close()
	it.smatch = nil
	it.vmatch.Close()
	it.vmatch = nil
	it.snode = nil
	if len(it.match) > 1 {
		arena.Free(it.match)
	}
	it.match = nil
	it.reverse = false
}

func (it *Iterator) IsValid() bool {
	return it != nil && it.snode != nil
}

// query, merge
func (it Iterator) Key() uint32 {
	if it.snode == nil {
		return 0
	}
	return it.snode.spack.Uint32(STATS_ROW_KEY, int(it.match[it.n]))
}

// merge
func (it Iterator) IsFull() bool {
	if it.snode == nil {
		return false
	}
	nvals := it.snode.spack.Int64(STATS_ROW_NVALS, int(it.match[it.n]))
	return int(nvals) == it.idx.nmax
}

// query
func (it *Iterator) MinMax(col int) (any, any) {
	if it.snode == nil {
		return nil, nil
	}
	minx, maxx := minColIndex(col), maxColIndex(col)
	minv := it.snode.spack.Block(minx).Get(int(it.match[it.n]))
	maxv := it.snode.spack.Block(maxx).Get(int(it.match[it.n]))
	return minv, maxv
}

// query (range filter access only)
func (it Iterator) NValues() int {
	if it.snode == nil {
		return 0
	}
	nvals := it.snode.spack.Int64(STATS_ROW_NVALS, int(it.match[it.n]))
	return int(nvals)
}

// query
func (it Iterator) ReadWire() []byte {
	if it.snode == nil {
		return nil
	}
	buf, err := it.snode.spack.ReadWire(int(it.match[it.n]))
	if err != nil {
		panic(err)
	}
	return buf
}

// merge
func (it Iterator) MinMaxPk() (any, any) {
	return it.MinMax(it.idx.px)
}

// query, merge?
func (it *Iterator) Next() bool {
	if it.reverse {
		return it.prev()
	} else {
		return it.next()
	}
}

func (it *Iterator) next() bool {
	// find the next snode with matches if any
	it.n++
	if it.n == len(it.match) {
		it.n = 0
		it.match = it.match[:0]
		for {
			it.snode = nil
			// have we exhausted all potential snodes
			if it.sx >= it.smatch.Len() {
				return false
			}

			// find the next snode
			it.sx++
			if !it.smatch.IsSet(it.sx) {
				continue
			}
			it.snode = it.idx.snodes[it.sx]

			// TODO: we could rewind the iterator if we did not clear bits here
			it.smatch.Clear(it.sx)

			// query snode statistics pack and filters
			if err := it.snode.Query(it); err != nil {
				// what to do?
				panic(err)
			}
			if len(it.match) > 0 {
				break
			}
		}
	}

	return true
}

func (it *Iterator) prev() bool {
	// find the next snode with matches if any
	it.n--
	if it.n < 0 {
		it.n = -1
		it.match = it.match[:0]
		for {
			it.snode = nil
			// have we exhausted all potential snodes
			if it.sx < 0 {
				return false
			}

			// find the next snode
			it.sx--
			if !it.smatch.IsSet(it.sx) {
				continue
			}
			it.snode = it.idx.snodes[it.sx]

			// TODO: we could rewind the iterator if we did not clear bits here
			it.smatch.Clear(it.sx)

			// query snode statistics pack and filters
			if err := it.snode.Query(it); err != nil {
				// what to do?
				panic(err)
			}
			if l := len(it.match); l > 0 {
				it.n = l - 1
				break
			}
		}
	}

	return true
}

// query
func (it Iterator) Range() pack.Range {
	// get max upper bound
	nRows := it.NValues()

	// return full range when no int column is used
	if !it.use.Is(FeatRangeFilter) {
		return pack.Range{0, uint32(nRows)}
	}

	// lookup data pack key
	key := it.Key()

	// run inside storage tx
	var rg pack.Range
	it.idx.db.View(func(tx store.Tx) error {
		rg = it.combinedRange(it.idx.rangeBucket(tx), key, it.flt, nRows)
		return nil
	})

	return rg
}

// query and aggregate range filters for all integer columns
// stop early when max range i.e. full pack (OR) or empty range (AND) is reached
func (it *Iterator) combinedRange(b store.Bucket, key uint32, n *query.FilterTreeNode, nRows int) pack.Range {
	if n.IsLeaf() {
		// load range index data
		idx := RangeIndexFromBytes(b.Get(filterKey(key, n.Filter.Index)))
		defer idx.Close()

		// ignore errors
		if !idx.IsValid() {
			return pack.InvalidRange
		}

		// load min value for this column
		minx := minColIndex(n.Filter.Index)
		minv := it.snode.spack.Block(int(minx)).Get(int(it.match[it.n]))

		return idx.Query(n.Filter, minv, nRows)
	}

	rg := pack.InvalidRange
	for _, v := range n.Children {
		if n.OrKind {
			rg = rg.Union(it.combinedRange(b, key, v, nRows))
			// stop early
			if rg.IsFull(nRows) {
				break
			}
		} else {
			rg = rg.Intersect(it.combinedRange(b, key, v, nRows))
			// stop early
			if !rg.IsValid() {
				break
			}
		}
	}
	return rg
}
