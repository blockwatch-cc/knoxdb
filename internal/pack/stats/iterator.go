// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"context"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/store"
)

// stats iterator
type Iterator struct {
	ctx     context.Context
	idx     *Index         // back reference to index
	flt     *filter.Node   // ptr to current query conditions (may be nil)
	ids     []uint16       // required metadata blocks for the filter (nil == all)
	use     Features       // flags indicating which filter to use
	smatch  *bitset.Bitset // snode matches
	vmatch  *bitset.Bitset // spack matches
	sx      int            // current snode index
	snode   *SNode         // current matching snode
	match   []uint32       // row matches in current stats pack
	n       int            // current offset inside match rows
	reverse bool           // iteration order
}

var _ engine.StatsReader = (*Iterator)(nil)

func (it *Iterator) Close() {
	if it == nil {
		return
	}
	it.idx = nil
	it.flt = nil
	it.ids = nil
	it.use = 0
	it.smatch.Close()
	it.smatch = nil
	it.vmatch.Close()
	it.vmatch = nil
	it.snode = nil
	arena.Free(it.match)
	it.match = nil
	it.reverse = false
	it.sx = 0
	it.n = 0
}

func (it *Iterator) IsValid() bool {
	return it != nil && it.snode != nil
}

// returns key, version, nvalues in a single call
func (it *Iterator) PackInfo() (uint32, uint32, int) {
	if it.snode == nil {
		return 0, 0, 0
	}
	pos := int(it.match[it.n])
	pkg := it.snode.spack.Load()
	k := pkg.Uint32(STATS_ROW_KEY, pos)
	v := pkg.Uint32(STATS_ROW_VERSION, pos)
	n := pkg.Uint64(STATS_ROW_NVALS, pos)
	return k, v, int(n)
}

// query, merge
func (it *Iterator) Key() uint32 {
	if it.snode == nil {
		return 0
	}
	return it.snode.spack.Load().Uint32(STATS_ROW_KEY, int(it.match[it.n]))
}

// query, merge
func (it *Iterator) Version() uint32 {
	if it.snode == nil {
		return 0
	}
	return it.snode.spack.Load().Uint32(STATS_ROW_VERSION, int(it.match[it.n]))
}

// merge
func (it *Iterator) IsFull() bool {
	if it.snode == nil {
		return false
	}
	nvals := it.snode.spack.Load().Uint64(STATS_ROW_NVALS, int(it.match[it.n]))
	return int(nvals) == it.idx.nmax
}

// query
func (it *Iterator) MinMax(col int) (any, any) {
	if it.snode == nil {
		return nil, nil
	}
	minx, maxx := minColIndex(col), maxColIndex(col)
	pkg := it.snode.spack.Load()
	minv := pkg.Block(minx).Get(int(it.match[it.n]))
	maxv := pkg.Block(maxx).Get(int(it.match[it.n]))
	return minv, maxv
}

// query (range filter access only)
func (it *Iterator) NValues() int {
	if it.snode == nil {
		return 0
	}
	nvals := it.snode.spack.Load().Uint64(STATS_ROW_NVALS, int(it.match[it.n]))
	return int(nvals)
}

// query
func (it *Iterator) ReadWire() []byte {
	if it.snode == nil {
		return nil
	}
	buf, err := it.snode.spack.Load().ReadWire(int(it.match[it.n]))
	if err != nil {
		assert.Unreachable("invalid snode wire layout", err)
	}
	return buf
}

// merge, query
func (it *Iterator) MinMaxRid() (uint64, uint64) {
	a, b := it.MinMax(it.idx.rx)
	return a.(uint64), b.(uint64)
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
			if !it.smatch.Contains(it.sx) {
				continue
			}
			it.snode = it.idx.snodes[it.sx]

			// Note: we could rewind the iterator if we did not clear bits here
			it.smatch.Unset(it.sx)

			// query snode statistics pack and filters
			if err := it.snode.Query(it); err != nil {
				// it.idx.log.Errorf("it: query failed: %v", err)
				assert.Unreachable("snode query failed", err)
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
			if !it.smatch.Contains(it.sx) {
				continue
			}
			it.snode = it.idx.snodes[it.sx]

			// Note: we could rewind the iterator if we did not clear bits here
			it.smatch.Unset(it.sx)

			// query snode statistics pack and filters
			if err := it.snode.Query(it); err != nil {
				// it.idx.log.Errorf("it: query failed: %v", err)
				assert.Unreachable("snode query failed", err)
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
func (it *Iterator) Range() types.Range {
	// get max upper bound
	nRows := it.NValues()

	// return full range when no int column is used
	if !it.use.Is(FeatRangeFilter) {
		return types.Range{0, uint32(nRows)}
	}

	// lookup data pack key and version
	key := it.Key()
	ver := it.Version()

	// run inside storage tx
	var rg types.Range
	it.idx.db.View(func(tx store.Tx) error {
		rg = it.combinedRange(it.idx.rangeBucket(tx), key, ver, it.flt, nRows)
		return nil
	})

	return rg
}

// query and aggregate range filters for all integer columns
// stop early when max range i.e. full pack (OR) or empty range (AND) is reached
func (it *Iterator) combinedRange(b store.Bucket, key, ver uint32, n *filter.Node, nRows int) types.Range {
	if n.IsLeaf() {
		// load range index data
		idx := RangeIndexFromBytes(b.Get(encodeFilterKey(key, ver, n.Filter.Id)))
		defer idx.Close()

		// ignore errors
		if !idx.IsValid() {
			return types.InvalidRange
		}

		// load min value for this column
		minx := minColIndex(n.Filter.Index)
		minv := it.snode.spack.Load().Block(minx).Get(int(it.match[it.n]))

		return idx.Query(n.Filter, minv, nRows)
	}

	rg := types.InvalidRange
	for _, node := range n.Children {
		if n.OrKind {
			rg = rg.Union(it.combinedRange(b, key, ver, node, nRows))
			// stop early
			if rg.IsFull(nRows) {
				break
			}
		} else {
			rg = rg.Intersect(it.combinedRange(b, key, ver, node, nRows))
			// stop early
			if !rg.IsValid() {
				break
			}
		}
	}
	return rg
}
