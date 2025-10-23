// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/engine"
	hashfilter "blockwatch.cc/knoxdb/internal/filter"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/filter/fuse"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/store"
)

var _ engine.StatsReader = (*ViewReader)(nil)

type ViewReader struct {
	*schema.View
}

func (v ViewReader) MinMax(col int) (any, any) {
	// calculate data column positions inside statistics schema
	minx, maxx := minColIndex(col), maxColIndex(col)

	// load min/max values
	minv, _ := v.GetPhy(minx)
	maxv, _ := v.GetPhy(maxx)

	return minv, maxv
}

// Match matches a query condition tree against meta statistics.
// It returns true if the combined filter tree is likely to match
// all (AND) or any (OR) statistics ranges.
func Match(n *filter.Node, r engine.StatsReader) bool {
	// nil tree and nil accessor match
	if n == nil || r == nil {
		return true
	}

	// always match?
	if n.IsAnyMatch() {
		return true
	}
	// no match?
	if n.IsNoMatch() {
		return false
	}

	// match single leafs
	if n.IsLeaf() {
		return matchFilter(n.Filter, r)
	}

	// combine leaf decisions along the tree
	for _, v := range n.Children {
		if n.OrKind {
			// for OR nodes, stop at the first successful hint
			if Match(v, r) {
				return true
			}
		} else {
			// for AND nodes stop at the first non-successful hint
			if !Match(v, r) {
				return false
			}
		}
	}

	// no OR nodes match, all AND nodes match
	return !n.OrKind
}

// matchFilter checks an individual filter condition in a query
// against meta statistics. It returns true if the filter is within
// statistics range.
func matchFilter(f *filter.Filter, r engine.StatsReader) bool {
	minv, maxv := r.MinMax(f.Index)

	// matcher is selected and configured during compile stage
	return f.Matcher.MatchRange(minv, maxv)
}

// matchVector performs a vectorized check of a query condition tree
// against the contents of statistics package pkg. It returns a bitset with
// matching statistics rows.
func matchVector(n *filter.Node, pkg *pack.Package, b map[int]store.Bucket, bits *bitset.Bitset) (int, *bitset.Bitset) {
	if n.IsLeaf() {
		return matchFilterVector(n.Filter, pkg, bits, nil, b)
	}

	// recurse into filter tree children
	if n.OrKind {
		return matchVectorOr(n, pkg, b, bits)
	} else {
		return matchVectorAnd(n, pkg, b, bits)
	}
}

// matchFilterVector matches a single query filter against statistics package pkg.
// It returns a bitset of the same length as the package with bits set to true
// where the match is successful.
//
// Note statistics are min/max ranges, hence to find a potential match we translate
// each filter condition into a min/max range. Low level vectors matches are
// vectorized using custom assembly routines.
func matchFilterVector(f *filter.Filter, pkg *pack.Package, bits, mask *bitset.Bitset, b map[int]store.Bucket) (int, *bitset.Bitset) {
	if bits == nil {
		bits = bitset.New(pkg.Len())
	}

	// translate filter field index into statistics pack index for min&max columns
	minx, maxx := minColIndex(f.Index), maxColIndex(f.Index)

	// let the matcher translate its query into a min/max range check
	f.Matcher.MatchRangeVectors(pkg.Block(minx), pkg.Block(maxx), bits, mask)

	// stop early (no match, no bloom bucket, incompatible with bloom)
	if bits.None() || b == nil {
		return 0, bits
	}

	ftyp := filterType(f, pkg, minx)
	if ftyp == types.IndexTypeNone {
		return 0, bits
	}

	// Check filters for all stats records that have matched above.
	// Filters have limited scope, they only work for EQ/IN conditions.
	// A filter no-match will flip the result bit for a data pack off
	// so that the pack will not be loaded for this query.

	// use bits.Iterator() instead of bits.Indexes() to avoid allocating a full sized
	// []uint32 slice here in case we have a full match
	var n int
	for v := range bits.Iterator() {
		// filter key is data-pack-key + data-pack-col-id + data-pack-version
		key := pkg.Uint32(STATS_ROW_KEY, v)
		ver := pkg.Uint32(STATS_ROW_VERSION, v)
		bkey := encodeFilterKey(key, ver, f.Id)

		// select filter type
		var (
			flt hashfilter.Filter
			err error
		)

		// load filter from bucket and check
		switch ftyp {
		case types.IndexTypeBloom:
			buf := b[STATS_FILTER_KEY].Get(bkey)
			n += len(buf)
			flt, err = bloom.NewFilterBuffer(buf)
		case types.IndexTypeBfuse:
			buf := b[STATS_FILTER_KEY].Get(bkey)
			n += len(buf)
			flt, err = fuse.NewBinaryFuseFromBytes[uint8](buf)
		case types.IndexTypeBits:
			buf := b[STATS_FILTER_KEY].Get(bkey)
			if len(buf) > 0 {
				n += len(buf)
				flt = xroar.NewFromBytes(buf)
			}
		}

		// ignore errors (e.g. when buf is nil or filter size mismatch)
		if flt == nil && err != nil {
			continue
		}

		// reset match bit when filter check is negative
		if !f.Matcher.MatchFilter(flt) {
			bits.Unset(v)
		}
	}

	return n, bits
}

// TODO: replace with filter tree node flags (FilterFlagUseBloom)
func filterType(f *filter.Filter, pkg *pack.Package, idx int) types.IndexType {
	switch f.Mode {
	case types.FilterModeEqual, types.FilterModeIn:
		field := pkg.Schema().Fields[idx]
		if field.IsIndexed() {
			switch field.Index.Type {
			case types.IndexTypeBloom, types.IndexTypeBfuse, types.IndexTypeBits:
				return field.Index.Type
			}
		}
	}
	return types.IndexTypeNone
}

// matchVectorAnd aggregates match bitsets and stops eary when no more match is possible.
func matchVectorAnd(n *filter.Node, pkg *pack.Package, b map[int]store.Bucket, bits *bitset.Bitset) (int, *bitset.Bitset) {
	// start with a full bitset
	if bits == nil {
		bits = bitset.New(pkg.Len())
	}
	bits.One()

	// match conditions and merge bit vectors, empty condition lists or always true
	// filters result in a full match; stop early when result contains all zeros
	var (
		scratch *bitset.Bitset
		m, c    int
	)
	for _, node := range n.Children {
		// skip always true nodes (AND branches may contain a single always true filter)
		if node.IsAnyMatch() {
			continue
		}

		if node.IsLeaf() {
			// match vector against condition using last match as mask
			c, scratch = matchFilterVector(node.Filter, pkg, scratch, bits, b)
		} else {
			// recurse into another AND or OR condition subtree
			c, scratch = matchVector(node, pkg, b, scratch)
		}
		m += c

		// merge
		_, any, _ := bits.AndFlag(scratch)

		// early stop on empty aggregate match
		if !any {
			break
		}
		scratch.Zero()
	}
	scratch.Close()
	return m, bits
}

// matchVectorOr aggregates match bitsets and stops early when all bits are set.
func matchVectorOr(n *filter.Node, pkg *pack.Package, b map[int]store.Bucket, bits *bitset.Bitset) (int, *bitset.Bitset) {
	// start with an empty bitset
	if bits == nil {
		bits = bitset.New(pkg.Len())
	} else {
		bits.Zero()
	}

	// match conditions and merge bit vectors, always true/false conditions
	// are optimized away at this point, stop early when result contains all ones
	var (
		scratch *bitset.Bitset
		m, c    int
	)
	for i, node := range n.Children {
		if node.IsLeaf() {
			// match vector against condition using last match as mask;
			// since this is an OR match we only have to test all values
			// with unset mask bits, that's why we negate the mask first
			//
			// Note that an optimization exists for IN/NIN on all types
			// which implicitly assumes an AND between mask and vector,
			// i.e. it skips checks for all elems with a mask bit set.
			// For correctness this still works because we merge mask
			// and pack match set using OR below. However we cannot
			// use a shortcut (on all pack bits == 1).
			mask := bits.Clone().Neg()
			c, scratch = matchFilterVector(node.Filter, pkg, scratch, mask, b)
			mask.Close()
		} else {
			// recurse into another AND or OR condition subtree
			c, scratch = matchVector(node, pkg, b, scratch)
		}
		m += c

		// merge
		bits.Or(scratch)

		// early stop on full aggregate match
		if i < len(n.Children)-1 && bits.All() {
			break
		}
		scratch.Zero()
	}
	scratch.Close()
	return m, bits
}
