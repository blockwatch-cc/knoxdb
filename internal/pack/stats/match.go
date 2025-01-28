// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// matchView matches a query condition tree against meta statistics.
// It returns true if the combined filter tree is likely to match
// all (AND) or any (OR) statistics ranges.
func matchView(n *query.FilterTreeNode, view *schema.View) bool {
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
		return matchFilterView(n.Filter, view)
	}

	// combine leaf decisions along the tree
	for _, v := range n.Children {
		if n.OrKind {
			// for OR nodes, stop at the first successful hint
			if matchView(v, view) {
				return true
			}
		} else {
			// for AND nodes stop at the first non-successful hint
			if !matchView(v, view) {
				return false
			}
		}
	}

	// no OR nodes match, all AND nodes match
	return !n.OrKind
}

// matchFilterView checks an individual filter condition in a query
// against meta statistics. It returns true if the filter is within
// statistics range.
func matchFilterView(f *query.Filter, view *schema.View) bool {
	// calculate data column positions inside statistics schema
	minx, maxx := minColIndex(int(f.Index)), maxColIndex(int(f.Index))

	// load min/max values
	minv, _ := view.GetPhy(minx)
	maxv, _ := view.GetPhy(maxx)

	// matcher is selected and configured during compile stage
	return f.Matcher.MatchRange(minv, maxv)
}

// matchVector performs a vectorized check of a query condition tree
// against the contents of statistics package pkg. It returns a bitset with
// matching statistics rows.
func matchVector(n *query.FilterTreeNode, pkg *pack.Package, b store.Bucket, bits *bitset.Bitset) *bitset.Bitset {
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
func matchFilterVector(f *query.Filter, pkg *pack.Package, bits, mask *bitset.Bitset, b store.Bucket) *bitset.Bitset {
	if bits == nil {
		bits = bitset.NewBitset(pkg.Len())
	}

	// translate filter field index into statistics pack index for min&max columns
	minx, maxx := minColIndex(int(f.Index)), maxColIndex(int(f.Index))

	// let the matcher translate its query into a min/max range check
	bits = f.Matcher.MatchRangeVectors(pkg.Block(minx), pkg.Block(maxx), bits, mask)

	// stop early (no match, no bloom bucket, incompatible with bloom)
	if bits.Count() == 0 || b == nil || !hasBloom(f, pkg) {
		return bits
	}

	// check bloom filters for all matches, only a no-match flips a result bit to zero
	// bloom filters work only for EQ and IN conditions and are optional user defined

	// use bits.Iterate() instead of bits.Indexs() to avoid allocating a full sized
	// []uint32 slice here in case we have a full match
	var hits [16]int
	for n, hits := bits.Iterate(0, hits[:]); len(hits) > 0; n, hits = bits.Iterate(n, hits) {
		for _, v := range hits {
			// bloom key is data-pack-key + data-pack-col-index
			bkey := filterKey(pkg.Uint32(STATS_ROW_KEY, v), f.Index)

			// load filter from bucket and check
			filter, err := bloom.NewFilterBuffer(b.Get(bkey))

			// ignore errors (e.g. when buf is nil or filter size mismatch)
			if err != nil {
				continue
			}

			// reset match bit when bloom check is negative
			if !f.Matcher.MatchBloom(filter) {
				bits.Clear(v)
			}
		}
	}

	return bits
}

// TODO: replace with filter tree node flags (FilterFlagUseBloom)
func hasBloom(f *query.Filter, pkg *pack.Package) bool {
	switch f.Mode {
	case types.FilterModeEqual, types.FilterModeIn:
		return pkg.Schema().Exported()[f.Index].Index == types.IndexTypeBloom
	default:
		return false
	}
}

// matchVectorAnd aggregates match bitsets and stops eary when no more match is possible.
func matchVectorAnd(n *query.FilterTreeNode, pkg *pack.Package, b store.Bucket, bits *bitset.Bitset) *bitset.Bitset {
	// start with a full bitset
	if bits == nil {
		bits = bitset.NewBitset(pkg.Len())
	}
	bits.One()

	// match conditions and merge bit vectors, empty condition lists or always true
	// filters result in a full match; stop early when result contains all zeros
	var scratch *bitset.Bitset
	for _, node := range n.Children {
		// skip always true nodes (AND branches may contain a single always true filter)
		if node.IsAnyMatch() {
			continue
		}

		if node.IsLeaf() {
			// match vector against condition using last match as mask
			scratch = matchFilterVector(node.Filter, pkg, scratch, bits, b)
		} else {
			// recurse into another AND or OR condition subtree
			scratch = matchVector(node, pkg, b, scratch)
		}

		// merge
		_, any, _ := bits.AndFlag(scratch)

		// early stop on empty aggregate match
		if !any {
			break
		}
		scratch.Zero()
	}
	scratch.Close()
	return bits
}

// matchVectorOr aggregates match bitsets and stops early when all bits are set.
func matchVectorOr(n *query.FilterTreeNode, pkg *pack.Package, b store.Bucket, bits *bitset.Bitset) *bitset.Bitset {
	// start with an empty bitset
	if bits == nil {
		bits = bitset.NewBitset(pkg.Len())
	} else {
		bits.Zero()
	}

	// match conditions and merge bit vectors, always true/false conditions
	// are optimized away at this point, stop early when result contains all ones
	var scratch *bitset.Bitset
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
			scratch = matchFilterVector(node.Filter, pkg, scratch, mask, b)
			mask.Close()
		} else {
			// recurse into another AND or OR condition subtree
			scratch = matchVector(node, pkg, b, scratch)
		}

		// merge
		bits.Or(scratch)

		// early stop on full aggregate match
		if i < len(n.Children)-1 && bits.Count() == bits.Len() {
			break
		}
		scratch.Zero()
	}
	scratch.Close()
	return bits
}
