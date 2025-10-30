// Copyright (c) 2024-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package filter

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
)

// Match matches pack contents against a query condition (sub)tree.
func Match(n *Node, pkg *pack.Package, r engine.StatsReader, bits *bitset.Bitset) *bitset.Bitset {
	if n.IsLeaf() {
		return MatchSingle(n.Filter, pkg, bits, nil)
	}

	if n.OrKind {
		return MatchOr(n, pkg, r, bits)
	} else {
		return MatchAnd(n, pkg, r, bits)
	}
}

// MatchSingle matches all elements in package pkg against the defined condition
// and returns a bitset of the same length as the package with bits set to true
// where the match is successful.
//
// This implementation uses low level block vectors to efficiently execute
// vectorized checks with custom assembly-optimized routines.
func MatchSingle(f *Filter, pkg *pack.Package, bits, mask *bitset.Bitset) *bitset.Bitset {
	if bits == nil {
		bits = bitset.New(pkg.Len())
	}
	f.Matcher.MatchVector(pkg.Block(f.Index), bits, mask)
	return bits
}

// MatchAnd matches siblings from the same level in a filter tree.
// It return a bit vector from combining child matches with a logical AND
// and does so efficiently by skipping unnecessary matches and aggregations.
//
// TODO: concurrent condition matches and cascading bitset merge
func MatchAnd(n *Node, pkg *pack.Package, r engine.StatsReader, bits *bitset.Bitset) *bitset.Bitset {
	// start with a full bitset
	if bits == nil {
		bits = bitset.New(pkg.Len())
	}
	bits.One()

	// match conditions and merge bit vectors, empty condition lists or always true
	// filters result in a full match; stop early when result contains all zeros
	for _, node := range n.Children {
		// skip always true nodes (AND branches may contain a single always true filter)
		if node.IsAnyMatch() {
			continue
		}

		var scratch *bitset.Bitset
		if !node.IsLeaf() {
			// recurse into another AND or OR condition subtree
			scratch = Match(node, pkg, r, scratch)
		} else {
			f := node.Filter
			// Quick inclusion check to skip matching when the current condition
			// would return an all-true vector. Note that we do not have to check
			// for an all-false vector because MaybeMatchTree() has already deselected
			// packs of that kind (except the journal)
			if r != nil {
				min, max := r.MinMax(f.Index)
				switch f.Mode {
				case types.FilterModeEqual:
					// condition is always true iff min == max == f.Value
					if f.Type.EQ(min, f.Value) && f.Type.EQ(max, f.Value) {
						continue
					}
				case types.FilterModeNotEqual:
					// condition is always true iff f.Value < min || f.Value > max
					if f.Type.LT(f.Value, min) || f.Type.GT(f.Value, max) {
						continue
					}
				case types.FilterModeRange:
					// condition is always true iff pack range <= condition range
					rg := f.Value.(RangeValue)
					if f.Type.LE(rg[0], min) && f.Type.GE(rg[1], max) {
						continue
					}
				case types.FilterModeGt:
					// condition is always true iff min > f.Value
					if f.Type.GT(min, f.Value) {
						continue
					}
				case types.FilterModeGe:
					// condition is always true iff min >= f.Value
					if f.Type.GE(min, f.Value) {
						continue
					}
				case types.FilterModeLt:
					// condition is always true iff max < f.Value
					if f.Type.LT(max, f.Value) {
						continue
					}
				case types.FilterModeLe:
					// condition is always true iff max <= f.Value
					if f.Type.LE(max, f.Value) {
						continue
					}
				}
			}

			// match vector against condition using last match as mask
			scratch = MatchSingle(f, pkg, scratch, bits)
		}

		// merge
		_, any, _ := bits.AndFlag(scratch)
		scratch.Close()

		// early stop on empty aggregate match
		if !any {
			break
		}
	}

	return bits
}

// Return a bit vector containing matching positions in the pack combining
// multiple OR conditions with efficient skipping and aggregation.
func MatchOr(n *Node, pkg *pack.Package, r engine.StatsReader, bits *bitset.Bitset) *bitset.Bitset {
	// start with an empty bitset
	if bits == nil {
		bits = bitset.New(pkg.Len())
	} else {
		bits.Zero()
	}

	// match conditions and merge bit vectors, always true/false conditions
	// are optimized away at this point, stop early when result contains all ones
	for i, node := range n.Children {
		var scratch *bitset.Bitset
		if !node.IsLeaf() {
			// recurse into another AND or OR condition subtree
			scratch = Match(node, pkg, r, scratch)
		} else {
			f := node.Filter
			// Quick inclusion check to skip matching when the current condition
			// would return an all-true vector. Note that we do not have to check
			// for an all-false vector because MaybeMatchPack() has already deselected
			// packs of that kind (except the journal).
			if r != nil {
				min, max := r.MinMax(f.Index)
				skipEarly := false
				switch f.Mode {
				case types.FilterModeEqual:
					// condition is always true iff min == max == f.Value
					skipEarly = f.Type.EQ(min, f.Value) && f.Type.EQ(max, f.Value)

				case types.FilterModeNotEqual:
					// condition is always true iff f.Value < min || f.Value > max
					skipEarly = f.Type.LT(f.Value, min) || f.Type.GT(f.Value, max)

				case types.FilterModeRange:
					// condition is always true iff pack range <= condition range
					rg := f.Value.(RangeValue)
					skipEarly = f.Type.LE(rg[0], min) && f.Type.GE(rg[1], max)

				case types.FilterModeGt:
					// condition is always true iff min > f.Value
					skipEarly = f.Type.GT(min, f.Value)

				case types.FilterModeGe:
					// condition is always true iff min >= f.Value
					skipEarly = f.Type.GE(min, f.Value)

				case types.FilterModeLt:
					// condition is always true iff max < f.Value
					skipEarly = f.Type.LT(max, f.Value)

				case types.FilterModeLe:
					// condition is always true iff max <= f.Value
					skipEarly = f.Type.LE(max, f.Value)
				}
				if skipEarly {
					return bits.One()
				}
			}

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
			scratch = MatchSingle(f, pkg, scratch, mask)
			mask.Close()
		}

		// merge
		bits.Or(scratch)
		scratch.Close()

		// early stop on full aggregate match
		if i < len(n.Children)-1 && bits.All() {
			break
		}
	}
	return bits
}
