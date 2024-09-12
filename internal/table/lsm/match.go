// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package lsm

import (
	"math"

	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

const (
	FilterModeInvalid  = types.FilterModeInvalid
	FilterModeEqual    = types.FilterModeEqual
	FilterModeNotEqual = types.FilterModeNotEqual
	FilterModeGt       = types.FilterModeGt
	FilterModeGe       = types.FilterModeGe
	FilterModeLt       = types.FilterModeLt
	FilterModeLe       = types.FilterModeLe
	FilterModeIn       = types.FilterModeIn
	FilterModeNotIn    = types.FilterModeNotIn
	FilterModeRange    = types.FilterModeRange
	FilterModeRegexp   = types.FilterModeRegexp
)

func MatchNode(n *query.FilterTreeNode, v *schema.View) bool {
	// if root is empty and no leaf is defined, return a full match
	if n.IsEmpty() {
		return true
	}

	// if root contains a single leaf only, match it
	if n.IsLeaf() {
		return MatchFilter(n.Filter, v)
	}

	// process all children
	if n.OrKind {
		for _, c := range n.Children {
			if MatchNode(c, v) {
				return true
			}
		}
		return false
	} else {
		for _, c := range n.Children {
			if !MatchNode(c, v) {
				return false
			}
		}
		return true
	}
}

func MatchFilter(f *query.Filter, view *schema.View) bool {
	// get data value as interface
	v, ok := view.Get(int(f.Index))
	if !ok {
		return false
	}
	// compare against condition value
	return f.Matcher.MatchValue(v)
}

const (
	NoPk  uint64 = 0
	MinPk uint64 = 1
	MaxPk uint64 = math.MaxUint64
)

// PkRange attempts to extract a PK range from a condition tree. If one or more
// conditions are defined for the primary key field then their ranges are
// aggregated. If no primary key condition exists [MinPk, MaxPk] is returned.
func PkRange(n *query.FilterTreeNode, s *schema.Schema) (uint64, uint64) {
	// if root is empty and no leaf is defined, return full range
	if n.IsEmpty() {
		return MinPk, MaxPk
	}

	// if root contains a single leaf only, use its range
	if n.IsLeaf() {
		return pkRangeFilter(n.Filter, s)
	}

	// process all children
	if n.OrKind {
		// smallest min / largest max of all children
		minPk, maxPk := MaxPk, MinPk
		for i := range n.Children {
			cmin, cmax := pkRangeFilter(n.Children[i].Filter, s)
			minPk = min(minPk, cmin)
			maxPk = max(maxPk, cmax)
		}
		return minPk, maxPk
	} else {
		// intersection of all cildren
		minPk, maxPk := MinPk, MaxPk
		for i := range n.Children {
			cmin, cmax := pkRangeFilter(n.Children[i].Filter, s)
			minPk = max(minPk, cmin)
			maxPk = min(maxPk, cmax)
		}
		return minPk, maxPk
	}
}

// Notes:
// - 0 is an illegal PK, legal range is 1..uint64_max
// - compiled conditions guarantee:
//   - value type is uint64
//   - from/to and IN/NIN slices are sorted
func pkRangeFilter(f *query.Filter, s *schema.Schema) (uint64, uint64) {
	if s.PkId() != f.Index {
		return MinPk, MaxPk
	}
	switch f.Mode {
	case FilterModeEqual:
		u := f.Value.(uint64)
		return u, u
	case FilterModeRange:
		return f.Value.([2]any)[0].(uint64), f.Value.([2]any)[1].(uint64)
	case FilterModeIn:
		u := f.Value.([]uint64)
		if len(u) == 0 {
			return NoPk, NoPk
		}
		return u[0], u[len(u)-1]
	case FilterModeGt:
		u := f.Value.(uint64)
		return u + 1, MaxPk
	case FilterModeGe:
		u := f.Value.(uint64)
		return u, MaxPk
	case FilterModeLt:
		u := f.Value.(uint64)
		return MinPk, u - 1
	case FilterModeLe:
		u := f.Value.(uint64)
		return MinPk, u
	default:
		// FilterModeNotEqual, FilterModeNotIn, FilterModeRegexp, (other)
		return MinPk, MaxPk
	}
}
