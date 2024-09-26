// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"math"

	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

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
	case types.FilterModeEqual:
		u := f.Value.(uint64)
		return u, u
	case types.FilterModeRange:
		return f.Value.([2]any)[0].(uint64), f.Value.([2]any)[1].(uint64)
	case types.FilterModeIn:
		u := f.Value.([]uint64)
		if len(u) == 0 {
			return NoPk, NoPk
		}
		return u[0], u[len(u)-1]
	case types.FilterModeGt:
		u := f.Value.(uint64)
		return u + 1, MaxPk
	case types.FilterModeGe:
		u := f.Value.(uint64)
		return u, MaxPk
	case types.FilterModeLt:
		u := f.Value.(uint64)
		return MinPk, u - 1
	case types.FilterModeLe:
		u := f.Value.(uint64)
		return MinPk, u
	default:
		// FilterModeNotEqual, FilterModeNotIn, FilterModeRegexp, (other)
		return MinPk, MaxPk
	}
}
