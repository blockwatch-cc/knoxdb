// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"sort"

	"blockwatch.cc/knoxdb/internal/types"
)

type PackageSorter struct {
	cols   []int
	orders []types.OrderType
	pkg    *Package
}

func NewPackageSorter(cols []int, orders []types.OrderType) *PackageSorter {
	s := &PackageSorter{
		cols:   cols,
		orders: orders,
	}
	if len(s.orders) != len(cols) {
		s.orders = make([]types.OrderType, len(s.cols))
		copy(s.orders, orders)
	}
	return s
}

func (s *PackageSorter) Sort(pkg *Package) {
	// sort selected rows only
	// if nil, create a full selection vector
	if pkg.selected == nil {
		pkg.selected = make([]uint32, pkg.nRows)
		for i := range pkg.selected {
			pkg.selected[i] = uint32(i)
		}
	}

	// link pkg so we can use sort's interface based API on PackageSorter
	s.pkg = pkg
	sort.Sort(s)
	s.pkg = nil
}

func (s *PackageSorter) Len() int {
	return len(s.pkg.selected)
}

func (s *PackageSorter) Less(i, j int) bool {
	x, y := int(s.pkg.selected[i]), int(s.pkg.selected[j])
	for n, col := range s.cols {
		o := s.orders[n]
		var cmp int
		if o.IsCaseSensitive() {
			cmp = s.pkg.blocks[col].Cmp(x, y)
		} else {
			cmp = s.pkg.blocks[col].Cmpi(x, y)
		}
		if cmp == 0 {
			// on equal, continue with next column
			continue
		}
		return o.IsForward()
	}
	// all equal
	return false
}

func (s *PackageSorter) Swap(i, j int) {
	s.pkg.selected[i], s.pkg.selected[j] = s.pkg.selected[j], s.pkg.selected[i]
}
