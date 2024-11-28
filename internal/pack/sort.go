// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"sort"

	"blockwatch.cc/knoxdb/internal/types"
)

type PackageSorter struct {
	pkg    *Package
	cols   []int
	sorted []int32 // init: 0..n
	order  types.OrderType
}

func NewPackageSorter(p *Package, fieldId uint16, moreIds ...uint16) *PackageSorter {
	s := &PackageSorter{
		pkg:    p,
		sorted: make([]int32, p.Len(), p.Cap()),
		order:  types.OrderAsc,
	}
	for i := range s.sorted {
		s.sorted[i] = int32(i)
	}
	for _, id := range append([]uint16{fieldId}, moreIds...) {
		for i, f := range p.schema.Exported() {
			if id == f.Id {
				s.cols = append(s.cols, i)
				break
			}
		}
	}
	return s
}

func (s *PackageSorter) Order() types.OrderType {
	return s.order
}

func (s *PackageSorter) SortedCols() []string {
	cols := make([]string, len(s.cols))
	for i := range s.cols {
		f, _ := s.pkg.schema.FieldByIndex(i)
		cols[i] = f.Name()
	}
	return cols
}

func (s *PackageSorter) N(i int) int {
	return int(s.sorted[i])
}

func (s *PackageSorter) SortAsc() *PackageSorter {
	if !sort.IsSorted(s) {
		sort.Sort(s)
	}
	return s
}

func (s *PackageSorter) SortDesc() *PackageSorter {
	if !sort.IsSorted(sort.Reverse(s)) {
		sort.Sort(sort.Reverse(s))
	}
	return s
}

func (s *PackageSorter) SortOrder(o types.OrderType) {
	s.order = o
	switch o {
	case types.OrderAsc:
		// ascending case sensitive
		if !sort.IsSorted(s) {
			sort.Sort(s)
		}
	case types.OrderDesc:
		// descending case sensitive
		if !sort.IsSorted(sort.Reverse(s)) {
			sort.Sort(sort.Reverse(s))
		}
	case types.OrderAscCaseInsensitive:
		// ascending case insensitive
		if !sort.IsSorted(s) {
			sort.Sort(s)
		}
	case types.OrderDescCaseInsensitive:
		// descending case insensitive
		if !sort.IsSorted(sort.Reverse(s)) {
			sort.Sort(sort.Reverse(s))
		}
	}
}

func (s *PackageSorter) Len() int {
	return len(s.sorted)
}

func (s *PackageSorter) Less(i, j int) bool {
	for _, col := range s.cols {
		var cmp int
		if s.order.IsCaseSensitive() {
			cmp = s.pkg.blocks[col].Cmp(i, j)
		} else {
			cmp = s.pkg.blocks[col].Cmpi(i, j)
		}
		if cmp < 0 {
			return true
		}
		if cmp > 0 {
			return false
		}
		// on equal, continue with next column
	}
	return false
}

func (s *PackageSorter) Swap(i, j int) {
	s.sorted[i], s.sorted[j] = s.sorted[j], s.sorted[i]
}
