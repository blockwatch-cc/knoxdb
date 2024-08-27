// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"sort"
)

// TODO: integrate with Result

type PackageSorter struct {
	pkg      *Package
	cols     []int
	sorted   []int32 // init: 0..n
	withcase bool    // string matches are case sensitive or insensitive
}

func NewPackageSorter(p *Package, fieldId uint16, moreIds ...uint16) *PackageSorter {
	s := &PackageSorter{
		pkg:      p,
		sorted:   make([]int32, p.Len()),
		withcase: true,
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

func (s *PackageSorter) SortAsc() {
	if !sort.IsSorted(s) {
		sort.Sort(s)
	}
}

func (s *PackageSorter) SortDesc() {
	if !sort.IsSorted(sort.Reverse(s)) {
		sort.Sort(sort.Reverse(s))
	}
}

// Order o is compatible with query.OrderType
func (s *PackageSorter) SortOrder(o byte) {
	switch o {
	case 0:
		// ascending case sensitive
		s.withcase = true
		if !sort.IsSorted(s) {
			sort.Sort(s)
		}
	case 1:
		// descending case sensitive
		s.withcase = true
		if !sort.IsSorted(sort.Reverse(s)) {
			sort.Sort(sort.Reverse(s))
		}
	case 2:
		// ascending case insensitive
		s.withcase = false
		if !sort.IsSorted(s) {
			sort.Sort(s)
		}
	case 3:
		// descending case insensitive
		s.withcase = false
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
		if s.withcase {
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
	// for _, b := range s.pkg.blocks {
	// 	b.Swap(i, j)
	// }
}
