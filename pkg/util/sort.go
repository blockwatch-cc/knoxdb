// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"sort"

	"golang.org/x/exp/constraints"
)

type PairSorter[S, T constraints.Ordered] struct {
	s []S
	t []T
}

func (s PairSorter[S, T]) Len() int {
	return min(len(s.s), len(s.t))
}

func (s PairSorter[S, T]) Less(i, j int) bool {
	return s.s[i] < s.s[j] || (s.s[i] == s.s[j] && s.t[i] < s.t[j])
}

func (s PairSorter[S, T]) Swap(i, j int) {
	s.s[i], s.s[j] = s.s[j], s.s[i]
	s.t[i], s.t[j] = s.t[j], s.t[i]
}

func Sort2[S, T constraints.Ordered](s []S, t []T) {
	sort.Sort(PairSorter[S, T]{s, t})
}

const bits = 8

// custom radix sort, faster than slices.Sort
func Sort[T Integer](vs []T, shift int) {
	w := SizeOf[T]() * 8
	s := w - bits - shift

	if len(vs) < 1<<6 {
		// Insertion sort for small inputs
		for i := 0; i < len(vs); i++ {
			for j := i; j > 0 && vs[j-1] > vs[j]; j-- {
				vs[j-1], vs[j] = vs[j], vs[j-1]
			}
		}
		return
	}

	// First pass: count each bin size
	var bins [1 << bits]int
	for _, v := range vs {
		b := uint(v>>s) & 0xFF
		bins[b]++
	}

	// Locate bin ranges in the sorted array
	accum := 0
	var ends [1 << bits]int
	for b := 0; b < len(bins); b++ {
		beg := accum
		accum += bins[b]
		ends[b] = accum
		bins[b] = beg
	}

	// Second pass: move elements into allotted bins
	for b := 0; b < len(bins); b++ {
		for i := bins[b]; i < ends[b]; {
			bin := int(vs[i]>>s) & 0xFF
			if bin == b {
				i++
			} else {
				vs[bins[bin]], vs[i] = vs[i], vs[bins[bin]]
				bins[bin]++
			}
		}
	}

	// Recursively sort each bin on the next digit
	if shift < w-bits {
		beg := 0
		for b := 0; b < len(bins); b++ {
			Sort(vs[beg:ends[b]], shift+bits)
			beg = ends[b]
		}
	}
}
