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
