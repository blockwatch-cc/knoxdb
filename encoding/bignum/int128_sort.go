// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bignum

import (
	"sort"
)

func (s Int128Slice) Sort() Int128Slice {
	Int128Sorter(s).Sort()
	return s
}

func (s Int128Slice) Less(i, j int) bool { return s[i].Lt(s[j]) }
func (s Int128Slice) Len() int           { return len(s) }
func (s Int128Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type Int128Sorter []Int128

func (s Int128Sorter) Sort() {
	if !sort.IsSorted(s) {
		sort.Sort(s)
	}
}

func (s Int128Sorter) Len() int           { return len(s) }
func (s Int128Sorter) Less(i, j int) bool { return s[i].Lt(s[j]) }
func (s Int128Sorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// all functions below assume slices are sorted

func UniqueInt128Slice(a []Int128) []Int128 {
	if len(a) == 0 {
		return a
	}
	b := make([]Int128, len(a))
	copy(b, a)
	Int128Sorter(b).Sort()
	j := 0
	for i := 1; i < len(b); i++ {
		if b[j] == b[i] {
			continue
		}
		j++
		// preserve the original data
		// in[i], in[j] = in[j], in[i]
		// only set what is required
		b[j] = b[i]
	}
	return b[:j+1]
}

func IntersectSortedInt128(x, y []Int128) []Int128 {
	res := make([]Int128, 0, min(len(x), len(y)))
	count := 0
	for i, j, il, jl := 0, 0, len(x), len(y); i < il && j < jl; {
		if x[i].Lt(y[j]) {
			i++
			continue
		}
		if x[i].Gt(y[j]) {
			j++
			continue
		}
		if count > 0 {
			// skip duplicates
			last := res[count-1]
			if last == x[i] {
				i++
				continue
			}
			if last == y[j] {
				j++
				continue
			}
		}
		if i == il || j == jl {
			break
		}
		if x[i] == y[j] {
			res = append(res, x[i])
			count++
			i++
			j++
		}
	}
	return res
}
