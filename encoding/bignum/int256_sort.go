// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bignum

import (
	"sort"
)

func (s Int256Slice) Sort() Int256Slice {
	Int256Sorter(s).Sort()
	return s
}

func (s Int256Slice) Less(i, j int) bool { return s[i].Lt(s[j]) }
func (s Int256Slice) Len() int           { return len(s) }
func (s Int256Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type Int256Sorter []Int256

func (s Int256Sorter) Sort() {
	if !sort.IsSorted(s) {
		sort.Sort(s)
	}
}

func (s Int256Sorter) Len() int           { return len(s) }
func (s Int256Sorter) Less(i, j int) bool { return s[i].Lt(s[j]) }
func (s Int256Sorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// all functions below assume slices are sorted

func UniqueInt256Slice(a []Int256) []Int256 {
	if len(a) == 0 {
		return a
	}
	b := make([]Int256, len(a))
	copy(b, a)
	Int256Sorter(b).Sort()
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

func IntersectSortedInt256(x, y []Int256) []Int256 {
	res := make([]Int256, 0, min(len(x), len(y)))
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
