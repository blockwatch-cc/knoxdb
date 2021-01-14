// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package vec

import (
	"sort"
)

type Int8Sorter []int8

func (s Int8Sorter) Sort() []int8 {
	if !sort.IsSorted(s) {
		sort.Sort(s)
	}
	return s
}

func (s Int8Sorter) Len() int           { return len(s) }
func (s Int8Sorter) Less(i, j int) bool { return s[i] < s[j] }
func (s Int8Sorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// all functions below assume slices are sorted

func UniqueInt8Slice(a []int8) []int8 {
	if len(a) == 0 {
		return a
	}
	b := make([]int8, len(a))
	copy(b, a)
	Int8Sorter(b).Sort()
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

func IntersectSortedInt8(x, y, out []int8) []int8 {
	if out == nil {
		out = make([]int8, 0, min(len(x), len(y)))
	}
	count := 0
	for i, j, il, jl := 0, 0, len(x), len(y); i < il && j < jl; {
		if x[i] < y[j] {
			i++
			continue
		}
		if x[i] > y[j] {
			j++
			continue
		}
		if count > 0 {
			// skip duplicates
			last := out[count-1]
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
			out = append(out, x[i])
			count++
			i++
			j++
		}
	}
	return out
}
