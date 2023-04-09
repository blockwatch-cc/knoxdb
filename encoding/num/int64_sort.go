// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"sort"

	"blockwatch.cc/knoxdb/util"
)

type Int64Sorter []int64

func (s Int64Sorter) Sort() []int64 {
	if !sort.IsSorted(s) {
		sort.Sort(s)
	}
	return s
}

func (s Int64Sorter) Len() int           { return len(s) }
func (s Int64Sorter) Less(i, j int) bool { return s[i] < s[j] }
func (s Int64Sorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// all functions below assume slices are sorted

func UniqueInt64Slice(a []int64) []int64 {
	if len(a) == 0 {
		return a
	}
	b := make([]int64, len(a))
	copy(b, a)
	Int64Sorter(b).Sort()
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

func IntersectSortedInt64(x, y, out []int64) []int64 {
	if out == nil {
		out = make([]int64, 0, util.Min(len(x), len(y)))
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
