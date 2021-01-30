// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package vec

import (
	"sort"
)

type Uint32Sorter []uint32

func (s Uint32Sorter) Sort() []uint32 {
	if !sort.IsSorted(s) {
		sort.Sort(s)
	}
	return s
}

func (s Uint32Sorter) Len() int           { return len(s) }
func (s Uint32Sorter) Less(i, j int) bool { return s[i] < s[j] }
func (s Uint32Sorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// all functions below assume slices are sorted

func UniqueUint32Slice(a []uint32) []uint32 {
	if len(a) == 0 {
		return a
	}
	b := make([]uint32, len(a))
	copy(b, a)
	Uint32Sorter(b).Sort()
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

func IntersectSortedUint32(x, y, out []uint32) []uint32 {
	if out == nil {
		out = make([]uint32, 0, min(len(x), len(y)))
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
