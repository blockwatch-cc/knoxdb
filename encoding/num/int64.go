// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"blockwatch.cc/knoxdb/vec"
)

func MatchInt64Equal(src []int64, val int64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt64Equal(src, val, bits.Bytes())))
	return bits
}

func MatchInt64NotEqual(src []int64, val int64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt64NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt64LessThan(src []int64, val int64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt64LessThan(src, val, bits.Bytes())))
	return bits
}

func MatchInt64LessThanEqual(src []int64, val int64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt64LessThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt64GreaterThan(src []int64, val int64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt64GreaterThan(src, val, bits.Bytes())))
	return bits
}

func MatchInt64GreaterThanEqual(src []int64, val int64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt64GreaterThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt64Between(src []int64, a, b int64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt64Between(src, a, b, bits.Bytes())))
	return bits
}

var Int64 = struct {
	Sort          func([]int64) []int64
	Unique        func([]int64) []int64
	RemoveZeros   func([]int64) []int64
	AddUnique     func([]int64, int64) []int64
	Insert        func([]int64, int, ...int64) []int64
	Remove        func([]int64, int64) []int64
	Contains      func([]int64, int64) bool
	Index         func([]int64, int64, int) int
	MinMax        func([]int64) (int64, int64)
	ContainsRange func([]int64, int64, int64) bool
	Intersect     func([]int64, []int64, []int64) []int64
	MatchEqual    func([]int64, int64, *vec.Bitset, *vec.Bitset) *vec.Bitset
}{
	Sort: func(s []int64) []int64 {
		return Int64Sorter(s).Sort()
	},
	Unique: func(s []int64) []int64 {
		return UniqueInt64Slice(s)
	},
	RemoveZeros: func(s []int64) []int64 {
		s, _ = RemoveZeros(s)
		return s
	},
	AddUnique: func(s []int64, v int64) []int64 {
		s, _ = int64AddUnique(s, v)
		return s
	},
	Insert: func(s []int64, k int, v ...int64) []int64 {
		return Insert(s, k, v...)
	},
	Remove: func(s []int64, v int64) []int64 {
		s, _ = Remove(s, v)
		return s
	},
	Contains: func(s []int64, v int64) bool {
		return Contains(s, v)
	},
	Index: func(s []int64, v int64, last int) int {
		return index(s, v, last)
	},
	MinMax: func(s []int64) (int64, int64) {
		return MinMax(s)
	},
	ContainsRange: func(s []int64, from, to int64) bool {
		return ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []int64) []int64 {
		return IntersectSortedInt64(x, y, out)
	},
	MatchEqual: func(s []int64, val int64, bits, mask *vec.Bitset) *vec.Bitset {
		return MatchInt64Equal(s, val, bits, mask)
	},
}

func int64AddUnique(s []int64, val int64) ([]int64, bool) {
	idx := index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Int64Sorter(s).Sort()
	return s, true
}
