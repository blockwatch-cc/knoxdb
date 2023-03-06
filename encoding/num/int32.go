// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package num

import (
	"blockwatch.cc/knoxdb/vec"
)

func MatchInt32Equal(src []int32, val int32, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt32Equal(src, val, bits.Bytes())))
	return bits
}

func MatchInt32NotEqual(src []int32, val int32, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt32NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt32LessThan(src []int32, val int32, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt32LessThan(src, val, bits.Bytes())))
	return bits
}

func MatchInt32LessThanEqual(src []int32, val int32, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt32LessThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt32GreaterThan(src []int32, val int32, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt32GreaterThan(src, val, bits.Bytes())))
	return bits
}

func MatchInt32GreaterThanEqual(src []int32, val int32, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt32GreaterThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt32Between(src []int32, a, b int32, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt32Between(src, a, b, bits.Bytes())))
	return bits
}

var Int32 = struct {
	Sort          func([]int32) []int32
	Unique        func([]int32) []int32
	RemoveZeros   func([]int32) []int32
	AddUnique     func([]int32, int32) []int32
	Insert        func([]int32, int, ...int32) []int32
	Remove        func([]int32, int32) []int32
	Contains      func([]int32, int32) bool
	Index         func([]int32, int32, int) int
	MinMax        func([]int32) (int32, int32)
	ContainsRange func([]int32, int32, int32) bool
	Intersect     func([]int32, []int32, []int32) []int32
	MatchEqual    func([]int32, int32, *vec.Bitset, *vec.Bitset) *vec.Bitset
}{
	Sort: func(s []int32) []int32 {
		return Int32Sorter(s).Sort()
	},
	Unique: func(s []int32) []int32 {
		return UniqueInt32Slice(s)
	},
	RemoveZeros: func(s []int32) []int32 {
		s, _ = RemoveZeros(s)
		return s
	},
	AddUnique: func(s []int32, v int32) []int32 {
		s, _ = int32AddUnique(s, v)
		return s
	},
	Insert: func(s []int32, k int, v ...int32) []int32 {
		return Insert(s, k, v...)
	},
	Remove: func(s []int32, v int32) []int32 {
		s, _ = Remove(s, v)
		return s
	},
	Contains: func(s []int32, v int32) bool {
		return Contains(s, v)
	},
	Index: func(s []int32, v int32, last int) int {
		return index(s, v, last)
	},
	MinMax: func(s []int32) (int32, int32) {
		return MinMax(s)
	},
	ContainsRange: func(s []int32, from, to int32) bool {
		return ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []int32) []int32 {
		return IntersectSortedInt32(x, y, out)
	},
	MatchEqual: func(s []int32, val int32, bits, mask *vec.Bitset) *vec.Bitset {
		return MatchInt32Equal(s, val, bits, mask)
	},
}

func int32AddUnique(s []int32, val int32) ([]int32, bool) {
	idx := index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Int32Sorter(s).Sort()
	return s, true
}
