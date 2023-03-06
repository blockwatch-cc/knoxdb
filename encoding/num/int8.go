// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package num

import (
	"blockwatch.cc/knoxdb/vec"
)

func MatchInt8Equal(src []int8, val int8, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt8Equal(src, val, bits.Bytes())))
	return bits
}

func MatchInt8NotEqual(src []int8, val int8, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt8NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt8LessThan(src []int8, val int8, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt8LessThan(src, val, bits.Bytes())))
	return bits
}

func MatchInt8LessThanEqual(src []int8, val int8, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt8LessThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt8GreaterThan(src []int8, val int8, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt8GreaterThan(src, val, bits.Bytes())))
	return bits
}

func MatchInt8GreaterThanEqual(src []int8, val int8, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt8GreaterThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt8Between(src []int8, a, b int8, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt8Between(src, a, b, bits.Bytes())))
	return bits
}

var Int8 = struct {
	Sort          func([]int8) []int8
	Unique        func([]int8) []int8
	RemoveZeros   func([]int8) []int8
	AddUnique     func([]int8, int8) []int8
	Insert        func([]int8, int, ...int8) []int8
	Remove        func([]int8, int8) []int8
	Contains      func([]int8, int8) bool
	Index         func([]int8, int8, int) int
	MinMax        func([]int8) (int8, int8)
	ContainsRange func([]int8, int8, int8) bool
	Intersect     func([]int8, []int8, []int8) []int8
	MatchEqual    func([]int8, int8, *vec.Bitset, *vec.Bitset) *vec.Bitset
}{
	Sort: func(s []int8) []int8 {
		return Int8Sorter(s).Sort()
	},
	Unique: func(s []int8) []int8 {
		return UniqueInt8Slice(s)
	},
	RemoveZeros: func(s []int8) []int8 {
		s, _ = RemoveZeros(s)
		return s
	},
	AddUnique: func(s []int8, v int8) []int8 {
		s, _ = int8AddUnique(s, v)
		return s
	},
	Insert: func(s []int8, k int, v ...int8) []int8 {
		return Insert(s, k, v...)
	},
	Remove: func(s []int8, v int8) []int8 {
		s, _ = Remove(s, v)
		return s
	},
	Contains: func(s []int8, v int8) bool {
		return Contains(s, v)
	},
	Index: func(s []int8, v int8, last int) int {
		return index(s, v, last)
	},
	MinMax: func(s []int8) (int8, int8) {
		return MinMax(s)
	},
	ContainsRange: func(s []int8, from, to int8) bool {
		return ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []int8) []int8 {
		return IntersectSortedInt8(x, y, out)
	},
	MatchEqual: func(s []int8, val int8, bits, mask *vec.Bitset) *vec.Bitset {
		return MatchInt8Equal(s, val, bits, mask)
	},
}

func int8AddUnique(s []int8, val int8) ([]int8, bool) {
	idx := index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Int8Sorter(s).Sort()
	return s, true
}
