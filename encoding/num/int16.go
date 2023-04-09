// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package num

import "blockwatch.cc/knoxdb/encoding/bitset"

func MatchInt16Equal(src []int16, val int16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt16Equal(src, val, bits.Bytes())))
	return bits
}

func MatchInt16NotEqual(src []int16, val int16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt16NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt16LessThan(src []int16, val int16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt16LessThan(src, val, bits.Bytes())))
	return bits
}

func MatchInt16LessThanEqual(src []int16, val int16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt16LessThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt16GreaterThan(src []int16, val int16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt16GreaterThan(src, val, bits.Bytes())))
	return bits
}

func MatchInt16GreaterThanEqual(src []int16, val int16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt16GreaterThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt16Between(src []int16, a, b int16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt16Between(src, a, b, bits.Bytes())))
	return bits
}

var Int16 = struct {
	Sort          func([]int16) []int16
	Unique        func([]int16) []int16
	RemoveZeros   func([]int16) []int16
	AddUnique     func([]int16, int16) []int16
	Insert        func([]int16, int, ...int16) []int16
	Remove        func([]int16, int16) []int16
	Contains      func([]int16, int16) bool
	Index         func([]int16, int16, int) int
	MinMax        func([]int16) (int16, int16)
	ContainsRange func([]int16, int16, int16) bool
	Intersect     func([]int16, []int16, []int16) []int16
	MatchEqual    func([]int16, int16, *bitset.Bitset, *bitset.Bitset) *bitset.Bitset
}{
	Sort: func(s []int16) []int16 {
		return Int16Sorter(s).Sort()
	},
	Unique: func(s []int16) []int16 {
		return UniqueInt16Slice(s)
	},
	RemoveZeros: func(s []int16) []int16 {
		s, _ = RemoveZeros(s)
		return s
	},
	AddUnique: func(s []int16, v int16) []int16 {
		s, _ = int16AddUnique(s, v)
		return s
	},
	Insert: func(s []int16, k int, v ...int16) []int16 {
		return Insert(s, k, v...)
	},
	Remove: func(s []int16, v int16) []int16 {
		s, _ = Remove(s, v)
		return s
	},
	Contains: func(s []int16, v int16) bool {
		return Contains(s, v)
	},
	Index: func(s []int16, v int16, last int) int {
		return index(s, v, last)
	},
	MinMax: func(s []int16) (int16, int16) {
		return MinMax(s)
	},
	ContainsRange: func(s []int16, from, to int16) bool {
		return ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []int16) []int16 {
		return IntersectSortedInt16(x, y, out)
	},
	MatchEqual: func(s []int16, val int16, bits, mask *bitset.Bitset) *bitset.Bitset {
		return MatchInt16Equal(s, val, bits, mask)
	},
}

func int16AddUnique(s []int16, val int16) ([]int16, bool) {
	idx := index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Int16Sorter(s).Sort()
	return s, true
}
