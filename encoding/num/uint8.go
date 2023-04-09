// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package num

import "blockwatch.cc/knoxdb/encoding/bitset"

func MatchUint8Equal(src []uint8, val uint8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint8Equal(src, val, bits.Bytes())))
	return bits
}

func MatchUint8NotEqual(src []uint8, val uint8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint8NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint8LessThan(src []uint8, val uint8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint8LessThan(src, val, bits.Bytes())))
	return bits
}

func MatchUint8LessThanEqual(src []uint8, val uint8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint8LessThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint8GreaterThan(src []uint8, val uint8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint8GreaterThan(src, val, bits.Bytes())))
	return bits
}

func MatchUint8GreaterThanEqual(src []uint8, val uint8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint8GreaterThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint8Between(src []uint8, a, b uint8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint8Between(src, a, b, bits.Bytes())))
	return bits
}

var Uint8 = struct {
	Sort          func([]uint8) []uint8
	Unique        func([]uint8) []uint8
	RemoveZeros   func([]uint8) []uint8
	AddUnique     func([]uint8, uint8) []uint8
	Insert        func([]uint8, int, ...uint8) []uint8
	Remove        func([]uint8, uint8) []uint8
	Contains      func([]uint8, uint8) bool
	Index         func([]uint8, uint8, int) int
	MinMax        func([]uint8) (uint8, uint8)
	ContainsRange func([]uint8, uint8, uint8) bool
	Intersect     func([]uint8, []uint8, []uint8) []uint8
	MatchEqual    func([]uint8, uint8, *bitset.Bitset, *bitset.Bitset) *bitset.Bitset
}{
	Sort: func(s []uint8) []uint8 {
		return Uint8Sorter(s).Sort()
	},
	Unique: func(s []uint8) []uint8 {
		return UniqueUint8Slice(s)
	},
	RemoveZeros: func(s []uint8) []uint8 {
		s, _ = RemoveZeros(s)
		return s
	},
	AddUnique: func(s []uint8, v uint8) []uint8 {
		s, _ = uint8AddUnique(s, v)
		return s
	},
	Insert: func(s []uint8, k int, v ...uint8) []uint8 {
		return Insert(s, k, v...)
	},
	Remove: func(s []uint8, v uint8) []uint8 {
		s, _ = Remove(s, v)
		return s
	},
	Contains: func(s []uint8, v uint8) bool {
		return Contains(s, v)
	},
	Index: func(s []uint8, v uint8, last int) int {
		return index(s, v, last)
	},
	MinMax: func(s []uint8) (uint8, uint8) {
		return MinMax(s)
	},
	ContainsRange: func(s []uint8, from, to uint8) bool {
		return ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []uint8) []uint8 {
		return IntersectSortedUint8(x, y, out)
	},
	MatchEqual: func(s []uint8, val uint8, bits, mask *bitset.Bitset) *bitset.Bitset {
		return MatchUint8Equal(s, val, bits, mask)
	},
}

func uint8AddUnique(s []uint8, val uint8) ([]uint8, bool) {
	idx := index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Uint8Sorter(s).Sort()
	return s, true
}
