// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package num

import "blockwatch.cc/knoxdb/encoding/bitset"

func MatchUint16Equal(src []uint16, val uint16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint16Equal(src, val, bits.Bytes())))
	return bits
}

func MatchUint16NotEqual(src []uint16, val uint16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint16NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint16LessThan(src []uint16, val uint16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint16LessThan(src, val, bits.Bytes())))
	return bits
}

func MatchUint16LessThanEqual(src []uint16, val uint16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint16LessThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint16GreaterThan(src []uint16, val uint16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint16GreaterThan(src, val, bits.Bytes())))
	return bits
}

func MatchUint16GreaterThanEqual(src []uint16, val uint16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint16GreaterThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint16Between(src []uint16, a, b uint16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint16Between(src, a, b, bits.Bytes())))
	return bits
}

var Uint16 = struct {
	Sort          func([]uint16) []uint16
	Unique        func([]uint16) []uint16
	RemoveZeros   func([]uint16) []uint16
	AddUnique     func([]uint16, uint16) []uint16
	Insert        func([]uint16, int, ...uint16) []uint16
	Remove        func([]uint16, uint16) []uint16
	Contains      func([]uint16, uint16) bool
	Index         func([]uint16, uint16, int) int
	MinMax        func([]uint16) (uint16, uint16)
	ContainsRange func([]uint16, uint16, uint16) bool
	Intersect     func([]uint16, []uint16, []uint16) []uint16
	MatchEqual    func([]uint16, uint16, *bitset.Bitset, *bitset.Bitset) *bitset.Bitset
}{
	Sort: func(s []uint16) []uint16 {
		return Uint16Sorter(s).Sort()
	},
	Unique: func(s []uint16) []uint16 {
		return UniqueUint16Slice(s)
	},
	RemoveZeros: func(s []uint16) []uint16 {
		s, _ = RemoveZeros(s)
		return s
	},
	AddUnique: func(s []uint16, v uint16) []uint16 {
		s, _ = uint16AddUnique(s, v)
		return s
	},
	Insert: func(s []uint16, k int, v ...uint16) []uint16 {
		return Insert(s, k, v...)
	},
	Remove: func(s []uint16, v uint16) []uint16 {
		s, _ = Remove(s, v)
		return s
	},
	Contains: func(s []uint16, v uint16) bool {
		return Contains(s, v)
	},
	Index: func(s []uint16, v uint16, last int) int {
		return index(s, v, last)
	},
	MinMax: func(s []uint16) (uint16, uint16) {
		return MinMax(s)
	},
	ContainsRange: func(s []uint16, from, to uint16) bool {
		return ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []uint16) []uint16 {
		return IntersectSortedUint16(x, y, out)
	},
	MatchEqual: func(s []uint16, val uint16, bits, mask *bitset.Bitset) *bitset.Bitset {
		return MatchUint16Equal(s, val, bits, mask)
	},
}

func uint16AddUnique(s []uint16, val uint16) ([]uint16, bool) {
	idx := index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Uint16Sorter(s).Sort()
	return s, true
}
