// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package num

import "blockwatch.cc/knoxdb/encoding/bitset"

func MatchUint32Equal(src []uint32, val uint32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint32Equal(src, val, bits.Bytes())))
	return bits
}

func MatchUint32NotEqual(src []uint32, val uint32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint32NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint32LessThan(src []uint32, val uint32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint32LessThan(src, val, bits.Bytes())))
	return bits
}

func MatchUint32LessThanEqual(src []uint32, val uint32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint32LessThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint32GreaterThan(src []uint32, val uint32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint32GreaterThan(src, val, bits.Bytes())))
	return bits
}

func MatchUint32GreaterThanEqual(src []uint32, val uint32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint32GreaterThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint32Between(src []uint32, a, b uint32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint32Between(src, a, b, bits.Bytes())))
	return bits
}

var Uint32 = struct {
	Sort          func([]uint32) []uint32
	Unique        func([]uint32) []uint32
	RemoveZeros   func([]uint32) []uint32
	AddUnique     func([]uint32, uint32) []uint32
	Insert        func([]uint32, int, ...uint32) []uint32
	Remove        func([]uint32, uint32) []uint32
	Contains      func([]uint32, uint32) bool
	Index         func([]uint32, uint32, int) int
	MinMax        func([]uint32) (uint32, uint32)
	ContainsRange func([]uint32, uint32, uint32) bool
	Intersect     func([]uint32, []uint32, []uint32) []uint32
	MatchEqual    func([]uint32, uint32, *bitset.Bitset, *bitset.Bitset) *bitset.Bitset
}{
	Sort: func(s []uint32) []uint32 {
		return Uint32Sorter(s).Sort()
	},
	Unique: func(s []uint32) []uint32 {
		return UniqueUint32Slice(s)
	},
	RemoveZeros: func(s []uint32) []uint32 {
		s, _ = RemoveZeros(s)
		return s
	},
	AddUnique: func(s []uint32, v uint32) []uint32 {
		s, _ = uint32AddUnique(s, v)
		return s
	},
	Insert: func(s []uint32, k int, v ...uint32) []uint32 {
		return Insert(s, k, v...)
	},
	Remove: func(s []uint32, v uint32) []uint32 {
		s, _ = Remove(s, v)
		return s
	},
	Contains: func(s []uint32, v uint32) bool {
		return Contains(s, v)
	},
	Index: func(s []uint32, v uint32, last int) int {
		return index(s, v, last)
	},
	MinMax: func(s []uint32) (uint32, uint32) {
		return MinMax(s)
	},
	ContainsRange: func(s []uint32, from, to uint32) bool {
		return ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []uint32) []uint32 {
		return IntersectSortedUint32(x, y, out)
	},
	MatchEqual: func(s []uint32, val uint32, bits, mask *bitset.Bitset) *bitset.Bitset {
		return MatchUint32Equal(s, val, bits, mask)
	},
}

func uint32AddUnique(s []uint32, val uint32) ([]uint32, bool) {
	idx := index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Uint32Sorter(s).Sort()
	return s, true
}
