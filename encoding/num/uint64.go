// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"blockwatch.cc/knoxdb/encoding/bitset"
	"blockwatch.cc/knoxdb/util"
)

func MatchUint64Equal(src []uint64, val uint64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint64Equal(src, val, bits.Bytes())))
	return bits
}

func MatchUint64NotEqual(src []uint64, val uint64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint64NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint64LessThan(src []uint64, val uint64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint64LessThan(src, val, bits.Bytes())))
	return bits
}

func MatchUint64LessThanEqual(src []uint64, val uint64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint64LessThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint64GreaterThan(src []uint64, val uint64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint64GreaterThan(src, val, bits.Bytes())))
	return bits
}

func MatchUint64GreaterThanEqual(src []uint64, val uint64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint64GreaterThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint64Between(src []uint64, a, b uint64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchUint64Between(src, a, b, bits.Bytes())))
	return bits
}

var Uint64 = struct {
	Sort          func([]uint64) []uint64
	Unique        func([]uint64) []uint64
	RemoveZeros   func([]uint64) []uint64
	AddUnique     func([]uint64, uint64) []uint64
	Insert        func([]uint64, int, ...uint64) []uint64
	Remove        func([]uint64, uint64) []uint64
	Contains      func([]uint64, uint64) bool
	Index         func([]uint64, uint64, int) int
	MinMax        func([]uint64) (uint64, uint64)
	ContainsRange func([]uint64, uint64, uint64) bool
	Intersect     func([]uint64, []uint64, []uint64) []uint64
	MatchEqual    func([]uint64, uint64, *bitset.Bitset, *bitset.Bitset) *bitset.Bitset
}{
	Sort: func(s []uint64) []uint64 {
		return Uint64Sorter(s).Sort()
	},
	Unique: func(s []uint64) []uint64 {
		return UniqueUint64Slice(s)
	},
	RemoveZeros: func(s []uint64) []uint64 {
		s, _ = RemoveZeros(s)
		return s
	},
	AddUnique: func(s []uint64, v uint64) []uint64 {
		s, _ = uint64AddUnique(s, v)
		return s
	},
	Insert: func(s []uint64, k int, v ...uint64) []uint64 {
		return Insert(s, k, v...)
	},
	Remove: func(s []uint64, v uint64) []uint64 {
		s, _ = Remove(s, v)
		return s
	},
	Contains: func(s []uint64, v uint64) bool {
		return Contains(s, v)
	},
	Index: func(s []uint64, v uint64, last int) int {
		return index(s, v, last)
	},
	MinMax: func(s []uint64) (uint64, uint64) {
		return MinMax(s)
	},
	ContainsRange: func(s []uint64, from, to uint64) bool {
		return ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []uint64) []uint64 {
		if out == nil {
			out = make([]uint64, 0, util.Max(len(x), len(y)))
		}
		return IntersectSortedUint64(x, y, out)
	},
	MatchEqual: func(s []uint64, val uint64, bits, mask *bitset.Bitset) *bitset.Bitset {
		return MatchUint64Equal(s, val, bits, mask)
	},
}

func uint64AddUnique(s []uint64, val uint64) ([]uint64, bool) {
	idx := index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Uint64Sorter(s).Sort()
	return s, true
}
