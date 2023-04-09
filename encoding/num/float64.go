// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import "blockwatch.cc/knoxdb/encoding/bitset"

func MatchFloat64Equal(src []float64, val float64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat64Equal(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64NotEqual(src []float64, val float64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat64NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64LessThan(src []float64, val float64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat64LessThan(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64LessThanEqual(src []float64, val float64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat64LessThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64GreaterThan(src []float64, val float64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat64GreaterThan(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64GreaterThanEqual(src []float64, val float64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat64GreaterThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64Between(src []float64, a, b float64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat64Between(src, a, b, bits.Bytes())))
	return bits
}

var Float64 = struct {
	Sort          func([]float64) []float64
	Unique        func([]float64) []float64
	RemoveZeros   func([]float64) []float64
	AddUnique     func([]float64, float64) []float64
	Insert        func([]float64, int, ...float64) []float64
	Remove        func([]float64, float64) []float64
	Contains      func([]float64, float64) bool
	Index         func([]float64, float64, int) int
	MinMax        func([]float64) (float64, float64)
	ContainsRange func([]float64, float64, float64) bool
	Intersect     func([]float64, []float64, []float64) []float64
	MatchEqual    func([]float64, float64, *bitset.Bitset, *bitset.Bitset) *bitset.Bitset
}{
	Sort: func(s []float64) []float64 {
		return Float64Sorter(s).Sort()
	},
	Unique: func(s []float64) []float64 {
		return UniqueFloat64Slice(s)
	},
	RemoveZeros: func(s []float64) []float64 {
		s, _ = RemoveZeros(s)
		return s
	},
	AddUnique: func(s []float64, v float64) []float64 {
		s, _ = float64AddUnique(s, v)
		return s
	},
	Insert: func(s []float64, k int, v ...float64) []float64 {
		return Insert(s, k, v...)
	},
	Remove: func(s []float64, v float64) []float64 {
		s, _ = Remove(s, v)
		return s
	},
	Contains: func(s []float64, v float64) bool {
		return Contains(s, v)
	},
	Index: func(s []float64, v float64, last int) int {
		return index(s, v, last)
	},
	MinMax: func(s []float64) (float64, float64) {
		return MinMax(s)
	},
	ContainsRange: func(s []float64, from, to float64) bool {
		return ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []float64) []float64 {
		return IntersectSortedFloat64(x, y, out)
	},
	MatchEqual: func(s []float64, val float64, bits, mask *bitset.Bitset) *bitset.Bitset {
		return MatchFloat64Equal(s, val, bits, mask)
	},
}

func float64AddUnique(s []float64, val float64) ([]float64, bool) {
	idx := index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Float64Sorter(s).Sort()
	return s, true
}
