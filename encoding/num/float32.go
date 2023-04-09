// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package num

import "blockwatch.cc/knoxdb/encoding/bitset"

func MatchFloat32Equal(src []float32, val float32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat32Equal(src, val, bits.Bytes())))
	return bits
}

func MatchFloat32NotEqual(src []float32, val float32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat32NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchFloat32LessThan(src []float32, val float32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat32LessThan(src, val, bits.Bytes())))
	return bits
}

func MatchFloat32LessThanEqual(src []float32, val float32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat32LessThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchFloat32GreaterThan(src []float32, val float32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat32GreaterThan(src, val, bits.Bytes())))
	return bits
}

func MatchFloat32GreaterThanEqual(src []float32, val float32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat32GreaterThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchFloat32Between(src []float32, a, b float32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat32Between(src, a, b, bits.Bytes())))
	return bits
}

var Float32 = struct {
	Sort          func([]float32) []float32
	Unique        func([]float32) []float32
	RemoveZeros   func([]float32) []float32
	AddUnique     func([]float32, float32) []float32
	Insert        func([]float32, int, ...float32) []float32
	Remove        func([]float32, float32) []float32
	Contains      func([]float32, float32) bool
	Index         func([]float32, float32, int) int
	MinMax        func([]float32) (float32, float32)
	ContainsRange func([]float32, float32, float32) bool
	Intersect     func([]float32, []float32, []float32) []float32
	MatchEqual    func([]float32, float32, *bitset.Bitset, *bitset.Bitset) *bitset.Bitset
}{
	Sort: func(s []float32) []float32 {
		return Float32Sorter(s).Sort()
	},
	Unique: func(s []float32) []float32 {
		return UniqueFloat32Slice(s)
	},
	RemoveZeros: func(s []float32) []float32 {
		s, _ = RemoveZeros(s)
		return s
	},
	AddUnique: func(s []float32, v float32) []float32 {
		s, _ = float32AddUnique(s, v)
		return s
	},
	Insert: func(s []float32, k int, v ...float32) []float32 {
		return Insert(s, k, v...)
	},
	Remove: func(s []float32, v float32) []float32 {
		s, _ = Remove(s, v)
		return s
	},
	Contains: func(s []float32, v float32) bool {
		return Contains(s, v)
	},
	Index: func(s []float32, v float32, last int) int {
		return index(s, v, last)
	},
	MinMax: func(s []float32) (float32, float32) {
		return MinMax(s)
	},
	ContainsRange: func(s []float32, from, to float32) bool {
		return ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []float32) []float32 {
		return IntersectSortedFloat32(x, y, out)
	},
	MatchEqual: func(s []float32, val float32, bits, mask *bitset.Bitset) *bitset.Bitset {
		return MatchFloat32Equal(s, val, bits, mask)
	},
}

func float32AddUnique(s []float32, val float32) ([]float32, bool) {
	idx := index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Float32Sorter(s).Sort()
	return s, true
}
