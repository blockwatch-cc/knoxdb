// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package cmp

import (
	"blockwatch.cc/knoxdb/internal/bitset"
)

func MatchFloat64Equal(src []float64, val float64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchFloat64Equal(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64NotEqual(src []float64, val float64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchFloat64NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64Less(src []float64, val float64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchFloat64Less(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64LessEqual(src []float64, val float64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchFloat64LessEqual(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64Greater(src []float64, val float64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchFloat64Greater(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64GreaterEqual(src []float64, val float64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchFloat64GreaterEqual(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64Between(src []float64, a, b float64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchFloat64Between(src, a, b, bits.Bytes())))
	return bits
}
