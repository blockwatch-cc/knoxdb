// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package cmp

import (
	"blockwatch.cc/knoxdb/internal/bitset"
)

func MatchFloat32Equal(src []float32, val float32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchFloat32Equal(src, val, bits.Bytes())))
	return bits
}

func MatchFloat32NotEqual(src []float32, val float32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchFloat32NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchFloat32Less(src []float32, val float32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchFloat32Less(src, val, bits.Bytes())))
	return bits
}

func MatchFloat32LessEqual(src []float32, val float32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchFloat32LessEqual(src, val, bits.Bytes())))
	return bits
}

func MatchFloat32Greater(src []float32, val float32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchFloat32Greater(src, val, bits.Bytes())))
	return bits
}

func MatchFloat32GreaterEqual(src []float32, val float32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchFloat32GreaterEqual(src, val, bits.Bytes())))
	return bits
}

func MatchFloat32Between(src []float32, a, b float32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchFloat32Between(src, a, b, bits.Bytes())))
	return bits
}
