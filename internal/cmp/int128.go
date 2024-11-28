// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package cmp

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/pkg/num"
)

func MatchInt128Equal(src num.Int128Stride, val num.Int128, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(src.Len())
	bits.ResetCount(int(matchInt128Equal(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchInt128NotEqual(src num.Int128Stride, val num.Int128, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(src.Len())
	bits.ResetCount(int(matchInt128NotEqual(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchInt128Less(src num.Int128Stride, val num.Int128, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(src.Len())
	bits.ResetCount(int(matchInt128Less(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchInt128LessEqual(src num.Int128Stride, val num.Int128, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(src.Len())
	bits.ResetCount(int(matchInt128LessEqual(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchInt128Greater(src num.Int128Stride, val num.Int128, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(src.Len())
	bits.ResetCount(int(matchInt128Greater(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchInt128GreaterEqual(src num.Int128Stride, val num.Int128, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(src.Len())
	bits.ResetCount(int(matchInt128GreaterEqual(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchInt128Between(src num.Int128Stride, a, b num.Int128, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(src.Len())
	bits.ResetCount(int(matchInt128Between(src, a, b, bits.Bytes(), mask.Bytes())))
	return bits
}
