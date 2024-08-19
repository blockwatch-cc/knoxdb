// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/pkg/num"
)

func MatchInt256Equal(src num.Int256Stride, val num.Int256, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(src.Len())
	bits.ResetCount(int(matchInt256Equal(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchInt256NotEqual(src num.Int256Stride, val num.Int256, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(src.Len())
	bits.ResetCount(int(matchInt256NotEqual(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchInt256Less(src num.Int256Stride, val num.Int256, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(src.Len())
	bits.ResetCount(int(matchInt256Less(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchInt256LessEqual(src num.Int256Stride, val num.Int256, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(src.Len())
	bits.ResetCount(int(matchInt256LessEqual(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchInt256Greater(src num.Int256Stride, val num.Int256, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(src.Len())
	bits.ResetCount(int(matchInt256Greater(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchInt256GreaterEqual(src num.Int256Stride, val num.Int256, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(src.Len())
	bits.ResetCount(int(matchInt256GreaterEqual(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchInt256Between(src num.Int256Stride, a, b num.Int256, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(src.Len())
	bits.ResetCount(int(matchInt256Between(src, a, b, bits.Bytes(), mask.Bytes())))
	return bits
}
