// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package cmp

import (
	"blockwatch.cc/knoxdb/internal/bitset"
)

func MatchInt64Equal(src []int64, val int64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt64Equal(src, val, bits.Bytes())))
	return bits
}

func MatchInt64NotEqual(src []int64, val int64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt64NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt64Less(src []int64, val int64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt64Less(src, val, bits.Bytes())))
	return bits
}

func MatchInt64LessEqual(src []int64, val int64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt64LessEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt64Greater(src []int64, val int64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt64Greater(src, val, bits.Bytes())))
	return bits
}

func MatchInt64GreaterEqual(src []int64, val int64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt64GreaterEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt64Between(src []int64, a, b int64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt64Between(src, a, b, bits.Bytes())))
	return bits
}
