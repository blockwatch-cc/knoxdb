// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package cmp

import (
	"blockwatch.cc/knoxdb/internal/bitset"
)

func MatchInt32Equal(src []int32, val int32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt32Equal(src, val, bits.Bytes())))
	return bits
}

func MatchInt32NotEqual(src []int32, val int32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt32NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt32Less(src []int32, val int32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt32Less(src, val, bits.Bytes())))
	return bits
}

func MatchInt32LessEqual(src []int32, val int32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt32LessEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt32Greater(src []int32, val int32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt32Greater(src, val, bits.Bytes())))
	return bits
}

func MatchInt32GreaterEqual(src []int32, val int32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt32GreaterEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt32Between(src []int32, a, b int32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt32Between(src, a, b, bits.Bytes())))
	return bits
}
