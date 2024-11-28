// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package cmp

import (
	"blockwatch.cc/knoxdb/internal/bitset"
)

func MatchInt16Equal(src []int16, val int16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt16Equal(src, val, bits.Bytes())))
	return bits
}

func MatchInt16NotEqual(src []int16, val int16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt16NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt16Less(src []int16, val int16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt16Less(src, val, bits.Bytes())))
	return bits
}

func MatchInt16LessEqual(src []int16, val int16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt16LessEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt16Greater(src []int16, val int16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt16Greater(src, val, bits.Bytes())))
	return bits
}

func MatchInt16GreaterEqual(src []int16, val int16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt16GreaterEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt16Between(src []int16, a, b int16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt16Between(src, a, b, bits.Bytes())))
	return bits
}
