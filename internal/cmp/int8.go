// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package cmp

import (
	"blockwatch.cc/knoxdb/internal/bitset"
)

func MatchInt8Equal(src []int8, val int8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt8Equal(src, val, bits.Bytes())))
	return bits
}

func MatchInt8NotEqual(src []int8, val int8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt8NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt8Less(src []int8, val int8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt8Less(src, val, bits.Bytes())))
	return bits
}

func MatchInt8LessEqual(src []int8, val int8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt8LessEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt8Greater(src []int8, val int8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt8Greater(src, val, bits.Bytes())))
	return bits
}

func MatchInt8GreaterEqual(src []int8, val int8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt8GreaterEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt8Between(src []int8, a, b int8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchInt8Between(src, a, b, bits.Bytes())))
	return bits
}
