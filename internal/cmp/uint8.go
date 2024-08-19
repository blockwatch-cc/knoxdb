// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package cmp

import (
	"blockwatch.cc/knoxdb/internal/bitset"
)

func MatchUint8Equal(src []uint8, val uint8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint8Equal(src, val, bits.Bytes())))
	return bits
}

func MatchUint8NotEqual(src []uint8, val uint8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint8NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint8Less(src []uint8, val uint8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint8Less(src, val, bits.Bytes())))
	return bits
}

func MatchUint8LessEqual(src []uint8, val uint8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint8LessEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint8Greater(src []uint8, val uint8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint8Greater(src, val, bits.Bytes())))
	return bits
}

func MatchUint8GreaterEqual(src []uint8, val uint8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint8GreaterEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint8Between(src []uint8, a, b uint8, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint8Between(src, a, b, bits.Bytes())))
	return bits
}
