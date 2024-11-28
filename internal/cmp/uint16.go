// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package cmp

import (
	"blockwatch.cc/knoxdb/internal/bitset"
)

func MatchUint16Equal(src []uint16, val uint16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint16Equal(src, val, bits.Bytes())))
	return bits
}

func MatchUint16NotEqual(src []uint16, val uint16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint16NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint16Less(src []uint16, val uint16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint16Less(src, val, bits.Bytes())))
	return bits
}

func MatchUint16LessEqual(src []uint16, val uint16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint16LessEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint16Greater(src []uint16, val uint16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint16Greater(src, val, bits.Bytes())))
	return bits
}

func MatchUint16GreaterEqual(src []uint16, val uint16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint16GreaterEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint16Between(src []uint16, a, b uint16, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint16Between(src, a, b, bits.Bytes())))
	return bits
}
