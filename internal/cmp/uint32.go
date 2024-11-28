// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package cmp

import (
	"blockwatch.cc/knoxdb/internal/bitset"
)

func MatchUint32Equal(src []uint32, val uint32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint32Equal(src, val, bits.Bytes())))
	return bits
}

func MatchUint32NotEqual(src []uint32, val uint32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint32NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint32Less(src []uint32, val uint32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint32Less(src, val, bits.Bytes())))
	return bits
}

func MatchUint32LessEqual(src []uint32, val uint32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint32LessEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint32Greater(src []uint32, val uint32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint32Greater(src, val, bits.Bytes())))
	return bits
}

func MatchUint32GreaterEqual(src []uint32, val uint32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint32GreaterEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint32Between(src []uint32, a, b uint32, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint32Between(src, a, b, bits.Bytes())))
	return bits
}
