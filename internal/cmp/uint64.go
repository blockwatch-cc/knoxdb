// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package cmp

import (
	"blockwatch.cc/knoxdb/internal/bitset"
)

func MatchUint64Equal(src []uint64, val uint64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint64Equal(src, val, bits.Bytes())))
	return bits
}

func MatchUint64NotEqual(src []uint64, val uint64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint64NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint64Less(src []uint64, val uint64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint64Less(src, val, bits.Bytes())))
	return bits
}

func MatchUint64LessEqual(src []uint64, val uint64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint64LessEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint64Greater(src []uint64, val uint64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint64Greater(src, val, bits.Bytes())))
	return bits
}

func MatchUint64GreaterEqual(src []uint64, val uint64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint64GreaterEqual(src, val, bits.Bytes())))
	return bits
}

func MatchUint64Between(src []uint64, a, b uint64, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchUint64Between(src, a, b, bits.Bytes())))
	return bits
}
