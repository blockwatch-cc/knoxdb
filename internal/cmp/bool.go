// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"blockwatch.cc/knoxdb/internal/bitset"
)

func MatchBoolEqual(src []bool, val bool, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchBoolEqual(src, val, bits.Bytes())))
	return bits
}

func MatchBoolNotEqual(src []bool, val bool, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchBoolNotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchBoolLess(src []bool, val bool, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchBoolLess(src, val, bits.Bytes())))
	return bits
}

func MatchBoolLessEqual(src []bool, val bool, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchBoolLessEqual(src, val, bits.Bytes())))
	return bits
}

func MatchBoolGreater(src []bool, val bool, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchBoolGreater(src, val, bits.Bytes())))
	return bits
}

func MatchBoolGreaterEqual(src []bool, val bool, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchBoolGreaterEqual(src, val, bits.Bytes())))
	return bits
}

func MatchBoolBetween(src []bool, a, b bool, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchBoolBetween(src, a, b, bits.Bytes())))
	return bits
}
