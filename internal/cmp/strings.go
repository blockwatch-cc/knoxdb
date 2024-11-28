// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import "blockwatch.cc/knoxdb/internal/bitset"

func MatchStringsEqual(src []string, val string, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchStringsEqual(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchStringsNotEqual(src []string, val string, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchStringsNotEqual(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchStringsLess(src []string, val string, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchStringsLess(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchStringsLessEqual(src []string, val string, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchStringsLessEqual(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchStringsGreater(src []string, val string, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchStringsGreater(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchStringsGreaterEqual(src []string, val string, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchStringsGreaterEqual(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchStringsBetween(src []string, a, b string, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchStringsBetween(src, a, b, bits.Bytes(), mask.Bytes())))
	return bits
}
