// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"blockwatch.cc/knoxdb/internal/bitset"
)

func MatchBytesEqual(src [][]byte, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchBytesEqual(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchBytesNotEqual(src [][]byte, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchBytesNotEqual(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchBytesLess(src [][]byte, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchBytesLess(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchBytesLessEqual(src [][]byte, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchBytesLessEqual(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchBytesGreater(src [][]byte, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchBytesGreater(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchBytesGreaterEqual(src [][]byte, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchBytesGreaterEqual(src, val, bits.Bytes(), mask.Bytes())))
	return bits
}

func MatchBytesBetween(src [][]byte, a, b []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(len(src))
	bits.ResetCount(int(matchBytesBetween(src, a, b, bits.Bytes(), mask.Bytes())))
	return bits
}
