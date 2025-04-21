// Copyright (c) 2022-2025 Blockwatch Data Inc.
// Author: alex@blockwatch,stefan@blockwatch.cc

package s8b

import (
	"math/bits"

	"blockwatch.cc/knoxdb/internal/encode/s8b/avx2"
	"blockwatch.cc/knoxdb/internal/encode/s8b/avx512"
	"blockwatch.cc/knoxdb/internal/encode/s8b/generic"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	// Legacy format and implementations
	CountLegacy        = generic.CountLegacy
	EncodeLegacy       = generic.EncodeLegacy
	DecodeLegacyUint64 = generic.DecodeLegacyUint64
	DecodeLegacyUint32 = generic.DecodeLegacyUint32
	DecodeLegacyUint16 = generic.DecodeLegacyUint16
	DecodeLegacyUint8  = generic.DecodeLegacyUint8

	// Encoders
	EncodeUint64 = generic.Encode[uint64]
	EncodeInt64  = generic.Encode[int64]
	EncodeUint32 = generic.Encode[uint32]
	EncodeInt32  = generic.Encode[int32]
	EncodeUint16 = generic.Encode[uint16]
	EncodeInt16  = generic.Encode[int16]
	EncodeUint8  = generic.Encode[uint8]
	EncodeInt8   = generic.Encode[int8]

	// Decoders
	DecodeUint64 = generic.Decode[uint64]
	DecodeUint32 = generic.Decode[uint32]
	DecodeUint16 = generic.Decode[uint16]
	DecodeUint8  = generic.Decode[uint8]

	// Comparers
	Equal        = generic.Equal
	NotEqual     = generic.NotEqual
	Less         = generic.Less
	LessEqual    = generic.LessEqual
	Greater      = generic.Greater
	GreaterEqual = generic.GreaterEqual
	Between      = generic.Between

	// Helpers
	CountValues = generic.CountValues

	MaxValue   = uint64((1 << 60) - 1)
	MaxValue32 = uint64((1 << 30) - 1)
	MaxValue16 = uint64((1 << 15) - 1)
)

func init() {
	if util.UseAVX2 {
		DecodeUint64 = avx2.DecodeUint64
		DecodeUint32 = avx2.DecodeUint32
		DecodeUint16 = avx2.DecodeUint16
		DecodeUint8 = avx2.DecodeUint8
		CountValues = avx2.CountValues
	}
	if util.UseAVX512_F {
		DecodeUint64 = avx512.DecodeUint64
	}
}

func EstimateMaxSize[T types.Integer](srcLen int, minv, maxv T) int {
	var log2 int
	if types.IsSigned[T]() {
		log2 = bits.Len64(uint64(int64(maxv) - int64(minv)))
	} else {
		log2 = bits.Len64(uint64(maxv - minv))
	}

	if log2 == 0 { // All values zero after min-FOR
		// add one in case early values fit denser packing and we're having some overhang later
		return (srcLen+59)/60 + packRemainder(srcLen%60) + 1
	}

	// Map to values per word
	var valuesPerWord int
	switch {
	case log2 <= 1:
		valuesPerWord = 60
	case log2 <= 2:
		valuesPerWord = 30
	case log2 <= 3:
		valuesPerWord = 20
	case log2 <= 4:
		valuesPerWord = 15
	case log2 <= 5:
		valuesPerWord = 12
	case log2 <= 6:
		valuesPerWord = 10
	case log2 <= 7:
		valuesPerWord = 8
	case log2 <= 8:
		valuesPerWord = 7
	case log2 <= 10:
		valuesPerWord = 6
	case log2 <= 12:
		valuesPerWord = 5
	case log2 <= 15:
		valuesPerWord = 4
	case log2 <= 20:
		valuesPerWord = 3
	case log2 <= 30:
		valuesPerWord = 2
	default:
		valuesPerWord = 1
	}

	// add one in case early values fit denser packing and we're having some overhang later
	return (srcLen+valuesPerWord-1)/valuesPerWord + packRemainder(srcLen%valuesPerWord) + 1
}

func packRemainder(k int) (n int) {
	for k > 0 {
		switch {
		case k > 30:
			k -= 30
			n++
		case k > 20:
			k -= 20
			n++
		case k > 15:
			k -= 15
			n++
		case k > 12:
			k -= 12
			n++
		case k > 10:
			k -= 10
			n++
		case k > 8:
			k -= 8
			n++
		default:
			k = 0
			n++
		}
	}
	return
}
