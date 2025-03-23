// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package s8b

import (
	"blockwatch.cc/knoxdb/internal/encode/s8b/avx2"
	"blockwatch.cc/knoxdb/internal/encode/s8b/avx512"
	"blockwatch.cc/knoxdb/internal/encode/s8b/generic"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	EncodeLegacy = generic.EncodeLegacy

	EncodeUint64 = generic.Encode[uint64]
	EncodeInt64  = generic.Encode[int64]
	EncodeUint32 = generic.Encode[uint32]
	EncodeInt32  = generic.Encode[int32]
	EncodeUint16 = generic.Encode[uint16]
	EncodeInt16  = generic.Encode[int16]
	EncodeUint8  = generic.Encode[uint8]
	EncodeInt8   = generic.Encode[int8]

	DecodeUint64 = generic.Decode[uint64]
	DecodeUint32 = generic.Decode[uint32]
	DecodeUint16 = generic.Decode[uint16]
	DecodeUint8  = generic.Decode[uint8]

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
	rangeVal := uint64(maxv) - uint64(minv)
	if rangeVal == 0 { // All values equal after shift
		return (srcLen + 59) / 60 // Ceiling division for runs of 0s
	}

	// Find bits needed for rangeVal
	bitsPerValue := 1
	for rangeVal >= (1 << uint(bitsPerValue)) {
		bitsPerValue++
	}

	// Map to values per word
	var valuesPerWord int
	switch {
	case bitsPerValue <= 1:
		valuesPerWord = 60
	case bitsPerValue <= 2:
		valuesPerWord = 30
	case bitsPerValue <= 3:
		valuesPerWord = 20
	case bitsPerValue <= 4:
		valuesPerWord = 15
	case bitsPerValue <= 5:
		valuesPerWord = 12
	case bitsPerValue <= 6:
		valuesPerWord = 10
	case bitsPerValue <= 7:
		valuesPerWord = 8
	case bitsPerValue <= 8:
		valuesPerWord = 7
	case bitsPerValue <= 10:
		valuesPerWord = 6
	case bitsPerValue <= 12:
		valuesPerWord = 5
	case bitsPerValue <= 15:
		valuesPerWord = 4
	case bitsPerValue <= 20:
		valuesPerWord = 3
	case bitsPerValue <= 30:
		valuesPerWord = 2
	default:
		valuesPerWord = 1
	}

	return (srcLen + valuesPerWord - 1) / valuesPerWord
}
