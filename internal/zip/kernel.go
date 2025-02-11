// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package zip

import (
	"blockwatch.cc/knoxdb/internal/zip/avx2"
	"blockwatch.cc/knoxdb/internal/zip/generic"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	// slice encoders
	zzDeltaEncodeUint64 = generic.ZzDeltaEncodeUint64
	zzDeltaEncodeUint32 = generic.ZzDeltaEncodeUint32
	zzDeltaEncodeUint16 = generic.ZzDeltaEncodeUint16
	zzDeltaEncodeUint8  = generic.ZzDeltaEncodeUint8

	// slice decoders
	zzDeltaDecodeInt64 = generic.ZzDeltaDecodeInt64
	zzDeltaDecodeInt32 = generic.ZzDeltaDecodeInt32
	zzDeltaDecodeInt16 = generic.ZzDeltaDecodeInt16
	zzDeltaDecodeInt8  = generic.ZzDeltaDecodeInt8
	zzDeltaDecodeTime  = generic.ZzDeltaDecodeTime
	// deltaDecodeTime    = generic.DeltaDecodeTime

	// bit packing
	packBytes8Bit    = generic.PackBytes8Bit
	packBytes16Bit   = generic.PackBytes16Bit
	packBytes24Bit   = generic.PackBytes24Bit
	packBytes32Bit   = generic.PackBytes32Bit
	unpackBytes8Bit  = generic.UnpackBytes8Bit
	unpackBytes16Bit = generic.UnpackBytes16Bit
	unpackBytes24Bit = generic.UnpackBytes24Bit
	unpackBytes32Bit = generic.UnpackBytes32Bit

	// value encoders
	zigZagEncodeInt64 = generic.ZigZagEncodeInt64

	// value decoders
	zigZagDecodeUint64 = generic.ZigZagDecodeUint64

	// slice type mappers
	asU64 = util.ReinterpretSlice[int64, uint64]
	asU32 = util.ReinterpretSlice[int32, uint32]
	asU16 = util.ReinterpretSlice[int16, uint16]
	asU8  = util.ReinterpretSlice[int8, uint8]

	asI64 = util.ReinterpretSlice[uint64, int64]
	asI32 = util.ReinterpretSlice[uint32, int32]
	asI16 = util.ReinterpretSlice[uint16, int16]
	asI8  = util.ReinterpretSlice[uint8, int8]
)

func init() {
	if util.UseAVX2 {
		// not implemented yet
		// zzDeltaEncodeUint64 = avx2.ZzDeltaEncodeUint64
		// zzDeltaEncodeUint32 = avx2.ZzDeltaEncodeUint32
		// zzDeltaEncodeUint16 = avx2.ZzDeltaEncodeUint16
		// zzDeltaEncodeUint8 = avx2.ZzDeltaEncodeUint8
		zzDeltaDecodeInt64 = avx2.ZzDeltaDecodeInt64
		zzDeltaDecodeInt32 = avx2.ZzDeltaDecodeInt32
		zzDeltaDecodeInt16 = avx2.ZzDeltaDecodeInt16
		zzDeltaDecodeInt8 = avx2.ZzDeltaDecodeInt8
		zzDeltaDecodeTime = avx2.ZzDeltaDecodeTime
		// deltaDecodeTime = avx2.DeltaDecodeTime
		packBytes16Bit = avx2.PackBytes16Bit
		packBytes32Bit = avx2.PackBytes32Bit
		unpackBytes16Bit = avx2.UnpackBytes16Bit
		unpackBytes32Bit = avx2.UnpackBytes32Bit
	}
}
