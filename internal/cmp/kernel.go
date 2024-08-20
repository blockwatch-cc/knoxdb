// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package cmp

import (
	"blockwatch.cc/knoxdb/internal/cmp/avx2"
	"blockwatch.cc/knoxdb/internal/cmp/avx512"
	"blockwatch.cc/knoxdb/internal/cmp/generic"
	"blockwatch.cc/knoxdb/pkg/util"
)

// AVX2 on non-amd64 arch will fall back to generic imports
var (
	// uint8
	matchUint8Equal        = avx2.MatchUint8Equal
	matchUint8NotEqual     = avx2.MatchUint8NotEqual
	matchUint8Less         = avx2.MatchUint8Less
	matchUint8LessEqual    = avx2.MatchUint8LessEqual
	matchUint8Greater      = avx2.MatchUint8Greater
	matchUint8GreaterEqual = avx2.MatchUint8GreaterEqual
	matchUint8Between      = avx2.MatchUint8Between

	// uint16
	matchUint16Equal        = avx2.MatchUint16Equal
	matchUint16NotEqual     = avx2.MatchUint16NotEqual
	matchUint16Less         = avx2.MatchUint16Less
	matchUint16LessEqual    = avx2.MatchUint16LessEqual
	matchUint16Greater      = avx2.MatchUint16Greater
	matchUint16GreaterEqual = avx2.MatchUint16GreaterEqual
	matchUint16Between      = avx2.MatchUint16Between

	// uint32
	matchUint32Equal        = avx2.MatchUint32Equal
	matchUint32NotEqual     = avx2.MatchUint32NotEqual
	matchUint32Less         = avx2.MatchUint32Less
	matchUint32LessEqual    = avx2.MatchUint32LessEqual
	matchUint32Greater      = avx2.MatchUint32Greater
	matchUint32GreaterEqual = avx2.MatchUint32GreaterEqual
	matchUint32Between      = avx2.MatchUint32Between

	// uint64
	matchUint64Equal        = avx2.MatchUint64Equal
	matchUint64NotEqual     = avx2.MatchUint64NotEqual
	matchUint64Less         = avx2.MatchUint64Less
	matchUint64LessEqual    = avx2.MatchUint64LessEqual
	matchUint64Greater      = avx2.MatchUint64Greater
	matchUint64GreaterEqual = avx2.MatchUint64GreaterEqual
	matchUint64Between      = avx2.MatchUint64Between

	// int8
	matchInt8Equal        = avx2.MatchInt8Equal
	matchInt8NotEqual     = avx2.MatchInt8NotEqual
	matchInt8Less         = avx2.MatchInt8Less
	matchInt8LessEqual    = avx2.MatchInt8LessEqual
	matchInt8Greater      = avx2.MatchInt8Greater
	matchInt8GreaterEqual = avx2.MatchInt8GreaterEqual
	matchInt8Between      = avx2.MatchInt8Between

	// int16
	matchInt16Equal        = avx2.MatchInt16Equal
	matchInt16NotEqual     = avx2.MatchInt16NotEqual
	matchInt16Less         = avx2.MatchInt16Less
	matchInt16LessEqual    = avx2.MatchInt16LessEqual
	matchInt16Greater      = avx2.MatchInt16Greater
	matchInt16GreaterEqual = avx2.MatchInt16GreaterEqual
	matchInt16Between      = avx2.MatchInt16Between

	// int32
	matchInt32Equal        = avx2.MatchInt32Equal
	matchInt32NotEqual     = avx2.MatchInt32NotEqual
	matchInt32Less         = avx2.MatchInt32Less
	matchInt32LessEqual    = avx2.MatchInt32LessEqual
	matchInt32Greater      = avx2.MatchInt32Greater
	matchInt32GreaterEqual = avx2.MatchInt32GreaterEqual
	matchInt32Between      = avx2.MatchInt32Between

	// int64
	matchInt64Equal        = avx2.MatchInt64Equal
	matchInt64NotEqual     = avx2.MatchInt64NotEqual
	matchInt64Less         = avx2.MatchInt64Less
	matchInt64LessEqual    = avx2.MatchInt64LessEqual
	matchInt64Greater      = avx2.MatchInt64Greater
	matchInt64GreaterEqual = avx2.MatchInt64GreaterEqual
	matchInt64Between      = avx2.MatchInt64Between

	// float32
	matchFloat32Equal        = avx2.MatchFloat32Equal
	matchFloat32NotEqual     = avx2.MatchFloat32NotEqual
	matchFloat32Less         = avx2.MatchFloat32Less
	matchFloat32LessEqual    = avx2.MatchFloat32LessEqual
	matchFloat32Greater      = avx2.MatchFloat32Greater
	matchFloat32GreaterEqual = avx2.MatchFloat32GreaterEqual
	matchFloat32Between      = avx2.MatchFloat32Between

	// float64
	matchFloat64Equal        = avx2.MatchFloat64Equal
	matchFloat64NotEqual     = avx2.MatchFloat64NotEqual
	matchFloat64Less         = avx2.MatchFloat64Less
	matchFloat64LessEqual    = avx2.MatchFloat64LessEqual
	matchFloat64Greater      = avx2.MatchFloat64Greater
	matchFloat64GreaterEqual = avx2.MatchFloat64GreaterEqual
	matchFloat64Between      = avx2.MatchFloat64Between

	// bytes
	matchBytesEqual        = generic.MatchBytesEqual
	matchBytesNotEqual     = generic.MatchBytesNotEqual
	matchBytesLess         = generic.MatchBytesLess
	matchBytesLessEqual    = generic.MatchBytesLessEqual
	matchBytesGreater      = generic.MatchBytesGreater
	matchBytesGreaterEqual = generic.MatchBytesGreaterEqual
	matchBytesBetween      = generic.MatchBytesBetween

	// string
	matchStringsEqual        = generic.MatchStringsEqual
	matchStringsNotEqual     = generic.MatchStringsNotEqual
	matchStringsLess         = generic.MatchStringsLess
	matchStringsLessEqual    = generic.MatchStringsLessEqual
	matchStringsGreater      = generic.MatchStringsGreater
	matchStringsGreaterEqual = generic.MatchStringsGreaterEqual
	matchStringsBetween      = generic.MatchStringsBetween

	// int128
	matchInt128Equal        = avx2.MatchInt128Equal
	matchInt128NotEqual     = avx2.MatchInt128NotEqual
	matchInt128Less         = avx2.MatchInt128Less
	matchInt128LessEqual    = avx2.MatchInt128LessEqual
	matchInt128Greater      = avx2.MatchInt128Greater
	matchInt128GreaterEqual = avx2.MatchInt128GreaterEqual
	matchInt128Between      = avx2.MatchInt128Between

	// int256
	matchInt256Equal        = avx2.MatchInt256Equal
	matchInt256NotEqual     = avx2.MatchInt256NotEqual
	matchInt256Less         = avx2.MatchInt256Less
	matchInt256LessEqual    = avx2.MatchInt256LessEqual
	matchInt256Greater      = avx2.MatchInt256Greater
	matchInt256GreaterEqual = avx2.MatchInt256GreaterEqual
	matchInt256Between      = avx2.MatchInt256Between

	// bool
	matchBoolEqual        = generic.MatchBoolEqual
	matchBoolNotEqual     = generic.MatchBoolNotEqual
	matchBoolLess         = generic.MatchBoolLess
	matchBoolLessEqual    = generic.MatchBoolLessEqual
	matchBoolGreater      = generic.MatchBoolGreater
	matchBoolGreaterEqual = generic.MatchBoolGreaterEqual
	matchBoolBetween      = generic.MatchBoolBetween
)

func init() {
	if util.UseAVX512_F {
		// uint16
		matchUint16Equal = avx512.MatchUint16Equal
		matchUint16NotEqual = avx512.MatchUint16NotEqual
		matchUint16Less = avx512.MatchUint16Less
		matchUint16LessEqual = avx512.MatchUint16LessEqual
		matchUint16Greater = avx512.MatchUint16Greater
		matchUint16GreaterEqual = avx512.MatchUint16GreaterEqual
		matchUint16Between = avx512.MatchUint16Between

		// uint32
		matchUint32Equal = avx512.MatchUint32Equal
		matchUint32NotEqual = avx512.MatchUint32NotEqual
		matchUint32Less = avx512.MatchUint32Less
		matchUint32LessEqual = avx512.MatchUint32LessEqual
		matchUint32Greater = avx512.MatchUint32Greater
		matchUint32GreaterEqual = avx512.MatchUint32GreaterEqual
		matchUint32Between = avx512.MatchUint32Between

		// uint64
		matchUint64Equal = avx512.MatchUint64Equal
		matchUint64NotEqual = avx512.MatchUint64NotEqual
		matchUint64Less = avx512.MatchUint64Less
		matchUint64LessEqual = avx512.MatchUint64LessEqual
		matchUint64Greater = avx512.MatchUint64Greater
		matchUint64GreaterEqual = avx512.MatchUint64GreaterEqual
		matchUint64Between = avx512.MatchUint64Between

		// int16
		matchInt16Equal = avx512.MatchInt16Equal
		matchInt16NotEqual = avx512.MatchInt16NotEqual
		matchInt16Less = avx512.MatchInt16Less
		matchInt16LessEqual = avx512.MatchInt16LessEqual
		matchInt16Greater = avx512.MatchInt16Greater
		matchInt16GreaterEqual = avx512.MatchInt16GreaterEqual
		matchInt16Between = avx512.MatchInt16Between

		// int32
		matchInt32Equal = avx512.MatchInt32Equal
		matchInt32NotEqual = avx512.MatchInt32NotEqual
		matchInt32Less = avx512.MatchInt32Less
		matchInt32LessEqual = avx512.MatchInt32LessEqual
		matchInt32Greater = avx512.MatchInt32Greater
		matchInt32GreaterEqual = avx512.MatchInt32GreaterEqual
		matchInt32Between = avx512.MatchInt32Between

		// int64
		matchInt64Equal = avx512.MatchInt64Equal
		matchInt64NotEqual = avx512.MatchInt64NotEqual
		matchInt64Less = avx512.MatchInt64Less
		matchInt64LessEqual = avx512.MatchInt64LessEqual
		matchInt64Greater = avx512.MatchInt64Greater
		matchInt64GreaterEqual = avx512.MatchInt64GreaterEqual
		matchInt64Between = avx512.MatchInt64Between

		// float32
		matchFloat32Equal = avx512.MatchFloat32Equal
		matchFloat32NotEqual = avx512.MatchFloat32NotEqual
		matchFloat32Less = avx512.MatchFloat32Less
		matchFloat32LessEqual = avx512.MatchFloat32LessEqual
		matchFloat32Greater = avx512.MatchFloat32Greater
		matchFloat32GreaterEqual = avx512.MatchFloat32GreaterEqual
		matchFloat32Between = avx512.MatchFloat32Between

		// float64
		matchFloat64Equal = avx512.MatchFloat64Equal
		matchFloat64NotEqual = avx512.MatchFloat64NotEqual
		matchFloat64Less = avx512.MatchFloat64Less
		matchFloat64LessEqual = avx512.MatchFloat64LessEqual
		matchFloat64Greater = avx512.MatchFloat64Greater
		matchFloat64GreaterEqual = avx512.MatchFloat64GreaterEqual
		matchFloat64Between = avx512.MatchFloat64Between
	}

	if util.UseAVX512_BW {
		// uint8
		matchUint8Equal = avx512.MatchUint8Equal
		matchUint8NotEqual = avx512.MatchUint8NotEqual
		matchUint8Less = avx512.MatchUint8Less
		matchUint8LessEqual = avx512.MatchUint8LessEqual
		matchUint8Greater = avx512.MatchUint8Greater
		matchUint8GreaterEqual = avx512.MatchUint8GreaterEqual
		matchUint8Between = avx512.MatchUint8Between

		// int8
		matchInt8Equal = avx512.MatchInt8Equal
		matchInt8NotEqual = avx512.MatchInt8NotEqual
		matchInt8Less = avx512.MatchInt8Less
		matchInt8LessEqual = avx512.MatchInt8LessEqual
		matchInt8Greater = avx512.MatchInt8Greater
		matchInt8GreaterEqual = avx512.MatchInt8GreaterEqual
		matchInt8Between = avx512.MatchInt8Between
	}
}
