// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package avx2

import (
	"blockwatch.cc/knoxdb/internal/cmp/generic"
)

var (
	// uint8
	MatchUint8Equal        = generic.MatchEqual[uint8]
	MatchUint8NotEqual     = generic.MatchNotEqual[uint8]
	MatchUint8Less         = generic.MatchLess[uint8]
	MatchUint8LessEqual    = generic.MatchLessEqual[uint8]
	MatchUint8Greater      = generic.MatchGreater[uint8]
	MatchUint8GreaterEqual = generic.MatchGreaterEqual[uint8]
	MatchUint8Between      = generic.MatchBetween[uint8]

	// uint16
	MatchUint16Equal        = generic.MatchEqual[uint16]
	MatchUint16NotEqual     = generic.MatchNotEqual[uint16]
	MatchUint16Less         = generic.MatchLess[uint16]
	MatchUint16LessEqual    = generic.MatchLessEqual[uint16]
	MatchUint16Greater      = generic.MatchGreater[uint16]
	MatchUint16GreaterEqual = generic.MatchGreaterEqual[uint16]
	MatchUint16Between      = generic.MatchBetween[uint16]

	// uint32
	MatchUint32Equal        = generic.MatchEqual[uint32]
	MatchUint32NotEqual     = generic.MatchNotEqual[uint32]
	MatchUint32Less         = generic.MatchLess[uint32]
	MatchUint32LessEqual    = generic.MatchLessEqual[uint32]
	MatchUint32Greater      = generic.MatchGreater[uint32]
	MatchUint32GreaterEqual = generic.MatchGreaterEqual[uint32]
	MatchUint32Between      = generic.MatchBetween[uint32]

	// uint64
	MatchUint64Equal        = generic.MatchEqual[uint64]
	MatchUint64NotEqual     = generic.MatchNotEqual[uint64]
	MatchUint64Less         = generic.MatchLess[uint64]
	MatchUint64LessEqual    = generic.MatchLessEqual[uint64]
	MatchUint64Greater      = generic.MatchGreater[uint64]
	MatchUint64GreaterEqual = generic.MatchGreaterEqual[uint64]
	MatchUint64Between      = generic.MatchBetween[uint64]

	// int8
	MatchInt8Equal        = generic.MatchEqual[int8]
	MatchInt8NotEqual     = generic.MatchNotEqual[int8]
	MatchInt8Less         = generic.MatchLess[int8]
	MatchInt8LessEqual    = generic.MatchLessEqual[int8]
	MatchInt8Greater      = generic.MatchGreater[int8]
	MatchInt8GreaterEqual = generic.MatchGreaterEqual[int8]
	MatchInt8Between      = generic.MatchBetween[int8]

	// int16
	MatchInt16Equal        = generic.MatchEqual[int16]
	MatchInt16NotEqual     = generic.MatchNotEqual[int16]
	MatchInt16Less         = generic.MatchLess[int16]
	MatchInt16LessEqual    = generic.MatchLessEqual[int16]
	MatchInt16Greater      = generic.MatchGreater[int16]
	MatchInt16GreaterEqual = generic.MatchGreaterEqual[int16]
	MatchInt16Between      = generic.MatchBetween[int16]

	// int32
	MatchInt32Equal        = generic.MatchEqual[int32]
	MatchInt32NotEqual     = generic.MatchNotEqual[int32]
	MatchInt32Less         = generic.MatchLess[int32]
	MatchInt32LessEqual    = generic.MatchLessEqual[int32]
	MatchInt32Greater      = generic.MatchGreater[int32]
	MatchInt32GreaterEqual = generic.MatchGreaterEqual[int32]
	MatchInt32Between      = generic.MatchBetween[int32]

	// int64
	MatchInt64Equal        = generic.MatchEqual[int64]
	MatchInt64NotEqual     = generic.MatchNotEqual[int64]
	MatchInt64Less         = generic.MatchLess[int64]
	MatchInt64LessEqual    = generic.MatchLessEqual[int64]
	MatchInt64Greater      = generic.MatchGreater[int64]
	MatchInt64GreaterEqual = generic.MatchGreaterEqual[int64]
	MatchInt64Between      = generic.MatchBetween[int64]

	// float32
	MatchFloat32Equal        = generic.MatchFloatEqual[float32]
	MatchFloat32NotEqual     = generic.MatchFloatNotEqual[float32]
	MatchFloat32Less         = generic.MatchFloatLess[float32]
	MatchFloat32LessEqual    = generic.MatchFloatLessEqual[float32]
	MatchFloat32Greater      = generic.MatchFloatGreater[float32]
	MatchFloat32GreaterEqual = generic.MatchFloatGreaterEqual[float32]
	MatchFloat32Between      = generic.MatchFloatBetween[float32]

	// float64
	MatchFloat64Equal        = generic.MatchFloatEqual[float64]
	MatchFloat64NotEqual     = generic.MatchFloatNotEqual[float64]
	MatchFloat64Less         = generic.MatchFloatLess[float64]
	MatchFloat64LessEqual    = generic.MatchFloatLessEqual[float64]
	MatchFloat64Greater      = generic.MatchFloatGreater[float64]
	MatchFloat64GreaterEqual = generic.MatchFloatGreaterEqual[float64]
	MatchFloat64Between      = generic.MatchFloatBetween[float64]

	// bytes
	MatchBytesEqual        = generic.MatchBytesEqual
	MatchBytesNotEqual     = generic.MatchBytesNotEqual
	MatchBytesLess         = generic.MatchBytesLess
	MatchBytesLessEqual    = generic.MatchBytesLessEqual
	MatchBytesGreater      = generic.MatchBytesGreater
	MatchBytesGreaterEqual = generic.MatchBytesGreaterEqual
	MatchBytesBetween      = generic.MatchBytesBetween

	// string
	// MatchStringsEqual        = generic.MatchStringsEqual
	// MatchStringsNotEqual     = generic.MatchStringsNotEqual
	// MatchStringsLess         = generic.MatchStringsLess
	// MatchStringsLessEqual    = generic.MatchStringsLessEqual
	// MatchStringsGreater      = generic.MatchStringsGreater
	// MatchStringsGreaterEqual = generic.MatchStringsGreaterEqual
	// MatchStringsBetween      = generic.MatchStringsBetween

	// int128
	MatchInt128Equal        = generic.MatchInt128Equal
	MatchInt128NotEqual     = generic.MatchInt128NotEqual
	MatchInt128Less         = generic.MatchInt128Less
	MatchInt128LessEqual    = generic.MatchInt128LessEqual
	MatchInt128Greater      = generic.MatchInt128Greater
	MatchInt128GreaterEqual = generic.MatchInt128GreaterEqual
	MatchInt128Between      = generic.MatchInt128Between

	// int256
	MatchInt256Equal        = generic.MatchInt256Equal
	MatchInt256NotEqual     = generic.MatchInt256NotEqual
	MatchInt256Less         = generic.MatchInt256Less
	MatchInt256LessEqual    = generic.MatchInt256LessEqual
	MatchInt256Greater      = generic.MatchInt256Greater
	MatchInt256GreaterEqual = generic.MatchInt256GreaterEqual
	MatchInt256Between      = generic.MatchInt256Between
)
