// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"math"

	"blockwatch.cc/knoxdb/internal/bitset"
)

type Integer interface {
	int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8
}

type Signed interface {
	int64 | int32 | int16 | int8
}

type Unsigned interface {
	uint64 | uint32 | uint16 | uint8
}

type Float interface {
	float64 | float32
}

type Number interface {
	Integer | Float
}

func MinVal[T Integer]() any {
	switch any(T(0)).(type) {
	case int64:
		return int64(math.MinInt64)
	case int32:
		return int32(math.MinInt32)
	case int16:
		return int16(math.MinInt16)
	case int8:
		return int8(math.MinInt8)
	case uint64:
		return uint64(0)
	case uint32:
		return uint32(0)
	case uint16:
		return uint16(0)
	case uint8:
		return uint8(0)
	default:
		return nil
	}
}

func MaxVal[T Integer]() any {
	switch any(T(0)).(type) {
	case int64:
		return int64(math.MaxInt64)
	case int32:
		return int32(math.MaxInt32)
	case int16:
		return int16(math.MaxInt16)
	case int8:
		return int8(math.MaxInt8)
	case uint64:
		return uint64(math.MaxUint64)
	case uint32:
		return uint32(math.MaxUint32)
	case uint16:
		return uint16(math.MaxUint16)
	case uint8:
		return uint8(math.MaxUint8)
	default:
		return nil
	}
}

type Bitset = bitset.Bitset

type NumberMatcher[T Number] interface {
	MatchEqual(val T, bits, mask *Bitset) *Bitset
	MatchNotEqual(val T, bits, mask *Bitset) *Bitset
	MatchLess(val T, bits, mask *Bitset) *Bitset
	MatchLessEqual(val T, bits, mask *Bitset) *Bitset
	MatchGreater(val T, bits, mask *Bitset) *Bitset
	MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset
	MatchBetween(a, b T, bits, mask *Bitset) *Bitset

	// Int: *xorar.Bitmap
	// Float: []float64, []float32
	MatchSet(s any, bits, mask *Bitset) *Bitset
	MatchNotSet(s any, bits, mask *Bitset) *Bitset
}

type NumberAccessor[T Number] interface {
	Get(int) T
	AppendTo([]uint32, []T) []T
}
