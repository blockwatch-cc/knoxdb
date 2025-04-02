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

func IsSigned[T Integer]() bool {
	// Check if -1 is less than 0 in the type T
	// For signed types, this is true (e.g., -1 < 0)
	// For unsigned types, -1 wraps to MaxValue (e.g., 0xFF...FF), so it's false
	return T(0)-T(1) < T(0)
}

func MinVal[T Integer]() T {
	switch any(T(0)).(type) {
	case int64:
		return any(int64(math.MinInt64)).(T)
	case int32:
		return any(int32(math.MinInt32)).(T)
	case int16:
		return any(int16(math.MinInt16)).(T)
	case int8:
		return any(int8(math.MinInt8)).(T)
	case uint64:
		return 0
	case uint32:
		return 0
	case uint16:
		return 0
	case uint8:
		return 0
	default:
		return 0
	}
}

func MaxVal[T Integer]() T {
	switch any(T(0)).(type) {
	case int64:
		return any(int64(math.MaxInt64)).(T)
	case int32:
		return any(int32(math.MaxInt32)).(T)
	case int16:
		return any(int16(math.MaxInt16)).(T)
	case int8:
		return any(int8(math.MaxInt8)).(T)
	case uint64:
		return any(uint64(math.MaxUint64)).(T)
	case uint32:
		return any(uint32(math.MaxUint32)).(T)
	case uint16:
		return any(uint16(math.MaxUint16)).(T)
	case uint8:
		return any(uint8(math.MaxUint8)).(T)
	default:
		return 0
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
