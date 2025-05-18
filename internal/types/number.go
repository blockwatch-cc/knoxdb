// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"math"
	"math/bits"

	"blockwatch.cc/knoxdb/internal/bitset"
)

// we use only types with strict cross-platform width
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

type Bitset = bitset.Bitset

type NumberMatcher[T Number] interface {
	MatchEqual(val T, bits, mask *Bitset)
	MatchNotEqual(val T, bits, mask *Bitset)
	MatchLess(val T, bits, mask *Bitset)
	MatchLessEqual(val T, bits, mask *Bitset)
	MatchGreater(val T, bits, mask *Bitset)
	MatchGreaterEqual(val T, bits, mask *Bitset)
	MatchBetween(a, b T, bits, mask *Bitset)

	// Int: *xorar.Bitmap
	// Float: []float64, []float32
	MatchInSet(s any, bits, mask *Bitset)
	MatchNotInSet(s any, bits, mask *Bitset)
}

type NumberAccessor[T Number] interface {
	Get(int) T
	AppendTo([]uint32, []T) []T
}

func IsSigned[T Number]() bool {
	// Check if -1 is less than 0 in the type T
	// For signed types, this is true (e.g., -1 < 0)
	// For unsigned types, -1 wraps to MaxValue (e.g., 0xFF...FF), so it's false
	return T(0)-T(1) < T(0)
}

func IsInteger[T Number]() bool {
	switch any(T(0)).(type) {
	case float64:
		return false
	case float32:
		return false
	default:
		return true
	}
}

func Log2Range[T Integer](minv, maxv T) int {
	if IsSigned[T]() {
		return bits.Len64(uint64(int64(maxv) - int64(minv)))
	} else {
		return bits.Len64(uint64(maxv - minv))
	}
}

func MinVal[T Number]() T {
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
	case float32:
		return any(float32(-math.MaxFloat32)).(T)
	case float64:
		return any(float64(-math.MaxFloat64)).(T)
	default:
		return 0
	}
}

func MaxVal[T Number]() T {
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
	case float32:
		return any(float32(math.MaxFloat32)).(T)
	case float64:
		return any(float64(math.MaxFloat64)).(T)
	default:
		return 0
	}
}

func Cast[T Integer](val any) (t T, ok bool) {
	ok = true
	switch v := val.(type) {
	case int:
		t = T(v)
	case int64:
		t = T(v)
	case int32:
		t = T(v)
	case int16:
		t = T(v)
	case int8:
		t = T(v)
	case uint:
		t = T(v)
	case uint64:
		t = T(v)
	case uint32:
		t = T(v)
	case uint16:
		t = T(v)
	case uint8:
		t = T(v)
	default:
		ok = false
	}
	return
}
