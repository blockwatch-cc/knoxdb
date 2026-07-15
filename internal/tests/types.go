// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"math"
	"math/bits"

	"blockwatch.cc/knoxdb/internal/types"
)

var (
	BlockTypes = types.BlockTypes

	FieldTypes = [...]types.FieldType{
		types.BlockBool:    types.FT_BOOL,
		types.BlockBytes:   types.FT_BYTES,
		types.BlockInt8:    types.FT_I8,
		types.BlockInt16:   types.FT_I16,
		types.BlockInt32:   types.FT_I32,
		types.BlockInt64:   types.FT_I64,
		types.BlockInt128:  types.FT_I128,
		types.BlockInt256:  types.FT_I256,
		types.BlockUint8:   types.FT_U8,
		types.BlockUint16:  types.FT_U16,
		types.BlockUint32:  types.FT_U32,
		types.BlockUint64:  types.FT_U64,
		types.BlockFloat32: types.FT_F32,
		types.BlockFloat64: types.FT_F64,
	}
)

func MinVal[T types.Number]() T {
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

func MaxVal[T types.Number]() T {
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

func Log2Range[T types.Integer](minv, maxv T) int {
	isSigned := T(0)-T(1) < T(0)
	if isSigned {
		return bits.Len64(uint64(int64(maxv) - int64(minv)))
	} else {
		return bits.Len64(uint64(maxv - minv))
	}
}

func IsSigned[T types.Number]() bool {
	// Check if -1 is less than 0 in the type T
	// For signed types, this is true (e.g., -1 < 0)
	// For unsigned types, -1 wraps to MaxValue (e.g., 0xFF...FF), so it's false
	return T(0)-T(1) < T(0)
}
