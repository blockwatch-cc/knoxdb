// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"math/bits"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
)

var (
	BitLen64 = bits.Len64
)

func SizeOf[T types.Number]() int {
	return int(unsafe.Sizeof(T(0)))
}

func BlockType[T types.Number]() types.BlockType {
	switch any(T(0)).(type) {
	case uint64:
		return types.BlockUint64
	case int64:
		return types.BlockInt64
	case uint32:
		return types.BlockUint32
	case int32:
		return types.BlockInt32
	case uint16:
		return types.BlockUint16
	case int16:
		return types.BlockInt16
	case uint8:
		return types.BlockUint8
	case int8:
		return types.BlockInt8
	case float64:
		return types.BlockFloat64
	case float32:
		return types.BlockFloat32
	default:
		return types.BlockUint64
	}
}

func TypeName[T types.Number]() string {
	switch any(T(0)).(type) {
	case uint64:
		return "u64"
	case uint32:
		return "u32"
	case uint16:
		return "u16"
	case uint8:
		return "u8"
	case int64:
		return "i64"
	case int32:
		return "i32"
	case int16:
		return "i16"
	case int8:
		return "i8"
	case float64:
		return "f64"
	case float32:
		return "f32"
	default:
		return fmt.Sprintf("%T", T(0))
	}
}
