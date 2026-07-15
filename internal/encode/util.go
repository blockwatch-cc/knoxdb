// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"encoding/binary"
	"fmt"
	"math/bits"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

var (
	BitLen64 = bits.Len64
)

type (
	Bitset = bitset.Bitset

	NumberMatchFunc[T types.Number]      func(src []T, val T, bits []byte) int64
	NumberRangeMatchFunc[T types.Number] func(src []T, from, to T, bits []byte) int64

	I128MatchFunc      func(src *num.Int128Stride, val num.Int128, bits, mask []byte) int64
	I128RangeMatchFunc func(src *num.Int128Stride, a, b num.Int128, bits, mask []byte) int64

	I256MatchFunc      func(src *num.Int256Stride, val num.Int256, bits, mask []byte) int64
	I256RangeMatchFunc func(src *num.Int256Stride, a, b num.Int256, bits, mask []byte) int64
)

func AsBlockType[T types.Number]() types.BlockType {
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

func UvarintLen[T types.Integer | int](v T) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], uint64(v))
}

func log2Range[T types.Integer](minv, maxv T) int {
	isSigned := T(0)-T(1) < T(0)
	if isSigned {
		return bits.Len64(uint64(int64(maxv) - int64(minv)))
	} else {
		return bits.Len64(uint64(maxv - minv))
	}
}
