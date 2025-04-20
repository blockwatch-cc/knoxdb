// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
)

type (
	packFunc   func(unsafe.Pointer, uint64) uint64
	unpackFunc func(uint64, unsafe.Pointer)
)

var (
	// min-FOR fusion encode kernels
	pack_for_u64 = makePackSelector[uint64](true)
	pack_for_u32 = makePackSelector[uint32](true)
	pack_for_u16 = makePackSelector[uint16](true)
	pack_for_u8  = makePackSelector[uint8](true)
	pack_for_i64 = makePackSelector[int64](true)
	pack_for_i32 = makePackSelector[int32](true)
	pack_for_i16 = makePackSelector[int16](true)
	pack_for_i8  = makePackSelector[int8](true)

	// simple encode kernels
	pack_u64 = makePackSelector[uint64](false)
	pack_u32 = makePackSelector[uint32](false)
	pack_u16 = makePackSelector[uint16](false)
	pack_u8  = makePackSelector[uint8](false)
	pack_i64 = makePackSelector[int64](false)
	pack_i32 = makePackSelector[int32](false)
	pack_i16 = makePackSelector[int16](false)
	pack_i8  = makePackSelector[int8](false)

	// simple decode kernels
	unpack_u64 = makeUnpackSelector[uint64]()
	unpack_u32 = makeUnpackSelector[uint32]()
	unpack_u16 = makeUnpackSelector[uint16]()
	unpack_u8  = makeUnpackSelector[uint8]()
)

func packSelector[T types.Integer](minv T) *[16]packFunc {
	switch any(T(0)).(type) {
	case uint64:
		if minv == 0 {
			return &pack_u64
		} else {
			return &pack_for_u64
		}
	case uint32:
		if minv == 0 {
			return &pack_u32
		} else {
			return &pack_for_u32
		}
	case uint16:
		if minv == 0 {
			return &pack_u16
		} else {
			return &pack_for_u16
		}
	case uint8:
		if minv == 0 {
			return &pack_u8
		} else {
			return &pack_for_u8
		}
	case int64:
		if minv == 0 {
			return &pack_i64
		} else {
			return &pack_for_i64
		}
	case int32:
		if minv == 0 {
			return &pack_i32
		} else {
			return &pack_for_i32
		}
	case int16:
		if minv == 0 {
			return &pack_i16
		} else {
			return &pack_for_i16
		}
	case int8:
		if minv == 0 {
			return &pack_i8
		} else {
			return &pack_for_i8
		}
	default:
		return nil
	}
}

// unpack currently does not support min-FOR reversal and therefore
// we only create unsigned kernels
func unpackSelector[T types.Integer]() *[16]unpackFunc {
	switch any(T(0)).(type) {
	case uint64:
		return &unpack_u64
	case uint32:
		return &unpack_u32
	case uint16:
		return &unpack_u16
	case uint8:
		return &unpack_u8
	case int64:
		return &unpack_u64
	case int32:
		return &unpack_u32
	case int16:
		return &unpack_u16
	case int8:
		return &unpack_u8
	default:
		return nil
	}
}

func makePackSelector[T types.Integer](withMinFor bool) [16]packFunc {
	if withMinFor {
		return [16]packFunc{
			pack_for_zero[T],
			pack_for_one[T],
			pack_for_60[T],
			pack_for_30[T],
			pack_for_20[T],
			pack_for_15[T],
			pack_for_12[T],
			pack_for_10[T],
			pack_for_8[T],
			pack_for_7[T],
			pack_for_6[T],
			pack_for_5[T],
			pack_for_4[T],
			pack_for_3[T],
			pack_for_2[T],
			pack_for_1[T],
		}
	}
	return [16]packFunc{
		pack_zero[T],
		pack_one[T],
		pack_60[T],
		pack_30[T],
		pack_20[T],
		pack_15[T],
		pack_12[T],
		pack_10[T],
		pack_8[T],
		pack_7[T],
		pack_6[T],
		pack_5[T],
		pack_4[T],
		pack_3[T],
		pack_2[T],
		pack_1[T],
	}
}

func makeUnpackSelector[T types.Integer]() [16]unpackFunc {
	return [16]unpackFunc{
		unpack_zero[T],
		unpack_one[T],
		unpack_60[T],
		unpack_30[T],
		unpack_20[T],
		unpack_15[T],
		unpack_12[T],
		unpack_10[T],
		unpack_8[T],
		unpack_7[T],
		unpack_6[T],
		unpack_5[T],
		unpack_4[T],
		unpack_3[T],
		unpack_2[T],
		unpack_1[T],
	}
}
