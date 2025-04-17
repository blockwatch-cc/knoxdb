// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

var (
	// uint8
	Uint8Equal        = cmp_eq[uint8]
	Uint8NotEqual     = cmp_ne[uint8]
	Uint8Less         = cmp_lt[uint8]
	Uint8LessEqual    = cmp_le[uint8]
	Uint8Greater      = cmp_gt[uint8]
	Uint8GreaterEqual = cmp_ge[uint8]
	Uint8Between      = cmp_bu[uint8]

	// uint16
	Uint16Equal        = cmp_eq[uint16]
	Uint16NotEqual     = cmp_ne[uint16]
	Uint16Less         = cmp_lt[uint16]
	Uint16LessEqual    = cmp_le[uint16]
	Uint16Greater      = cmp_gt[uint16]
	Uint16GreaterEqual = cmp_ge[uint16]
	Uint16Between      = cmp_bu[uint16]

	// uint32
	Uint32Equal        = cmp_eq[uint32]
	Uint32NotEqual     = cmp_ne[uint32]
	Uint32Less         = cmp_lt[uint32]
	Uint32LessEqual    = cmp_le[uint32]
	Uint32Greater      = cmp_gt[uint32]
	Uint32GreaterEqual = cmp_ge[uint32]
	Uint32Between      = cmp_bu[uint32]

	// uint64
	Uint64Equal        = cmp_eq[uint64]
	Uint64NotEqual     = cmp_ne[uint64]
	Uint64Less         = cmp_lt[uint64]
	Uint64LessEqual    = cmp_le[uint64]
	Uint64Greater      = cmp_gt[uint64]
	Uint64GreaterEqual = cmp_ge[uint64]
	Uint64Between      = cmp_bu[uint64]

	// int8
	Int8Equal        = cmp_eq[int8]
	Int8NotEqual     = cmp_ne[int8]
	Int8Less         = cmp_lt[int8]
	Int8LessEqual    = cmp_le[int8]
	Int8Greater      = cmp_gt[int8]
	Int8GreaterEqual = cmp_ge[int8]
	Int8Between      = cmp_bs[int8]

	// int16
	Int16Equal        = cmp_eq[int16]
	Int16NotEqual     = cmp_ne[int16]
	Int16Less         = cmp_lt[int16]
	Int16LessEqual    = cmp_le[int16]
	Int16Greater      = cmp_gt[int16]
	Int16GreaterEqual = cmp_ge[int16]
	Int16Between      = cmp_bs[int16]

	// int32
	Int32Equal        = cmp_eq[int32]
	Int32NotEqual     = cmp_ne[int32]
	Int32Less         = cmp_lt[int32]
	Int32LessEqual    = cmp_le[int32]
	Int32Greater      = cmp_gt[int32]
	Int32GreaterEqual = cmp_ge[int32]
	Int32Between      = cmp_bs[int32]

	// int64
	Int64Equal        = cmp_eq[int64]
	Int64NotEqual     = cmp_ne[int64]
	Int64Less         = cmp_lt[int64]
	Int64LessEqual    = cmp_le[int64]
	Int64Greater      = cmp_gt[int64]
	Int64GreaterEqual = cmp_ge[int64]
	Int64Between      = cmp_bs[int64]

	// float32
	Float32Equal        = cmp_eq_f[float32]
	Float32NotEqual     = cmp_ne_f[float32]
	Float32Less         = cmp_lt_f[float32]
	Float32LessEqual    = cmp_le_f[float32]
	Float32Greater      = cmp_gt_f[float32]
	Float32GreaterEqual = cmp_ge_f[float32]
	Float32Between      = cmp_bw_f[float32]

	// float64
	Float64Equal        = cmp_eq_f[float64]
	Float64NotEqual     = cmp_ne_f[float64]
	Float64Less         = cmp_lt_f[float64]
	Float64LessEqual    = cmp_le_f[float64]
	Float64Greater      = cmp_gt_f[float64]
	Float64GreaterEqual = cmp_ge_f[float64]
	Float64Between      = cmp_bw_f[float64]

	// bytes
	BytesEqual        = cmp_bytes_eq
	BytesNotEqual     = cmp_bytes_ne
	BytesLess         = cmp_bytes_lt
	BytesLessEqual    = cmp_bytes_le
	BytesGreater      = cmp_bytes_gt
	BytesGreaterEqual = cmp_bytes_ge
	BytesBetween      = cmp_bytes_bw

	// int128
	Int128Equal        = cmp_i128_eq
	Int128NotEqual     = cmp_i128_ne
	Int128Less         = cmp_i128_lt
	Int128LessEqual    = cmp_i128_le
	Int128Greater      = cmp_i128_gt
	Int128GreaterEqual = cmp_i128_ge
	Int128Between      = cmp_i128_bw

	// int256
	Int256Equal        = cmp_i256_eq
	Int256NotEqual     = cmp_i256_ne
	Int256Less         = cmp_i256_lt
	Int256LessEqual    = cmp_i256_le
	Int256Greater      = cmp_i256_gt
	Int256GreaterEqual = cmp_i256_ge
	Int256Between      = cmp_i256_bw
)
