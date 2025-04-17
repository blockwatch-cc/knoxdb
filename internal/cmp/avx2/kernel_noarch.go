// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package avx2

import (
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

type nop[T types.Number] func(src []T, val T, bits []byte) int64
type nop2[T types.Number] func(src []T, a, b T, bits []byte) int64
type nop128 func(src num.Int128Stride, val num.Int128, bits, mask []byte) int64
type nop128_2 func(src num.Int128Stride, a, b num.Int128, bits, mask []byte) int64
type nop256 func(src num.Int256Stride, val num.Int256, bits, mask []byte) int64
type nop256_2 func(src num.Int256Stride, a, b num.Int256, bits, mask []byte) int64

var (
	// uint8
	Uint8Equal        nop[uint8]  = nil
	Uint8NotEqual     nop[uint8]  = nil
	Uint8Less         nop[uint8]  = nil
	Uint8LessEqual    nop[uint8]  = nil
	Uint8Greater      nop[uint8]  = nil
	Uint8GreaterEqual nop[uint8]  = nil
	Uint8Between      nop2[uint8] = nil

	// uint16
	Uint16Equal        nop[uint16]  = nil
	Uint16NotEqual     nop[uint16]  = nil
	Uint16Less         nop[uint16]  = nil
	Uint16LessEqual    nop[uint16]  = nil
	Uint16Greater      nop[uint16]  = nil
	Uint16GreaterEqual nop[uint16]  = nil
	Uint16Between      nop2[uint16] = nil

	// uint32
	Uint32Equal        nop[uint32]  = nil
	Uint32NotEqual     nop[uint32]  = nil
	Uint32Less         nop[uint32]  = nil
	Uint32LessEqual    nop[uint32]  = nil
	Uint32Greater      nop[uint32]  = nil
	Uint32GreaterEqual nop[uint32]  = nil
	Uint32Between      nop2[uint32] = nil

	// uint64
	Uint64Equal        nop[uint64]  = nil
	Uint64NotEqual     nop[uint64]  = nil
	Uint64Less         nop[uint64]  = nil
	Uint64LessEqual    nop[uint64]  = nil
	Uint64Greater      nop[uint64]  = nil
	Uint64GreaterEqual nop[uint64]  = nil
	Uint64Between      nop2[uint64] = nil

	// int8
	Int8Equal        nop[int8]  = nil
	Int8NotEqual     nop[int8]  = nil
	Int8Less         nop[int8]  = nil
	Int8LessEqual    nop[int8]  = nil
	Int8Greater      nop[int8]  = nil
	Int8GreaterEqual nop[int8]  = nil
	Int8Between      nop2[int8] = nil

	// int16
	Int16Equal        nop[int16]  = nil
	Int16NotEqual     nop[int16]  = nil
	Int16Less         nop[int16]  = nil
	Int16LessEqual    nop[int16]  = nil
	Int16Greater      nop[int16]  = nil
	Int16GreaterEqual nop[int16]  = nil
	Int16Between      nop2[int16] = nil

	// int32
	Int32Equal        nop[int32]  = nil
	Int32NotEqual     nop[int32]  = nil
	Int32Less         nop[int32]  = nil
	Int32LessEqual    nop[int32]  = nil
	Int32Greater      nop[int32]  = nil
	Int32GreaterEqual nop[int32]  = nil
	Int32Between      nop2[int32] = nil

	// int64
	Int64Equal        nop[int64]  = nil
	Int64NotEqual     nop[int64]  = nil
	Int64Less         nop[int64]  = nil
	Int64LessEqual    nop[int64]  = nil
	Int64Greater      nop[int64]  = nil
	Int64GreaterEqual nop[int64]  = nil
	Int64Between      nop2[int64] = nil

	// float32
	Float32Equal        nop[float32]  = nil
	Float32NotEqual     nop[float32]  = nil
	Float32Less         nop[float32]  = nil
	Float32LessEqual    nop[float32]  = nil
	Float32Greater      nop[float32]  = nil
	Float32GreaterEqual nop[float32]  = nil
	Float32Between      nop2[float32] = nil

	// float64
	Float64Equal        nop[float64]  = nil
	Float64NotEqual     nop[float64]  = nil
	Float64Less         nop[float64]  = nil
	Float64LessEqual    nop[float64]  = nil
	Float64Greater      nop[float64]  = nil
	Float64GreaterEqual nop[float64]  = nil
	Float64Between      nop2[float64] = nil

	// int128
	Int128Equal        nop128   = nil
	Int128NotEqual     nop128   = nil
	Int128Less         nop128   = nil
	Int128LessEqual    nop128   = nil
	Int128Greater      nop128   = nil
	Int128GreaterEqual nop128   = nil
	Int128Between      nop128_2 = nil

	// int256
	Int256Equal        nop256   = nil
	Int256NotEqual     nop256   = nil
	Int256Less         nop256   = nil
	Int256LessEqual    nop256   = nil
	Int256Greater      nop256   = nil
	Int256GreaterEqual nop256   = nil
	Int256Between      nop256_2 = nil
)
