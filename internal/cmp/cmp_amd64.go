// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64
// +build amd64

package cmp

import (
	"blockwatch.cc/knoxdb/internal/cmp/avx2"
	"blockwatch.cc/knoxdb/internal/cmp/avx512"
	"blockwatch.cc/knoxdb/internal/cpu"
)

func init() {
	if cpu.UseAVX2 {
		// uint8
		Uint8Equal = avx2.Uint8Equal
		Uint8NotEqual = avx2.Uint8NotEqual
		Uint8Less = avx2.Uint8Less
		Uint8LessEqual = avx2.Uint8LessEqual
		Uint8Greater = avx2.Uint8Greater
		Uint8GreaterEqual = avx2.Uint8GreaterEqual
		Uint8Between = avx2.Uint8Between

		// uint16
		Uint16Equal = avx2.Uint16Equal
		Uint16NotEqual = avx2.Uint16NotEqual
		Uint16Less = avx2.Uint16Less
		Uint16LessEqual = avx2.Uint16LessEqual
		Uint16Greater = avx2.Uint16Greater
		Uint16GreaterEqual = avx2.Uint16GreaterEqual
		Uint16Between = avx2.Uint16Between

		// uint32
		Uint32Equal = avx2.Uint32Equal
		Uint32NotEqual = avx2.Uint32NotEqual
		Uint32Less = avx2.Uint32Less
		Uint32LessEqual = avx2.Uint32LessEqual
		Uint32Greater = avx2.Uint32Greater
		Uint32GreaterEqual = avx2.Uint32GreaterEqual
		Uint32Between = avx2.Uint32Between

		// uint64
		Uint64Equal = avx2.Uint64Equal
		Uint64NotEqual = avx2.Uint64NotEqual
		Uint64Less = avx2.Uint64Less
		Uint64LessEqual = avx2.Uint64LessEqual
		Uint64Greater = avx2.Uint64Greater
		Uint64GreaterEqual = avx2.Uint64GreaterEqual
		Uint64Between = avx2.Uint64Between

		// int8
		Int8Equal = avx2.Int8Equal
		Int8NotEqual = avx2.Int8NotEqual
		Int8Less = avx2.Int8Less
		Int8LessEqual = avx2.Int8LessEqual
		Int8Greater = avx2.Int8Greater
		Int8GreaterEqual = avx2.Int8GreaterEqual
		Int8Between = avx2.Int8Between

		// int16
		Int16Equal = avx2.Int16Equal
		Int16NotEqual = avx2.Int16NotEqual
		Int16Less = avx2.Int16Less
		Int16LessEqual = avx2.Int16LessEqual
		Int16Greater = avx2.Int16Greater
		Int16GreaterEqual = avx2.Int16GreaterEqual
		Int16Between = avx2.Int16Between

		// int32
		Int32Equal = avx2.Int32Equal
		Int32NotEqual = avx2.Int32NotEqual
		Int32Less = avx2.Int32Less
		Int32LessEqual = avx2.Int32LessEqual
		Int32Greater = avx2.Int32Greater
		Int32GreaterEqual = avx2.Int32GreaterEqual
		Int32Between = avx2.Int32Between

		// int64
		Int64Equal = avx2.Int64Equal
		Int64NotEqual = avx2.Int64NotEqual
		Int64Less = avx2.Int64Less
		Int64LessEqual = avx2.Int64LessEqual
		Int64Greater = avx2.Int64Greater
		Int64GreaterEqual = avx2.Int64GreaterEqual
		Int64Between = avx2.Int64Between

		// float32
		Float32Equal = avx2.Float32Equal
		Float32NotEqual = avx2.Float32NotEqual
		Float32Less = avx2.Float32Less
		Float32LessEqual = avx2.Float32LessEqual
		Float32Greater = avx2.Float32Greater
		Float32GreaterEqual = avx2.Float32GreaterEqual
		Float32Between = avx2.Float32Between

		// float64
		Float64Equal = avx2.Float64Equal
		Float64NotEqual = avx2.Float64NotEqual
		Float64Less = avx2.Float64Less
		Float64LessEqual = avx2.Float64LessEqual
		Float64Greater = avx2.Float64Greater
		Float64GreaterEqual = avx2.Float64GreaterEqual
		Float64Between = avx2.Float64Between

		// int128
		Int128Equal = avx2.Int128Equal
		Int128NotEqual = avx2.Int128NotEqual
		Int128Less = avx2.Int128Less
		Int128LessEqual = avx2.Int128LessEqual
		Int128Greater = avx2.Int128Greater
		Int128GreaterEqual = avx2.Int128GreaterEqual
		Int128Between = avx2.Int128Between

		Int128Equal = avx2.Int128Equal
		Int128NotEqual = avx2.Int128NotEqual
		Int128Less = avx2.Int128Less
		Int128LessEqual = avx2.Int128LessEqual
		Int128Greater = avx2.Int128Greater
		Int128GreaterEqual = avx2.Int128GreaterEqual
		Int128Between = avx2.Int128Between

		// int256
		Int256Equal = avx2.Int256Equal
		Int256NotEqual = avx2.Int256NotEqual
		Int256Less = avx2.Int256Less
		Int256LessEqual = avx2.Int256LessEqual
		Int256Greater = avx2.Int256Greater
		Int256GreaterEqual = avx2.Int256GreaterEqual
		Int256Between = avx2.Int256Between
	}

	if cpu.UseAVX512_F {
		// uint16
		Uint16Equal = avx512.Uint16Equal
		Uint16NotEqual = avx512.Uint16NotEqual
		Uint16Less = avx512.Uint16Less
		Uint16LessEqual = avx512.Uint16LessEqual
		Uint16Greater = avx512.Uint16Greater
		Uint16GreaterEqual = avx512.Uint16GreaterEqual
		Uint16Between = avx512.Uint16Between

		// uint32
		Uint32Equal = avx512.Uint32Equal
		Uint32NotEqual = avx512.Uint32NotEqual
		Uint32Less = avx512.Uint32Less
		Uint32LessEqual = avx512.Uint32LessEqual
		Uint32Greater = avx512.Uint32Greater
		Uint32GreaterEqual = avx512.Uint32GreaterEqual
		Uint32Between = avx512.Uint32Between

		// uint64
		Uint64Equal = avx512.Uint64Equal
		Uint64NotEqual = avx512.Uint64NotEqual
		Uint64Less = avx512.Uint64Less
		Uint64LessEqual = avx512.Uint64LessEqual
		Uint64Greater = avx512.Uint64Greater
		Uint64GreaterEqual = avx512.Uint64GreaterEqual
		Uint64Between = avx512.Uint64Between

		// int16
		Int16Equal = avx512.Int16Equal
		Int16NotEqual = avx512.Int16NotEqual
		Int16Less = avx512.Int16Less
		Int16LessEqual = avx512.Int16LessEqual
		Int16Greater = avx512.Int16Greater
		Int16GreaterEqual = avx512.Int16GreaterEqual
		Int16Between = avx512.Int16Between

		// int32
		Int32Equal = avx512.Int32Equal
		Int32NotEqual = avx512.Int32NotEqual
		Int32Less = avx512.Int32Less
		Int32LessEqual = avx512.Int32LessEqual
		Int32Greater = avx512.Int32Greater
		Int32GreaterEqual = avx512.Int32GreaterEqual
		Int32Between = avx512.Int32Between

		// int64
		Int64Equal = avx512.Int64Equal
		Int64NotEqual = avx512.Int64NotEqual
		Int64Less = avx512.Int64Less
		Int64LessEqual = avx512.Int64LessEqual
		Int64Greater = avx512.Int64Greater
		Int64GreaterEqual = avx512.Int64GreaterEqual
		Int64Between = avx512.Int64Between

		// float32
		Float32Equal = avx512.Float32Equal
		Float32NotEqual = avx512.Float32NotEqual
		Float32Less = avx512.Float32Less
		Float32LessEqual = avx512.Float32LessEqual
		Float32Greater = avx512.Float32Greater
		Float32GreaterEqual = avx512.Float32GreaterEqual
		Float32Between = avx512.Float32Between

		// float64
		Float64Equal = avx512.Float64Equal
		Float64NotEqual = avx512.Float64NotEqual
		Float64Less = avx512.Float64Less
		Float64LessEqual = avx512.Float64LessEqual
		Float64Greater = avx512.Float64Greater
		Float64GreaterEqual = avx512.Float64GreaterEqual
		Float64Between = avx512.Float64Between
	}

	if cpu.UseAVX512_BW {
		// uint8
		Uint8Equal = avx512.Uint8Equal
		Uint8NotEqual = avx512.Uint8NotEqual
		Uint8Less = avx512.Uint8Less
		Uint8LessEqual = avx512.Uint8LessEqual
		Uint8Greater = avx512.Uint8Greater
		Uint8GreaterEqual = avx512.Uint8GreaterEqual
		Uint8Between = avx512.Uint8Between

		// int8
		Int8Equal = avx512.Int8Equal
		Int8NotEqual = avx512.Int8NotEqual
		Int8Less = avx512.Int8Less
		Int8LessEqual = avx512.Int8LessEqual
		Int8Greater = avx512.Int8Greater
		Int8GreaterEqual = avx512.Int8GreaterEqual
		Int8Between = avx512.Int8Between
	}
}
