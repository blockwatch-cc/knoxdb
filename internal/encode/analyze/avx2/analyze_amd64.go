// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64
// +build amd64

package avx2

import (
	"blockwatch.cc/knoxdb/internal/types"
)

type Context[T types.Number] struct {
	Min     T      // vector minimum
	Max     T      // vector maximum
	Delta   T      // common delta between vector values
	NumRuns uint32 // vector runs
	_       uint32 // padding
}

// ASM imports

//go:noescape
func analyze_i64_avx2(vals *int64, ctx *Context[int64], len int)

//go:noescape
func analyze_u64_avx2(vals *uint64, ctx *Context[uint64], len int)

//go:noescape
func analyze_i32_avx2(vals *int32, ctx *Context[int32], len int)

//go:noescape
func analyze_u32_avx2(vals *uint32, ctx *Context[uint32], len int)

//go:noescape
func analyze_i16_avx2(vals *int16, ctx *Context[int16], len int)

//go:noescape
func analyze_u16_avx2(vals *uint16, ctx *Context[uint16], len int)

//go:noescape
func analyze_i8_avx2(vals *int8, ctx *Context[int8], len int)

//go:noescape
func analyze_u8_avx2(vals *uint8, ctx *Context[uint8], len int)

//go:noescape
func analyze_f64_avx2(vals *float64, ctx *Context[float64], len int)

//go:noescape
func analyze_f32_avx2(vals *float32, ctx *Context[float32], len int)

// Go exports

func AnalyzeInt64(vals []int64) (int64, int64, int64, int) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[int64]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	analyze_i64_avx2(&vals[0], &ctx, len(vals))
	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
}

func AnalyzeUint64(vals []uint64) (uint64, uint64, uint64, int) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[uint64]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	analyze_u64_avx2(&vals[0], &ctx, len(vals))
	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
}

func AnalyzeInt32(vals []int32) (int32, int32, int32, int) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[int32]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	analyze_i32_avx2(&vals[0], &ctx, len(vals))
	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
}

func AnalyzeUint32(vals []uint32) (uint32, uint32, uint32, int) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[uint32]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	analyze_u32_avx2(&vals[0], &ctx, len(vals))
	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
}

func AnalyzeInt16(vals []int16) (int16, int16, int16, int) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[int16]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	analyze_i16_avx2(&vals[0], &ctx, len(vals))
	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
}

func AnalyzeUint16(vals []uint16) (uint16, uint16, uint16, int) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[uint16]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	analyze_u16_avx2(&vals[0], &ctx, len(vals))
	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
}

func AnalyzeInt8(vals []int8) (int8, int8, int8, int) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[int8]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	analyze_i8_avx2(&vals[0], &ctx, len(vals))
	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
}

func AnalyzeUint8(vals []uint8) (uint8, uint8, uint8, int) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[uint8]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	analyze_u8_avx2(&vals[0], &ctx, len(vals))
	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
}

func AnalyzeFloat64(vals []float64) (float64, float64, int) {
	if len(vals) == 0 {
		return 0, 0, 0
	}
	var ctx Context[float64]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	analyze_f64_avx2(&vals[0], &ctx, len(vals))
	return ctx.Min, ctx.Max, int(ctx.NumRuns)
}

func AnalyzeFloat32(vals []float32) (float32, float32, int) {
	if len(vals) == 0 {
		return 0, 0, 0
	}
	var ctx Context[float32]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	analyze_f32_avx2(&vals[0], &ctx, len(vals))
	return ctx.Min, ctx.Max, int(ctx.NumRuns)
}
