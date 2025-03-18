// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx2

import (
	"blockwatch.cc/knoxdb/internal/encode/analyze/generic"
	"blockwatch.cc/knoxdb/internal/types"
)

type Context[T types.Integer] struct {
	Min     T // vector minimum
	Max     T // vector maximum
	Delta   T // common delta between vector values
	NumRuns T // vector runs
}

// stubs for unimplemented functions
var (
	AnalyzeInt64 = generic.Analyze[int64]
	AnalyzeInt32 = generic.Analyze[int32]
	AnalyzeInt16 = generic.Analyze[int16]
	AnalyzeInt8  = generic.Analyze[int8]

	AnalyzeUint64 = generic.Analyze[uint64]
	AnalyzeUint32 = generic.Analyze[uint32]
	AnalyzeUint16 = generic.Analyze[uint16]
	AnalyzeUint8  = generic.Analyze[uint8]
)

// ASM imports

// //go:noescape
// func analyze_i64_avx2(vals []int64, ret *Context[int64])

// //go:noescape
// func analyze_i32_avx2(vals []int32, ret *Context[int32])

// //go:noescape
// func analyze_i16_avx2(vals []int16, ret *Context[int16])

// //go:noescape
// func analyze_i8_avx2(vals []int8, ret *Context[int8])

// //go:noescape
// func analyze_u64_avx2(vals []uint64, ret *Context[uint64])

// //go:noescape
// func analyze_u32_avx2(vals []uint32, ret *Context[uint32])

// //go:noescape
// func analyze_u16_avx2(vals []uint16, ret *Context[uint16])

// //go:noescape
// func analyze_u8_avx2(vals []uint8, ret *Context[uint8])

// Go exports

// func AnalyzeInt64(vals []int64) (int64, int64, int64, int) {
// 	var ctx Context[int64]
// 	analyze_i64_avx2(vals, &ctx)
// 	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
// }

// func AnalyzeInt32(vals []int32) (int32, int32, int32, int) {
// 	var ctx Context[int32]
// 	analyze_i32_avx2(vals, &ctx)
// 	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
// }

// func AnalyzeInt16(vals []int16) (int16, int16, int16, int) {
// 	var ctx Context[int16]
// 	analyze_i16_avx2(vals, &ctx)
// 	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
// }

// func AnalyzeInt8(vals []int8) (int8, int8, int8, int) {
// 	var ctx Context[int8]
// 	analyze_i8_avx2(vals, &ctx)
// 	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
// }

// func AnalyzeUint64(vals []uint64) (uint64, uint64, uint64, int) {
// 	var ctx Context[uint64]
// 	analyze_u64_avx2(vals, &ctx)
// 	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
// }

// func AnalyzeUint32(vals []uint32) (uint32, uint32, uint32, int) {
// 	var ctx Context[uint32]
// 	analyze_u32_avx2(vals, &ctx)
// 	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
// }

// func AnalyzeUint16(vals []uint16) (uint16, uint16, uint16, int) {
// 	var ctx Context[uint16]
// 	analyze_u16_avx2(vals, &ctx)
// 	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
// }

// func AnalyzeUint8(vals []uint8) (uint8, uint8, uint8, int) {
// 	var ctx Context[uint8]
// 	analyze_u8_avx2(vals, &ctx)
// 	return ctx.Min, ctx.Max, ctx.Delta, int(ctx.NumRuns)
// }
