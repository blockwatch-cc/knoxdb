package main

// Howto run this
//
// scp ./internal/encode/analyze/avx2_cgo/* flex:/var/lib/docker-lvm-plugin/scratch/analyze/
// docker run --rm -v scratch:/usr/src -w /usr/src/analyze golang:1.24 env GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go test -c .
// docker run --rm -v scratch:/usr/src -w /usr/src/analyze golang:1.24 ./analyze.test -test.run=Analyze
// docker run --rm -v scratch:/usr/src -w /usr/src/analyze golang:1.24 ./analyze.test -test.bench=Analyze
//
// Compile and extract ASM
// docker run --rm -v scratch:/usr/src -w /usr/src/analyze golang:1.23 env GOOS=linux GOARCH=amd64 CGO_ENABLED=1 gcc -c analyze.c -mavx2 -O3
// objdump -d analyze.o > analyze.s
//

// #cgo CFLAGS: -mavx2 -O3
// #include "analyze.h"
import "C"
import (
	"unsafe"
)

type Integer interface {
	int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8
}

// Context matches the C struct
type Context[T Integer] struct {
	Min     T
	Max     T
	Delta   T
	NumRuns T
}

func AnalyzeInt64(vals []int64) (int64, int64, int64, int64) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[int64]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	C.analyze_i64_avx2(
		(*C.int64_t)(unsafe.Pointer(&vals[0])),
		(*C.I64Context)(unsafe.Pointer(&ctx)),
		C.size_t(len(vals)),
	)
	return ctx.Min, ctx.Max, ctx.Delta, ctx.NumRuns
}

func AnalyzeUint64(vals []uint64) (uint64, uint64, uint64, uint64) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[uint64]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	C.analyze_u64_avx2(
		(*C.uint64_t)(unsafe.Pointer(&vals[0])),
		(*C.U64Context)(unsafe.Pointer(&ctx)),
		C.size_t(len(vals)),
	)
	return ctx.Min, ctx.Max, ctx.Delta, ctx.NumRuns
}

func AnalyzeInt32(vals []int32) (int32, int32, int32, int32) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[int32]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	C.analyze_i32_avx2(
		(*C.int32_t)(unsafe.Pointer(&vals[0])),
		(*C.I32Context)(unsafe.Pointer(&ctx)),
		C.size_t(len(vals)),
	)
	return ctx.Min, ctx.Max, ctx.Delta, ctx.NumRuns
}

func AnalyzeUint32(vals []uint32) (uint32, uint32, uint32, uint32) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[uint32]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	C.analyze_u32_avx2(
		(*C.uint32_t)(unsafe.Pointer(&vals[0])),
		(*C.U32Context)(unsafe.Pointer(&ctx)),
		C.size_t(len(vals)),
	)
	return ctx.Min, ctx.Max, ctx.Delta, ctx.NumRuns
}

func AnalyzeInt16(vals []int16) (int16, int16, int16, int16) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[int16]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	C.analyze_i16_avx2(
		(*C.int16_t)(unsafe.Pointer(&vals[0])),
		(*C.I16Context)(unsafe.Pointer(&ctx)),
		C.size_t(len(vals)),
	)
	return ctx.Min, ctx.Max, ctx.Delta, ctx.NumRuns
}

func AnalyzeUint16(vals []uint16) (uint16, uint16, uint16, uint16) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[uint16]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	C.analyze_u16_avx2(
		(*C.uint16_t)(unsafe.Pointer(&vals[0])),
		(*C.U16Context)(unsafe.Pointer(&ctx)),
		C.size_t(len(vals)),
	)
	return ctx.Min, ctx.Max, ctx.Delta, ctx.NumRuns
}

func AnalyzeInt8(vals []int8) (int8, int8, int8, int8) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[int8]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	C.analyze_i8_avx2(
		(*C.int8_t)(unsafe.Pointer(&vals[0])),
		(*C.I8Context)(unsafe.Pointer(&ctx)),
		C.size_t(len(vals)),
	)
	return ctx.Min, ctx.Max, ctx.Delta, ctx.NumRuns
}

func AnalyzeUint8(vals []uint8) (uint8, uint8, uint8, uint8) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	var ctx Context[uint8]
	if len(vals) > 1 {
		ctx.Delta = vals[1] - vals[0]
	}
	C.analyze_u8_avx2(
		(*C.uint8_t)(unsafe.Pointer(&vals[0])),
		(*C.U8Context)(unsafe.Pointer(&ctx)),
		C.size_t(len(vals)),
	)
	return ctx.Min, ctx.Max, ctx.Delta, ctx.NumRuns
}
