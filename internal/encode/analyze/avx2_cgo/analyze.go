package main

// Howto run this
//
// scp * flex:/var/lib/docker-lvm-plugin/scratch/analyze/
// docker run --rm -v scratch:/usr/src -w /usr/src/analyze golang:1.24 env GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go test -c .
// docker run --rm -v scratch:/usr/src -w /usr/src/analyze golang:1.24 ./analyze.test -test.run=Analyze
// docker run --rm -v scratch:/usr/src -w /usr/src/analyze golang:1.24 ./analyze.test -test.bench=Analyze
//
// Compile and extract ASM
// docker run --rm -v scratch:/usr/src -w /usr/src/analyze golang:1.23 env GOOS=linux GOARCH=amd64 CGO_ENABLED=1 gcc -c analyze.c -mavx2 -O3
// objdump -d analyze.o > analyze.s
//
//
// cpu: 12th Gen Intel(R) Core(TM) i9-12900K
// BenchmarkAnalyzeInt64/dups_64K-24     23621 ns/op   22195.60 MB/s
// BenchmarkAnalyzeUint64/dups_64K-24    23391 ns/op   22413.69 MB/s
// BenchmarkAnalyzeInt32/dups_64K-24      6395 ns/op   40990.52 MB/s
// BenchmarkAnalyzeUint32/dups_64K-24     6663 ns/op   39342.49 MB/s
// BenchmarkAnalyzeInt16/dups_64K-24      3376 ns/op   38828.25 MB/s
// BenchmarkAnalyzeUint16/dups_64K-24     3517 ns/op   37268.73 MB/s
// BenchmarkAnalyzeInt8/dups_64K-24       2714 ns/op   24143.76 MB/s
// BenchmarkAnalyzeUint8/dups_64K-24      2906 ns/op   22552.19 MB/s

// BenchmarkAnalyzeInt64/runs_64K-24     23234 ns/op   22565.50 MB/s
// BenchmarkAnalyzeUint64/runs_64K-24    23367 ns/op   22437.34 MB/s
// BenchmarkAnalyzeInt32/runs_64K-24      6318 ns/op   41490.70 MB/s
// BenchmarkAnalyzeUint32/runs_64K-24     6568 ns/op   39911.34 MB/s
// BenchmarkAnalyzeInt16/runs_64K-24      3541 ns/op   37018.31 MB/s
// BenchmarkAnalyzeUint16/runs_64K-24     3510 ns/op   37343.58 MB/s
// BenchmarkAnalyzeInt8/runs_64K-24       1462 ns/op   44835.70 MB/s
// BenchmarkAnalyzeUint8/runs_64K-24      1472 ns/op   44521.05 MB/s

// BenchmarkAnalyzeInt64/seq_64K-24      29386 ns/op   17841.20 MB/s
// BenchmarkAnalyzeUint64/seq_64K-24     30859 ns/op   16989.75 MB/s
// BenchmarkAnalyzeInt32/seq_64K-24      12661 ns/op   20705.38 MB/s
// BenchmarkAnalyzeUint32/seq_64K-24     12760 ns/op   20543.78 MB/s
// BenchmarkAnalyzeInt16/seq_64K-24       6405 ns/op   20464.42 MB/s
// BenchmarkAnalyzeUint16/seq_64K-24      6337 ns/op   20683.61 MB/s
// BenchmarkAnalyzeInt8/seq_64K-24        2745 ns/op   23877.01 MB/s
// BenchmarkAnalyzeUint8/seq_64K-24       2730 ns/op   24001.71 MB/s

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
	NumRuns uint32
}

func AnalyzeInt64(vals []int64) (int64, int64, int64, uint32) {
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

func AnalyzeUint64(vals []uint64) (uint64, uint64, uint64, uint32) {
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

func AnalyzeInt32(vals []int32) (int32, int32, int32, uint32) {
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

func AnalyzeInt16(vals []int16) (int16, int16, int16, uint32) {
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

func AnalyzeUint16(vals []uint16) (uint16, uint16, uint16, uint32) {
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

func AnalyzeInt8(vals []int8) (int8, int8, int8, uint32) {
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

func AnalyzeUint8(vals []uint8) (uint8, uint8, uint8, uint32) {
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
