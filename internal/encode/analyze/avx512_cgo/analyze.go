package main

// Howto run this
//
// scp ./internal/encode/analyze/avx2_cgo/* flex:/var/lib/docker-lvm-plugin/scratch/analyze/
// docker run --rm -v scratch:/usr/src -w /usr/src/analyze golang:1.24 env GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go test -c .
// docker run --rm -v scratch:/usr/src -w /usr/src/analyze golang:1.24 ./analyze.test -test.run=Analyze
// docker run --rm -v scratch:/usr/src -w /usr/src/analyze golang:1.24 ./analyze.test -test.bench=Analyze
//
// Compile and extract ASM
// docker run --rm -v scratch:/usr/src -w /usr/src/analyze golang:1.23 env GOOS=linux GOARCH=amd64 CGO_ENABLED=1 gcc -c analyze.c -mavx512f -mavx512dq -O3
// objdump -d analyze.o > analyze.s
//

// #cgo CFLAGS: -mavx2 -mavx512vl -mavx512f -mavx512dq -O3
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
	C.analyze_i64_avx512(
		(*C.int64_t)(unsafe.Pointer(&vals[0])),
		(*C.I64Context)(unsafe.Pointer(&ctx)),
		C.size_t(len(vals)),
	)
	return ctx.Min, ctx.Max, ctx.Delta, ctx.NumRuns
}
