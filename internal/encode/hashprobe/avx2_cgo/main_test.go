package main

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

func BenchmarkDictHashUint64(b *testing.B) {
	for _, p := range BenchmarkPatterns {
		for _, c := range BenchmarkSizes {
			data := GenDups[uint64](c.N, min(c.N, p.Size), BENCH_WIDTH)
			ctx := AnalyzeInt(data, true)
			var card int
			b.Run(fmt.Sprintf("%s/%s", c.Name, p.Name), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for range b.N {
					dict, codes := EncodeDictHash64(data, ctx.NumUnique)
					card = len(dict)
					FreeT(dict)
					FreeT(codes)
				}
				_ = card
			})
		}
	}
}

func BenchmarkDictHashUint32(b *testing.B) {
	for _, p := range BenchmarkPatterns {
		for _, c := range BenchmarkSizes {
			data := GenDups[uint32](c.N, min(c.N, p.Size), 32)
			ctx := AnalyzeInt(data, true)
			var card int
			b.Run(fmt.Sprintf("%s/%s", c.Name, p.Name), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 4))
				for range b.N {
					dict, codes := EncodeDictHash32(data, ctx.NumUnique)
					card = len(dict)
					FreeT(dict)
					FreeT(codes)
				}
				_ = card
			})
		}
	}
}

type BenchmarkPattern struct {
	Name string
	Size int
}

var BenchmarkPatterns = []BenchmarkPattern{
	{"D1", 128},
	{"D2", 2 * 1024},
	{"D8", 8 * 1024}, // dict better for W > 15, N = 64k
	{"D16", 16 * 1024},
}

type BenchmarkSize struct {
	Name string
	N    int
}

var BenchmarkSizes = []BenchmarkSize{
	{"1k", 1024},
	{"16k", 16 * 1024},
	{"64k", 64 * 1024},
}

const BENCH_WIDTH = 60

var (
	RandIntn    = rand.IntN
	RandInt64   = rand.Int64
	RandInt64n  = rand.Int64N
	RandUint64n = rand.Uint64N
	RandUint64  = rand.Uint64
)

func RandIntsn[T Signed](sz int, max T) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandInt64n(int64(max)))
	}
	return s
}

func RandInts[T Signed](sz int) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandInt64())
	}
	return s
}

func RandUintsn[T Unsigned](sz int, max T) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandUint64n(uint64(max)))
	}
	return s
}

func RandUints[T Unsigned](sz int) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(RandUint64())
	}
	return s
}

func GenDups[T Integer](n, c, w int) []T {
	if w > BENCH_WIDTH {
		panic(fmt.Errorf("w=%d must be smaller than %d", w, BENCH_WIDTH))
	}
	if c > n {
		panic(fmt.Errorf("c=%d must be smaller than n=%d", c, n))
	}
	if w <= 0 {
		w = BENCH_WIDTH
	}
	if c <= 0 {
		c = 1
	}
	res := make([]T, n)
	var t T
	switch any(t).(type) {
	case int64:
		unique := RandIntsn[int64](c, 1<<w-1)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	case int32:
		unique := RandIntsn[int32](c, 1<<min(w, 31)-1)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	case int16:
		unique := RandInts[int16](c)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	case int8:
		unique := RandInts[int8](c)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	case uint64:
		unique := RandUintsn[uint64](c, 1<<w-1)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	case uint32:
		unique := RandUintsn[uint32](c, 1<<min(w, 32)-1)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	case uint16:
		unique := RandUints[uint16](c)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	case uint8:
		unique := RandUints[uint8](c)
		for i := range res {
			res[i] = T(unique[RandIntn(c)])
		}
	}
	return res
}
