// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
)

type BenchmarkSize struct {
	Name string
	N    int
}

var BenchmarkSizes = []BenchmarkSize{
	{"1k", 1024},
	{"16k", 16 * 1024},
	{"64k", 64 * 1024},
}

type Benchmark[T types.Number] struct {
	Name string
	Data []T
	N    int
	F    *RawFile[T]
}

func (b *Benchmark[T]) Next() ([]T, bool) {
	if b.F == nil {
		return nil, false
	}
	dst, n := b.F.NextN(b.N, b.Data)
	if n == 0 {
		return nil, false
	}
	b.Data = dst[:n]
	return b.Data, true
}

func MakeBenchmarks[T types.Number]() []Benchmark[T] {
	return []Benchmark[T]{
		{"dups_1k", GenDups[T](1024, 128, 24), 1024, nil},           // 8% unique, 24 bit width
		{"dups_16k", GenDups[T](16*1024, 2048, 24), 16 * 1024, nil}, // 12% unique, 24 bit width
		{"dups_64k", GenDups[T](64*1024, 8192, 24), 64 * 1024, nil}, // 16% unique, 24 bit width

		{"runs_1k", GenRuns[T](1024, 10, 24), 1024, nil},          // run length 10, 24 bit width
		{"runs_16k", GenRuns[T](16*1024, 10, 24), 16 * 1024, nil}, // run length 10, 24 bit width
		{"runs_64k", GenRuns[T](64*1024, 10, 24), 64 * 1024, nil}, // run length 10, 24 bit width

		{"seq_1k", GenSeq[T](1024, 1), 1024, nil},
		{"seq_16k", GenSeq[T](16*1024, 1), 16 * 1024, nil},
		{"seq_64k", GenSeq[T](64*1024, 1), 64 * 1024, nil},
	}
}

type BenchmarkPercent struct {
	Name string
	Pct  int
}

var BenchmarkPercents = []BenchmarkPercent{
	{"10%", 10},
	{"50%", 50},
	{"90%", 90},
}

type BenchmarkPattern struct {
	Name string
	Size int
}

var BenchmarkPatterns = []BenchmarkPattern{
	{"D1", 128},
	{"D2", 2 * 1024},
	{"D8", 8 * 1024},
	{"D16", 16 * 1024},
	{"D32", 32 * 1024},
	{"D48", 48 * 1024},
}

// 64k vector, dict wins when
// w = 8, c < 128
// w = 16, c < 8192
// w = 32, c < ≈32768 (33920)
// w = 63, c < ≈48000 (48792)

// ----------------------------------------
// File based benchmarks
// ----------------------------------------

var GO_BENCH_PATH = os.Getenv("GO_BENCH_PATH")

func CheckFileBenchmarks(b *testing.B) {
	if GO_BENCH_PATH == "" {
		b.Skip("no benchmark files, set GO_BENCH_PATH env")
	}
}

func MakeRawBenchmarks[T types.Number](n int) []Benchmark[T] {
	if GO_BENCH_PATH == "" {
		return nil
	}
	files, err := filepath.Glob(filepath.Join(GO_BENCH_PATH, "*.bin"))
	if err != nil {
		panic(err)
	}
	bench := make([]Benchmark[T], len(files))
	for i, name := range files {
		f, err := OpenRawFile[T](name)
		if err != nil {
			panic(err)
		}
		bench[i].Name = strings.TrimSuffix(filepath.Base(name), filepath.Ext(name))
		bench[i].Data = make([]T, 0, n)
		bench[i].N = n
		bench[i].F = f
	}
	runtime.AddCleanup(&bench, func(_ *RawFile[T]) {
		for _, b := range bench {
			b.F.Close()
		}
	}, nil)
	return bench
}
