// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
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
}

func MakeBenchmarks[T types.Number]() []Benchmark[T] {
	return []Benchmark[T]{
		{"dups_1k", GenDups[T](1024, 128, 24), 1024},           // 8% unique, 24 bit width
		{"dups_16k", GenDups[T](16*1024, 2048, 24), 16 * 1024}, // 12% unique, 24 bit width
		{"dups_64k", GenDups[T](64*1024, 8192, 24), 64 * 1024}, // 16% unique, 24 bit width

		{"runs_1k", GenRuns[T](1024, 10, 24), 1024},          // run length 10, 24 bit width
		{"runs_16k", GenRuns[T](16*1024, 10, 24), 16 * 1024}, // run length 10, 24 bit width
		{"runs_64k", GenRuns[T](64*1024, 10, 24), 64 * 1024}, // run length 10, 24 bit width

		{"seq_1k", GenSeq[T](1024), 1024},
		{"seq_16k", GenSeq[T](16 * 1024), 16 * 1024},
		{"seq_64k", GenSeq[T](64 * 1024), 64 * 1024},
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
	{"D8", 8 * 1024}, // dict better for W > 15, N = 64k
	{"D16", 16 * 1024},
	{"D32", 32 * 1024},
	{"D48", 48 * 1024},
}

// 64k vector, dict wins when
// w = 8, c < 128
// w = 16, c < 8192
// w = 32, c < ≈32768 (33920)
// w = 63, c < ≈48000 (48792)
