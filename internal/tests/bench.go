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
		{"dups_1k", GenDups[T](1024, 10), 1024}, // 10% unique
		{"dups_16k", GenDups[T](16*1024, 10), 16 * 1024},
		{"dups_64k", GenDups[T](64*1024, 10), 64 * 1024},

		{"runs_1k", GenRuns[T](1024, 10), 1024}, // run length 10
		{"runs_16k", GenRuns[T](16*1024, 10), 16 * 1024},
		{"runs_64k", GenRuns[T](64*1024, 10), 64 * 1024},

		{"seq_1k", GenSeq[T](1024), 1024},
		{"seq_16k", GenSeq[T](16 * 1024), 16 * 1024},
		{"seq_64k", GenSeq[T](64 * 1024), 64 * 1024},
	}
}

type BenchmarkPattern struct {
	Name string
	Pct  int
}

var BenchmarkPatterns = []BenchmarkPattern{
	{"10%", 10},
	{"50%", 50},
	{"90%", 90},
}
