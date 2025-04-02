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
		{"dups_1K", GenDups[T](1024, 10), 1024}, // 10% unique
		{"dups_16K", GenDups[T](16*1024, 10), 16 * 1024},
		{"dups_64K", GenDups[T](64*1024, 10), 64 * 1024},

		{"runs_1K", GenRuns[T](1024, 10), 1024}, // run length 10
		{"runs_16K", GenRuns[T](16*1024, 10), 16 * 1024},
		{"runs_64K", GenRuns[T](64*1024, 10), 64 * 1024},

		{"seq_1K", GenSeq[T](1024), 1024},
		{"seq_16K", GenSeq[T](16 * 1024), 16 * 1024},
		{"seq_64K", GenSeq[T](64 * 1024), 64 * 1024},
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
