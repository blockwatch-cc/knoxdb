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

type Benchmark[T types.Integer] struct {
	Name string
	Data []T
}

func MakeBenchmarks[T types.Integer]() []Benchmark[T] {
	return []Benchmark[T]{
		{"dups_1K", GenDups[T](1024, 10)}, // 10% unique
		{"dups_16K", GenDups[T](16*1024, 10)},
		{"dups_64K", GenDups[T](64*1024, 10)},

		{"runs_1K", GenRuns[T](1024, 10)}, // run length 10
		{"runs_16K", GenRuns[T](16*1024, 10)},
		{"runs_64K", GenRuns[T](64*1024, 10)},

		{"seq_1K", GenSequence[T](1024)},
		{"seq_16K", GenSequence[T](16 * 1024)},
		{"seq_64K", GenSequence[T](64 * 1024)},
	}
}
