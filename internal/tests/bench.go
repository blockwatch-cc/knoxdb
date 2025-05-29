// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/stringx"
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
	F    *File[T]
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

type StringBenchmark struct {
	Name string
	Data *stringx.StringPool
	N    int
}

const BENCH_STR_LEN = 32

func MakeStringBenchmarks() []StringBenchmark {
	return []StringBenchmark{
		{"dups_1k", GenStringDups(1024, 128, BENCH_STR_LEN), 1024},           // 8% unique
		{"dups_16k", GenStringDups(16*1024, 2048, BENCH_STR_LEN), 16 * 1024}, // 12% unique
		{"dups_64k", GenStringDups(64*1024, 8192, BENCH_STR_LEN), 64 * 1024}, // 16% unique

		{"runs_1k", GenStringRuns(1024, 10, BENCH_STR_LEN), 1024},          // run length 10
		{"runs_16k", GenStringRuns(16*1024, 10, BENCH_STR_LEN), 16 * 1024}, // run length 10
		{"runs_64k", GenStringRuns(64*1024, 10, BENCH_STR_LEN), 64 * 1024}, // run length 10

		{"seq_1k", GenStringSeq(1024, 1), 1024},
		{"seq_16k", GenStringSeq(16*1024, 1), 16 * 1024},
		{"seq_64k", GenStringSeq(64*1024, 1), 64 * 1024},
	}
}
