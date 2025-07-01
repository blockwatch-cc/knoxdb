// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build amd64 && wasm
// +build amd64,wasm

package generic

// cpu: 12th Gen Intel(R) Core(TM) i9-12900K
// BenchmarkAnalyze/float64/dups_1k-24      933.9 ns/op	  8771.47 MB/s    1.096 vals/ns
// BenchmarkAnalyze/float64/dups_16k-24   14900 ns/op	  8796.96 MB/s    1.100 vals/ns
// BenchmarkAnalyze/float64/dups_64k-24   68843 ns/op	  7615.75 MB/s    0.9520 vals/ns
// BenchmarkAnalyze/float64/runs_1k-24      944.9 ns/op	  8669.83 MB/s    1.084 vals/ns
// BenchmarkAnalyze/float64/runs_16k-24   14944 ns/op	  8770.75 MB/s    1.096 vals/ns
// BenchmarkAnalyze/float64/runs_64k-24   59004 ns/op	  8885.59 MB/s    1.111 vals/ns
// BenchmarkAnalyze/float64/seq_1k-24       993.2 ns/op	  8248.24 MB/s    1.031 vals/ns
// BenchmarkAnalyze/float64/seq_16k-24    15850 ns/op	  8269.46 MB/s    1.034 vals/ns
// BenchmarkAnalyze/float64/seq_64k-24    68632 ns/op	  7639.15 MB/s    0.9549 vals/ns
func AnalyzeFloat[T float64 | float32](vals []T) (minv T, maxv T, numRuns int) {
	if len(vals) == 0 {
		return
	}
	minv = vals[0]
	maxv = vals[0]
	numRuns = 1

	for i := 1; i < len(vals); i++ {
		v0 := vals[i-1]
		v1 := vals[i]
		if v1 < minv {
			minv = v1
		} else if v1 > maxv {
			maxv = v1
		}
		numRuns += b2i(v0 != v1)
	}

	return
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
