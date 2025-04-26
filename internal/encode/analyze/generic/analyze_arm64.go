// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build arm64
// +build arm64

package generic

import (
	"blockwatch.cc/knoxdb/internal/types"
)

// BenchmarkAnalyze/float64/dups_64k-10     41259 ns/op    12707.20 MB/s  1.588 vals/ns
// BenchmarkAnalyze/float64/runs_64k-10     41228 ns/op    12716.91 MB/s  1.590 vals/ns
// BenchmarkAnalyze/float64/seq_64k-10      41049 ns/op    12772.17 MB/s  1.597 vals/ns
// BenchmarkAnalyze/float32/dups_64k-10     41063 ns/op    6383.89 MB/s   1.596 vals/ns
// BenchmarkAnalyze/float32/runs_64k-10     41170 ns/op    6367.36 MB/s   1.592 vals/ns
// BenchmarkAnalyze/float32/seq_64k-10      41246 ns/op    6355.65 MB/s   1.589 vals/ns
func AnalyzeFloat[T types.Float](vals []T) (minv T, maxv T, numRuns int) {
	if len(vals) == 0 {
		return
	}
	minv = vals[0]
	maxv = vals[0]
	numRuns = 1
	i := 1

	// 4x loop unrolled
	for range (len(vals) - i) / 4 {
		v0 := vals[i-1]
		v1 := vals[i]
		v2 := vals[i+1]
		v3 := vals[i+2]
		v4 := vals[i+3]
		minv = min(minv, v1, v2, v3, v4)
		maxv = max(maxv, v1, v2, v3, v4)
		numRuns += b2i(v0 != v1) +
			b2i(v1 != v2) +
			b2i(v2 != v3) +
			b2i(v3 != v4)
		i += 4
	}

	// tail
	for ; i < len(vals); i++ {
		v0 := vals[i-1]
		v1 := vals[i]
		minv = min(minv, v1)
		maxv = max(maxv, v1)
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
