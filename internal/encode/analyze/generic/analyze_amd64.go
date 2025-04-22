// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build amd64
// +build amd64

package generic

import (
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

// cpu: 12th Gen Intel(R) Core(TM) i9-12900K
// BenchmarkAnalyze/float64/dups_1k-24      2627 ns/op   3118.13 MB/s    0.3898 vals/ns
// BenchmarkAnalyze/float64/dups_16k-24    41858 ns/op   3131.35 MB/s    0.3914 vals/ns
// BenchmarkAnalyze/float64/dups_64k-24   167407 ns/op   3131.82 MB/s    0.3915 vals/ns
// BenchmarkAnalyze/float64/runs_1k-24      2573 ns/op   3184.35 MB/s    0.3980 vals/ns
// BenchmarkAnalyze/float64/runs_16k-24    42074 ns/op   3115.27 MB/s    0.3894 vals/ns
// BenchmarkAnalyze/float64/runs_64k-24   168020 ns/op   3120.40 MB/s    0.3900 vals/ns
// BenchmarkAnalyze/float64/seq_1k-24       2598 ns/op   3153.31 MB/s    0.3942 vals/ns
// BenchmarkAnalyze/float64/seq_16k-24     41824 ns/op   3133.88 MB/s    0.3917 vals/ns
// BenchmarkAnalyze/float64/seq_64k-24    167999 ns/op   3120.77 MB/s    0.3901 vals/ns
func AnalyzeFloat[T types.Float](vals []T) (minv T, maxv T, numRuns int) {
	if len(vals) == 0 {
		return
	}
	minv = vals[0]
	maxv = vals[0]
	numRuns = 1

	for i := 1; i < len(vals); i++ {
		v0 := vals[i-1]
		v1 := vals[i]
		minv = min(minv, v1)
		maxv = max(maxv, v1)
		numRuns += util.Bool2int(v0 != v1)
	}

	return
}
