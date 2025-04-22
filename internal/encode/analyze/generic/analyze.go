// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"blockwatch.cc/knoxdb/internal/types"
)

// Scalar loop (unrolling does not benefit)
//
// cpu: Apple M1 Max
// BenchmarkAnalyze/int64/dups_1k-10        907.7 ns/op	  9025.13 MB/s	 1.128 vals/ns
// BenchmarkAnalyze/int64/dups_16k-10     13697 ns/op	  9569.55 MB/s	 1.196 vals/ns
// BenchmarkAnalyze/int64/dups_64k-10     53341 ns/op	  9828.94 MB/s	 1.229 vals/ns
// BenchmarkAnalyze/int64/runs_1k-10        711.3 ns/op	  11516.33 MB/s	 1.440 vals/ns
// BenchmarkAnalyze/int64/runs_16k-10     12294 ns/op	  10661.44 MB/s	 1.333 vals/ns
// BenchmarkAnalyze/int64/runs_64k-10     49197 ns/op	  10656.83 MB/s	 1.332 vals/ns
// BenchmarkAnalyze/int64/seq_1k-10         973.6 ns/op	  8413.95 MB/s	 1.052 vals/ns
// BenchmarkAnalyze/int64/seq_16k-10      16816 ns/op	  7794.67 MB/s	 0.9743 vals/ns
// BenchmarkAnalyze/int64/seq_64k-10      67162 ns/op	  7806.35 MB/s	 0.9758 vals/ns
//
// cpu: 12th Gen Intel(R) Core(TM) i9-12900K
// BenchmarkAnalyze/int64/dups_1k-24      552 ns/op	    14837.27 MB/s    1.855 vals/ns
// BenchmarkAnalyze/int64/dups_16k-24    8052 ns/op	    16278.11 MB/s    2.035 vals/ns
// BenchmarkAnalyze/int64/dups_64k-24   32370 ns/op	    16196.64 MB/s    2.025 vals/ns
// BenchmarkAnalyze/int64/runs_1k-24      419 ns/op	    19523.79 MB/s    2.440 vals/ns
// BenchmarkAnalyze/int64/runs_16k-24    6537 ns/op	    20052.14 MB/s    2.507 vals/ns
// BenchmarkAnalyze/int64/runs_64k-24   25991 ns/op	    20171.52 MB/s    2.521 vals/ns
// BenchmarkAnalyze/int64/seq_1k-24       603 ns/op	    13582.17 MB/s    1.698 vals/ns
// BenchmarkAnalyze/int64/seq_16k-24     9547 ns/op	    13728.79 MB/s    1.716 vals/ns
// BenchmarkAnalyze/int64/seq_64k-24    38591 ns/op	    13585.86 MB/s    1.698 vals/ns
func Analyze[T types.Integer](vals []T) (minv T, maxv T, delta T, numRuns int) {
	if len(vals) == 0 {
		return
	}
	minv = vals[0]
	maxv = vals[0]
	if len(vals) > 1 {
		delta = vals[1] - vals[0]
	}
	numRuns = 1
	hasDelta := delta != 0

	i := 1
	for ; i < len(vals); i++ {
		v0 := vals[i-1]
		v1 := vals[i]
		minv = min(minv, v1)
		maxv = max(maxv, v1)
		if v0 != v1 {
			numRuns++
			hasDelta = hasDelta && delta == v1-v0
		} else {
			hasDelta = false
		}
	}

	if !hasDelta {
		delta = 0
	}
	return
}
