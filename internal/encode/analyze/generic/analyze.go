// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"blockwatch.cc/knoxdb/internal/types"
)

// Vectorized loop (process 4 elements at a time)
//
// cpu: Apple M1 Max
// BenchmarkAnalyzeInt64/dups_1K-10       1064 ns/op       7698.03 MB/s
// BenchmarkAnalyzeInt64/dups_16K-10     15620 ns/op       8391.50 MB/s
// BenchmarkAnalyzeInt64/dups_64K-10     62346 ns/op       8409.34 MB/s
// BenchmarkAnalyzeInt64/runs_1K-10        780 ns/op       10506.89 MB/s
// BenchmarkAnalyzeInt64/runs_16K-10     11853 ns/op       11058.33 MB/s
// BenchmarkAnalyzeInt64/runs_64K-10     47429 ns/op       11054.23 MB/s
// BenchmarkAnalyzeInt64/seq_1K-10         975 ns/op       8402.93 MB/s
// BenchmarkAnalyzeInt64/seq_16K-10      15923 ns/op       8231.37 MB/s
// BenchmarkAnalyzeInt64/seq_64K-10      63104 ns/op       8308.37 MB/s
//
// cpu: 12th Gen Intel(R) Core(TM) i9-12900K
// BenchmarkAnalyzeInt64/dups_1K-24       697 ns/op        11748.83 MB/s
// BenchmarkAnalyzeInt64/dups_16K-24     8663 ns/op        15129.72 MB/s
// BenchmarkAnalyzeInt64/dups_64K-24    34571 ns/op        15165.76 MB/s
// BenchmarkAnalyzeInt64/runs_1K-24       493 ns/op        16614.15 MB/s
// BenchmarkAnalyzeInt64/runs_16K-24     6877 ns/op        19058.27 MB/s
// BenchmarkAnalyzeInt64/runs_64K-24    26947 ns/op        19456.06 MB/s
// BenchmarkAnalyzeInt64/seq_1K-24        684 ns/op        11972.24 MB/s
// BenchmarkAnalyzeInt64/seq_16K-24     10787 ns/op        12151.42 MB/s
// BenchmarkAnalyzeInt64/seq_64K-24     43339 ns/op        12097.38 MB/s
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
		v := vals[i]
		if v < minv {
			minv = v
		} else if v > maxv {
			maxv = v
		}
		if vals[i-1] != v {
			numRuns++
			hasDelta = hasDelta && delta == v-vals[i-1]
		}
	}

	if !hasDelta {
		delta = 0
	}
	return
}
