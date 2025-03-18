// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"blockwatch.cc/knoxdb/internal/types"
)

// Vectorized loop (process 4 elements at a time)
// BenchmarkAnalyzeInt/dups_1K-10        971 ns/op   8434.43 MB/s
// BenchmarkAnalyzeInt/dups_16K-10     14893 ns/op   8801.13 MB/s
// BenchmarkAnalyzeInt/dups_64K-10     57478 ns/op   9121.50 MB/s
// BenchmarkAnalyzeInt/runs_1K-10        770 ns/op   10633.43 MB/s
// BenchmarkAnalyzeInt/runs_16K-10     12804 ns/op   10237.15 MB/s
// BenchmarkAnalyzeInt/runs_64K-10     51268 ns/op   10226.43 MB/s
// BenchmarkAnalyzeInt/seq_1K-10        1077 ns/op   7603.72 MB/s
// BenchmarkAnalyzeInt/seq_16K-10      16983 ns/op   7717.72 MB/s
// BenchmarkAnalyzeInt/seq_64K-10      67802 ns/op   7732.62 MB/s
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

	// unrolled loop
	i := 1
	for ; i < len(vals)-3; i += 4 {
		v0 := vals[i-1]
		v1 := vals[i]
		v2 := vals[i+1]
		v3 := vals[i+2]
		v4 := vals[i+3]

		// Min/Max updates for 4 elements
		minv = min(minv, v1, v2, v3, v4)
		maxv = max(maxv, v1, v2, v3, v4)

		// Run counting and delta checking
		if v0 != v1 {
			numRuns++
			if delta != 0 && delta != v1-v0 {
				delta = 0
			}
		}
		if v1 != v2 {
			numRuns++
			if delta != 0 && delta != v2-v1 {
				delta = 0
			}
		}
		if v2 != v3 {
			numRuns++
			if delta != 0 && delta != v3-v2 {
				delta = 0
			}
		}
		if v3 != v4 {
			numRuns++
			if delta != 0 && delta != v4-v3 {
				delta = 0
			}
		}
	}

	// Scalar loop for remaining elements
	for ; i < len(vals); i++ {
		v := vals[i]
		if v < minv {
			minv = v
		} else if v > maxv {
			maxv = v
		}
		if vals[i-1] != v {
			numRuns++
			if delta != 0 && delta != v-vals[i-1] {
				delta = 0
			}
		}
	}
	return
}
