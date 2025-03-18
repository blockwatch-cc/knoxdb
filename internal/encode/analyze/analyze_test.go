// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package analyze

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/encode/tests"
	"github.com/stretchr/testify/assert"
)

func TestAnalyzeInt64(t *testing.T) {
	// delta, no dups
	minv, maxv, delta, numRuns := AnalyzeInt64([]int64{-1, 0, 1, 2})
	assert.Equal(t, int64(-1), minv, "min")
	assert.Equal(t, int64(2), maxv, "max")
	assert.Equal(t, int64(1), delta, "delta")
	assert.Equal(t, 4, numRuns, "num_runs")

	// runs
	minv, maxv, delta, numRuns = AnalyzeInt64([]int64{-1, -1, 5, 5, 1, 1})
	assert.Equal(t, int64(-1), minv, "min")
	assert.Equal(t, int64(5), maxv, "max")
	assert.Equal(t, int64(0), delta, "delta")
	assert.Equal(t, 3, numRuns, "num_runs")

	// dict-friendly
	minv, maxv, delta, numRuns = AnalyzeInt64([]int64{-1, 1, 5, 1, -1, 1})
	assert.Equal(t, int64(-1), minv, "min")
	assert.Equal(t, int64(5), maxv, "max")
	assert.Equal(t, int64(0), delta, "delta")
	assert.Equal(t, 6, numRuns, "num_runs")
}

func BenchmarkAnalyzeInt64(b *testing.B) {
	for _, c := range tests.Benchmarks {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			for i := 0; i < b.N; i++ {
				AnalyzeInt64(c.Data)
			}
		})
	}
}
