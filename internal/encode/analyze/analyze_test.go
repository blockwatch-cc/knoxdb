// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package analyze

import (
	"math"
	"testing"

	"blockwatch.cc/knoxdb/internal/encode/tests"
	"github.com/stretchr/testify/assert"
)

// func TestAnalyzeInt64(t *testing.T) {
// 	// delta, no dups
// 	minv, maxv, delta, numRuns := AnalyzeInt64([]int64{-1, 0, 1, 2})
// 	assert.Equal(t, int64(-1), minv, "min")
// 	assert.Equal(t, int64(2), maxv, "max")
// 	assert.Equal(t, int64(1), delta, "delta")
// 	assert.Equal(t, 4, numRuns, "num_runs")

// 	// runs
// 	minv, maxv, delta, numRuns = AnalyzeInt64([]int64{-1, -1, 5, 5, 1, 1})
// 	assert.Equal(t, int64(-1), minv, "min")
// 	assert.Equal(t, int64(5), maxv, "max")
// 	assert.Equal(t, int64(0), delta, "delta")
// 	assert.Equal(t, 3, numRuns, "num_runs")

// 	// dict-friendly
// 	minv, maxv, delta, numRuns = AnalyzeInt64([]int64{-1, 1, 5, 1, -1, 1})
// 	assert.Equal(t, int64(-1), minv, "min")
// 	assert.Equal(t, int64(5), maxv, "max")
// 	assert.Equal(t, int64(0), delta, "delta")
// 	assert.Equal(t, 6, numRuns, "num_runs")
// }

func TestAnalyzeInt64(t *testing.T) {
	tests := []struct {
		name     string
		input    []int64
		expMin   int64
		expMax   int64
		expDelta int64
		expRuns  int
	}{
		{"Empty", []int64{}, 0, 0, 0, 0},
		{"Single", []int64{42}, 42, 42, 0, 1},
		{"DeltaNoDups", []int64{-1, 0, 1, 2}, -1, 2, 1, 4},
		{"Runs", []int64{-1, -1, 5, 5, 1, 1}, -1, 5, 0, 3},
		{"DictFriendly", []int64{-1, 1, 5, 1, -1, 1}, -1, 5, 0, 6},
		{"AllSame", []int64{5, 5, 5, 5, 5}, 5, 5, 0, 1},
		{"Alternating", []int64{1, 0, 1, 0, 1}, 0, 1, 0, 5},
		{"LargeDelta", []int64{10, 20, 30, 40, 50}, 10, 50, 10, 5},
		{"NegDelta", []int64{-10, -7, -4, -1, 2}, -10, 2, 3, 5},
		{"Bounds", []int64{math.MinInt64, 0, math.MaxInt64}, math.MinInt64, math.MaxInt64, 0, 3},
		{"Short", []int64{1, 2, 3}, 1, 3, 1, 3},
		{"MixedRuns", []int64{1, 1, 2, 2, 5, 8, 8}, 1, 8, 0, 4},
		{"Unaligned", []int64{1, 2, 3, 4, 5, 6, 7}, 1, 7, 1, 7},
		{"Random", []int64{3, 1, 4, 1, 5, 9, 2, 6, 5, 3}, 1, 9, 0, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			minv, maxv, delta, numRuns := AnalyzeInt64(tt.input)
			assert.Equal(t, tt.expMin, minv, "min")
			assert.Equal(t, tt.expMax, maxv, "max")
			assert.Equal(t, tt.expDelta, delta, "delta")
			assert.Equal(t, tt.expRuns, numRuns, "num_runs")
		})
	}
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
