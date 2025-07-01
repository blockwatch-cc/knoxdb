// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build !amd64 && !arm64
// +build !amd64,!arm64

package generic

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
