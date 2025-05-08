// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package alp

import (
	"blockwatch.cc/knoxdb/internal/types"
)

const SAMPLE_SIZE = 32

func Sample[T types.Float](dst, src []T) []T {
	if len(src) <= SAMPLE_SIZE {
		return src
	}
	step := len(src) / SAMPLE_SIZE
	var j int
	for i := 0; i < len(src)-SAMPLE_SIZE+1; i += step {
		val := src[i]
		if val == 0 {
			// find the next non-zero value
			for k := i + 1; k < i+step; k++ {
				val = src[k]
				if val != 0 {
					break
				}
			}
		}
		dst[j] = val
		j++
	}
	return dst
}
