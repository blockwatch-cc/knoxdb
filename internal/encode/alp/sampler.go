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
	var j int
	for i := 0; i < len(src)-SAMPLE_SIZE+1; i += len(src) / SAMPLE_SIZE {
		dst[j] = src[i]
		j++
	}
	return dst
}
