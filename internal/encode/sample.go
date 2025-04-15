// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
)

const (
	MAX_CASCADE  = 3
	SAMPLE_SIZE  = 64
	SAMPLE_COUNT = 10
)

// TODO: change to 64 items from every 1024 chunk for better coverage of dict cases

// Sample extracts a deterministic sample from float slice v. It is used
// when estimating the effectiveness of different encoders.
func Sample[T types.Number](v []T) ([]T, bool) {
	// check minimum sample size
	sz := SAMPLE_COUNT * SAMPLE_SIZE
	if len(v) <= sz {
		return v, false
	}

	// allocate enough space for the sample
	s := arena.Alloc[T](sz)[:sz]

	// calculate sampling chunk offsets, we don't sample from the last
	// chunk if its not full size
	chunk := len(v) / SAMPLE_COUNT
	sz = 0

	for i := 0; i < SAMPLE_COUNT; i++ {
		// Note: btrblocks uses a random sample, but we need determinism
		// start := chunk * i + util.RandIntn(chunk-SAMPLE_SIZE)
		start := chunk * i
		end := start + SAMPLE_SIZE
		sz += copy(s[SAMPLE_SIZE*i:], v[start:end])
	}

	return s[:sz], true
}
