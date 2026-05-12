// Copyright (c) 2020-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package llb

import (
	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/hash"
)

// Deprecation Note
// Multi-value add algos are unused because their AVX variants use
// the deprecated xxhash32 instead of xxh3!
// Disabled in favor of hash-first-then-add structure.

var (
	// llb_add_u32     = llb_add_u32_purego
	// llb_add_u64     = llb_add_u64_purego
	llb_merge       = llb_merge_purego
	llb_cardinality = llb_cardinality_purego
)

func llb_add_u32_purego(llb *LogLogBeta, val []uint32) {
	hashes := arena.AllocUint64(len(val))[:len(val)]
	hashes = hash.Vec32(val, hashes)
	llb.Add(hashes...)
	arena.Free(hashes)
}

func llb_add_u64_purego(llb *LogLogBeta, val []uint64) {
	hashes := arena.AllocUint64(len(val))[:len(val)]
	hashes = hash.Vec64(val, hashes)
	llb.Add(hashes...)
	arena.Free(hashes)
}

// Cardinality returns the number of unique elements added to the sketch
func llb_cardinality_purego(llb *LogLogBeta) uint64 {
	sum, ez := regSumAndZeros(llb.buf)
	m := float64(llb.m)
	return uint64(llb.alpha * m * (m - ez) / (beta(ez) + sum))
}

func llb_merge_purego(dst, src []byte) {
	for i, v := range dst {
		if v < src[i] {
			dst[i] = src[i]
		}
	}
}
