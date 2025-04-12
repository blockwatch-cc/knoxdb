// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package llb

import (
	"blockwatch.cc/knoxdb/internal/hash/xxhash"
)

var (
	llb_add_u32     = llb_add_u32_purego
	llb_add_u64     = llb_add_u64_purego
	llb_merge       = llb_merge_purego
	llb_cardinality = llb_cardinality_purego
)

func llb_add_u32_purego(llb *LogLogBeta, val []uint32, seed uint32) {
	for _, v := range val {
		llb.AddHash(xxhash.Hash32u32(v, seed))
	}
}

func llb_add_u64_purego(llb *LogLogBeta, val []uint64, seed uint32) {
	for _, v := range val {
		llb.AddHash(xxhash.Hash32u64(v, seed))
	}
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
