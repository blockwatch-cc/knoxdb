// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package loglogbeta

import (
	"blockwatch.cc/knoxdb/internal/hash/xxhash"
)

func filterAddManyUint32Generic(llb *LogLogBeta, val []uint32, seed uint32) {
	for _, v := range val {
		llb.AddHash(xxhash.Hash32u32(v, seed))
	}
}

func filterAddManyUint64Generic(llb *LogLogBeta, val []uint64, seed uint32) {
	for _, v := range val {
		llb.AddHash(xxhash.Hash32u64(v, seed))
	}
}

// Cardinality returns the number of unique elements added to the sketch
func filterCardinalityGeneric(llb *LogLogBeta) uint64 {
	sum, ez := regSumAndZeros(llb.buf)
	m := float64(llb.m)
	return uint64(llb.alpha * m * (m - ez) / (beta(ez) + sum))
}

func filterMergeGeneric(dst, src []byte) {
	for i, v := range dst {
		if v < src[i] {
			dst[i] = src[i]
		}
	}
}
