// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package bloomVec

import (
	"blockwatch.cc/knoxdb/hash/xxhashVec"
)

// AddMany inserts multiple data points to the filter.
func filterAddManyUint32Generic(f Filter, l []uint32, seed uint32) {
	for _, v := range l {
		h := [2]uint32{xxhashVec.XXHash32Uint32(v, seed), xxhashVec.XXHash32Uint32(v, 0)}
		for i := uint32(0); i < f.k; i++ {
			loc := f.location(h, i)
			f.b[loc>>3] |= 1 << (loc & 7)
		}
	}
}

// AddMany inserts multiple data points to the filter.
func filterAddManyInt32Generic(f Filter, l []int32, seed uint32) {
	for _, v := range l {
		h := [2]uint32{xxhashVec.XXHash32Int32(v, seed), xxhashVec.XXHash32Int32(v, 0)}
		for i := uint32(0); i < f.k; i++ {
			loc := f.location(h, i)
			f.b[loc>>3] |= 1 << (loc & 7)
		}
	}
}

// AddMany inserts multiple data points to the filter.
func filterAddManyUint64Generic(f Filter, l []uint64, seed uint32) {
	for _, v := range l {
		h := [2]uint32{xxhashVec.XXHash32Uint64(v, seed), xxhashVec.XXHash32Uint64(v, 0)}
		for i := uint32(0); i < f.k; i++ {
			loc := f.location(h, i)
			f.b[loc>>3] |= 1 << (loc & 7)
		}
	}
}

// AddMany inserts multiple data points to the filter.
func filterAddManyInt64Generic(f Filter, l []int64, seed uint32) {
	for _, v := range l {
		h := [2]uint32{xxhashVec.XXHash32Int64(v, seed), xxhashVec.XXHash32Int64(v, 0)}
		for i := uint32(0); i < f.k; i++ {
			loc := f.location(h, i)
			f.b[loc>>3] |= 1 << (loc & 7)
		}
	}
}

func filterMergeGeneric(dst, src []byte) {
	// Perform union of each byte.
	for i := range dst {
		dst[i] |= src[i]
	}
}
