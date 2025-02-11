// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package bloom

import (
	"blockwatch.cc/knoxdb/internal/hash"
)

// AddMany inserts multiple data points to the filter.
func filterAddManyUint32Generic(f *Filter, l []uint32) {
	for _, v := range l {
		f.add(f, hash.HashUint32(v))
	}
}

// AddMany inserts multiple data points to the filter.
func filterAddManyInt32Generic(f *Filter, l []int32) {
	for _, v := range l {
		f.add(f, hash.HashInt32(v))
	}
}

// AddMany inserts multiple data points to the filter.
func filterAddManyUint64Generic(f *Filter, l []uint64) {
	for _, v := range l {
		f.add(f, hash.HashUint64(v))
	}
}

// AddMany inserts multiple data points to the filter.
func filterAddManyInt64Generic(f *Filter, l []int64) {
	for _, v := range l {
		f.add(f, hash.HashInt64(v))
	}
}

func filterMergeGeneric(dst, src []byte) {
	// Perform union of each byte.
	for i := range dst {
		dst[i] |= src[i]
	}
}
