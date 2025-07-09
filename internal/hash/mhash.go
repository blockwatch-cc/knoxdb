// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package hash

const knuth = 2654435769

func MHash32Uint32(val uint32, p int) uint32 {
	val *= knuth
	return val >> (32 - p)
}

func MHash32Uint64(val uint64, p int) uint32 {
	val *= knuth
	return uint32(val >> (32 - p))
}
