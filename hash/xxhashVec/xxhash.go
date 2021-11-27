// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package xxhashVec

import (
	"math/bits"
)

const (
	prime32_1 = 2654435761
	prime32_2 = 2246822519
	prime32_3 = 3266489917
	prime32_4 = 668265263
	prime32_5 = 374761393

	prime64_1 = 11400714785074694791
	prime64_2 = 14029467366897019727
	prime64_3 = 1609587929392839161
	prime64_4 = 9650029242287828579
	prime64_5 = 2870177450012600261

	key64_008 = 0x1cad21f72c81017c
	key64_016 = 0xdb979083e96dd4de
)

func XXHash32Uint32(val uint32, seed uint32) uint32 {
	h := seed + prime32_5 + 4
	h += val * prime32_3
	h = rol32_17(h) * prime32_4

	h ^= h >> 15
	h *= prime32_2
	h ^= h >> 13
	h *= prime32_3
	h ^= h >> 16

	return h
}

func XXHash32Int32(val int32, seed uint32) uint32 {
	h := seed + prime32_5 + 4
	h += uint32(val) * prime32_3
	h = rol32_17(h) * prime32_4

	h ^= h >> 15
	h *= prime32_2
	h ^= h >> 13
	h *= prime32_3
	h ^= h >> 16

	return h
}

func XXHash32Uint64(val uint64, seed uint32) uint32 {
	h := seed + prime32_5 + 8
	h += uint32(val&0xffffffff) * prime32_3
	h = rol32_17(h) * prime32_4
	h += uint32(val>>32) * prime32_3
	h = rol32_17(h) * prime32_4

	h ^= h >> 15
	h *= prime32_2
	h ^= h >> 13
	h *= prime32_3
	h ^= h >> 16

	return h
}

func XXHash32Int64(val int64, seed uint32) uint32 {
	h := seed + prime32_5 + 8
	h += uint32(val&0xffffffff) * prime32_3
	h = rol32_17(h) * prime32_4
	h += uint32(val>>32) * prime32_3
	h = rol32_17(h) * prime32_4

	h ^= h >> 15
	h *= prime32_2
	h ^= h >> 13
	h *= prime32_3
	h ^= h >> 16

	return h
}

func XXHash64Uint32(val uint32) uint64 {
	var h uint64
	h = prime64_5 + 4
	h ^= uint64(val) * prime64_1
	h = rol64_23(h)*prime64_2 + prime64_3

	h ^= h >> 33
	h *= prime64_2
	h ^= h >> 29
	h *= prime64_3
	h ^= h >> 32

	return h
}

func XXHash64Uint64(val uint64) uint64 {
	var h uint64
	h = prime64_5 + 8

	k1 := val * prime64_2
	k1 = rol64_31(k1)
	k1 *= prime64_1

	h ^= k1
	h = rol64_27(h)*prime64_1 + prime64_4

	h ^= h >> 33
	h *= prime64_2
	h ^= h >> 29
	h *= prime64_3
	h ^= h >> 32

	return h
}

func XXH3Uint32(val uint32) uint64 {
	input64 := uint64(val) + uint64(val)<<32
	h := input64 ^ (key64_008 ^ key64_016)

	h ^= rol64_49(h) ^ rol64_24(h)
	h *= 0x9fb21c651e98df25
	h ^= (h >> 35) + 4
	h *= 0x9fb21c651e98df25
	h ^= (h >> 28)

	return h
}

func XXH3Uint64(val uint64) uint64 {
	input64 := val>>32 + val<<32
	h := input64 ^ (key64_008 ^ key64_016)

	h ^= rol64_49(h) ^ rol64_24(h)
	h *= 0x9fb21c651e98df25
	h ^= (h >> 35) + 8
	h *= 0x9fb21c651e98df25
	h ^= (h >> 28)

	return h
}

const knuth = 2654435769

func MHash32Uint32(val uint32, p int) uint32 {
	val *= knuth
	return val >> (32 - p)
}

func MHash32Uint64(val uint64, p int) uint32 {
	val *= knuth
	return uint32(val >> (32 - p))
}

func XXHash32Uint32Slice(src []uint32, res []uint32, seed uint32) []uint32 {
	res = ensureSizeUint32(res, len(src))
	xxhash32Uint32Slice(src, res, seed)
	return res
}

func XXHash32Uint64Slice(src []uint64, res []uint32, seed uint32) []uint32 {
	res = ensureSizeUint32(res, len(src))
	xxhash32Uint64Slice(src, res, seed)
	return res
}

func XXHash64Uint32Slice(src []uint32, res []uint64) []uint64 {
	res = ensureSizeUint64(res, len(src))
	xxhash64Uint32Slice(src, res)
	return res
}

func XXHash64Uint64Slice(src []uint64, res []uint64) []uint64 {
	res = ensureSizeUint64(res, len(src))
	xxhash64Uint64Slice(src, res)
	return res
}

func XXH3Uint32Slice(src []uint32, res []uint64) []uint64 {
	res = ensureSizeUint64(res, len(src))
	xxh3Uint32Slice(src, res)
	return res
}

func XXH3Uint64Slice(src []uint64, res []uint64) []uint64 {
	res = ensureSizeUint64(res, len(src))
	xxh3Uint64Slice(src, res)
	return res
}

func ensureSizeUint32(src []uint32, size int) []uint32 {
	if cap(src) < size {
		return make([]uint32, size)
	}
	return src[:size]
}

func ensureSizeUint64(src []uint64, size int) []uint64 {
	if cap(src) < size {
		return make([]uint64, size)
	}
	return src[:size]
}

func rol32_17(x uint32) uint32 { return bits.RotateLeft32(x, 17) }

func rol64_23(x uint64) uint64 { return bits.RotateLeft64(x, 23) }
func rol64_24(x uint64) uint64 { return bits.RotateLeft64(x, 24) }
func rol64_27(x uint64) uint64 { return bits.RotateLeft64(x, 27) }
func rol64_31(x uint64) uint64 { return bits.RotateLeft64(x, 31) }
func rol64_49(x uint64) uint64 { return bits.RotateLeft64(x, 49) }
