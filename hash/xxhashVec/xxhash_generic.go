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
)

func xxhash32Uint32SliceGeneric(src, res []uint32, seed uint32) {
	for i, val := range src {

		h := seed + prime32_5 + 4
		h += val * prime32_3
		h = rol32_17(h) * prime32_4

		h ^= h >> 15
		h *= prime32_2
		h ^= h >> 13
		h *= prime32_3
		h ^= h >> 16

		res[i] = h
	}
}

func xxhash32Uint64SliceGeneric(src []uint64, res []uint32, seed uint32) {
	for i, val := range src {

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

		res[i] = h
	}
}

func xxhash64Uint32SliceGeneric(src []uint32, res []uint64) {
	for j, val := range src {

		var h uint64
		h = prime64_5 + 4
		h ^= uint64(val) * prime64_1
		h = rol64_23(h)*prime64_2 + prime64_3

		h ^= h >> 33
		h *= prime64_2
		h ^= h >> 29
		h *= prime64_3
		h ^= h >> 32

		res[j] = h
	}
}

func xxhash64Uint64SliceGeneric(src, res []uint64) {
	for j, val := range src {

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

		res[j] = h
	}
}

func xxh3Uint32SliceGeneric(src []uint32, res []uint64) {
	for i, val := range src {
		input64 := u64(val) + u64(val)<<32
		h := input64 ^ (key64_008 ^ key64_016)

		h ^= rol64_49(h) ^ rol64_24(h)
		h *= 0x9fb21c651e98df25
		h ^= (h >> 35) + 4
		h *= 0x9fb21c651e98df25
		h ^= (h >> 28)

		res[i] = h
	}
}

func xxh3Uint64SliceGeneric(src, res []uint64) {
	for i, val := range src {
		input64 := val>>32 + val<<32
		h := input64 ^ (key64_008 ^ key64_016)

		h ^= rol64_49(h) ^ rol64_24(h)
		h *= 0x9fb21c651e98df25
		h ^= (h >> 35) + 8
		h *= 0x9fb21c651e98df25
		h ^= (h >> 28)

		res[i] = h
	}
}

func rol32_17(x uint32) uint32 { return bits.RotateLeft32(x, 17) }

func rol64_11(x uint64) uint64 { return bits.RotateLeft64(x, 11) }
func rol64_23(x uint64) uint64 { return bits.RotateLeft64(x, 23) }
func rol64_24(x uint64) uint64 { return bits.RotateLeft64(x, 24) }
func rol64_27(x uint64) uint64 { return bits.RotateLeft64(x, 27) }
func rol64_31(x uint64) uint64 { return bits.RotateLeft64(x, 31) }
func rol64_49(x uint64) uint64 { return bits.RotateLeft64(x, 49) }
