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

		h32 := seed + prime32_5 + 4
		h32 += val * prime32_3
		h32 = rol32_17(h32) * prime32_4

		h32 ^= h32 >> 15
		h32 *= prime32_2
		h32 ^= h32 >> 13
		h32 *= prime32_3
		h32 ^= h32 >> 16

		res[i] = h32
	}
}

func xxhash32Uint64SliceGeneric(src []uint64, res []uint32, seed uint32) {
	for i, val := range src {

		h32 := seed + prime32_5 + 8
		h32 += uint32(val&0xffffffff) * prime32_3
		h32 = rol32_17(h32) * prime32_4
		h32 += uint32(val>>32) * prime32_3
		h32 = rol32_17(h32) * prime32_4

		h32 ^= h32 >> 15
		h32 *= prime32_2
		h32 ^= h32 >> 13
		h32 *= prime32_3
		h32 ^= h32 >> 16

		res[i] = h32
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
		keyed := input64 ^ (key64_008 ^ key64_016)
		res[i] = rrmxmx(keyed, 4)
	}
}

func xxh3Uint64SliceGeneric(src, res []uint64) {
	for i, val := range src {
		input64 := val>>32 + val<<32
		keyed := input64 ^ (key64_008 ^ key64_016)
		res[i] = rrmxmx(keyed, u64(8))
	}
}

func rol32_17(x uint32) uint32 { return bits.RotateLeft32(x, 17) }

func rol64_11(x uint64) uint64 { return bits.RotateLeft64(x, 11) }
func rol64_23(x uint64) uint64 { return bits.RotateLeft64(x, 23) }
func rol64_27(x uint64) uint64 { return bits.RotateLeft64(x, 27) }
func rol64_31(x uint64) uint64 { return bits.RotateLeft64(x, 31) }
