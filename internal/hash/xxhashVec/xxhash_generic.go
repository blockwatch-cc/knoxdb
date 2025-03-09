// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package xxhashVec

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

func xxhash32Int32SliceGeneric(src []int32, res []uint32, seed uint32) {
	for i, val := range src {

		h := seed + prime32_5 + 4
		h += uint32(val) * prime32_3
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

func xxhash32Int64SliceGeneric(src []int64, res []uint32, seed uint32) {
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

func xxhash64Uint8SliceGeneric(src []uint8, res []uint64) {
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

func xxhash64Uint16SliceGeneric(src []uint16, res []uint64) {
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
		input64 := uint64(val) + uint64(val)<<32
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
