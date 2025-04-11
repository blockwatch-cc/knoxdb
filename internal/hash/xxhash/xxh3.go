// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package xxhash

import "unsafe"

var (
	VecXXH3u32 = xxh3_u32_purego
	VecXXH3u64 = xxh3_u64_purego
)

const (
	key64_008 = 0x1cad21f72c81017c
	key64_016 = 0xdb979083e96dd4de
)

func XXH3u32(val uint32) uint64 {
	input64 := uint64(val) + uint64(val)<<32
	h := input64 ^ (key64_008 ^ key64_016)

	h ^= rol64_49(h) ^ rol64_24(h)
	h *= 0x9fb21c651e98df25
	h ^= (h >> 35) + 4
	h *= 0x9fb21c651e98df25
	h ^= (h >> 28)

	return h
}

func XXH3u64(val uint64) uint64 {
	input64 := val>>32 + val<<32
	h := input64 ^ (key64_008 ^ key64_016)

	h ^= rol64_49(h) ^ rol64_24(h)
	h *= 0x9fb21c651e98df25
	h ^= (h >> 35) + 8
	h *= 0x9fb21c651e98df25
	h ^= (h >> 28)

	return h
}

func xxh3_u32_purego(src []uint32, dst []uint64) []uint64 {
	if len(src) == 0 {
		return dst[:0]
	}
	var i int
	sp := unsafe.Pointer(&src[0])
	rp := unsafe.Pointer(&dst[0])
	for range len(src) / 128 {
		s := (*[128]uint32)(unsafe.Add(sp, i*8))
		r := (*[128]uint64)(unsafe.Add(rp, i*8))
		xxh3_u32_core(s, r)
		i += 128
	}
	for i < len(src) {
		dst[i] = XXH3u32(src[i])
		i++
	}
	return dst
}

func xxh3_u32_core(src *[128]uint32, dst *[128]uint64) {
	for i := 0; i < len(src); i += 16 {
		dst[0] = XXH3u32(src[i])
		dst[i+1] = XXH3u32(src[i+1])
		dst[i+2] = XXH3u32(src[i+2])
		dst[i+3] = XXH3u32(src[i+3])
		dst[i+4] = XXH3u32(src[i+4])
		dst[i+5] = XXH3u32(src[i+5])
		dst[i+6] = XXH3u32(src[i+6])
		dst[i+7] = XXH3u32(src[i+7])

		dst[i+8] = XXH3u32(src[i+8])
		dst[i+9] = XXH3u32(src[i+9])
		dst[i+10] = XXH3u32(src[i+10])
		dst[i+11] = XXH3u32(src[i+11])
		dst[i+12] = XXH3u32(src[i+12])
		dst[i+13] = XXH3u32(src[i+13])
		dst[i+14] = XXH3u32(src[i+14])
		dst[i+15] = XXH3u32(src[i+15])
	}
}

func xxh3_u64_purego(src, dst []uint64) []uint64 {
	if len(src) == 0 {
		return dst[:0]
	}
	var i int
	sp := unsafe.Pointer(&src[0])
	rp := unsafe.Pointer(&dst[0])
	for range len(src) / 128 {
		s := (*[128]uint64)(unsafe.Add(sp, i*8))
		r := (*[128]uint64)(unsafe.Add(rp, i*8))
		xxh3_u64_core(s, r)
		i += 128
	}
	for i < len(src) {
		dst[i] = XXH3u64(src[i])
		i++
	}
	return dst
}

func xxh3_u64_core(src, dst *[128]uint64) {
	for i := 0; i < len(src); i += 16 {
		dst[0] = XXH3u64(src[i])
		dst[i+1] = XXH3u64(src[i+1])
		dst[i+2] = XXH3u64(src[i+2])
		dst[i+3] = XXH3u64(src[i+3])
		dst[i+4] = XXH3u64(src[i+4])
		dst[i+5] = XXH3u64(src[i+5])
		dst[i+6] = XXH3u64(src[i+6])
		dst[i+7] = XXH3u64(src[i+7])

		dst[i+8] = XXH3u64(src[i+8])
		dst[i+9] = XXH3u64(src[i+9])
		dst[i+10] = XXH3u64(src[i+10])
		dst[i+11] = XXH3u64(src[i+11])
		dst[i+12] = XXH3u64(src[i+12])
		dst[i+13] = XXH3u64(src[i+13])
		dst[i+14] = XXH3u64(src[i+14])
		dst[i+15] = XXH3u64(src[i+15])
	}
}
