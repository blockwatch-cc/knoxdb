// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package xxhash

import "unsafe"

var (
	Vec32u32 = x32_u32_purego
	Vec32u64 = x32_u64_purego
)

const (
	prime32_1 = 2654435761
	prime32_2 = 2246822519
	prime32_3 = 3266489917
	prime32_4 = 668265263
	prime32_5 = 374761393
)

func Hash32u32(val uint32, seed uint32) uint32 {
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

func Hash32u64(val uint64, seed uint32) uint32 {
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

func x32_u32_purego(src []uint32, dst []uint32, seed uint32) []uint32 {
	if len(src) == 0 {
		return dst[:0]
	}
	var i int
	sp := unsafe.Pointer(&src[0])
	rp := unsafe.Pointer(&dst[0])
	for range len(src) / 128 {
		s := (*[128]uint32)(unsafe.Add(sp, i*8))
		r := (*[128]uint32)(unsafe.Add(rp, i*8))
		x32_u32_core(s, r, seed)
		i += 128
	}
	for i < len(src) {
		dst[i] = Hash32u32(src[i], seed)
		i++
	}
	return dst
}

func x32_u32_core(src, dst *[128]uint32, seed uint32) {
	for i := 0; i < len(src); i += 16 {
		dst[0] = Hash32u32(src[i], seed)
		dst[i+1] = Hash32u32(src[i+1], seed)
		dst[i+2] = Hash32u32(src[i+2], seed)
		dst[i+3] = Hash32u32(src[i+3], seed)
		dst[i+4] = Hash32u32(src[i+4], seed)
		dst[i+5] = Hash32u32(src[i+5], seed)
		dst[i+6] = Hash32u32(src[i+6], seed)
		dst[i+7] = Hash32u32(src[i+7], seed)

		dst[i+8] = Hash32u32(src[i+8], seed)
		dst[i+9] = Hash32u32(src[i+9], seed)
		dst[i+10] = Hash32u32(src[i+10], seed)
		dst[i+11] = Hash32u32(src[i+11], seed)
		dst[i+12] = Hash32u32(src[i+12], seed)
		dst[i+13] = Hash32u32(src[i+13], seed)
		dst[i+14] = Hash32u32(src[i+14], seed)
		dst[i+15] = Hash32u32(src[i+15], seed)
	}
}

func x32_u64_purego(src []uint64, dst []uint32, seed uint32) []uint32 {
	if len(src) == 0 {
		return dst[:0]
	}
	var i int
	sp := unsafe.Pointer(&src[0])
	rp := unsafe.Pointer(&dst[0])
	for range len(src) / 128 {
		s := (*[128]uint64)(unsafe.Add(sp, i*8))
		r := (*[128]uint32)(unsafe.Add(rp, i*8))
		x32_u64_core(s, r, seed)
		i += 128
	}
	for i < len(src) {
		dst[i] = Hash32u64(src[i], seed)
		i++
	}
	return dst
}

func x32_u64_core(src *[128]uint64, dst *[128]uint32, seed uint32) {
	for i := 0; i < len(src); i += 16 {
		dst[0] = Hash32u64(src[i], seed)
		dst[i+1] = Hash32u64(src[i+1], seed)
		dst[i+2] = Hash32u64(src[i+2], seed)
		dst[i+3] = Hash32u64(src[i+3], seed)
		dst[i+4] = Hash32u64(src[i+4], seed)
		dst[i+5] = Hash32u64(src[i+5], seed)
		dst[i+6] = Hash32u64(src[i+6], seed)
		dst[i+7] = Hash32u64(src[i+7], seed)

		dst[i+8] = Hash32u64(src[i+8], seed)
		dst[i+9] = Hash32u64(src[i+9], seed)
		dst[i+10] = Hash32u64(src[i+10], seed)
		dst[i+11] = Hash32u64(src[i+11], seed)
		dst[i+12] = Hash32u64(src[i+12], seed)
		dst[i+13] = Hash32u64(src[i+13], seed)
		dst[i+14] = Hash32u64(src[i+14], seed)
		dst[i+15] = Hash32u64(src[i+15], seed)
	}
}
