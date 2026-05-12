// Copyright (c) 2021-2026 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc, alex@blockwatch.cc

package hash

import (
	"math/bits"
	"unsafe"
)

const (
	key32_000 = 0xbe4ba423
	key32_004 = 0x396cfeb8

	key64_008 = 0x1cad21f72c81017c
	key64_016 = 0xdb979083e96dd4de

	prime64_2 = 14029467366897019727
	prime64_3 = 1609587929392839161
)

func xxh3_u64(val uint64) uint64 {
	input64 := val>>32 + val<<32
	h := input64 ^ (key64_008 ^ key64_016)

	h ^= rol64_49(h) ^ rol64_24(h)
	h *= 0x9fb21c651e98df25
	h ^= (h >> 35) + 8
	h *= 0x9fb21c651e98df25
	h ^= (h >> 28)

	return h
}

func xxh3_u32(val uint32) uint64 {
	input64 := uint64(val) + uint64(val)<<32
	h := input64 ^ (key64_008 ^ key64_016)

	h ^= rol64_49(h) ^ rol64_24(h)
	h *= 0x9fb21c651e98df25
	h ^= (h >> 35) + 4
	h *= 0x9fb21c651e98df25
	h ^= (h >> 28)

	return h
}

func xxh3_u16(val uint16) uint64 {
	h := uint64(val)*(1<<24+1)>>8 + 2<<8
	h ^= key32_000 ^ key32_004
	return xxhAvalancheSmall(h)
}

func xxh3_u8(val uint8) uint64 {
	h := uint64(val)*(1<<24+1<<16+1) + 1<<8
	h ^= key32_000 ^ key32_004
	return xxhAvalancheSmall(h)
}

func xxh3_u64_purego(src, dst []uint64) []uint64 {
	if len(src) == 0 {
		return dst[:0]
	}
	if cap(dst) < len(src) {
		dst = make([]uint64, len(src))
	}
	dst = dst[:len(src)]
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
		dst[i] = xxh3_u64(src[i])
		i++
	}
	return dst
}

func xxh3_u64_core(src, dst *[128]uint64) {
	for i := 0; i < len(src); i += 16 {
		dst[i] = xxh3_u64(src[i])
		dst[i+1] = xxh3_u64(src[i+1])
		dst[i+2] = xxh3_u64(src[i+2])
		dst[i+3] = xxh3_u64(src[i+3])
		dst[i+4] = xxh3_u64(src[i+4])
		dst[i+5] = xxh3_u64(src[i+5])
		dst[i+6] = xxh3_u64(src[i+6])
		dst[i+7] = xxh3_u64(src[i+7])

		dst[i+8] = xxh3_u64(src[i+8])
		dst[i+9] = xxh3_u64(src[i+9])
		dst[i+10] = xxh3_u64(src[i+10])
		dst[i+11] = xxh3_u64(src[i+11])
		dst[i+12] = xxh3_u64(src[i+12])
		dst[i+13] = xxh3_u64(src[i+13])
		dst[i+14] = xxh3_u64(src[i+14])
		dst[i+15] = xxh3_u64(src[i+15])
	}
}

func xxh3_u32_purego(src []uint32, dst []uint64) []uint64 {
	if len(src) == 0 {
		return dst[:0]
	}
	if cap(dst) < len(src) {
		dst = make([]uint64, len(src))
	}
	dst = dst[:len(src)]
	var i int
	sp := unsafe.Pointer(&src[0])
	rp := unsafe.Pointer(&dst[0])
	for range len(src) / 128 {
		s := (*[128]uint32)(unsafe.Add(sp, i*4))
		r := (*[128]uint64)(unsafe.Add(rp, i*8))
		xxh3_u32_core(s, r)
		i += 128
	}
	for i < len(src) {
		dst[i] = xxh3_u32(src[i])
		i++
	}
	return dst
}

func xxh3_u32_core(src *[128]uint32, dst *[128]uint64) {
	for i := 0; i < len(src); i += 16 {
		dst[i] = xxh3_u32(src[i])
		dst[i+1] = xxh3_u32(src[i+1])
		dst[i+2] = xxh3_u32(src[i+2])
		dst[i+3] = xxh3_u32(src[i+3])
		dst[i+4] = xxh3_u32(src[i+4])
		dst[i+5] = xxh3_u32(src[i+5])
		dst[i+6] = xxh3_u32(src[i+6])
		dst[i+7] = xxh3_u32(src[i+7])

		dst[i+8] = xxh3_u32(src[i+8])
		dst[i+9] = xxh3_u32(src[i+9])
		dst[i+10] = xxh3_u32(src[i+10])
		dst[i+11] = xxh3_u32(src[i+11])
		dst[i+12] = xxh3_u32(src[i+12])
		dst[i+13] = xxh3_u32(src[i+13])
		dst[i+14] = xxh3_u32(src[i+14])
		dst[i+15] = xxh3_u32(src[i+15])
	}
}

func xxh3_u16_purego(src []uint16, dst []uint64) []uint64 {
	if len(src) == 0 {
		return dst[:0]
	}
	if cap(dst) < len(src) {
		dst = make([]uint64, len(src))
	}
	dst = dst[:len(src)]
	var i int
	sp := unsafe.Pointer(&src[0])
	rp := unsafe.Pointer(&dst[0])
	for range len(src) / 128 {
		s := (*[128]uint16)(unsafe.Add(sp, i*2))
		r := (*[128]uint64)(unsafe.Add(rp, i*8))
		xxh3_u16_core(s, r)
		i += 128
	}
	for i < len(src) {
		dst[i] = xxh3_u16(src[i])
		i++
	}
	return dst
}

func xxh3_u16_core(src *[128]uint16, dst *[128]uint64) {
	for i := 0; i < len(src); i += 16 {
		dst[i] = xxh3_u16(src[i])
		dst[i+1] = xxh3_u16(src[i+1])
		dst[i+2] = xxh3_u16(src[i+2])
		dst[i+3] = xxh3_u16(src[i+3])
		dst[i+4] = xxh3_u16(src[i+4])
		dst[i+5] = xxh3_u16(src[i+5])
		dst[i+6] = xxh3_u16(src[i+6])
		dst[i+7] = xxh3_u16(src[i+7])

		dst[i+8] = xxh3_u16(src[i+8])
		dst[i+9] = xxh3_u16(src[i+9])
		dst[i+10] = xxh3_u16(src[i+10])
		dst[i+11] = xxh3_u16(src[i+11])
		dst[i+12] = xxh3_u16(src[i+12])
		dst[i+13] = xxh3_u16(src[i+13])
		dst[i+14] = xxh3_u16(src[i+14])
		dst[i+15] = xxh3_u16(src[i+15])
	}
}

func xxh3_u8_purego(src []uint8, dst []uint64) []uint64 {
	if len(src) == 0 {
		return dst[:0]
	}
	if cap(dst) < len(src) {
		dst = make([]uint64, len(src))
	}
	dst = dst[:len(src)]
	var i int
	sp := unsafe.Pointer(&src[0])
	rp := unsafe.Pointer(&dst[0])
	for range len(src) / 128 {
		s := (*[128]uint8)(unsafe.Add(sp, i))
		r := (*[128]uint64)(unsafe.Add(rp, i*8))
		xxh3_u8_core(s, r)
		i += 128
	}
	for i < len(src) {
		dst[i] = xxh3_u8(src[i])
		i++
	}
	return dst
}

func xxh3_u8_core(src *[128]uint8, dst *[128]uint64) {
	for i := 0; i < len(src); i += 16 {
		dst[i] = xxh3_u8(src[i])
		dst[i+1] = xxh3_u8(src[i+1])
		dst[i+2] = xxh3_u8(src[i+2])
		dst[i+3] = xxh3_u8(src[i+3])
		dst[i+4] = xxh3_u8(src[i+4])
		dst[i+5] = xxh3_u8(src[i+5])
		dst[i+6] = xxh3_u8(src[i+6])
		dst[i+7] = xxh3_u8(src[i+7])

		dst[i+8] = xxh3_u8(src[i+8])
		dst[i+9] = xxh3_u8(src[i+9])
		dst[i+10] = xxh3_u8(src[i+10])
		dst[i+11] = xxh3_u8(src[i+11])
		dst[i+12] = xxh3_u8(src[i+12])
		dst[i+13] = xxh3_u8(src[i+13])
		dst[i+14] = xxh3_u8(src[i+14])
		dst[i+15] = xxh3_u8(src[i+15])
	}
}

func rol64_24(x uint64) uint64 { return bits.RotateLeft64(x, 24) }
func rol64_49(x uint64) uint64 { return bits.RotateLeft64(x, 49) }

func xxhAvalancheSmall(x uint64) uint64 {
	x ^= x >> 33
	x *= prime64_2
	x ^= x >> 29
	x *= prime64_3
	x ^= x >> 32
	return x
}
