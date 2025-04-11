// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package xxhash

import (
	"math/bits"
	"unsafe"
)

var (
	Vec64u8  = x64_u8_purego
	Vec64u16 = x64_u16_purego
	Vec64u32 = x64_u32_purego
	Vec64u64 = x64_u64_purego
)

const (
	prime64_1 = 11400714785074694791
	prime64_2 = 14029467366897019727
	prime64_3 = 1609587929392839161
	prime64_4 = 9650029242287828579
	prime64_5 = 2870177450012600261
)

func Hash64u32(val uint32) uint64 {
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

func Hash64u64(val uint64) uint64 {
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

func rol32_17(x uint32) uint32 { return bits.RotateLeft32(x, 17) }
func rol64_23(x uint64) uint64 { return bits.RotateLeft64(x, 23) }
func rol64_24(x uint64) uint64 { return bits.RotateLeft64(x, 24) }
func rol64_27(x uint64) uint64 { return bits.RotateLeft64(x, 27) }
func rol64_31(x uint64) uint64 { return bits.RotateLeft64(x, 31) }
func rol64_49(x uint64) uint64 { return bits.RotateLeft64(x, 49) }

func x64_u8_purego(src []uint8, dst []uint64) []uint64 {
	if len(src) == 0 {
		return dst[:0]
	}
	var i int
	sp := unsafe.Pointer(&src[0])
	rp := unsafe.Pointer(&dst[0])
	for range len(src) / 128 {
		s := (*[128]uint8)(unsafe.Add(sp, i))
		r := (*[128]uint64)(unsafe.Add(rp, i*8))
		x64_u8_core(s, r)
		i += 128
	}
	for i < len(src) {
		dst[i] = Hash64u64(uint64(src[i]))
		i++
	}
	return dst
}

func x64_u8_core(src *[128]uint8, dst *[128]uint64) {
	for i := 0; i < len(src); i += 16 {
		dst[0] = Hash64u64(uint64(src[i]))
		dst[i+1] = Hash64u64(uint64(src[i+1]))
		dst[i+2] = Hash64u64(uint64(src[i+2]))
		dst[i+3] = Hash64u64(uint64(src[i+3]))
		dst[i+4] = Hash64u64(uint64(src[i+4]))
		dst[i+5] = Hash64u64(uint64(src[i+5]))
		dst[i+6] = Hash64u64(uint64(src[i+6]))
		dst[i+7] = Hash64u64(uint64(src[i+7]))

		dst[i+8] = Hash64u64(uint64(src[i+8]))
		dst[i+9] = Hash64u64(uint64(src[i+9]))
		dst[i+10] = Hash64u64(uint64(src[i+10]))
		dst[i+11] = Hash64u64(uint64(src[i+11]))
		dst[i+12] = Hash64u64(uint64(src[i+12]))
		dst[i+13] = Hash64u64(uint64(src[i+13]))
		dst[i+14] = Hash64u64(uint64(src[i+14]))
		dst[i+15] = Hash64u64(uint64(src[i+15]))
	}
}

func x64_u16_purego(src []uint16, dst []uint64) []uint64 {
	if len(src) == 0 {
		return dst[:0]
	}
	var i int
	sp := unsafe.Pointer(&src[0])
	rp := unsafe.Pointer(&dst[0])
	for range len(src) / 128 {
		s := (*[128]uint16)(unsafe.Add(sp, i*2))
		r := (*[128]uint64)(unsafe.Add(rp, i*8))
		x64_u16_core(s, r)
		i += 128
	}
	for i < len(src) {
		dst[i] = Hash64u64(uint64(src[i]))
		i++
	}
	return dst
}

func x64_u16_core(src *[128]uint16, dst *[128]uint64) {
	for i := 0; i < len(src); i += 16 {
		dst[0] = Hash64u64(uint64(src[i]))
		dst[i+1] = Hash64u64(uint64(src[i+1]))
		dst[i+2] = Hash64u64(uint64(src[i+2]))
		dst[i+3] = Hash64u64(uint64(src[i+3]))
		dst[i+4] = Hash64u64(uint64(src[i+4]))
		dst[i+5] = Hash64u64(uint64(src[i+5]))
		dst[i+6] = Hash64u64(uint64(src[i+6]))
		dst[i+7] = Hash64u64(uint64(src[i+7]))

		dst[i+8] = Hash64u64(uint64(src[i+8]))
		dst[i+9] = Hash64u64(uint64(src[i+9]))
		dst[i+10] = Hash64u64(uint64(src[i+10]))
		dst[i+11] = Hash64u64(uint64(src[i+11]))
		dst[i+12] = Hash64u64(uint64(src[i+12]))
		dst[i+13] = Hash64u64(uint64(src[i+13]))
		dst[i+14] = Hash64u64(uint64(src[i+14]))
		dst[i+15] = Hash64u64(uint64(src[i+15]))
	}
}

func x64_u32_purego(src []uint32, dst []uint64) []uint64 {
	if len(src) == 0 {
		return dst[:0]
	}
	var i int
	sp := unsafe.Pointer(&src[0])
	rp := unsafe.Pointer(&dst[0])
	for range len(src) / 128 {
		s := (*[128]uint32)(unsafe.Add(sp, i*4))
		r := (*[128]uint64)(unsafe.Add(rp, i*8))
		x64_u32_core(s, r)
		i += 128
	}
	for i < len(src) {
		dst[i] = Hash64u32(src[i])
		i++
	}
	return dst
}

func x64_u32_core(src *[128]uint32, dst *[128]uint64) {
	for i := 0; i < len(src); i += 16 {
		dst[0] = Hash64u32(src[i])
		dst[i+1] = Hash64u32(src[i+1])
		dst[i+2] = Hash64u32(src[i+2])
		dst[i+3] = Hash64u32(src[i+3])
		dst[i+4] = Hash64u32(src[i+4])
		dst[i+5] = Hash64u32(src[i+5])
		dst[i+6] = Hash64u32(src[i+6])
		dst[i+7] = Hash64u32(src[i+7])

		dst[i+8] = Hash64u32(src[i+8])
		dst[i+9] = Hash64u32(src[i+9])
		dst[i+10] = Hash64u32(src[i+10])
		dst[i+11] = Hash64u32(src[i+11])
		dst[i+12] = Hash64u32(src[i+12])
		dst[i+13] = Hash64u32(src[i+13])
		dst[i+14] = Hash64u32(src[i+14])
		dst[i+15] = Hash64u32(src[i+15])
	}
}

func x64_u64_purego(src, dst []uint64) []uint64 {
	if len(src) == 0 {
		return dst[:0]
	}
	var i int
	sp := unsafe.Pointer(&src[0])
	rp := unsafe.Pointer(&dst[0])
	for range len(src) / 128 {
		s := (*[128]uint64)(unsafe.Add(sp, i*8))
		r := (*[128]uint64)(unsafe.Add(rp, i*8))
		x64_u64_core(s, r)
		i += 128
	}
	for i < len(src) {
		dst[i] = Hash64u64(src[i])
		i++
	}
	return dst
}

func x64_u64_core(src, dst *[128]uint64) {
	for i := 0; i < len(src); i += 16 {
		dst[0] = Hash64u64(src[i])
		dst[i+1] = Hash64u64(src[i+1])
		dst[i+2] = Hash64u64(src[i+2])
		dst[i+3] = Hash64u64(src[i+3])
		dst[i+4] = Hash64u64(src[i+4])
		dst[i+5] = Hash64u64(src[i+5])
		dst[i+6] = Hash64u64(src[i+6])
		dst[i+7] = Hash64u64(src[i+7])

		dst[i+8] = Hash64u64(src[i+8])
		dst[i+9] = Hash64u64(src[i+9])
		dst[i+10] = Hash64u64(src[i+10])
		dst[i+11] = Hash64u64(src[i+11])
		dst[i+12] = Hash64u64(src[i+12])
		dst[i+13] = Hash64u64(src[i+13])
		dst[i+14] = Hash64u64(src[i+14])
		dst[i+15] = Hash64u64(src[i+15])
	}
}
