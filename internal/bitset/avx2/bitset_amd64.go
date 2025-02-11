// Copyright (c) 2020-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx2

import (
	"blockwatch.cc/knoxdb/internal/bitset/generic"
)

// ASM imports

//go:noescape
func bit_and(dst, src []byte)

//go:noescape
func bit_and_flag(dst, src []byte) (bool, bool)

//go:noescape
func bit_and_not(dst, src []byte)

//go:noescape
func bit_or(dst, src []byte)

//go:noescape
func bit_or_flag(dst, src []byte) (bool, bool)

//go:noescape
func bit_xor(dst, src []byte)

//go:noescape
func bit_neg(src []byte)

//go:noescape
func popcount(src []byte) int64

//go:noescape
func bit_idx_skip(bitmap []byte, out []uint32, decodeTable []uint32, lengthTable []uint8) int

// Go imports
var (
	counts      = generic.Counts
	lengthTable = generic.LengthTable
	decodeTable = generic.DecodeTable
	bitFieldLen = generic.BitFieldLen
	bytemask    = generic.Bytemask
)

// Go exports
func And(dst, src []byte, size int) {
	bit_and(dst, src)
	dst[len(dst)-1] &= bytemask(size)
}

func AndFlag(dst, src []byte, size int) (bool, bool) {
	l := size >> 3
	any, all := bit_and_flag(dst[:l], src[:l])
	if size&0x07 != 0 {
		dst[l] &= src[l]
		dst[l] &= bytemask(size)
		any = any || dst[l] != 0
		if dst[l] != bytemask(size) {
			all = false
		}
	}
	return any, all
}

func AndNot(dst, src []byte, size int) {
	bit_and_not(dst, src)
	dst[len(dst)-1] &= bytemask(size)
}

func Or(dst, src []byte, size int) {
	bit_or(dst, src)
	dst[len(dst)-1] &= bytemask(size)
}

func OrFlag(dst, src []byte, size int) (bool, bool) {
	l := size >> 3
	any, all := bit_or_flag(dst[:l], src[:l])
	if size&0x07 != 0 {
		dst[l] |= src[l]
		dst[l] &= bytemask(size)
		any = any || dst[l] != 0
		if dst[l] != bytemask(size) {
			all = false
		}
	}
	return any, all
}

func Xor(dst, src []byte, size int) {
	bit_xor(dst, src)
	dst[len(dst)-1] &= bytemask(size)
}

func Neg(src []byte, size int) {
	bit_neg(src)
	src[len(src)-1] &= bytemask(size)
}

func Indexes(src []byte, size int, dst []uint32) int {
	if size > 0 {
		src[len(src)-1] &= bytemask(size)
	}
	return bit_idx_skip(src, dst, decodeTable, lengthTable)
}

func PopCount(src []byte, size int) int64 {
	switch {
	case size == 0:
		return 0
	case size <= 8:
		return int64(counts[src[0]&bytemask(size)])
	case size&0x7 == 0:
		return popcount(src)
	default:
		cnt := popcount(src[:len(src)-1])
		return cnt + int64(counts[src[len(src)-1]&bytemask(size)])
	}
}
