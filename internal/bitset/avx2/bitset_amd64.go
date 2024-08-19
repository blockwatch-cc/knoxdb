// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

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
func bit_reverse(src []byte, reverseLut256 []uint8)

//go:noescape
func popcount(src []byte) int64

//go:noescape
func bit_idx_skip(bitmap []byte, out []uint32, decodeTable []uint32, lengthTable []uint8) int

//go:noescape
func bit_next_one(src []byte, index uint64) uint64

//go:noescape
func bit_next_zero(src []byte, index uint64) uint64

// Go imports
var (
	counts        = generic.Counts
	leadingZeros  = generic.LeadingZeros
	reverseLut256 = generic.ReverseLut256
	lengthTable   = generic.LengthTable
	decodeTable   = generic.DecodeTable
	bitFieldLen   = generic.BitFieldLen
	bytemask      = generic.Bytemask
	bitmask       = generic.Bitmask
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

func Reverse(src []byte) {
	bit_reverse(src, reverseLut256)
}

func Indexes(src []byte, size int, dst []uint32) int {
	if size > 0 {
		src[len(src)-1] &= bytemask(size)
	}
	return bit_idx_skip(src, dst, decodeTable, lengthTable)
}

func PopCount(src []byte, size int) int64 {
	switch true {
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

func Run(src []byte, index, size int) (int, int) {
	if len(src) == 0 || index < 0 || index >= size {
		return -1, 0
	}
	var (
		start  int = -1
		length int
	)
	i := index >> 3

	// mask leading bits of the first byte
	offset := index & 0x7
	mask := byte(0xff) << uint(offset)
	first := src[i] & mask
	if first > 0 {
		// start is in same byte as index
		start = index - offset + leadingZeros[first]
		length = -leadingZeros[first]
	} else {
		// find next 1 bit
		i++

		// Note: function call overhead makes this perform only for large strides
		i = int(bit_next_one(src, uint64(i)))

		// no more one's
		if i == len(src) {
			return -1, 0
		}
		start = i<<3 + leadingZeros[src[i]]
		length = -leadingZeros[src[i]]
		if start+length > size {
			length = size - start
		}
	}

	// find next 0 bit beginning at 'start' position in the current byte:
	// we first negate the byte to reuse the bitsetLeadingZeros lookup table,
	// then mask out leading bits before and including the start position, and
	// finally lookup the number of unmasked leading zeros; if there is any bit
	// set to one (remember, that's a negated zero bit) the run ends in the same
	// byte where it started.
	if pos := leadingZeros[(^src[i])&(byte(0xff)<<uint((start&0x7)+1))]; pos < 8 {
		length += pos
		return start, length
	}

	// now that the start byte is processed, we continue scan in the
	// remainder of the bitset
	i++
	length += 8

	// Note: function call overhead makes this perform only for large strides
	j := int(bit_next_zero(src, uint64(i)))
	length += 8 * (j - i)
	i = j

	// rewind when we've moved past the slice end
	if i == len(src) {
		i--
	}

	// count trailing one bits
	if src[i] != 0xff {
		length += leadingZeros[^src[i]]
	}
	// corner-case overflow check
	if start+length > size {
		length = size - start
	}

	return start, length
}
