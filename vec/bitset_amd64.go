// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package vec

import (
	"blockwatch.cc/knoxdb/util"
)

//go:noescape
func bitsetAndAVX2(dst, src []byte)

//go:noescape
func bitsetAndAVX2FlagCore(dst, src []byte) (bool, bool)

//go:noescape
func bitsetAndNotAVX2(dst, src []byte)

//go:noescape
func bitsetOrAVX2(dst, src []byte)

//go:noescape
func bitsetOrAVX2FlagCore(dst, src []byte) (bool, bool)

//go:noescape
func bitsetXorAVX2(dst, src []byte)

//go:noescape
func bitsetNegAVX2(src []byte)

//go:noescape
func bitsetReverseAVX2(src []byte, bitsetReverseLut256 []uint8)

//go:noescape
func bitsetPopCountAVX2(src []byte) int64

//go:noescape
func bitsetIndexesAVX2FullCore(bitmap []byte, out []uint32, decodeTable []uint32, lengthTable []uint8) int

//go:noescape
func bitsetIndexesAVX2SkipCore(bitmap []byte, out []uint32, decodeTable []uint32, lengthTable []uint8) int

//go:noescape
func bitsetNextOneBitAVX2(src []byte, index uint64) uint64

//go:noescape
func bitsetNextZeroBitAVX2(src []byte, index uint64) uint64

func bitsetAnd(dst, src []byte, size int) {
	switch {
	case util.UseAVX2:
		bitsetAndAVX2(dst, src)
		dst[len(dst)-1] &= bytemask(size)
	default:
		bitsetAndGeneric(dst, src, size)
	}
}

func bitsetAndFlag(dst, src []byte, size int) (bool, bool) {
	switch {
	case util.UseAVX2:
		return bitsetAndAVX2Flag(dst, src, size)
	default:
		return bitsetAndGenericFlag(dst, src, size)
	}
}

func bitsetAndNot(dst, src []byte, size int) {
	switch {
	case util.UseAVX2:
		bitsetAndNotAVX2(dst, src)
		dst[len(dst)-1] &= bytemask(size)
	default:
		bitsetAndNotGeneric(dst, src, size)
	}
}

func bitsetOr(dst, src []byte, size int) {
	switch {
	case util.UseAVX2:
		bitsetOrAVX2(dst, src)
		dst[len(dst)-1] &= bytemask(size)
	default:
		bitsetOrGeneric(dst, src, size)
	}
}

func bitsetOrFlag(dst, src []byte, size int) (bool, bool) {
	switch {
	case util.UseAVX2:
		return bitsetOrAVX2Flag(dst, src, size)
	default:
		return bitsetOrGenericFlag(dst, src, size)
	}
}

func bitsetXor(dst, src []byte, size int) {
	switch {
	case util.UseAVX2:
		bitsetXorAVX2(dst, src)
		dst[len(dst)-1] &= bytemask(size)
	default:
		bitsetXorGeneric(dst, src, size)
	}
}

func bitsetNeg(src []byte, size int) {
	switch {
	case util.UseAVX2:
		bitsetNegAVX2(src)
		src[len(src)-1] &= bytemask(size)
	default:
		bitsetNegGeneric(src, size)
	}
}

func bitsetReverse(src []byte) {
	switch {
	case util.UseAVX2:
		bitsetReverseAVX2(src, bitsetReverseLut256)
	default:
		bitsetReverseGeneric(src)
	}
}

func bitsetIndexesAVX2Full(src []byte, size int, dst []uint32) int {
	if size > 0 {
		src[len(src)-1] &= bytemask(size)
	}
	return bitsetIndexesAVX2FullCore(src, dst, decodeTable, lengthTable)
}

func bitsetIndexesAVX2Skip(src []byte, size int, dst []uint32) int {
	if size > 0 {
		src[len(src)-1] &= bytemask(size)
	}
	return bitsetIndexesAVX2SkipCore(src, dst, decodeTable, lengthTable)
}

func bitsetIndexes(src []byte, size int, dst []uint32) int {
	switch {
	case util.UseAVX2:
		return bitsetIndexesAVX2Skip(src, size, dst)
	default:
		return bitsetIndexesGenericSkip64(src, size, dst)
	}
}

func bitsetPopCount(src []byte, size int) int64 {
	switch {
	case util.UseAVX2:
		switch true {
		case size == 0:
			return 0
		case size <= 8:
			return int64(bitsetLookup[src[0]&bytemask(size)])
		case size&0x7 == 0:
			return bitsetPopCountAVX2(src)
		default:
			cnt := bitsetPopCountAVX2(src[:len(src)-1])
			return cnt + int64(bitsetLookup[src[len(src)-1]&bytemask(size)])
		}
	default:
		return bitsetPopCountGeneric(src, size)
	}

}

func bitsetAndAVX2Flag(dst, src []byte, size int) (bool, bool) {
	l := size >> 3
	any, all := bitsetAndAVX2FlagCore(dst[:l], src[:l])
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

func bitsetOrAVX2Flag(dst, src []byte, size int) (bool, bool) {
	l := size >> 3
	any, all := bitsetOrAVX2FlagCore(dst[:l], src[:l])
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

func bitsetRun(src []byte, index, size int) (int, int) {
	switch {
	case util.UseAVX2:
		return bitsetRunAVX2Wrapper(src, index, size)
	default:
		return bitsetRunGeneric(src, index, size)
	}
}

func bitsetRunAVX2Wrapper(src []byte, index, size int) (int, int) {
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
		start = index - offset + bitsetLeadingZeros[first]
		length = -bitsetLeadingZeros[first]
	} else {
		// find next 1 bit
		i++

		// Note: function call overhead makes this perform only for large strides
		i = int(bitsetNextOneBitAVX2(src, uint64(i)))

		// no more one's
		if i == len(src) {
			return -1, 0
		}
		start = i<<3 + bitsetLeadingZeros[src[i]]
		length = -bitsetLeadingZeros[src[i]]
	}

	// find next 0 bit beginning at 'start' position in the current byte:
	// we first negate the byte to reuse the bitsetLeadingZeros lookup table,
	// then mask out leading bits before and including the start position, and
	// finally lookup the number of unmasked leading zeros; if there is any bit
	// set to one (remember, that's a negated zero bit) the run ends in the same
	// byte where it started.
	if pos := bitsetLeadingZeros[(^src[i])&(byte(0xff)<<uint((start&0x7)+1))]; pos < 8 {
		length += pos
		return start, length
	}

	// now that the start byte is processed, we continue scan in the
	// remainder of the bitset
	i++
	length += 8

	// Note: function call overhead makes this perform only for large strides
	j := int(bitsetNextZeroBitAVX2(src, uint64(i)))
	length += 8 * (j - i)
	i = j

	// rewind when we've moved past the slice end
	if i == len(src) {
		i--
	}

	// count trailing one bits
	if src[i] != 0xff {
		length += bitsetLeadingZeros[^src[i]]
		// corner-case overlow check
		if start+length > size {
			length = size - start
		}
	}

	return start, length
}
