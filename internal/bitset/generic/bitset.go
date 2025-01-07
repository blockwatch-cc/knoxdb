// Copyright (c) 2020-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"encoding/binary"
	"math/bits"

	"blockwatch.cc/knoxdb/pkg/util"
)

func And(dst, src []byte, size int) {
	l := (size + 7) >> 3

	// bounds check elimination
	_ = dst[l-1]
	_ = src[l-1]

	for i := 0; i < l; i++ {
		dst[i] &= src[i]
	}
	dst[l-1] &= bytemask(size)
}

func AndFlag(dst, src []byte, size int) (bool, bool) {
	l := size >> 3
	var any byte
	var all byte = 0xff

	// bounds check elimination
	if l > 0 {
		_ = dst[l-1]
		_ = src[l-1]

		for i := 0; i < l; i++ {
			dst[i] &= src[i]
			any |= dst[i]
			all &= dst[i]
		}
	}

	// tail
	if size&0x07 != 0 {
		dst[l] &= src[l]
		dst[l] &= bytemask(size)
		any |= dst[l]
		all &= (dst[l] | ^bytemask(size))
	}
	return any != 0, all == 0xff
}

func AndNot(dst, src []byte, size int) {
	l := (size + 7) >> 3

	// bounds check elimination
	_ = dst[l-1]
	_ = src[l-1]

	for i := 0; i < l; i++ {
		dst[i] &^= src[i]
	}
	dst[l-1] &= bytemask(size)
}

func Or(dst, src []byte, size int) {
	l := (size + 7) >> 3

	// bounds check elimination
	_ = dst[l-1]
	_ = src[l-1]

	for i := 0; i < l; i++ {
		dst[i] |= src[i]
	}
	dst[l-1] &= bytemask(size)
}

func OrFlag(dst, src []byte, size int) (bool, bool) {
	l := size >> 3
	var any byte
	var all byte = 0xff

	if l > 0 {
		// bounds check elimination
		_ = dst[l-1]
		_ = src[l-1]

		for i := 0; i < l; i++ {
			dst[i] |= src[i]
			any |= dst[i]
			all &= dst[i]
		}
	}

	// tail
	if size&0x07 != 0 {
		dst[l] |= src[l]
		dst[l] &= bytemask(size)
		any |= dst[l]
		all &= (dst[l] | ^bytemask(size))
	}
	return any != 0, all == 0xff
}

func Xor(dst, src []byte, size int) {
	l := (size + 7) >> 3

	// bounds check elimination
	_ = dst[l-1]
	_ = src[l-1]

	for i := 0; i < l; i++ {
		dst[i] ^= src[i]
	}
	dst[l-1] &= bytemask(size)
}

func Neg(src []byte, size int) {
	l := (size + 7) >> 3

	// bounds check elimination
	_ = src[l-1]

	for i := 0; i < l; i++ {
		src[i] = ^src[i]
	}
	src[l-1] &= bytemask(size)
}

func PopCount(src []byte, size int) int64 {
	if len(src) == 0 {
		return 0
	}

	var cnt int64

	// process 8 bytes per loop, byte order doesn't matter (Intel maybe faster)
	for _, v := range util.ByteSliceSliceAsUint64Slice(src[:(size-1)>>3]) {
		cnt += int64(bits.OnesCount64(v))
	}

	// process remaining bytes individually, except the last
	for i := (len(src) - 1) &^ 0x7; i < len(src)-1; i++ {
		cnt += int64(bits.OnesCount8(src[i]))
	}

	// process the last byte by masking leading bits according to size
	last := src[len(src)-1] & bytemask(size)
	cnt += int64(Counts[last])
	return cnt
}

func Indexes(src []byte, size int, dst []uint32) int {
	if size > 0 {
		src[len(src)-1] &= bytemask(size)
	}
	var j int
	var big int = (len(src) >> 3) << 3
	var i uint32 = 0xffffffff
	var n int
	for n = 0; n < big; n += 8 {
		if binary.BigEndian.Uint64(src[n:n+8]) == 0 {
			i += 64
			continue
		}
		for _, b := range src[n : n+8] {
			for k, l := 0, int(LengthTable[b]); k < l; k++ {
				dst[j] = DecodeTable[int(b)<<3+k] + i
				j++
			}
			i += 8
		}
	}
	for _, b := range src[n:] {
		for k, l := 0, int(LengthTable[b]); k < l; k++ {
			dst[j] = DecodeTable[int(b)<<3+k] + i
			j++
		}
		i += 8
	}
	return j
}

func MinMax(src []byte, size int) (minIdx int, maxIdx int) {
	// ensure last byte is clean
	if size > 0 {
		src[len(src)-1] &= bytemask(size)
	}

	// find the first non-zero byte
	var sz8 int = (len(src) >> 3) << 3
	var n int

	// skip leading zeros
	for n < sz8 && binary.BigEndian.Uint64(src[n:n+8]) == 0 {
		n += 8
	}
	for n < len(src) && src[n] == 0 {
		n++
	}
	if n >= len(src) {
		minIdx, maxIdx = -1, -1
		return
	}

	// read trailing bits (note that the layout inside bytes
	// is in reverse order)
	minIdx = n*8 + bits.TrailingZeros8(src[n])

	// find the last non-zero byte
	for i := len(src) - 1; i >= n; i-- {
		tz := bits.LeadingZeros8(src[i])
		if tz == 8 {
			continue
		}
		maxIdx = i*8 + 7 - tz
		break
	}

	return
}
