// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"encoding/binary"
	"math/bits"
)

func And(dst, src []byte, size int) {
	l := (size + 7) >> 3
	for i := 0; i < l; i++ {
		dst[i] &= src[i]
	}
	dst[l-1] &= bytemask(size)
}

func AndFlag(dst, src []byte, size int) (bool, bool) {
	l := size >> 3
	var any byte
	var all byte = 0xff
	for i := 0; i < l; i++ {
		dst[i] &= src[i]
		any |= dst[i]
		all &= dst[i]
	}
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
	for i := 0; i < l; i++ {
		dst[i] &^= src[i]
	}
	dst[l-1] &= bytemask(size)
}

func Or(dst, src []byte, size int) {
	l := (size + 7) >> 3
	for i := 0; i < l; i++ {
		dst[i] |= src[i]
	}
	dst[l-1] &= bytemask(size)
}

func OrFlag(dst, src []byte, size int) (bool, bool) {
	l := size >> 3
	var any byte
	var all byte = 0xff
	for i := 0; i < l; i++ {
		dst[i] |= src[i]
		any |= dst[i]
		all &= dst[i]
	}
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
	for i := 0; i < l; i++ {
		dst[i] ^= src[i]
	}
	dst[l-1] &= bytemask(size)
}

func Neg(src []byte, size int) {
	l := (size + 7) >> 3
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
	for i := 0; i < (len(src)-1)/8; i++ {
		v := binary.LittleEndian.Uint64(src[i*8 : i*8+8])
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
	var l int
	for l = 0; l < big; l += 8 {
		if binary.BigEndian.Uint64(src[l:l+8]) == 0 {
			i += 64
			continue
		}
		for _, b := range src[l : l+8] {
			for k := 0; k < int(LengthTable[b]); k++ {
				dst[j] = DecodeTable[int(b)<<3+k] + i
				j++
			}
			i += 8
		}
	}
	for _, b := range src[l:] {
		for k := 0; k < int(LengthTable[b]); k++ {
			dst[j] = DecodeTable[int(b)<<3+k] + i
			j++
		}
		i += 8
	}
	return j
}

func Run(src []byte, index, size int) (int, int) {
	if len(src) == 0 || index < 0 || index >= size {
		return -1, 0
	}
	var start, length int
	i := index >> 3

	// mask leading bits of the first byte
	offset := index & 0x7
	mask := byte(0xff) << uint(offset)
	first := src[i] & mask
	if first > 0 {
		// start is in same byte as index
		start = index - offset + LeadingZeros[first]
		length = -LeadingZeros[first]
	} else {
		// find next 1 bit
		i++

		// skip 8 bytes per loop
		for j, l := 0, (len(src)-i)/8; j < l; j++ {
			v := binary.LittleEndian.Uint64(src[i : i+8])
			if v > 0 {
				break
			}
			i += 8
		}

		// skip single byte
		for ; i < len(src) && src[i] == 0; i++ {
		}

		// no more one's
		if i == len(src) {
			return -1, 0
		}
		start = i<<3 + LeadingZeros[src[i]]
		length = -LeadingZeros[src[i]]
	}

	// find next 0 bit beginning at 'start' position in the current byte:
	// we first negate the byte to reuse the LeadingZeros lookup table,
	// then mask out leading bits before and including the start position, and
	// finally lookup the number of unmasked leading zeros; if there is any bit
	// set to one (remember, that's a negated zero bit) the run ends in the same
	// byte where it started.
	if pos := LeadingZeros[(^src[i])&(byte(0xff)<<uint((start&0x7)+1))]; pos < 8 {
		length += pos
		return start, length
	}

	// now that the start byte is processed, we continue scan in the
	// remainder of the bitset
	i++
	length += 8

	// skip 8 bytes per loop
	for j, l := 0, (len(src)-i)/8; j < l; j++ {
		v := binary.LittleEndian.Uint64(src[i : i+8])
		if v < 0xffffffffffffffff {
			break
		}
		i += 8
		length += 64
	}

	// skip single byte
	for ; i < len(src) && src[i] == 0xff; i++ {
		length += 8
	}

	// rewind when we've moved past slice end
	if i == len(src) {
		i--
	}

	// count trailing one bits
	if src[i] != 0xff {
		length += LeadingZeros[^src[i]]
	}

	// corner-case overflow check
	if start+length > size {
		length = size - start
	}

	return start, length
}

// converts trailing padding into leading padding!
func Reverse(src []byte) {
	// reverse slice while reversing bytes
	for l, r := 0, len(src)-1; l < r; l, r = l+1, r-1 {
		src[l], src[r] = ReverseLut256[src[r]], ReverseLut256[src[l]]
	}
	// bit-reverse center element, if len is uneven
	if l := len(src); l&0x1 > 0 {
		l = l / 2
		src[l] = ReverseLut256[src[l]]
	}
}
