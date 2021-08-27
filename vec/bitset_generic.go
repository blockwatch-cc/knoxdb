// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"encoding/binary"
	"math/bits"
)

var (
	bitsetLookup        [256]uint8
	bitsetLeadingZeros  [256]int
	bitsetReverseLut256 []uint8 = make([]uint8, 256)
)

func init() {
	for i := range bitsetLookup {
		bitsetLookup[i] = uint8(bits.OnesCount8(uint8(i)))
		bitsetLeadingZeros[i] = bits.TrailingZeros8(uint8(i))
		bitsetReverseLut256[i] = uint8(((uint64(i) * 0x80200802) & 0x0884422110) * 0x0101010101 >> 32)
	}
}

func bitsetAndGeneric(dst, src []byte, size int) {
	l := (size + 7) >> 3
	for i := 0; i < l; i++ {
		dst[i] &= src[i]
	}
	dst[l-1] &= bytemask(size)
}

func bitsetAndGenericFlag1(dst, src []byte, size int) int {
	l := (size + 7) >> 3
	var res byte
	for i := 0; i < l; i++ {
		dst[i] &= src[i]
		res |= dst[i]
	}
	dst[l-1] &= bytemask(size)
	return int(res)
}

func bitsetAndGenericFlag2(dst, src []byte, size int) (int, int) {
	l := size >> 3
	var any byte
	var all byte = 0xff
	for i := 0; i < l; i++ {
		dst[i] &= src[i]
		any |= dst[i]
		all &= dst[i]
	}
	if size&0x03 != 0 {
		dst[l] &= src[l]
		dst[l] &= bytemask(size)
		any |= dst[l]
		all &= (dst[l] | ^bytemask(size))
	}
	if all != 0xff {
		all = 0
	}
	return int(any), int(all)
}

func bitsetAndNotGeneric(dst, src []byte, size int) {
	l := (size + 7) >> 3
	for i := 0; i < l; i++ {
		dst[i] &^= src[i]
	}
	dst[l-1] &= bytemask(size)
}

func bitsetOrGeneric(dst, src []byte, size int) {
	l := (size + 7) >> 3
	for i := 0; i < l; i++ {
		dst[i] |= src[i]
	}
	dst[l-1] &= bytemask(size)
}

func bitsetOrGenericFlag1(dst, src []byte, size int) int {
	l := (size + 7) >> 3
	var res byte
	for i := 0; i < l; i++ {
		dst[i] |= src[i]
		res |= dst[i]
	}
	dst[l-1] &= bytemask(size)
	return int(res)
}

func bitsetXorGeneric(dst, src []byte, size int) {
	l := (size + 7) >> 3
	for i := 0; i < l; i++ {
		dst[i] ^= src[i]
	}
	dst[l-1] &= bytemask(size)
}

func bitsetNegGeneric(src []byte, size int) {
	l := (size + 7) >> 3
	for i := 0; i < l; i++ {
		src[i] = ^src[i]
	}
	src[l-1] &= bytemask(size)
}

func bitsetPopCountGeneric(src []byte, size int) int64 {
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
	cnt += int64(bitsetLookup[last])
	return cnt
}

func bitsetIndexesGeneric(src []byte, size int, dst []uint32) int {
	if size > 0 {
		src[len(src)-1] &= bytemask(size)
	}
	var j int
	var i uint32 = 0xffffffff
	for _, b := range src {
		for k := 0; k < int(lengthTable[b]); k++ {
			dst[j] = decodeTable[int(b)<<3+k] + i
			j++
		}
		i += 8
	}
	return j
}

func bitsetIndexesGenericSkip16(src []byte, size int, dst []uint32) int {
	if size > 0 {
		src[len(src)-1] &= bytemask(size)
	}
	var j int
	var big int = (len(src) >> 1) << 1
	var i uint32 = 0xffffffff
	var l int
	for l = 0; l < big; l += 2 {
		if binary.BigEndian.Uint16(src[l:l+2]) == 0 {
			i += 16
			continue
		}
		for _, b := range src[l : l+2] {
			for k := 0; k < int(lengthTable[b]); k++ {
				dst[j] = decodeTable[int(b)<<3+k] + i
				j++
			}
			i += 8
		}
	}
	for _, b := range src[l:] {
		for k := 0; k < int(lengthTable[b]); k++ {
			dst[j] = decodeTable[int(b)<<3+k] + i
			j++
		}
		i += 8
	}
	return j
}

func bitsetIndexesGenericSkip64(src []byte, size int, dst []uint32) int {
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
			for k := 0; k < int(lengthTable[b]); k++ {
				dst[j] = decodeTable[int(b)<<3+k] + i
				j++
			}
			i += 8
		}
	}
	for _, b := range src[l:] {
		for k := 0; k < int(lengthTable[b]); k++ {
			dst[j] = decodeTable[int(b)<<3+k] + i
			j++
		}
		i += 8
	}
	return j
}

func bitsetRunGeneric(src []byte, index, size int) (int, int) {
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
		length += bitsetLeadingZeros[^src[i]]
		// corner-case overflow check
		if start+length > size {
			length = size - start
		}
	}

	return start, length
}

// converts trailing padding into leading padding!
func bitsetReverseGeneric(src []byte) {
	// reverse slice while reversing bytes
	for l, r := 0, len(src)-1; l < r; l, r = l+1, r-1 {
		src[l], src[r] = bitsetReverseLut256[src[r]], bitsetReverseLut256[src[l]]
	}
	// bit-reverse center element, if len is uneven
	if l := len(src); l&0x1 > 0 {
		l = l / 2
		src[l] = bitsetReverseLut256[src[l]]
	}
}
