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
	bitsetReverseLut256 [256]uint8
)

func init() {
	for i := range bitsetLookup {
		bitsetLookup[i] = uint8(bits.OnesCount8(uint8(i)))
		bitsetLeadingZeros[i] = bits.LeadingZeros8(uint8(i))
		bitsetReverseLut256[i] = uint8(((uint64(i) * 0x80200802) & 0x0884422110) * 0x0101010101 >> 32)
	}
}

func bitsetAndGeneric(dst, src []byte, size int) {
	for i, _ := range src {
		dst[i] &= src[i]
	}
	dst[len(dst)-1] &= bitmask(size)
}

func bitsetAndNotGeneric(dst, src []byte, size int) {
	for i, _ := range src {
		dst[i] &^= src[i]
	}
	dst[len(dst)-1] &= bitmask(size)
}

func bitsetOrGeneric(dst, src []byte, size int) {
	for i, _ := range src {
		dst[i] |= src[i]
	}
	dst[len(dst)-1] &= bitmask(size)
}

func bitsetXorGeneric(dst, src []byte, size int) {
	for i, _ := range src {
		dst[i] ^= src[i]
	}
	dst[len(dst)-1] &= bitmask(size)
}

func bitsetNegGeneric(src []byte, size int) {
	for i, _ := range src {
		src[i] = ^src[i]
	}
	src[len(src)-1] &= bitmask(size)
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
	last := src[len(src)-1] & bitmask(size)
	cnt += int64(bitsetLookup[last])
	return cnt
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
	mask := byte(0xff) >> uint(offset)
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
	if pos := bitsetLeadingZeros[(^src[i])&(byte(0xff)>>uint((start&0x7)+1))]; pos < 8 {
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
