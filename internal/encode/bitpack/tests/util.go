// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"math/bits"

	"blockwatch.cc/knoxdb/internal/types"
)

// private pack func used in tests, confirmed correct
func pack(buf []byte, index, log2 int, val uint64) {
	// shift
	shift := (64 - log2) & 7 * (index + 1) & 7
	mask := uint64((1 << log2) - 1)

	// output position
	pos := (index * log2) >> 3

	// most significant byte
	msb := (log2 + shift - 1) >> 3

	// some large values >=59 bit do not fit 8 bytes
	if msb == 8 {
		// mask out extra bits
		val &= mask

		// merge top byte
		buf[pos] |= byte(val >> (64 - shift))

		// shift for correct remaining byte positions
		val <<= shift

		// write non-overlapping bytes
		for i := msb; i > 0; i-- {
			buf[pos+i] = byte(val)
			val >>= 8
		}

	} else {
		// mask & shift value
		val &= mask
		val <<= shift

		// write non-overlapping bytes
		for i := msb; i > 0; i-- {
			buf[pos+i] = byte(val)
			val >>= 8
		}

		// merge top byte
		buf[pos] |= byte(val)
	}
}

// private unpack func used in tests, confirmed correct
func unpack(buf []byte, index, log2 int) uint64 {
	// output shift
	shift := (64 - log2) & 7 * (index + 1) & 7
	mask := uint64((1 << log2) - 1)

	// input position
	pos := (index * log2) >> 3

	// most significant byte
	msb := (log2 + shift - 1) >> 3

	// assemble value, handle uint64 >= 59bit which do not fit 64 bit during assembly
	if msb == 8 {
		// some >= 59bit values occupy 9 bytes
		var val uint64
		for i := 1; i <= msb; i++ {
			val <<= 8
			val += uint64(buf[pos+i])
		}

		// shift into position
		val >>= shift

		// patch top byte
		val |= uint64(buf[pos]) << (64 - shift)

		return val & mask
	} else {
		// regular values
		var val uint64
		for i := 0; i <= msb; i++ {
			val <<= 8
			val += uint64(buf[pos+i])
		}

		// shift and mask output
		return (val >> shift) & mask
	}
}

func encode[T types.Integer](buf []byte, vals []T, minv, maxv T) ([]byte, int, error) {
	log2 := bits.Len64(uint64(maxv - minv))
	for i, v := range vals {
		pack(buf, i, log2, uint64(v-minv))
	}
	return buf, log2, nil
}

func decode[T types.Unsigned](dst []T, src []byte, log2 int, minv T) (int, error) {
	for i := range dst {
		dst[i] = T(unpack(src, i, log2)) + minv
	}
	return len(dst), nil
}
