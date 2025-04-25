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

func encode[T types.Integer](buf []byte, vals []T, minv, maxv T) ([]byte, int) {
	log2 := bits.Len64(uint64(maxv - minv))
	for i, v := range vals {
		pack(buf, i, log2, uint64(v-minv))
	}
	return buf, log2
}

func decode[T types.Integer](dst []T, src []byte, log2 int, minv T) (int, error) {
	dec := Unpack[T](src, log2, minv)
	for i := range dst {
		dst[i] = dec(i)
	}
	return len(dst), nil
}
