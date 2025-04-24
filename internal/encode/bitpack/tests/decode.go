// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package tests

import (
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

func Decoder[T types.Integer](buf []byte, log2 int) DecodeIndex[T] {
	mask := uint64((1 << log2) - 1)
	bits := 64
	inBuff := util.FromByteSlice[uint64](buf)
	return func(index int) T {
		idx := index * log2
		codeword := idx >> 6
		shift := idx & 63
		pack := inBuff[codeword] >> shift
		if diff := bits - shift; diff < log2 && codeword+1 < len(inBuff) {
			pack |= inBuff[codeword+1] << diff
		}
		return T(pack & mask)
	}
}

// private unpack func used in tests, confirmed correct
func Unpack[T types.Integer](buf []byte, log2 int) DecodeIndex[T] {
	mask := uint64((1 << log2) - 1)

	return func(index int) T {
		// output shift
		shift := (64 - log2) & 7 * (index + 1) & 7

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

			return T(val & mask)
		} else {
			// regular values
			var val uint64
			for i := 0; i <= msb; i++ {
				val <<= 8
				val += uint64(buf[pos+i])
			}

			// shift and mask output
			return T((val >> shift) & mask)
		}
	}
}
