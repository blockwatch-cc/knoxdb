// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package pack

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	ShiftAmount = [8]int{3, 4, 0, 5, 0, 0, 0, 6}
)

type DecodeFunc[T types.Integer] func(index int) T

func Decoder[T types.Integer](buf []byte, log2 int) DecodeFunc[T] {
	mask := uint64((1 << log2) - 1)
	bits := int(unsafe.Sizeof(T(0)) * 8)
	inBuff := util.FromByteSlice[T](buf)

	return func(index int) T {
		idx := index * log2
		codeword := idx >> ShiftAmount[bits>>3-1]

		shift := idx & (1<<bits - 1)
		if shift > bits {
			shift = shift - (codeword * bits)
		}
		pack := uint64(inBuff[codeword]) >> shift

		if diff := bits - shift; diff < log2 {
			pack |= uint64(inBuff[codeword+1]) << diff
		}

		pack &= mask

		return T(pack & mask)
	}
}

func Decode[T types.Integer](out []T, in []byte, log2 int, minv T) (int, error) {
	var pack uint64 // Current 64-bit word being unpacked
	var offset int  // Bit offset within the current word
	var inIdx int   // Index into the input byte slice
	var outIdx int  // Index into the output array
	var lost int    // must shift right next in word instead of left

	inBuff := util.FromByteSlice[T](in)

	mask := uint64((1 << log2) - 1) // Mask for b bits, e.g., b=3 -> 0b111
	bits := int(unsafe.Sizeof(T(0)) * 8)

	for outIdx = 0; outIdx < len(out); outIdx++ {
		// Ensure we have enough bits in pack
		for offset < log2 && inIdx < len(inBuff) {
			if lost > 0 {
				pack |= uint64(inBuff[inIdx]) >> (bits - offset - lost) &^ (1<<offset - 1)
				inIdx++
				offset += lost
				lost = 0
				if offset < log2 {
					pack |= uint64(inBuff[inIdx]) << offset
					lost = offset
					offset += bits - offset
				}
			} else {
				pack |= uint64(inBuff[inIdx]) << offset
				lost = offset
				inIdx += util.Bool2int(offset == 0)
				offset += bits - offset
			}
		}

		// Extract b bits from pack
		out[outIdx] = T(pack&mask) + minv
		pack >>= log2
		offset -= log2
	}

	return outIdx, nil
}
