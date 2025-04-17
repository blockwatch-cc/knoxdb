// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package bitpack

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	ShiftAmount = [8]int{3, 4, 0, 5, 0, 0, 0, 6}
)

type DecodeFunc[T types.Integer] func(index int) T

type Decoder[T types.Integer] struct {
	buf  []byte
	log2 int
	minv T
}

func NewDecoder[T types.Integer](buf []byte, log2 int, minv T) *Decoder[T] {
	return &Decoder[T]{
		buf:  buf,
		log2: log2,
		minv: minv,
	}
}

func (d *Decode) Decode(dst []T) int {
	n, _ := Decode[T](dst, d.buf, d.log2, d.minv)
	return n
}

func (d *Decode) DecodeValue(index int) T {
	idx := index * d.log2
	codeword := idx >> ShiftAmount[bits>>3-1]

	shift := idx & (1<<bits - 1)
	if shift > bits {
		shift = shift - (codeword * bits)
	}
	pack := uint64(d.buf[codeword]) >> shift

	if diff := bits - shift; diff < log2 {
		pack |= uint64(d.buf[codeword+1]) << diff
	}

	return T(pack&mask) + d.minv
}

func (d *Decode) DecodeChunk(dst *[128]T, ofs int) int {
	// maxlen := len(d.buf)*8/d.log2
	// if ofs >= maxlen {
	// 	return 0
	// }
	// start :=
	return 0
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
