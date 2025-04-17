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
	buf   []byte
	log2  int
	bits  int
	shift int
	rmask uint64 // read mask (to hide sign extension on signed code words)
	vmask uint64 // value mask
	minv  T
}

func NewDecoder[T types.Integer](buf []byte, log2 int, minv T) *Decoder[T] {
	w := util.SizeOf[T]() * 8
	return &Decoder[T]{
		buf:   buf,
		log2:  log2,
		bits:  w,
		shift: ShiftAmount[w>>3-1],
		rmask: uint64(1<<w - 1),
		vmask: uint64((1 << log2) - 1),
		minv:  minv,
	}
}

// TODO: use fast decode kernels
func (d *Decoder[T]) Decode(dst []T) int {
	n, _ := Decode[T](dst, d.buf, d.log2, d.minv)
	return n
}

func (d *Decoder[T]) DecodeValue(index int) T {
	if d.log2 == 0 {
		return d.minv
	}
	idx := index * d.log2
	pos := idx >> d.shift
	shift := idx & (d.bits - 1)

	cbuf := util.FromByteSlice[T](d.buf)
	word := uint64(cbuf[pos]) & d.rmask >> shift
	if diff := d.bits - shift; diff < d.log2 {
		word |= uint64(cbuf[pos+1]) << diff
	}

	return T(word&d.vmask) + d.minv
}

// TODO: use fast decode kernels
// ofs must be a multiple of 128!
func (d *Decoder[T]) DecodeChunk(dst *[128]T, ofs int) int {
	maxn := len(d.buf) * 8 / d.log2
	if ofs >= maxn {
		return 0
	}
	n := min(128, maxn-ofs)
	startpos := d.log2 * (ofs >> 3)
	endpos := min(startpos+d.log2*16, len(d.buf)) // for chunk-size 128
	Decode[T](dst[:n], d.buf[startpos:endpos], d.log2, d.minv)
	return n
}

func Decode[T types.Integer](out []T, in []byte, log2 int, minv T) (int, error) {
	var pack uint64 // Current 64-bit word being unpacked
	var offset int  // Bit offset within the current word
	var inIdx int   // Index into the input byte slice
	var outIdx int  // Index into the output array
	var lost int    // must shift right next in word instead of left

	inBuff := util.FromByteSlice[T](in)

	bits := int(unsafe.Sizeof(T(0)) * 8)
	vmask := uint64((1 << log2) - 1) // Mask for b bits, e.g., b=3 -> 0b111
	rmask := uint64(1<<bits - 1)

	for outIdx = 0; outIdx < len(out); outIdx++ {
		// Ensure we have enough bits in pack
		for offset < log2 && inIdx < len(inBuff) {
			if lost > 0 {
				pack |= uint64(inBuff[inIdx]) & rmask >> (bits - offset - lost) &^ (1<<offset - 1)
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
		out[outIdx] = T(pack&vmask) + minv
		pack >>= log2
		offset -= log2
	}

	return outIdx, nil
}
