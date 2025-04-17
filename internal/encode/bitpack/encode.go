// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package bitpack

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

const (
	BitPackingBlockSize8  = 32
	BitPackingBlockSize16 = 64
	BitPackingBlockSize32 = 128
	BitPackingBlockSize64 = 256
)

func Encode[T types.Integer](buf []byte, vals []T, minv, maxv T) ([]byte, int, error) {
	var n, log2 int
	var err error
	switch any(T(0)).(type) {
	case uint8:
		n, log2, err = Bitpack8(util.ReinterpretSlice[T, uint8](vals), buf, uint8(minv), uint8(maxv))

	case uint16:
		n, log2, err = Bitpack16(util.ReinterpretSlice[T, uint16](vals), buf, uint16(minv), uint16(maxv))

	case uint32:
		n, log2, err = Bitpack32(util.ReinterpretSlice[T, uint32](vals), buf, uint32(minv), uint32(maxv))

	case uint64:
		n, log2, err = Bitpack64(util.ReinterpretSlice[T, uint64](vals), buf, uint64(minv), uint64(maxv))

	case int8:
		n, log2, err = Bitpack8(util.ReinterpretSlice[T, int8](vals), buf, int8(minv), int8(maxv))

	case int16:
		n, log2, err = Bitpack16(util.ReinterpretSlice[T, int16](vals), buf, int16(minv), int16(maxv))

	case int32:
		n, log2, err = Bitpack32(util.ReinterpretSlice[T, int32](vals), buf, int32(minv), int32(maxv))

	case int64:
		n, log2, err = Bitpack64(util.ReinterpretSlice[T, int64](vals), buf, int64(minv), int64(maxv))
	}
	if err != nil {
		return nil, 0, err
	}

	return buf[:n], log2, nil
}

func Bitpack8[T int8 | uint8](src []T, dst []byte, minv, maxv T) (int, int, error) {
	in := src
	out := util.FromByteSlice[uint8](dst)
	log2 := types.Log2Range(minv, maxv)
	blockN := len(in) / BitPackingBlockSize8
	if blockN == 0 {
		// input less than block size, use generic encoder
		n, err := encode(dst, in, log2, minv)
		return len(n), log2, err
	}

	var outpos int

	const groupSize = BitPackingBlockSize8 / 4
	for blockI := range blockN {
		i := blockI * BitPackingBlockSize8
		group1 := in[i+0*groupSize : i+1*groupSize]
		group2 := in[i+1*groupSize : i+2*groupSize]
		group3 := in[i+2*groupSize : i+3*groupSize]
		group4 := in[i+3*groupSize : i+4*groupSize]

		// write groups (4 x 8 packed inputs)
		bitpack8(minv, group1, out[outpos:], log2)
		outpos += log2
		bitpack8(minv, group2, out[outpos:], log2)
		outpos += log2
		bitpack8(minv, group3, out[outpos:], log2)
		outpos += log2
		bitpack8(minv, group4, out[outpos:], log2)
		outpos += log2
	}

	// tail loop
	n, err := encode(dst[outpos:], in[blockN*BitPackingBlockSize8:], log2, minv)
	if err != nil {
		return 0, log2, err
	}

	return outpos + len(n), log2, err
}

func Bitpack16[T int16 | uint16](src []T, dst []byte, minv, maxv T) (int, int, error) {
	in := src
	out := util.FromByteSlice[uint16](dst)
	log2 := types.Log2Range(minv, maxv)
	blockN := len(in) / BitPackingBlockSize16
	if blockN == 0 {
		// input less than block size, use generic encoder
		n, err := encode(dst, in, log2, minv)
		return len(n), log2, err
	}

	var outpos int

	const groupSize = BitPackingBlockSize16 / 4
	for blockI := range blockN {
		i := blockI * BitPackingBlockSize16
		group1 := in[i+0*groupSize : i+1*groupSize]
		group2 := in[i+1*groupSize : i+2*groupSize]
		group3 := in[i+2*groupSize : i+3*groupSize]
		group4 := in[i+3*groupSize : i+4*groupSize]

		// write groups (4 x 16 packed inputs)
		bitpack16(minv, group1, out[outpos:], log2)
		outpos += log2
		bitpack16(minv, group2, out[outpos:], log2)
		outpos += log2
		bitpack16(minv, group3, out[outpos:], log2)
		outpos += log2
		bitpack16(minv, group4, out[outpos:], log2)
		outpos += log2
	}

	// tail loop
	n, err := encode(dst[outpos*2:], in[blockN*BitPackingBlockSize16:], log2, minv)
	if err != nil {
		return 0, log2, err
	}

	return outpos*2 + len(n), log2, err
}

func Bitpack32[T int32 | uint32](src []T, dst []byte, minv, maxv T) (int, int, error) {
	in := src
	out := util.FromByteSlice[uint32](dst)
	log2 := types.Log2Range(minv, maxv)
	blockN := len(in) / BitPackingBlockSize32
	if blockN == 0 {
		// input less than block size, use generic encoder
		n, err := encode(dst, in, log2, minv)
		return len(n), log2, err
	}

	var outpos int

	const groupSize = BitPackingBlockSize32 / 4
	for blockI := range blockN {
		i := blockI * BitPackingBlockSize32
		group1 := in[i+0*groupSize : i+1*groupSize]
		group2 := in[i+1*groupSize : i+2*groupSize]
		group3 := in[i+2*groupSize : i+3*groupSize]
		group4 := in[i+3*groupSize : i+4*groupSize]

		// write groups (4 x 32 packed inputs)
		bitpack32(minv, group1, out[outpos:], log2)
		outpos += log2
		bitpack32(minv, group2, out[outpos:], log2)
		outpos += log2
		bitpack32(minv, group3, out[outpos:], log2)
		outpos += log2
		bitpack32(minv, group4, out[outpos:], log2)
		outpos += log2
	}

	// tail loop
	n, err := encode(dst[outpos*4:], in[blockN*BitPackingBlockSize32:], log2, minv)
	if err != nil {
		return 0, log2, err
	}

	return outpos*4 + len(n), log2, err
}

func Bitpack64[T int64 | uint64](src []T, dst []byte, minv, maxv T) (int, int, error) {
	in := src
	out := util.FromByteSlice[uint64](dst)
	log2 := types.Log2Range(minv, maxv)
	blockN := len(in) / BitPackingBlockSize64
	if blockN == 0 {
		// input less than block size, use generic encoder
		n, err := encode(dst, in, log2, minv)
		return len(n), log2, err
	}

	var outpos int

	const groupSize = BitPackingBlockSize64 / 4
	for blockI := range blockN {
		i := blockI * BitPackingBlockSize64
		group1 := in[i+0*groupSize : i+1*groupSize]
		group2 := in[i+1*groupSize : i+2*groupSize]
		group3 := in[i+2*groupSize : i+3*groupSize]
		group4 := in[i+3*groupSize : i+4*groupSize]

		// write groups (4 x 64 packed inputs)
		bitpack64(minv, group1, out[outpos:], log2)
		outpos += log2
		bitpack64(minv, group2, out[outpos:], log2)
		outpos += log2
		bitpack64(minv, group3, out[outpos:], log2)
		outpos += log2
		bitpack64(minv, group4, out[outpos:], log2)
		outpos += log2
	}

	// tail loop
	n, err := encode(dst[outpos*8:], in[blockN*BitPackingBlockSize64:], log2, minv)
	if err != nil {
		return 0, log2, err
	}

	return outpos*8 + len(n), log2, err
}

func encode[T types.Integer](buf []byte, vals []T, log2 int, minv T) ([]byte, error) {
	var pack uint64            // Accumulator for packed bits
	var offset int             // Bit offset in the current 64-bit word
	bufIdx := 0                // Index into the output buffer
	mask := T((1 << log2) - 1) // e.g., b=3 -> mask=0b111

	buffer := util.FromByteSlice[T](buf)
	bits := int(unsafe.Sizeof(T(0)) * 8)

	for i := 0; i < len(vals); i++ {
		pack |= uint64((vals[i]-minv)&mask) << offset
		offset += log2

		if offset >= bits { // If we've filled a 64-bit word
			buffer[bufIdx] = T(pack) // Write to buffer

			bufIdx++
			offset -= bits // Reset offset
			// Carry over any remaining bits if b > (64 - previous offset)
			if offset > 0 {
				pack = uint64((vals[i]-minv)&mask) >> (log2 - offset)
			} else {
				pack = 0
			}
		}
	}

	if offset > 0 { // Write any remaining bits
		buffer[bufIdx] = T(pack)
		bufIdx++
	}

	return buf[:bufIdx*bits/8], nil
}
