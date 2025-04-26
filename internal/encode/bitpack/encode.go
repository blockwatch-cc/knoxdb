// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc,alex@blockwatch.cc

package bitpack

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

const (
	BitsSize  = 64 // we use 64bit code words
	BlockSize = 64 // we pack 64 elements at once
)

func Encode[T types.Integer](dst []byte, src []T, minv, maxv T) ([]byte, int) {
	var n, log2 int
	switch any(T(0)).(type) {
	case uint8:
		n, log2 = Bitpack8(dst, util.ReinterpretSlice[T, uint8](src), uint8(minv), uint8(maxv))
	case uint16:
		n, log2 = Bitpack16(dst, util.ReinterpretSlice[T, uint16](src), uint16(minv), uint16(maxv))
	case uint32:
		n, log2 = Bitpack32(dst, util.ReinterpretSlice[T, uint32](src), uint32(minv), uint32(maxv))
	case uint64:
		n, log2 = Bitpack64(dst, util.ReinterpretSlice[T, uint64](src), uint64(minv), uint64(maxv))
	case int8:
		n, log2 = Bitpack8(dst, util.ReinterpretSlice[T, int8](src), int8(minv), int8(maxv))
	case int16:
		n, log2 = Bitpack16(dst, util.ReinterpretSlice[T, int16](src), int16(minv), int16(maxv))
	case int32:
		n, log2 = Bitpack32(dst, util.ReinterpretSlice[T, int32](src), int32(minv), int32(maxv))
	case int64:
		n, log2 = Bitpack64(dst, util.ReinterpretSlice[T, int64](src), int64(minv), int64(maxv))
	}
	return dst[:n], log2
}

func Bitpack8[T int8 | uint8](dst []byte, src []T, minv, maxv T) (int, int) {
	out := util.FromByteSlice[uint64](dst)
	log2 := types.Log2Range(minv, maxv)
	blockN := len(src) / (4 * BlockSize)
	if blockN == 0 {
		// input less than block size, use generic encoder
		n := encode(out, src, log2, minv)
		return n, log2
	}

	outp := unsafe.Pointer(&out[0])
	inp := unsafe.Pointer(&src[0])
	nBlockInBytes := BlockSize
	nBlockOutBytes := log2 * 8

	// 4x loop unrolled packing 4x64 uint8 values into 4xlog2 64bit codewords
	for blockI := range blockN {
		i := blockI * nBlockInBytes * 4
		in1 := (*[BlockSize]uint8)(unsafe.Add(inp, i))
		in2 := (*[BlockSize]uint8)(unsafe.Add(inp, i+1*nBlockInBytes))
		in3 := (*[BlockSize]uint8)(unsafe.Add(inp, i+2*nBlockInBytes))
		in4 := (*[BlockSize]uint8)(unsafe.Add(inp, i+3*nBlockInBytes))
		o := blockI * nBlockOutBytes * 4
		out1 := unsafe.Add(outp, o)
		out2 := unsafe.Add(outp, o+1*nBlockOutBytes)
		out3 := unsafe.Add(outp, o+2*nBlockOutBytes)
		out4 := unsafe.Add(outp, o+3*nBlockOutBytes)

		// write groups (4 x 64 packed inputs)
		pack_u8[log2](in1, out1, uint64(minv))
		pack_u8[log2](in2, out2, uint64(minv))
		pack_u8[log2](in3, out3, uint64(minv))
		pack_u8[log2](in4, out4, uint64(minv))
	}
	outpos := blockN * 4 * log2

	// tail loop
	n := encode(out[outpos:], src[blockN*BlockSize*4:], log2, minv)

	// return output bytes written (64bit codewords)
	return outpos*8 + n, log2
}

func Bitpack16[T int16 | uint16](dst []byte, src []T, minv, maxv T) (int, int) {
	out := util.FromByteSlice[uint64](dst)
	log2 := types.Log2Range(minv, maxv)
	blockN := len(src) / (4 * BlockSize)
	if blockN == 0 {
		// input less than block size, use generic encoder
		n := encode(out, src, log2, minv)
		return n, log2
	}

	outp := unsafe.Pointer(&out[0])
	inp := unsafe.Pointer(&src[0])
	nBlockInBytes := BlockSize * 2
	nBlockOutBytes := log2 * 8

	// 4x loop unrolled packing 4x64 uint16 values into 4xlog2 64bit code words
	for blockI := range blockN {
		i := blockI * nBlockInBytes * 4
		in1 := (*[BlockSize]uint16)(unsafe.Add(inp, i))
		in2 := (*[BlockSize]uint16)(unsafe.Add(inp, i+1*nBlockInBytes))
		in3 := (*[BlockSize]uint16)(unsafe.Add(inp, i+2*nBlockInBytes))
		in4 := (*[BlockSize]uint16)(unsafe.Add(inp, i+3*nBlockInBytes))
		o := blockI * nBlockOutBytes * 4
		out1 := unsafe.Add(outp, o)
		out2 := unsafe.Add(outp, o+1*nBlockOutBytes)
		out3 := unsafe.Add(outp, o+2*nBlockOutBytes)
		out4 := unsafe.Add(outp, o+3*nBlockOutBytes)

		// write groups (4 x 64 packed inputs)
		pack_u16[log2](in1, out1, uint64(minv))
		pack_u16[log2](in2, out2, uint64(minv))
		pack_u16[log2](in3, out3, uint64(minv))
		pack_u16[log2](in4, out4, uint64(minv))
	}
	outpos := blockN * 4 * log2

	// tail loop
	n := encode(out[outpos:], src[blockN*BlockSize*4:], log2, minv)

	// return output bytes written (64bit codewords)
	return outpos*8 + n, log2
}

func Bitpack32[T int32 | uint32](dst []byte, src []T, minv, maxv T) (int, int) {
	out := util.FromByteSlice[uint64](dst)
	log2 := types.Log2Range(minv, maxv)
	blockN := len(src) / (4 * BlockSize)
	if blockN == 0 {
		// input less than block size, use generic encoder
		n := encode(out, src, log2, minv)
		return n, log2
	}

	outp := unsafe.Pointer(&out[0])
	inp := unsafe.Pointer(&src[0])
	nBlockInBytes := BlockSize * 4
	nBlockOutBytes := log2 * 8

	// 4x loop unrolled packing 4x64 uint32 values into 4xlog2 64bit codewords
	for blockI := range blockN {
		i := blockI * nBlockInBytes * 4
		in1 := (*[BlockSize]uint32)(unsafe.Add(inp, i))
		in2 := (*[BlockSize]uint32)(unsafe.Add(inp, i+1*nBlockInBytes))
		in3 := (*[BlockSize]uint32)(unsafe.Add(inp, i+2*nBlockInBytes))
		in4 := (*[BlockSize]uint32)(unsafe.Add(inp, i+3*nBlockInBytes))
		o := blockI * nBlockOutBytes * 4
		out1 := unsafe.Add(outp, o)
		out2 := unsafe.Add(outp, o+1*nBlockOutBytes)
		out3 := unsafe.Add(outp, o+2*nBlockOutBytes)
		out4 := unsafe.Add(outp, o+3*nBlockOutBytes)

		// write groups (4 x 64 packed inputs)
		pack_u32[log2](in1, out1, uint64(minv))
		pack_u32[log2](in2, out2, uint64(minv))
		pack_u32[log2](in3, out3, uint64(minv))
		pack_u32[log2](in4, out4, uint64(minv))
	}
	outpos := blockN * 4 * log2

	// tail loop
	n := encode(out[outpos:], src[blockN*BlockSize*4:], log2, minv)

	// return output bytes written (64bit codewords)
	return outpos*8 + n, log2
}

func Bitpack64[T int64 | uint64](dst []byte, src []T, minv, maxv T) (int, int) {
	out := util.FromByteSlice[uint64](dst)
	log2 := types.Log2Range(minv, maxv)
	blockN := len(src) / (4 * BlockSize)
	if blockN == 0 {
		// input less than block size, use generic encoder
		n := encode(out, src, log2, minv)
		return n, log2
	}

	outp := unsafe.Pointer(&out[0])
	inp := unsafe.Pointer(&src[0])
	nBlockInBytes := BlockSize * 8
	nBlockOutBytes := log2 * 8

	// 4x loop unrolled packing 4x64 uint64 values into 4xlog2 64bit codewords
	for blockI := range blockN {
		i := blockI * nBlockInBytes * 4
		in1 := (*[BlockSize]uint64)(unsafe.Add(inp, i))
		in2 := (*[BlockSize]uint64)(unsafe.Add(inp, i+1*nBlockInBytes))
		in3 := (*[BlockSize]uint64)(unsafe.Add(inp, i+2*nBlockInBytes))
		in4 := (*[BlockSize]uint64)(unsafe.Add(inp, i+3*nBlockInBytes))
		o := blockI * nBlockOutBytes * 4
		out1 := unsafe.Add(outp, o)
		out2 := unsafe.Add(outp, o+1*nBlockOutBytes)
		out3 := unsafe.Add(outp, o+2*nBlockOutBytes)
		out4 := unsafe.Add(outp, o+3*nBlockOutBytes)

		// write groups (4 x 64 packed inputs)
		pack_u64[log2](in1, out1, uint64(minv))
		pack_u64[log2](in2, out2, uint64(minv))
		pack_u64[log2](in3, out3, uint64(minv))
		pack_u64[log2](in4, out4, uint64(minv))
	}
	outpos := blockN * 4 * log2

	// tail loop
	n := encode(out[outpos:], src[blockN*BlockSize*4:], log2, minv)

	// return output bytes written (64bit codewords)
	return outpos*8 + n, log2
}

func encode[T types.Integer](dst []uint64, src []T, log2 int, minv T) int {
	var (
		word   uint64                    // Accumulator for packed bits
		offset int                       // Bit offset in the current 64-bit word
		n      int                       // Index into the output buffer
		mask   = uint64((1 << log2) - 1) // e.g., b=3 -> mask=0b111
	)

	for i := 0; i < len(src); i++ {
		word |= uint64((src[i] - minv)) & mask << offset
		offset += log2
		if offset >= BitsSize { // If we've filled a 64-bit word
			dst[n] = uint64(word) // Write to buffer

			n++
			offset -= BitsSize // Reset offset
			// Carry over any remaining bits if b > (64 - previous offset)
			if offset > 0 {
				word = uint64((src[i] - minv)) & mask >> (log2 - offset)
			} else {
				word = 0
			}
		}
	}

	if offset > 0 { // Write any remaining bits
		dst[n] = uint64(word)
		n++
	}
	return n * BitsSize / 8
}
