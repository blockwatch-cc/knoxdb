// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc,alex@blockwatch.cc

package bitpack

import (
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

const CHUNK_SIZE = types.CHUNK_SIZE // 128

type Decoder[T types.Integer] struct {
	src  []uint64
	log2 int
	len  int
	mask uint64
	minv T
}

func NewDecoder[T types.Integer](buf []byte, log2, n int, minv T) *Decoder[T] {
	d := newDecoder[T]()
	d.src = util.FromByteSlice[uint64](buf)
	d.log2 = log2
	d.len = n
	d.mask = uint64((1 << log2) - 1)
	d.minv = minv
	return d
}

func (d *Decoder[T]) Len() int {
	return d.len
}

func (d *Decoder[T]) Close() {
	d.src = nil
	d.log2 = 0
	d.len = 0
	d.mask = 0
	d.minv = 0
	putDecoder(d)
}

// Decode unpacks the full source vector into dst and returns the number of
// elements. Dst must have sufficient capacity.
func (d *Decoder[T]) Decode(dst []T) int {
	n, _ := Decode[T](dst[:d.len], util.ToByteSlice(d.src), d.log2, d.minv)
	return n
}

// DecodeValue unpacks a single value at position n where n must be within
// bounds [0, len]. Bounds checks are disabled for performance.
func (d *Decoder[T]) DecodeValue(n int) T {
	if d.log2 == 0 {
		return d.minv
	}
	idx := n * d.log2 // packed data start index in bits
	pos := idx >> 6   // code word position /64
	shift := idx & 63 // code word shift at this index

	// read code word
	word := d.src[pos] >> shift

	// mix bits when encoded data is split across code words
	if diff := 64 - shift; diff < d.log2 && pos+1 < len(d.src) {
		word |= d.src[pos+1] << diff
	}

	// mask value and undo min-FOR
	return T(word&d.mask) + d.minv
}

// DecodeChunk unpacks up to chunk size (128) values starting at position
// ofs. Ofs must be a multiple of 128 and within bounds. Returns the number
// of decoded values which is always chunk size unless at shorter tail ends.
func (d *Decoder[T]) DecodeChunk(dst *[CHUNK_SIZE]T, ofs int) int {
	if ofs >= d.len {
		return 0
	}
	n := min(CHUNK_SIZE, d.len-ofs)
	k := ofs >> 6 // = ofs/64 (must be multiple of CHUNK_SIZE)

	// take slow path for tails
	if n < CHUNK_SIZE {
		_, _ = decode(dst[:n], d.src[k*d.log2:], d.log2, d.minv)
		return n
	}

	// calculate source code word group offsets for chunks size 128
	// each group processes 64 elements, so we need 2 groups
	var group0, group1 unsafe.Pointer
	if d.log2 > 0 {
		group0 = unsafe.Pointer(&d.src[k*d.log2])
		group1 = unsafe.Pointer(&d.src[(k+1)*d.log2])
	}

	// fmt.Printf("BP: dec src1[%d:%d] src2[%d:%d] log2=%d ofs=%d k=%d\n ", k*d.log2, (k+1)*d.log2,
	// 	(k+1)*d.log2, (k+2)*d.log2, d.log2, ofs, k,
	// )

	// call the correct kernel
	switch any(T(0)).(type) {
	case uint8:
		d8 := util.ReinterpretSlice[T, uint8](dst[:])
		unpack_u8[d.log2]((*[64]uint8)(d8[:64]), group0, uint64(d.minv))
		unpack_u8[d.log2]((*[64]uint8)(d8[64:]), group1, uint64(d.minv))
	case uint16:
		d16 := util.ReinterpretSlice[T, uint16](dst[:])
		unpack_u16[d.log2]((*[64]uint16)(d16[:64]), group0, uint64(d.minv))
		unpack_u16[d.log2]((*[64]uint16)(d16[64:]), group1, uint64(d.minv))
	case uint32:
		d32 := util.ReinterpretSlice[T, uint32](dst[:])
		unpack_u32[d.log2]((*[64]uint32)(d32[:64]), group0, uint64(d.minv))
		unpack_u32[d.log2]((*[64]uint32)(d32[64:]), group1, uint64(d.minv))
	case uint64:
		d64 := util.ReinterpretSlice[T, uint64](dst[:])
		unpack_u64[d.log2]((*[64]uint64)(d64[:64]), group0, uint64(d.minv))
		unpack_u64[d.log2]((*[64]uint64)(d64[64:]), group1, uint64(d.minv))
	case int8:
		d8 := util.ReinterpretSlice[T, uint8](dst[:])
		unpack_u8[d.log2]((*[64]uint8)(d8[:64]), group0, uint64(d.minv))
		unpack_u8[d.log2]((*[64]uint8)(d8[64:]), group1, uint64(d.minv))
	case int16:
		d16 := util.ReinterpretSlice[T, uint16](dst[:])
		unpack_u16[d.log2]((*[64]uint16)(d16[:64]), group0, uint64(d.minv))
		unpack_u16[d.log2]((*[64]uint16)(d16[64:]), group1, uint64(d.minv))
	case int32:
		d32 := util.ReinterpretSlice[T, uint32](dst[:])
		unpack_u32[d.log2]((*[64]uint32)(d32[:64]), group0, uint64(d.minv))
		unpack_u32[d.log2]((*[64]uint32)(d32[64:]), group1, uint64(d.minv))
	case int64:
		d64 := util.ReinterpretSlice[T, uint64](dst[:])
		unpack_u64[d.log2]((*[64]uint64)(d64[:64]), group0, uint64(d.minv))
		unpack_u64[d.log2]((*[64]uint64)(d64[64:]), group1, uint64(d.minv))
	}

	return n
}

// decode is an internal loop decoder for full vectors. It is used as fallback
// for tail processing.
func decode[T types.Integer](out []T, in []uint64, log2 int, minv T) (int, error) {
	var pack uint64 // Current 64-bit word being unpacked
	var offset int  // Bit offset within the current word
	var inIdx int   // Index into the input byte slice
	var outIdx int  // Index into the output array
	var lost int    // must shift right next in word instead of left

	mask := uint64((1 << log2) - 1) // Mask for b bits, e.g., b=3 -> 0b111

	for outIdx = 0; outIdx < len(out); outIdx++ {
		// Ensure we have enough bits in pack
		for offset < log2 && inIdx < len(in) {
			if lost > 0 {
				pack |= in[inIdx] >> (BitsSize - offset - lost) &^ (1<<offset - 1)
				inIdx++
				offset += lost
				lost = 0
				if offset < log2 {
					pack |= in[inIdx] << offset
					lost = offset
					offset += BitsSize - offset
				}
			} else {
				pack |= in[inIdx] << offset
				lost = offset
				inIdx += util.Bool2int(offset == 0)
				offset += BitsSize - offset
			}
		}

		// Extract b bits from pack
		out[outIdx] = T(pack&mask) + minv
		pack >>= log2
		offset -= log2
	}

	return outIdx, nil
}

func Decode[T types.Integer](dst []T, src []byte, log2 int, minv T) (int, error) {
	var (
		n   int
		err error
	)
	switch any(T(0)).(type) {
	case uint8:
		n, err = Decode8(util.ReinterpretSlice[T, uint8](dst), src, log2, uint8(minv))
	case uint16:
		n, err = Decode16(util.ReinterpretSlice[T, uint16](dst), src, log2, uint16(minv))
	case uint32:
		n, err = Decode32(util.ReinterpretSlice[T, uint32](dst), src, log2, uint32(minv))
	case uint64:
		n, err = Decode64(util.ReinterpretSlice[T, uint64](dst), src, log2, uint64(minv))
	case int8:
		n, err = Decode8(util.ReinterpretSlice[T, int8](dst), src, log2, int8(minv))
	case int16:
		n, err = Decode16(util.ReinterpretSlice[T, int16](dst), src, log2, int16(minv))
	case int32:
		n, err = Decode32(util.ReinterpretSlice[T, int32](dst), src, log2, int32(minv))
	case int64:
		n, err = Decode64(util.ReinterpretSlice[T, int64](dst), src, log2, int64(minv))
	}
	return n, err
}

func Decode8[T int8 | uint8](dst []T, src []byte, log2 int, minv T) (int, error) {
	in := util.FromByteSlice[uint64](src)
	blockN := len(dst) / (4 * BlockSize)
	if blockN == 0 {
		// input less than block size, use generic decoder
		return decode(dst, in, log2, minv)
	}

	outp := unsafe.Pointer(&dst[0])
	var inp unsafe.Pointer
	if len(src) > 0 {
		inp = unsafe.Pointer(&src[0])
	}
	nBlockInBytes := log2 * 8
	nBlockOutBytes := BlockSize

	// 4x loop unrolled unpacking 4x64 uint8 values from 4xlog2 64bit codewords
	for blockI := range blockN {
		i := blockI * nBlockInBytes * 4
		in1 := unsafe.Add(inp, i)
		in2 := unsafe.Add(inp, i+1*nBlockInBytes)
		in3 := unsafe.Add(inp, i+2*nBlockInBytes)
		in4 := unsafe.Add(inp, i+3*nBlockInBytes)
		o := blockI * nBlockOutBytes * 4
		out1 := (*[BlockSize]uint8)(unsafe.Add(outp, o))
		out2 := (*[BlockSize]uint8)(unsafe.Add(outp, o+1*nBlockOutBytes))
		out3 := (*[BlockSize]uint8)(unsafe.Add(outp, o+2*nBlockOutBytes))
		out4 := (*[BlockSize]uint8)(unsafe.Add(outp, o+3*nBlockOutBytes))

		// unpack groups (4 x 64 packed inputs)
		unpack_u8[log2](out1, in1, uint64(minv))
		unpack_u8[log2](out2, in2, uint64(minv))
		unpack_u8[log2](out3, in3, uint64(minv))
		unpack_u8[log2](out4, in4, uint64(minv))
	}
	outpos := blockN * 4 * BlockSize

	// tail loop
	n, err := decode(dst[outpos:], in[blockN*log2*4:], log2, minv)

	// return output values written
	return outpos + n, err
}

func Decode16[T int16 | uint16](dst []T, src []byte, log2 int, minv T) (int, error) {
	in := util.FromByteSlice[uint64](src)
	blockN := len(dst) / (4 * BlockSize)
	if blockN == 0 {
		// input less than block size, use generic decoder
		return decode(dst, in, log2, minv)
	}

	outp := unsafe.Pointer(&dst[0])
	var inp unsafe.Pointer
	if len(src) > 0 {
		inp = unsafe.Pointer(&src[0])
	}
	nBlockInBytes := log2 * 8
	nBlockOutBytes := BlockSize * 2

	// 4x loop unrolled unpacking 4x64 uint16 values from 4xlog2 64bit codewords
	for blockI := range blockN {
		i := blockI * nBlockInBytes * 4
		in1 := unsafe.Add(inp, i)
		in2 := unsafe.Add(inp, i+1*nBlockInBytes)
		in3 := unsafe.Add(inp, i+2*nBlockInBytes)
		in4 := unsafe.Add(inp, i+3*nBlockInBytes)
		o := blockI * nBlockOutBytes * 4
		out1 := (*[BlockSize]uint16)(unsafe.Add(outp, o))
		out2 := (*[BlockSize]uint16)(unsafe.Add(outp, o+1*nBlockOutBytes))
		out3 := (*[BlockSize]uint16)(unsafe.Add(outp, o+2*nBlockOutBytes))
		out4 := (*[BlockSize]uint16)(unsafe.Add(outp, o+3*nBlockOutBytes))

		// unpack groups (4 x 64 packed inputs)
		unpack_u16[log2](out1, in1, uint64(minv))
		unpack_u16[log2](out2, in2, uint64(minv))
		unpack_u16[log2](out3, in3, uint64(minv))
		unpack_u16[log2](out4, in4, uint64(minv))
	}
	outpos := blockN * 4 * BlockSize

	// tail loop
	n, err := decode(dst[outpos:], in[blockN*log2*4:], log2, minv)

	// return output values written
	return outpos + n, err
}

func Decode32[T int32 | uint32](dst []T, src []byte, log2 int, minv T) (int, error) {
	in := util.FromByteSlice[uint64](src)
	blockN := len(dst) / (4 * BlockSize)
	if blockN == 0 {
		// input less than block size, use generic decoder
		n, err := decode(dst, in, log2, minv)
		return n, err
	}

	outp := unsafe.Pointer(&dst[0])
	var inp unsafe.Pointer
	if len(src) > 0 {
		inp = unsafe.Pointer(&src[0])
	}
	nBlockInBytes := log2 * 8
	nBlockOutBytes := BlockSize * 4

	// 4x loop unrolled unpacking 4x64 uint16 values from 4xlog2 64bit codewords
	for blockI := range blockN {
		i := blockI * nBlockInBytes * 4
		in1 := unsafe.Add(inp, i)
		in2 := unsafe.Add(inp, i+1*nBlockInBytes)
		in3 := unsafe.Add(inp, i+2*nBlockInBytes)
		in4 := unsafe.Add(inp, i+3*nBlockInBytes)
		o := blockI * nBlockOutBytes * 4
		out1 := (*[BlockSize]uint32)(unsafe.Add(outp, o))
		out2 := (*[BlockSize]uint32)(unsafe.Add(outp, o+1*nBlockOutBytes))
		out3 := (*[BlockSize]uint32)(unsafe.Add(outp, o+2*nBlockOutBytes))
		out4 := (*[BlockSize]uint32)(unsafe.Add(outp, o+3*nBlockOutBytes))

		// unpack groups (4 x 64 packed inputs)
		unpack_u32[log2](out1, in1, uint64(minv))
		unpack_u32[log2](out2, in2, uint64(minv))
		unpack_u32[log2](out3, in3, uint64(minv))
		unpack_u32[log2](out4, in4, uint64(minv))
	}
	outpos := blockN * 4 * BlockSize

	// tail loop
	n, err := decode(dst[outpos:], in[blockN*log2*4:], log2, minv)

	// return output values written
	return outpos + n, err
}

func Decode64[T int64 | uint64](dst []T, src []byte, log2 int, minv T) (int, error) {
	in := util.FromByteSlice[uint64](src)
	blockN := len(dst) / (4 * BlockSize)
	if blockN == 0 {
		// input less than block size, use generic decoder
		n, err := decode(dst, in, log2, minv)
		return n, err
	}

	outp := unsafe.Pointer(&dst[0])
	var inp unsafe.Pointer
	if len(src) > 0 {
		inp = unsafe.Pointer(&src[0])
	}
	nBlockInBytes := log2 * 8
	nBlockOutBytes := BlockSize * 8

	// 4x loop unrolled unpacking 4x64 uint16 values from 4xlog2 64bit codewords
	for blockI := range blockN {
		i := blockI * nBlockInBytes * 4
		in1 := unsafe.Add(inp, i)
		in2 := unsafe.Add(inp, i+1*nBlockInBytes)
		in3 := unsafe.Add(inp, i+2*nBlockInBytes)
		in4 := unsafe.Add(inp, i+3*nBlockInBytes)
		o := blockI * nBlockOutBytes * 4
		out1 := (*[BlockSize]uint64)(unsafe.Add(outp, o))
		out2 := (*[BlockSize]uint64)(unsafe.Add(outp, o+1*nBlockOutBytes))
		out3 := (*[BlockSize]uint64)(unsafe.Add(outp, o+2*nBlockOutBytes))
		out4 := (*[BlockSize]uint64)(unsafe.Add(outp, o+3*nBlockOutBytes))

		// unpack groups (4 x 64 packed inputs)
		unpack_u64[log2](out1, in1, uint64(minv))
		unpack_u64[log2](out2, in2, uint64(minv))
		unpack_u64[log2](out3, in3, uint64(minv))
		unpack_u64[log2](out4, in4, uint64(minv))
	}
	outpos := blockN * 4 * BlockSize

	// tail loop
	n, err := decode(dst[outpos:], in[blockN*log2*4:], log2, minv)

	// return output values written
	return outpos + n, err
}

func DecodeAlp[T types.Float](dst []T, src []byte, log2 int, minv, f, e T) (int, error) {
	var (
		n   int
		err error
	)
	switch any(T(0)).(type) {
	case float32:
		n, err = Decodef32(util.ReinterpretSlice[T, float32](dst), src, log2, float32(minv), float32(f), float32(e))
	case float64:
		n, err = Decodef64(util.ReinterpretSlice[T, float64](dst), src, log2, float64(minv), float64(f), float64(e))
	}
	return n, err
}

func Decodef32[T float32](dst []T, src []byte, log2 int, minv, f, e T) (int, error) {
	in := util.FromByteSlice[uint64](src)
	blockN := len(dst) / (4 * BlockSize)
	if blockN == 0 {
		// input less than block size, use generic decoder
		return decodeFused(dst, in, log2, minv, f, e)
	}

	outp := unsafe.Pointer(&dst[0])
	var inp unsafe.Pointer
	if len(src) > 0 {
		inp = unsafe.Pointer(&src[0])
	}
	nBlockInBytes := log2 * 8
	nBlockOutBytes := BlockSize * 4

	// 4x loop unrolled unpacking 4x64 uint16 values from 4xlog2 64bit codewords
	for blockI := range blockN {
		i := blockI * nBlockInBytes * 4
		in1 := unsafe.Add(inp, i)
		in2 := unsafe.Add(inp, i+1*nBlockInBytes)
		in3 := unsafe.Add(inp, i+2*nBlockInBytes)
		in4 := unsafe.Add(inp, i+3*nBlockInBytes)
		o := blockI * nBlockOutBytes * 4
		out1 := (*[BlockSize]float32)(unsafe.Add(outp, o))
		out2 := (*[BlockSize]float32)(unsafe.Add(outp, o+1*nBlockOutBytes))
		out3 := (*[BlockSize]float32)(unsafe.Add(outp, o+2*nBlockOutBytes))
		out4 := (*[BlockSize]float32)(unsafe.Add(outp, o+3*nBlockOutBytes))

		// unpack groups (4 x 64 packed inputs)
		unpack_f32[log2](out1, in1, uint64(minv), float32(f), float32(e))
		unpack_f32[log2](out2, in2, uint64(minv), float32(f), float32(e))
		unpack_f32[log2](out3, in3, uint64(minv), float32(f), float32(e))
		unpack_f32[log2](out4, in4, uint64(minv), float32(f), float32(e))
	}
	outpos := blockN * 4 * BlockSize

	// tail loop
	n, err := decodeFused(dst[outpos:], in[blockN*log2*4:], log2, minv, f, e)

	// return output values written
	return outpos + n, err
}

func Decodef64[T float64](dst []T, src []byte, log2 int, minv, f, e T) (int, error) {
	in := util.FromByteSlice[uint64](src)
	blockN := len(dst) / (4 * BlockSize)
	if blockN == 0 {
		// input less than block size, use generic decoder
		return decodeFused(dst, in, log2, minv, f, e)
	}

	outp := unsafe.Pointer(&dst[0])
	var inp unsafe.Pointer
	if len(src) > 0 {
		inp = unsafe.Pointer(&src[0])
	}
	nBlockInBytes := log2 * 8
	nBlockOutBytes := BlockSize * 8

	// 4x loop unrolled unpacking 4x64 uint16 values from 4xlog2 64bit codewords
	for blockI := range blockN {
		i := blockI * nBlockInBytes * 4
		in1 := unsafe.Add(inp, i)
		in2 := unsafe.Add(inp, i+1*nBlockInBytes)
		in3 := unsafe.Add(inp, i+2*nBlockInBytes)
		in4 := unsafe.Add(inp, i+3*nBlockInBytes)
		o := blockI * nBlockOutBytes * 4
		out1 := (*[BlockSize]float64)(unsafe.Add(outp, o))
		out2 := (*[BlockSize]float64)(unsafe.Add(outp, o+1*nBlockOutBytes))
		out3 := (*[BlockSize]float64)(unsafe.Add(outp, o+2*nBlockOutBytes))
		out4 := (*[BlockSize]float64)(unsafe.Add(outp, o+3*nBlockOutBytes))

		// unpack groups (4 x 64 packed inputs)
		unpack_f64[log2](out1, in1, uint64(minv), float64(f), float64(e))
		unpack_f64[log2](out2, in2, uint64(minv), float64(f), float64(e))
		unpack_f64[log2](out3, in3, uint64(minv), float64(f), float64(e))
		unpack_f64[log2](out4, in4, uint64(minv), float64(f), float64(e))
	}
	outpos := blockN * 4 * BlockSize

	// tail loop
	n, err := decodeFused(dst[outpos:], in[blockN*log2*4:], log2, minv, f, e)

	// return output values written
	return outpos + n, err
}

func decodeFused[T types.Float](out []T, in []uint64, log2 int, minv, f, e T) (int, error) {
	var pack uint64 // Current 64-bit word being unpacked
	var offset int  // Bit offset within the current word
	var inIdx int   // Index into the input byte slice
	var outIdx int  // Index into the output array
	var lost int    // must shift right next in word instead of left

	mask := uint64((1 << log2) - 1) // Mask for b bits, e.g., b=3 -> 0b111

	for outIdx = 0; outIdx < len(out); outIdx++ {
		// Ensure we have enough bits in pack
		for offset < log2 && inIdx < len(in) {
			if lost > 0 {
				pack |= in[inIdx] >> (BitsSize - offset - lost) &^ (1<<offset - 1)
				inIdx++
				offset += lost
				lost = 0
				if offset < log2 {
					pack |= in[inIdx] << offset
					lost = offset
					offset += BitsSize - offset
				}
			} else {
				pack |= in[inIdx] << offset
				lost = offset
				inIdx += util.Bool2int(offset == 0)
				offset += BitsSize - offset
			}
		}

		// Extract b bits from pack
		out[outIdx] = (T(pack&mask) + minv) * f * e
		pack >>= log2
		offset -= log2
	}

	return outIdx, nil
}

type decoderFactoryType struct {
	i64Pool sync.Pool
	i32Pool sync.Pool
	i16Pool sync.Pool
	i8Pool  sync.Pool
	u64Pool sync.Pool
	u32Pool sync.Pool
	u16Pool sync.Pool
	u8Pool  sync.Pool
}

func newDecoder[T types.Integer]() *Decoder[T] {
	switch any(T(0)).(type) {
	case int64:
		return decoderFactory.i64Pool.Get().(*Decoder[T])
	case int32:
		return decoderFactory.i32Pool.Get().(*Decoder[T])
	case int16:
		return decoderFactory.i16Pool.Get().(*Decoder[T])
	case int8:
		return decoderFactory.i8Pool.Get().(*Decoder[T])
	case uint64:
		return decoderFactory.u64Pool.Get().(*Decoder[T])
	case uint32:
		return decoderFactory.u32Pool.Get().(*Decoder[T])
	case uint16:
		return decoderFactory.u16Pool.Get().(*Decoder[T])
	case uint8:
		return decoderFactory.u8Pool.Get().(*Decoder[T])
	default:
		return nil
	}
}

func putDecoder[T types.Integer](c *Decoder[T]) {
	switch (any(T(0))).(type) {
	case int64:
		decoderFactory.i64Pool.Put(c)
	case int32:
		decoderFactory.i32Pool.Put(c)
	case int16:
		decoderFactory.i16Pool.Put(c)
	case int8:
		decoderFactory.i8Pool.Put(c)
	case uint64:
		decoderFactory.u64Pool.Put(c)
	case uint32:
		decoderFactory.u32Pool.Put(c)
	case uint16:
		decoderFactory.u16Pool.Put(c)
	case uint8:
		decoderFactory.u8Pool.Put(c)
	}
}

var decoderFactory = decoderFactoryType{
	i64Pool: sync.Pool{New: func() any { return new(Decoder[int64]) }},
	i32Pool: sync.Pool{New: func() any { return new(Decoder[int32]) }},
	i16Pool: sync.Pool{New: func() any { return new(Decoder[int16]) }},
	i8Pool:  sync.Pool{New: func() any { return new(Decoder[int8]) }},
	u64Pool: sync.Pool{New: func() any { return new(Decoder[uint64]) }},
	u32Pool: sync.Pool{New: func() any { return new(Decoder[uint32]) }},
	u16Pool: sync.Pool{New: func() any { return new(Decoder[uint16]) }},
	u8Pool:  sync.Pool{New: func() any { return new(Decoder[uint8]) }},
}
