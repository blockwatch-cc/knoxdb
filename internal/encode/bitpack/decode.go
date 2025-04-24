// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package bitpack

import (
	"sync"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

type DecodeFunc[T types.Integer] func(index int) T

type Decoder[T types.Integer] struct {
	src   []uint64
	log2  int
	len   int
	vmask uint64 // value mask
	minv  T
}

func NewDecoder[T types.Integer](buf []byte, log2, n int, minv T) *Decoder[T] {
	d := newDecoder[T]()
	d.src = util.FromByteSlice[uint64](buf)
	d.log2 = log2
	d.len = n
	d.vmask = uint64((1 << log2) - 1)
	d.minv = minv
	return d
}

func (d *Decoder[T]) Close() {
	d.src = nil
	d.log2 = 0
	d.len = 0
	d.vmask = 0
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
		word |= uint64(d.src[pos+1]) << diff
	}

	// mask value and undo min-FOR
	return T(word&d.vmask) + d.minv
}

// DecodeChunk unpacks up to chunk size (128) values starting at position
// ofs. Ofs must be a multiple of 128 and within bounds. Returns the number
// of decoded values which is always chunk size unless at shorter tail ends.
func (d *Decoder[T]) DecodeChunk(dst *[128]T, ofs int) int {
	if ofs >= d.len {
		return 0
	}
	n := min(128, d.len-ofs)
	k := ofs >> 6 // = ofs/64 (must be multiple of 128)

	// take slow path for tails
	if n < 128 {
		_, _ = decode(dst[:n], d.src[k*d.log2:], d.log2, d.minv)
		return n
	}

	// calculate source code word group offsets for chunks size 128
	group0 := d.src[k*d.log2 : (k+1)*d.log2]     // n words -> 64 values
	group1 := d.src[(k+1)*d.log2 : (k+2)*d.log2] // n words -> 64 values

	// call the correct kernel
	switch any(T(0)).(type) {
	case uint8:
		d8 := util.ReinterpretSlice[T, uint8](dst[:])
		bitread8(d8[:64], group0, d.log2, uint8(d.minv))
		bitread8(d8[64:], group1, d.log2, uint8(d.minv))
	case uint16:
		d16 := util.ReinterpretSlice[T, uint16](dst[:])
		bitread16(d16[:64], group0, d.log2, uint16(d.minv))
		bitread16(d16[64:], group1, d.log2, uint16(d.minv))
	case uint32:
		d32 := util.ReinterpretSlice[T, uint32](dst[:])
		bitread32(d32[:64], group0, d.log2, uint32(d.minv))
		bitread32(d32[64:], group1, d.log2, uint32(d.minv))
	case uint64:
		d64 := util.ReinterpretSlice[T, uint64](dst[:])
		bitread64(d64[:64], group0, d.log2, uint64(d.minv))
		bitread64(d64[64:], group1, d.log2, uint64(d.minv))
	case int8:
		d8 := util.ReinterpretSlice[T, int8](dst[:])
		bitread8(d8[:64], group0, d.log2, int8(d.minv))
		bitread8(d8[64:], group1, d.log2, int8(d.minv))
	case int16:
		d16 := util.ReinterpretSlice[T, int16](dst[:])
		bitread16(d16[:64], group0, d.log2, int16(d.minv))
		bitread16(d16[64:], group1, d.log2, int16(d.minv))
	case int32:
		d32 := util.ReinterpretSlice[T, int32](dst[:])
		bitread32(d32[:64], group0, d.log2, int32(d.minv))
		bitread32(d32[64:], group1, d.log2, int32(d.minv))
	case int64:
		d64 := util.ReinterpretSlice[T, int64](dst[:])
		bitread64(d64[:64], group0, d.log2, int64(d.minv))
		bitread64(d64[64:], group1, d.log2, int64(d.minv))
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

	vmask := uint64((1 << log2) - 1) // Mask for b bits, e.g., b=3 -> 0b111
	for outIdx = 0; outIdx < len(out); outIdx++ {
		// Ensure we have enough bits in pack
		for offset < log2 && inIdx < len(in) {
			if lost > 0 {
				pack |= uint64(in[inIdx]) >> (BitsSize - offset - lost) &^ (1<<offset - 1)
				inIdx++
				offset += lost
				lost = 0
				if offset < log2 {
					pack |= uint64(in[inIdx]) << offset
					lost = offset
					offset += BitsSize - offset
				}
			} else {
				pack |= uint64(in[inIdx]) << offset
				lost = offset
				inIdx += util.Bool2int(offset == 0)
				offset += BitsSize - offset
			}
		}

		// Extract b bits from pack
		out[outIdx] = T(pack&vmask) + minv
		pack >>= log2
		offset -= log2
	}

	return outIdx, nil
}

func Decode[T types.Integer](out []T, in []byte, log2 int, minv T) (int, error) {
	var outIdx int
	var err error
	switch any(T(0)).(type) {
	case uint8:
		outIdx, err = Decode8(util.ReinterpretSlice[T, uint8](out), in, log2, uint8(minv))
	case uint16:
		outIdx, err = Decode16(util.ReinterpretSlice[T, uint16](out), in, log2, uint16(minv))
	case uint32:
		outIdx, err = Decode32(util.ReinterpretSlice[T, uint32](out), in, log2, uint32(minv))
	case uint64:
		outIdx, err = Decode64(util.ReinterpretSlice[T, uint64](out), in, log2, uint64(minv))
	case int8:
		outIdx, err = Decode8(util.ReinterpretSlice[T, int8](out), in, log2, int8(minv))
	case int16:
		outIdx, err = Decode16(util.ReinterpretSlice[T, int16](out), in, log2, int16(minv))
	case int32:
		outIdx, err = Decode32(util.ReinterpretSlice[T, int32](out), in, log2, int32(minv))
	case int64:
		outIdx, err = Decode64(util.ReinterpretSlice[T, int64](out), in, log2, int64(minv))

	}
	return outIdx, err
}

func Decode8[T int8 | uint8](out []T, in []byte, log2 int, minv T) (int, error) {
	inBuff := util.FromByteSlice[uint64](in)
	var blockN int
	if inBufflen := len(inBuff); inBufflen > 0 {
		blockN = len(inBuff) / (log2 * BitReadingBlockSize)
	}
	if blockN == 0 {
		// input less than block size, use generic decoder
		n, err := decode(out, inBuff, log2, minv)
		return n, err
	}

	var outpos int
	groupSize := log2
	for blockI := range blockN {
		i := blockI * BitReadingBlockSize * groupSize
		group1 := inBuff[i+0*groupSize : i+1*groupSize]
		group2 := inBuff[i+1*groupSize : i+2*groupSize]
		group3 := inBuff[i+2*groupSize : i+3*groupSize]
		group4 := inBuff[i+3*groupSize : i+4*groupSize]

		bitread8(out[outpos:], group1, log2, minv)
		outpos += 64
		bitread8(out[outpos:], group2, log2, minv)
		outpos += 64
		bitread8(out[outpos:], group3, log2, minv)
		outpos += 64
		bitread8(out[outpos:], group4, log2, minv)
		outpos += 64
	}

	// tail loop
	n, err := decode(out[outpos:], inBuff[blockN*(groupSize*BitReadingBlockSize):], log2, minv)

	return outpos + n, err
}

func Decode16[T int16 | uint16](out []T, in []byte, log2 int, minv T) (int, error) {
	inBuff := util.FromByteSlice[uint64](in)
	var blockN int
	if inBufflen := len(inBuff); inBufflen > 0 {
		blockN = len(inBuff) / (log2 * BitReadingBlockSize)
	}
	if blockN == 0 {
		// input less than block size, use generic decoder
		n, err := decode(out, inBuff, log2, minv)
		return n, err
	}

	var outpos int
	groupSize := log2
	for blockI := range blockN {
		i := blockI * BitReadingBlockSize * groupSize
		group1 := inBuff[i+0*groupSize : i+1*groupSize]
		group2 := inBuff[i+1*groupSize : i+2*groupSize]
		group3 := inBuff[i+2*groupSize : i+3*groupSize]
		group4 := inBuff[i+3*groupSize : i+4*groupSize]

		bitread16(out[outpos:], group1, log2, minv)
		outpos += 64
		bitread16(out[outpos:], group2, log2, minv)
		outpos += 64
		bitread16(out[outpos:], group3, log2, minv)
		outpos += 64
		bitread16(out[outpos:], group4, log2, minv)
		outpos += 64
	}

	// tail loop
	n, err := decode(out[outpos:], inBuff[blockN*groupSize*BitReadingBlockSize:], log2, minv)

	return outpos + n, err
}

func Decode32[T int32 | uint32](out []T, in []byte, log2 int, minv T) (int, error) {
	inBuff := util.FromByteSlice[uint64](in)
	var blockN int
	if inBufflen := len(inBuff); inBufflen > 0 {
		blockN = len(inBuff) / (log2 * BitReadingBlockSize)
	}
	if blockN == 0 {
		// input less than block size, use generic decoder
		n, err := decode(out, inBuff, log2, minv)
		return n, err
	}

	var outpos int
	groupSize := log2
	for blockI := range blockN {
		i := blockI * BitReadingBlockSize * groupSize
		group1 := inBuff[i+0*groupSize : i+1*groupSize]
		group2 := inBuff[i+1*groupSize : i+2*groupSize]
		group3 := inBuff[i+2*groupSize : i+3*groupSize]
		group4 := inBuff[i+3*groupSize : i+4*groupSize]

		bitread32(out[outpos:], group1, log2, minv)
		outpos += 64
		bitread32(out[outpos:], group2, log2, minv)
		outpos += 64
		bitread32(out[outpos:], group3, log2, minv)
		outpos += 64
		bitread32(out[outpos:], group4, log2, minv)
		outpos += 64
	}

	// tail loop
	n, err := decode(out[outpos:], inBuff[blockN*groupSize*BitReadingBlockSize:], log2, minv)

	return outpos + n, err
}

func Decode64[T int64 | uint64](out []T, in []byte, log2 int, minv T) (int, error) {
	inBuff := util.FromByteSlice[uint64](in)
	var blockN int
	if inBufflen := len(inBuff); inBufflen > 0 {
		blockN = len(inBuff) / (log2 * BitReadingBlockSize)
	}
	if blockN == 0 {
		// input less than block size, use generic decoder
		n, err := decode(out, inBuff, log2, minv)
		return n, err
	}

	var outpos int
	groupSize := log2
	for blockI := range blockN {
		i := blockI * BitReadingBlockSize * groupSize
		group1 := inBuff[i+0*groupSize : i+1*groupSize]
		group2 := inBuff[i+1*groupSize : i+2*groupSize]
		group3 := inBuff[i+2*groupSize : i+3*groupSize]
		group4 := inBuff[i+3*groupSize : i+4*groupSize]

		bitread64(out[outpos:], group1, log2, minv)
		outpos += 64
		bitread64(out[outpos:], group2, log2, minv)
		outpos += 64
		bitread64(out[outpos:], group3, log2, minv)
		outpos += 64
		bitread64(out[outpos:], group4, log2, minv)
		outpos += 64
	}

	// tail loop
	n, err := decode(out[outpos:], inBuff[blockN*groupSize*BitReadingBlockSize:], log2, minv)

	return outpos + n, err
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
