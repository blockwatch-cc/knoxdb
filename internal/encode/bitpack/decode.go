// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package bitpack

import (
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
	in := util.FromByteSlice[uint64](d.buf)
	n, _ := decode[T](dst, in, d.log2, d.minv)
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
	in := util.FromByteSlice[uint64](d.buf[startpos:endpos])
	decode[T](dst[:n], in, d.log2, d.minv)
	return n
}

// out []T, in []uint64, log2 int, minv T

func decode[T types.Integer](out []T, in []uint64, log2 int, minv T) (int, error) {
	var pack uint64 // Current 64-bit word being unpacked
	var offset int  // Bit offset within the current word
	var inIdx int   // Index into the input byte slice
	var outIdx int  // Index into the output array
	var lost int    // must shift right next in word instead of left

	vmask := uint64((1 << log2) - 1) // Mask for b bits, e.g., b=3 -> 0b111
	// rmask := uint64(1<<bits - 1)

	for outIdx = 0; outIdx < len(out); outIdx++ {
		// Ensure we have enough bits in pack
		// fmt.Printf("offset => %d log2 => %d inDx => %d len(inBuff) => %d\n", offset, log2, inIdx, len(in))
		// fmt.Println("(offset < log2 && inIdx < len(inBuff))", offset < log2 && inIdx < len(in))
		for offset < log2 && inIdx < len(in) {
			// fmt.Println("hey")
			if lost > 0 {
				pack |= uint64(in[inIdx]) >> (BitsSize - offset - lost) &^ (1<<offset - 1)
				// fmt.Printf("pack(z) => %b\n", pack)
				inIdx++
				offset += lost
				lost = 0
				if offset < log2 {
					pack |= uint64(in[inIdx]) << offset
					// fmt.Printf("pack(x) => %b\n", pack)
					lost = offset
					offset += BitsSize - offset
				}
			} else {
				pack |= uint64(in[inIdx]) << offset
				// fmt.Printf("pack(c) => %b\n", pack)
				lost = offset
				inIdx += util.Bool2int(offset == 0)
				offset += BitsSize - offset
			}
		}

		// fmt.Printf("outIdx => %d out => %d\n", outIdx, out[outIdx])
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
