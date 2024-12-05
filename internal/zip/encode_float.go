// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// Original from: InfluxData, MIT
// https://github.com/influxdata/influxdb
package zip

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"

	"blockwatch.cc/knoxdb/internal/arena"
)

const (
	// floatUncompressed is an uncompressed format using 8 bytes per value.
	// Not yet implemented.
	// floatUncompressed = 0

	// floatCompressedGorilla is a compressed format using the gorilla paper encoding
	floatCompressedGorilla = 1

	// uvnan is the constant returned from math.NaN().
	uvnan = 0x7FF8000000000001
)

var (
	errFloatBatchDecodeShortBuffer = fmt.Errorf("compress: FloatArrayDecodeAll short buffer")
)

// upper bound
func FloatEncodedSize(n int) int {
	// empty slice still writes 19 bytes
	if n == 0 {
		return 19
	}
	return n*9 + 1
}

func EncodeFloat32(src []float32, w io.Writer) (int, error) {
	scratch := arena.Alloc(arena.AllocFloat64, len(src))
	defer arena.Free(arena.AllocFloat64, scratch)
	dst := scratch.([]float64)[:len(src)]
	for i, v := range src {
		dst[i] = float64(v)
	}
	return EncodeFloat64(dst, w)
}

// EncodeFloat64 encodes src directly into a writer returning number of bytes
// written and any error encountered. The compression scheme is Facebook's Gorilla.
func EncodeFloat64(src []float64, w io.Writer) (int, error) {
	// the original algorithm writes directly to a target []byte
	// slice and we don't want to change this. we allocate a slice
	// and write it to the io.Writer at the very end
	scratch := arena.Alloc(arena.AllocBytes, FloatEncodedSize(len(src)))
	defer arena.Free(arena.AllocBytes, scratch)
	b := scratch.([]byte)[:0]

	b = b[:1]
	b[0] = floatCompressedGorilla << 4

	var first float64
	var finished bool
	switch {
	case len(src) > 0 && math.IsNaN(src[0]):
		return 0, fmt.Errorf("compress: unsupported float value: NaN")
	case len(src) == 0:
		first = math.NaN() // Write sentinal value to terminate batch.
		finished = true
	default:
		first = src[0]
		src = src[1:]
	}

	b = b[:9]
	n := uint64(8 + 64) // Number of bits written.
	prev := math.Float64bits(first)

	// Write first value.
	binary.BigEndian.PutUint64(b[1:], prev)

	prevLeading, prevTrailing := ^uint64(0), uint64(0)
	var leading, trailing uint64
	var mask uint64
	var sum float64

	// Encode remaining values.
	for i := 0; !finished; i++ {
		var x float64
		if i < len(src) {
			x = src[i]
			sum += x
		} else {
			// Encode sentinal value to terminate batch
			x = math.NaN()
			finished = true
		}

		{
			cur := math.Float64bits(x)
			vDelta := cur ^ prev
			if vDelta == 0 {
				n++ // Write a zero bit. Nothing else to do.
				prev = cur
				continue
			}

			// First the current bit of the current byte is set to indicate we're
			// writing a delta value to the stream.
			for n>>3 >= uint64(len(b)) { // Keep growing b until we can fit all bits in.
				b = append(b, byte(0))
			}

			// n&7 - current bit in current byte.
			// n>>3 - the current byte.
			b[n>>3] |= 128 >> (n & 7) // Sets the current bit of the current byte.
			n++

			// Write the delta to b.

			// Determine the leading and trailing zeros.
			leading = uint64(bits.LeadingZeros64(vDelta))
			trailing = uint64(bits.TrailingZeros64(vDelta))

			// Clamp number of leading zeros to avoid overflow when encoding
			leading &= 0x1F
			if leading >= 32 {
				leading = 31
			}

			// At least 2 further bits will be required.
			if (n+2)>>3 >= uint64(len(b)) {
				b = append(b, byte(0))
			}

			if prevLeading != ^uint64(0) && leading >= prevLeading && trailing >= prevTrailing {
				n++ // Write a zero bit.

				// Write the l least significant bits of vDelta to b, most significant
				// bit first.
				l := 64 - prevLeading - prevTrailing
				for (n+l)>>3 >= uint64(len(b)) { // Keep growing b until we can fit all bits in.
					b = append(b, byte(0))
				}

				// Full value to write.
				v := (vDelta >> prevTrailing) << (64 - l) // l least signifciant bits of v.

				var m = n & 7 // Current bit in current byte.
				var written uint64
				if m > 0 { // In this case the current byte is not full.
					written = 8 - m
					if l < written {
						written = l
					}
					mask = v >> 56 // Move 8 MSB to 8 LSB
					b[n>>3] |= byte(mask >> m)
					n += written

					if l-written == 0 {
						prev = cur
						continue
					}
				}

				vv := v << written // Move written bits out of the way.

				// TODO(edd): Optimise this. It's unlikely we actually have 8 bytes to write.
				if (n>>3)+8 >= uint64(len(b)) {
					b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
				}
				binary.BigEndian.PutUint64(b[n>>3:], vv)
				n += (l - written)
			} else {
				prevLeading, prevTrailing = leading, trailing

				// Set a single bit to indicate a value will follow.
				b[n>>3] |= 128 >> (n & 7) // Set current bit on current byte
				n++

				// Write 5 bits of leading.
				if (n+5)>>3 >= uint64(len(b)) {
					b = append(b, byte(0))
				}

				// Enough room to write the 5 bits in the current byte?
				var m = n & 7
				l := uint64(5)
				v := leading << 59 // 5 LSB of leading.
				mask = v >> 56     // Move 5 MSB to 8 LSB

				if m <= 3 { // 5 bits fit into current byte.
					b[n>>3] |= byte(mask >> m)
					n += l
				} else { // In this case there are fewer than 5 bits available in current byte.
					// First step is to fill current byte
					written := 8 - m
					b[n>>3] |= byte(mask >> m) // Some of mask will get lost.
					n += written

					// Second step is to write the lost part of mask into the next byte.
					mask = v << written // Move written bits in previous byte out of way.
					mask >>= 56

					m = n & 7 // Recompute current bit.
					b[n>>3] |= byte(mask >> m)
					n += (l - written)
				}

				// Note that if leading == trailing == 0, then sigbits == 64.  But that
				// value doesn't actually fit into the 6 bits we have.
				// Luckily, we never need to encode 0 significant bits, since that would
				// put us in the other case (vdelta == 0).  So instead we write out a 0 and
				// adjust it back to 64 on unpacking.
				sigbits := 64 - leading - trailing

				if (n+6)>>3 >= uint64(len(b)) {
					b = append(b, byte(0))
				}

				m = n & 7
				l = uint64(6)
				v = sigbits << 58 // Move 6 LSB of sigbits to MSB
				mask = v >> 56    // Move 6 MSB to 8 LSB
				if m <= 2 {
					// The 6 bits fit into the current byte.
					b[n>>3] |= byte(mask >> m)
					n += l
				} else { // In this case there are fewer than 6 bits available in current byte.
					// First step is to fill the current byte.
					written := 8 - m
					b[n>>3] |= byte(mask >> m) // Write to the current bit.
					n += written

					// Second step is to write the lost part of mask into the next byte.
					// Write l remaining bits into current byte.
					mask = v << written // Remove bits written in previous byte out of way.
					mask >>= 56

					m = n & 7 // Recompute current bit.
					b[n>>3] |= byte(mask >> m)
					n += l - written
				}

				// Write final value.
				m = n & 7
				l = sigbits
				v = (vDelta >> trailing) << (64 - l) // Move l LSB into MSB
				for (n+l)>>3 >= uint64(len(b)) {     // Keep growing b until we can fit all bits in.
					b = append(b, byte(0))
				}

				var written uint64
				if m > 0 { // In this case the current byte is not full.
					written = 8 - m
					if l < written {
						written = l
					}
					mask = v >> 56 // Move 8 MSB to 8 LSB
					b[n>>3] |= byte(mask >> m)
					n += written

					if l-written == 0 {
						prev = cur
						continue
					}
				}

				// Shift remaining bits and write out in one go.
				vv := v << written // Remove bits written in previous byte.
				// TODO(edd): Optimise this.
				if (n>>3)+8 >= uint64(len(b)) {
					b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
				}

				binary.BigEndian.PutUint64(b[n>>3:], vv)
				n += (l - written)
			}
			prev = cur
		}
	}

	if math.IsNaN(sum) {
		return 0, fmt.Errorf("compress: unsupported float value: NaN")
	}

	length := n >> 3
	if n&7 > 0 {
		length++ // Add an extra byte to capture overflowing bits.
	}

	// write out to writer
	return w.Write(b[:length])
}
