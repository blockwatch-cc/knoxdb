// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// Original inspired by InfluxData, MIT https://github.com/influxdata/influxdb
package zip

import (
	"encoding/binary"
	"io"
	"math"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/s8b"
)

const (
	// timeUncompressed is a scaled uncompressed format using 8 bytes per timestamp
	timeUncompressed = 0
	// timeCompressedPacked is a bit-packed format using simple8b encoding
	timeCompressedPacked = 1
	// timeCompressedRLE is a run-length encoding format
	timeCompressedRLE = 2
	// timeCompressedZigZag is scaling + zigzag + simple8b encoding
	timeCompressedZigZagPacked = 3
	// timeCompressedZigZagRLE is scaling + zigzag + RLE encoding
	timeCompressedZigZagRLE = 4
)

// upper bound
func TimeEncodedSize(n int) int {
	return n*8 + 1
}

// Differences from original
// - scale timestamps before delta-encoding (vs. scale deltas)
// - zigzag encode when timestamps are unordered

// EncodeTime encodes src directly into a writer returning number of bytes
// written and any error encountered.
//
// EncodeTime implements six integer compression types: uncompressed, simple8b,
// RLE and their zigzag encoded versions. All versions first apply a scaling
// factor (integer division) and then compute delta values between neigbouring
// timestamps to decrease number sizes.
//
// When timestamp values are unsorted, we then perform zig-zag encoding to avoid
// large neg/pos numbers. This allows to still use simple8b even when timestamps
// are out of order.
//
// In serialized form, the first valueis the pre-scaled starting timestamp,
// subsequent values are the pre-scaled differences from the prior value.
//
// EncodeTime does not modify the contents of src.
func EncodeTime(s []int64, w io.Writer) (int, error) {
	var (
		src                  = asU64(s)
		maxdelta, div uint64 = 0, 1e9
		ordered       bool   = true
		l             int    = len(src)
	)

	if l == 0 {
		// Nothing to do on empty blocks
		return 0, nil
	}

	// alloc scratch space
	scratch := arena.Alloc(arena.AllocUint64, len(src))
	defer arena.Free(arena.AllocUint64, scratch)
	deltas := scratch.([]uint64)[:len(src)]
	deltas[0] = src[0]

	if len(src) > 1 {
		// find common divisor first, break early when we find a timestamp
		// at nanosec resolution
		for i := 0; i < l && div > 1; i++ {
			// If our value is divisible by 10, break.
			// Otherwise, try the next smallest divisor.
			v := src[i]
			for div > 1 && v%div != 0 {
				div /= 10
			}
		}

		// Only apply the divisor if it's greater than 1 since division is expensive.
		if div > 1 {
			// work backwards, but leave the first value unscaled to remain
			// compatible with the original simple and RLE encoding versions
			// that used to scale deltas instead of timestamps;

			deltas[l-1] = src[l-1] / div
			for i := l - 1; i > 1; i-- {
				// apply scaling factor
				deltas[i-1] = src[i-1] / div
				// detect ordering
				ordered = ordered && deltas[i-1] <= deltas[i]
				// delta-encode
				deltas[i] -= deltas[i-1]
				if deltas[i] > maxdelta {
					maxdelta = deltas[i]
				}
			}
			// remember to apply scaling to the first value, but without saving
			// the scaled value just yet because RLE encoding relies on unaltered
			// initial values
			ordered = ordered && deltas[0]/div <= deltas[1]
			deltas[1] -= deltas[0] / div
			if deltas[1] > maxdelta {
				maxdelta = deltas[1]
			}

		} else {
			for i := l - 1; i > 0; i-- {
				// detect ordering
				ordered = ordered && src[i-1] <= src[i]
				// delta-encode
				deltas[i] = src[i] - src[i-1]
				if deltas[i] > maxdelta {
					maxdelta = deltas[i]
				}
			}
		}

		// zigzag if unordered, leave the first value unchanged for RLE
		if !ordered {
			maxdelta = 0
			for i := 1; i < l; i++ {
				deltas[i] = zigZagEncodeInt64(int64(deltas[i]))
				// update maxdelta
				if deltas[i] > maxdelta {
					maxdelta = deltas[i]
				}
			}
		}

		// check deltas for RLE (both direct and zigzag encoded data may be eligible)
		var rle = true
		for i := 2; i < l; i++ {
			if deltas[1] != deltas[i] {
				rle = false
				break
			}
		}

		// Deltas are the same - encode with RLE
		if rle {
			// Large varints can take up to 10 bytes.  We're storing 3 + 1 type byte.

			// 4 high bits used for the encoding type
			typ := byte(timeCompressedRLE) << 4
			if !ordered {
				typ = byte(timeCompressedZigZagRLE) << 4
			}
			if div > 1 {
				// 4 low bits are the log10 divisor
				typ |= byte(math.Log10(float64(div)))
			}

			// Write the header
			w.Write([]byte{typ})
			count := 1

			// The first value
			var b [binary.MaxVarintLen64]byte
			binary.LittleEndian.PutUint64(b[:8], deltas[0])
			w.Write(b[:8])
			count += 8

			// The delta
			n := binary.PutUvarint(b[:], deltas[1])
			w.Write(b[:n])
			count += n

			// The number of times the delta is repeated
			n = binary.PutUvarint(b[:], uint64(len(deltas)-1))
			w.Write(b[:n])
			count += n

			return count, nil
		}
	} else {
		div = 1
	}

	// scale and zigzag first value too to simplify decoder loops
	if !ordered {
		deltas[0] = zigZagEncodeInt64(int64(deltas[0] / div))
	}

	// We can't compress this time-range, the deltas exceed 1 << 60
	if maxdelta > s8b.MaxValue || l == 1 {
		// Encode uncompressed.

		// 4 high bits of first byte store the encoding type
		typ := byte(timeUncompressed) << 4

		// Write the header
		w.Write([]byte{typ})
		count := 1

		// Write all source values
		for _, v := range src {
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], v)
			w.Write(b[:])
			count += 8
		}
		return count, nil
	}

	// Encode with simple8b - fist value is written unencoded using 8 bytes.
	encoded, err := s8b.EncodeUint64(deltas[1:])
	if err != nil {
		return 0, err
	}

	// 4 high bits of first byte store the encoding type for the block
	typ := byte(timeCompressedPacked) << 4
	if !ordered {
		typ = byte(timeCompressedZigZagPacked) << 4
	}

	// 4 low bits are the log10 divisor
	typ |= byte(math.Log10(float64(div)))
	w.Write([]byte{typ})
	count := 1

	// Write the first value since it's not part of the encoded values
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], deltas[0])
	w.Write(b[:])
	count += 8

	// Write the encoded values
	for _, v := range encoded {
		binary.LittleEndian.PutUint64(b[:], v)
		w.Write(b[:])
	}
	count += len(encoded) * 8
	return count, nil
}
