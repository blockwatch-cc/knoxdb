// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// Original From: InfluxData, MIT
// https://github.com/influxdata/influxdb
package compress

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"blockwatch.cc/knoxdb/encoding/simple8b"
)

const (
	// timeUncompressed is a scaled uncompressed format using 8 bytes per timestamp
	timeUncompressed = 0
	// timeCompressedPackedSimple is a bit-packed format using simple8b encoding
	timeCompressedPackedSimple = 1
	// timeCompressedRLE is a run-length encoding format
	timeCompressedRLE = 2
	// timeUncompressedZigZag is scaling + zigzag
	timeUncompressedZigZag = 3
	// timeCompressedZigZag is scaling + zigzag + simple8b encoding
	timeCompressedZigZagPacked = 4
	// timeCompressedZigZagRLE is scaling + zigzag + RLE encoding
	timeCompressedZigZagRLE = 5
	// timeCompressedInvalid is used as stop value
	timeCompressedInvalid = 6
)

// upper bound
func TimeArrayEncodedSize(src []int64) int {
	return len(src)*8 + 1
}

// Differences from original
// - scale timestamps before delta-encoding (vs. scale deltas)
// - zigzag encode when timestamps are unordered

// TimeArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capacity to b.
//
// TimeArrayEncodeAll implements batch oriented versions of the six integer
// encoding types we support: uncompressed, simple8b and RLE and their zigzag
// encoded versions.
//
// When timestamp values to be encoded are sorted, we use the first three algos,
// when unordered we perform zig-zag encoding to avoid large neg/pos numbers. This
// allows to still use simple8b even when timestamps are out of order.
//
// When encoded, the values are first scaled then delta-encoded. The first value
// is the pre-scaled starting timestamp, subsequent values are the pre-scaled
// difference from the prior value.
//
// Important: TimeArrayEncodeAll modifies the contents of src by using it as
// scratch space for delta encoded values. It is NOT SAFE to use src after
// passing it into TimeArrayEncodeAll.
func TimeArrayEncodeAll(src []int64, w io.Writer) error {
	var (
		maxdelta, div uint64 = 0, 1e12
		ordered       bool   = true
		l             int    = len(src)
	)

	if l == 0 {
		return nil // Nothing to do
	}

	// To prevent an allocation of the entire block we reuse the
	// src slice to store the encoded deltas.
	deltas := ReintepretInt64ToUint64Slice(src)

	if len(deltas) > 1 {
		// find common divisor first, break early when we find a timestamp
		// at nanosec resolution
		for i := 0; i < l && div > 1; i++ {
			// If our value is divisible by 10, break.
			// Otherwise, try the next smallest divisor.
			v := deltas[i]
			for div > 1 && v%div != 0 {
				div /= 10
			}
		}

		// Only apply the divisor if it's greater than 1 since division is expensive.
		if div > 1 {
			// work backwards, but leave the first value unscaled to remain
			// compatible with the original simple and RLE encoding versions
			// that used to scale deltas instead of timestamps;

			deltas[l-1] /= div
			for i := l - 1; i > 1; i-- {
				// apply scaling factor
				deltas[i-1] /= div
				// detect ordering
				ordered = ordered && deltas[i-1] <= deltas[i]
				// delta-encode
				deltas[i] = deltas[i] - deltas[i-1]
				if deltas[i] > maxdelta {
					maxdelta = deltas[i]
				}
			}
			// remember to apply scaling to the first value, but without saving
			// the scaled value just yet because RLE encoding relies on unaltered
			// initial values
			ordered = ordered && deltas[0]/div <= deltas[1]
			deltas[1] = deltas[1] - deltas[0]/div
			if deltas[1] > maxdelta {
				maxdelta = deltas[1]
			}

		} else {
			for i := l - 1; i > 0; i-- {
				// detect ordering
				ordered = ordered && deltas[i-1] <= deltas[i]
				// delta-encode
				deltas[i] = deltas[i] - deltas[i-1]
				if deltas[i] > maxdelta {
					maxdelta = deltas[i]
				}
			}
		}

		// zigzag if unordered, leave the first value unchanged for RLE
		if !ordered {
			maxdelta = 0
			for i := 1; i < l; i++ {
				deltas[i] = ZigZagEncode(src[i])
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

			// The first value
			var b [binary.MaxVarintLen64]byte
			binary.BigEndian.PutUint64(b[:8], deltas[0])
			w.Write(b[:8])

			// The delta
			n := binary.PutUvarint(b[:], deltas[1])
			w.Write(b[:n])

			// The number of times the delta is repeated
			n = binary.PutUvarint(b[:], uint64(len(deltas)))
			w.Write(b[:n])

			return nil
		}
	}

	// scale and zigzag first value too to simplify decoder loops
	if !ordered {
		deltas[0] = ZigZagEncode(int64(deltas[0] / div))
	}

	// We can't compress this time-range, the deltas exceed 1 << 60
	if maxdelta > simple8b.MaxValue {
		// Encode uncompressed.

		// 4 high bits of first byte store the encoding type for the block
		typ := byte(timeUncompressed) << 4
		if !ordered {
			typ = byte(timeUncompressedZigZag) << 4
		}
		if div > 1 {
			// 4 low bits are the log10 divisor
			typ |= byte(math.Log10(float64(div)))
		}

		// Write the header
		w.Write([]byte{typ})

		// Write all deltas
		for _, v := range deltas {
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], v)
			w.Write(b[:])
		}
		return nil
	}

	// Encode with simple8b - fist value is written unencoded using 8 bytes.
	encoded, err := simple8b.EncodeAll(deltas[1:])
	if err != nil {
		return err
	}

	// 4 high bits of first byte store the encoding type for the block
	typ := byte(timeCompressedPackedSimple) << 4
	if !ordered {
		typ = byte(timeCompressedZigZagPacked) << 4
	}

	// 4 low bits are the log10 divisor
	typ |= byte(math.Log10(float64(div)))
	w.Write([]byte{typ})

	// Write the first value since it's not part of the encoded values
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], deltas[0])
	w.Write(b[:])

	// Write the encoded values
	for _, v := range encoded {
		binary.BigEndian.PutUint64(b[:], v)
		w.Write(b[:])
	}
	return nil
}

var (
	timeBatchDecoderFunc = [...]func(b []byte, dst []int64) ([]int64, error){
		timeBatchDecodeAllUncompressed,
		timeBatchDecodeAllSimple,
		timeBatchDecodeAllRLE,
		timeBatchDecodeAllZigZag,
		timeBatchDecodeAllZigZagPacked,
		timeBatchDecodeAllZigZagRLE,
		timeBatchDecodeAllInvalid,
	}
)

func TimeArrayDecodeAll(b []byte, dst []int64) ([]int64, error) {
	if len(b) == 0 {
		return []int64{}, nil
	}

	encoding := b[0] >> 4
	if encoding >= timeCompressedInvalid {
		encoding = timeCompressedInvalid // timeBatchDecodeAllInvalid
	}
	// log.Infof("pack: time block is encoded with type %d mod %d", encoding, b[0]&0xf)

	return timeBatchDecoderFunc[encoding&7](b, dst)
}

// legacy uncompressed encoding
func timeBatchDecodeAllUncompressed(b []byte, dst []int64) ([]int64, error) {
	mod := int64(math.Pow10(int(b[0] & 0xF))) // multiplier

	b = b[1:]
	if len(b)&0x7 != 0 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll expected multiple of 8 bytes")
	}

	count := len(b) / 8
	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	prev := uint64(0)
	if mod > 1 {
		for i := range dst {
			prev += binary.BigEndian.Uint64(b[i*8:])
			dst[i] = int64(prev) * mod
		}
	} else {
		for i := range dst {
			prev += binary.BigEndian.Uint64(b[i*8:])
			dst[i] = int64(prev)
		}
	}

	return dst, nil
}

// legacy simple8b encoding with or without scaling
func timeBatchDecodeAllSimple(b []byte, dst []int64) ([]int64, error) {
	if len(b) < 9 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll not enough data to decode packed timestamps")
	}

	mod := uint64(math.Pow10(int(b[0] & 0xF))) // multiplier

	count, err := simple8b.CountBytes(b[9:])
	if err != nil {
		return []int64{}, err
	}

	count += 1

	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	buf := ReintepretInt64ToUint64Slice(dst)

	// first value
	buf[0] = binary.BigEndian.Uint64(b[1:9])
	n, err := simple8b.DecodeBytesBigEndian(buf[1:], b[9:])
	if err != nil {
		return []int64{}, err
	}
	if n != count-1 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll unexpected number of values decoded; got=%d, exp=%d", n, count-1)
	}

	// Compute the prefix sum and scale the deltas back up
	last := buf[0]
	if mod > 1 {
		for i := 1; i < len(buf); i++ {
			dgap := buf[i] * mod
			buf[i] = last + dgap
			last = buf[i]
		}
	} else {
		for i := 1; i < len(buf); i++ {
			buf[i] += last
			last = buf[i]
		}
	}

	return dst, nil
}

// legacy encoding without zigzag and pre-scaling (only delta's may be scaled)
func timeBatchDecodeAllRLE(b []byte, dst []int64) ([]int64, error) {
	if len(b) < 9 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll not enough data to decode RLE starting value")
	}

	var k, n int

	// Lower 4 bits hold the 10 based exponent so we can scale the values back up
	mod := int64(math.Pow10(int(b[k] & 0xF)))
	k++

	// Next 8 bytes is the starting timestamp, unaltered
	first := binary.BigEndian.Uint64(b[k:])
	k += 8

	// Next 1-10 bytes is our run length delta, either scaled down by factor of 10 itself
	// (version 1) or based on scaled down input data (version 2), both are essentially
	// similar from perspecive of this decoding algorithm
	delta, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll invalid run length in decodeRLE")
	}
	k += n

	// Scale the delta back up (this is a one-time operation and works for both
	// encoding styles: scaled deltas or scaled timestamps)
	delta *= uint64(mod)

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll invalid repeat value in decodeRLE")
	}

	if cap(dst) < int(count) {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	acc := first
	for i := range dst {
		dst[i] = int64(acc)
		acc += delta
	}

	return dst, nil
}

// uncompressed encding using scaling + delta + zigzag
func timeBatchDecodeAllZigZag(b []byte, dst []int64) ([]int64, error) {
	// Lower 4 bits hold the 10 based exponent so we can scale the values back up
	mod := int64(math.Pow10(int(b[0] & 0xF)))

	b = b[1:]
	if len(b)&0x7 != 0 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll expected multiple of 8 bytes")
	}

	count := len(b) / 8
	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	prev := int64(0)
	if mod > 1 {
		// scaled version
		for i := range dst {
			prev += ZigZagDecode(binary.BigEndian.Uint64(b[i*8:]))
			dst[i] = prev * mod
		}
	} else {
		// unscaled version
		for i := range dst {
			prev += ZigZagDecode(binary.BigEndian.Uint64(b[i*8:]))
			dst[i] = prev
		}
	}

	return dst, nil
}

func timeBatchDecodeAllZigZagPacked(b []byte, dst []int64) ([]int64, error) {
	if len(b) < 9 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll not enough data to decode packed timestamps")
	}

	mod := int64(math.Pow10(int(b[0] & 0xF))) // multiplier

	count, err := simple8b.CountBytes(b[9:])
	if err != nil {
		return []int64{}, err
	}

	count += 1

	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	buf := ReintepretInt64ToUint64Slice(dst)

	// first value
	buf[0] = binary.BigEndian.Uint64(b[1:9])
	n, err := simple8b.DecodeBytesBigEndian(buf[1:], b[9:])
	if err != nil {
		return []int64{}, err
	}
	if n != count-1 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll unexpected number of values decoded; got=%d, exp=%d", n, count-1)
	}

	// Compute the prefix sum and scale the timestamps back up
	prev := int64(0)
	if mod > 1 {
		for i := 0; i < len(buf); i++ {
			prev += ZigZagDecode(buf[i])
			dst[i] = prev * mod
		}
	} else {
		for i := 0; i < len(buf); i++ {
			prev += ZigZagDecode(buf[i])
			dst[i] = prev
		}
	}

	return dst, nil
}

// encoding with pre-scaling + zigzag + RLE delta encoding
func timeBatchDecodeAllZigZagRLE(b []byte, dst []int64) ([]int64, error) {
	if len(b) < 9 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll not enough data to decode RLE starting value")
	}

	var k, n int

	// Lower 4 bits hold the 10 based exponent so we can scale the values back up
	mod := int64(math.Pow10(int(b[k] & 0xF)))
	k++

	// Next 8 bytes is the starting timestamp
	first := binary.BigEndian.Uint64(b[k:])
	k += 8

	// Next 1-10 bytes is our run length delta
	delta, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll invalid run length in decodeRLE")
	}
	k += n

	// ZigZag decode the delta
	delta = uint64(ZigZagDecode(delta))

	// Scale the delta back up (this is a one-time operation and works for both
	// encoding styles: scaled deltas or scaled timestamps)
	delta *= uint64(mod)

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("pack: TimeArrayDecodeAll invalid repeat value in decodeRLE")
	}

	if cap(dst) < int(count) {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	acc := first
	for i := range dst {
		dst[i] = int64(acc)
		acc += delta
	}

	return dst, nil
}

func timeBatchDecodeAllInvalid(b []byte, _ []int64) ([]int64, error) {
	return []int64{}, fmt.Errorf("pack: unknown time encoding %v", b[0]>>4)
}
