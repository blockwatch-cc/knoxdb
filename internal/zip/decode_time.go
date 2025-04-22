// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// Original From: InfluxData, MIT
// https://github.com/influxdata/influxdb
package zip

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/s8b"
)

var (
	timeDecoderFunc = [...]func(dst []int64, buf []byte, mod int64) (int, error){
		decodeUncompressedTime,
		decodePackedTime,
		decodeRLETime,
		decodeZigZagPackedTime,
		decodeZigZagRLETime,
	}
)

// DecodeTime is a memory efficient integer time decoder.
// This is the preferred method for decoding compressed data.
// It returns the number of decoded elements and an error.
func DecodeTime(dst []int64, buf []byte) (int, error) {
	// call decoder with slice arg and return new slice dimension
	return decodeTime(dst, buf)
}

func decodeTime(dst []int64, buf []byte) (int, error) {
	// empty time buffers have zero length
	if len(buf) == 0 {
		return 0, nil
	}

	// read type and multiplier from header byte
	typ := (buf[0] >> 4) & 0xf
	mod := int64(math.Pow10(int(buf[0] & 0xF)))
	if typ > timeCompressedZigZagRLE {
		return 0, fmt.Errorf("zip: unknown time encoding %d", typ)
	}
	return timeDecoderFunc[typ](dst, buf[1:], mod)
}

// ReadTime is an io.Reader wrapper for decoding timestamp data.
// Since there is no framing (length) present, we assume the reader
// contains exactly the data that is required to decode a single block.
//
// Use this in combination with an entropy decoder like snappy or
// a bytes.Buffer().
//
// Internally DecodeTimeReader will first read all data into a scratch
// buffer and then forward to the related block decoder. This is because
// both generic and vector s8b and zigzag-delta decoders require all data
// to be present.
func ReadTime(dst []int64, r io.Reader) (int, int64, error) {
	// assume max length of encoded data
	scratch := arena.AllocBytes(TimeEncodedSize(cap(dst)))
	defer arena.Free(scratch)
	b := bytes.NewBuffer(scratch)
	_, err := io.Copy(b, r)
	if err != nil {
		return 0, 0, err
	}
	l, err := DecodeTime(dst, b.Bytes())
	return l, int64(b.Len()), err
}

// uncompressed encoding, no scaling
func decodeUncompressedTime(dst []int64, buf []byte, _ int64) (int, error) {
	// check data size requirement
	if len(buf)&7 != 0 {
		return 0, fmt.Errorf("zip: decodeUncompressedTime buffer size %d is not multiple of 8: \n%s", len(buf), hex.Dump(buf))
	}

	// uncompressed is src values in little endian byte order
	// without zigzag or delta encoding
	dst = dst[:len(buf)>>3]
	for i := range dst {
		dst[i] = int64(binary.LittleEndian.Uint64(buf[i<<3:]))
	}
	return len(dst), nil
}

// simple8b encoding with scaling (no zigzag because timestamps are strictly monotone)
func decodePackedTime(dst []int64, buf []byte, mod int64) (int, error) {
	// check buffer size
	if len(buf) < 8 {
		return 0, fmt.Errorf("zip: decodePackedTime not enough data")
	}

	// decode number of encoded values
	n := s8b.CountValues(buf[8:])
	if n < 0 {
		return 0, fmt.Errorf("zip: decodePackedTime bad count")
	}
	n += 1

	// pre-dimension target
	if cap(dst) < n {
		return 0, fmt.Errorf("zip: decodePackedTime target slice too small for %d values (max=%d)", n, cap(dst))
	}
	dst = dst[:n]

	// first value
	dst[0] = int64(binary.LittleEndian.Uint64(buf))

	// decode compressed values
	c, err := s8b.DecodeUint64(asU64(dst[1:]), buf[8:], 0)
	if err != nil {
		return 0, fmt.Errorf("zip: decodePackedTime decode: %v", err)
	}
	if c != n-1 {
		return 0, fmt.Errorf("zip: decodePackedTime unexpected number of decoded values, got=%d, exp=%d", c, n-1)
	}

	// reverse zigzag, delta and scaling
	// Compute the prefix sum and scale the deltas back up
	last := dst[0]
	if mod > 1 {
		for i := 1; i < len(dst); i++ {
			dgap := dst[i] * mod
			dst[i] = last + dgap
			last = dst[i]
		}
	} else {
		for i := 1; i < len(dst); i++ {
			dst[i] += last
			last = dst[i]
		}
	}

	return n, nil
}

// encoding without zigzag and pre-scaling (only delta's may be scaled)
func decodeRLETime(dst []int64, buf []byte, mod int64) (int, error) {
	if len(buf) < 10 {
		return 0, fmt.Errorf("zip: decodeRLETime not enough data")
	}

	// read start value
	first := int64(binary.LittleEndian.Uint64(buf))
	buf = buf[8:]

	// read RLE delta value
	delta, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("zip: decodeRLETime invalid delta value")
	}
	buf = buf[n:]

	// read RLE count
	count, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("zip: decodeRLETime: invalid repeat value")
	}
	n = int(count) + 1
	if cap(dst) < n {
		return 0, fmt.Errorf("zip: decodeRLETime: target slice too small for %d values (max=%d)", n, cap(dst))
	}
	dst = dst[:n]

	// fill slice
	if delta == 0 {
		for i := range dst {
			dst[i] = first
		}
	} else {
		// Scale the delta back up
		d := int64(delta) * mod
		acc := first
		for i := range dst {
			dst[i] = acc
			acc += d
		}
	}
	return n, nil
}

// simple8 with pre-scaling, zigzag and delta
func decodeZigZagPackedTime(dst []int64, buf []byte, mod int64) (int, error) {
	// check buffer size
	if len(buf) < 8 {
		return 0, fmt.Errorf("zip: decodeZigZagPackedTime not enough data")
	}

	// decode number of encoded values
	n := s8b.CountValues(buf[8:])
	if n < 0 {
		return 0, fmt.Errorf("zip: decodeZigZagPackedTime bad count")
	}
	n += 1

	// pre-dimension target
	if cap(dst) < n {
		return 0, fmt.Errorf("zip: decodeZigZagPackedTime target slice too small for %d values (max=%d)", n, cap(dst))
	}
	dst = dst[:n]

	// first value
	dst[0] = int64(binary.LittleEndian.Uint64(buf))

	// decode compressed values
	c, err := s8b.DecodeUint64(asU64(dst[1:]), buf[8:], 0)
	if err != nil {
		return 0, fmt.Errorf("zip: decodeZigZagPackedTime decode: %v", err)
	}
	if c != n-1 {
		return 0, fmt.Errorf("zip: decodeZigZagPackedTime unexpected number of decoded values, got=%d, exp=%d", c, n-1)
	}

	// FIXME: experimental (test AVX2 support)
	zzDeltaDecodeTime(asU64(dst), uint64(mod))

	// Compute the prefix sum and scale the timestamps back up
	// prev := int64(0)
	// if mod > 1 {
	// 	for i, v := range dst {
	// 		prev += zigZagDecodeUint64(uint64(v))
	// 		dst[i] = prev * mod
	// 	}
	// } else {
	// 	for i, v := range dst {
	// 		prev += zigZagDecodeUint64(uint64(v))
	// 		dst[i] = prev
	// 	}
	// }

	return n, nil
}

// RLE with pre-scaling, zigzag, delta encoding
func decodeZigZagRLETime(dst []int64, buf []byte, mod int64) (int, error) {
	if len(buf) < 10 {
		return 0, fmt.Errorf("zip: decodeZigZagRLETime not enough data")
	}

	// read start value
	first := int64(binary.LittleEndian.Uint64(buf))
	buf = buf[8:]

	// read RLE delta value
	delta, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("zip: decodeZigZagRLETime invalid delta value")
	}
	buf = buf[n:]

	// read RLE count
	count, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("zip: decodeZigZagRLETime: invalid repeat value")
	}

	n = int(count) + 1
	if cap(dst) < n {
		return 0, fmt.Errorf("zip: time zzRLE decode: target slice too small for %d values (max=%d)", n, cap(dst))
	}
	dst = dst[:n]

	// fill slice
	if delta == 0 {
		for i := range dst {
			dst[i] = first
		}
	} else {
		// ZigZag decode the delta and scale the delta back up
		di := zigZagDecodeUint64(delta) * mod
		acc := first
		for i := range dst {
			dst[i] = acc
			acc += di
		}
	}
	return n, nil
}
