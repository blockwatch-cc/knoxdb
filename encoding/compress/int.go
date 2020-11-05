// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// Original from: InfluxData, MIT
// https://github.com/influxdata/influxdb
package compress

import (
	"encoding/binary"
	"fmt"
	"io"

	"blockwatch.cc/packdb-pro/encoding/simple8b"
	"blockwatch.cc/packdb-pro/util"
)

// Integer encoding uses two different strategies depending on the range of values in
// the uncompressed data.  Encoded values are first encoding used zig zag encoding.
// This interleaves positive and negative integers across a range of positive integers.
//
// For example, [-2,-1,0,1] becomes [3,1,0,2]. See
// https://developers.google.com/protocol-buffers/docs/encoding?hl=en#signed-integers
// for more information.
//
// If all the zig zag encoded values are less than 1 << 60 - 1, they are compressed using
// simple8b encoding.  If any value is larger than 1 << 60 - 1, the values are stored uncompressed.
//
// Each encoded byte slice contains a 1 byte header followed by multiple 8 byte packed integers
// or 8 byte uncompressed integers.  The 4 high bits of the first byte indicate the encoding type
// for the remaining bytes.
//
// There are currently two encoding types that can be used with room for 16 total.  These additional
// encoding slots are reserved for future use.  One improvement to be made is to use a patched
// encoding such as PFOR if only a small number of values exceed the max compressed value range.
// This should improve compression ratios with very large integers near the ends of the int64 range.

const (
	// intUncompressed is an uncompressed format using 8 bytes per point
	intUncompressed = 0
	// intCompressedSimple is a bit-packed format using simple8b encoding
	intCompressedSimple = 1
	// intCompressedRLE is a run-length encoding format
	intCompressedRLE = 2
)

// upper bound
func IntegerArrayEncodedSize(src []int64) int {
	return len(src)*8 + 1
}

// upper bound
func Int32ArrayEncodedSize(src []int32) int {
	return len(src)*4 + 1
}

// upper bound
func UnsignedArrayEncodedSize(src []uint64) int {
	return len(src)*8 + 1
}

func IntegerArrayEncodeAll(src []int64, w io.Writer) (int64, int64, error) {
	return integerArrayEncodeAll(src, w, false)
}

// IntegerArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capactity to b.
//
// IntegerArrayEncodeAll implements batch oriented versions of the three integer
// encoding types we support: uncompressed, simple8b and RLE.
//
// Important: IntegerArrayEncodeAll modifies the contents of src by using it as
// scratch space for delta encoded values. It is NOT SAFE to use src after
// passing it into IntegerArrayEncodeAll.
func integerArrayEncodeAll(src []int64, w io.Writer, isUint bool) (int64, int64, error) {
	if len(src) == 0 {
		return 0, 0, nil
	}

	var maxdelta = uint64(0)
	min, max := src[0], src[0]

	// To prevent an allocation of the entire block we're encoding reuse the
	// src slice to store the encoded deltas.
	deltas := ReintepretInt64ToUint64Slice(src)

	for i := len(deltas) - 1; i > 0; i-- {
		if isUint {
			min = int64(util.MinU64(uint64(min), deltas[i]))
			max = int64(util.MaxU64(uint64(max), deltas[i]))
		} else {
			min = util.Min64(min, src[i])
			max = util.Max64(max, src[i])
		}
		deltas[i] = deltas[i] - deltas[i-1]
		deltas[i] = ZigZagEncode(int64(deltas[i]))
		if deltas[i] > maxdelta {
			maxdelta = deltas[i]
		}
	}

	deltas[0] = ZigZagEncode(int64(deltas[0]))

	if len(deltas) > 2 {
		var rle = true
		for i := 2; i < len(deltas); i++ {
			if deltas[1] != deltas[i] {
				rle = false
				break
			}
		}

		if rle {
			// 4 high bits used for the encoding type
			w.Write([]byte{intCompressedRLE << 4})
			var b [binary.MaxVarintLen64]byte
			// The first value
			binary.BigEndian.PutUint64(b[:8], deltas[0])
			w.Write(b[:8])
			// The first delta
			n := binary.PutUvarint(b[:], deltas[1])
			w.Write(b[:n])
			// The number of times the delta is repeated
			n = binary.PutUvarint(b[:], uint64(len(deltas)-1))
			w.Write(b[:n])
			return min, max, nil
		}
	}

	if maxdelta > simple8b.MaxValue {
		// There is an encoded value that's too big to simple8b encode, so
		// encode uncompressed.

		// 4 high bits of first byte store the encoding type for the block
		w.Write([]byte{intUncompressed << 4})
		for _, v := range deltas {
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(v))
			w.Write(b[:])
		}
		return min, max, nil
	}

	// Encode with simple8b - fist value is written unencoded using 8 bytes.
	encoded, err := simple8b.EncodeAll(deltas[1:])
	if err != nil {
		return 0, 0, err
	}

	// 4 high bits of first byte store the encoding type for the block
	w.Write([]byte{intCompressedSimple << 4})

	// Write the first value since it's not part of the encoded values
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], deltas[0])
	w.Write(b[:])

	// Write the encoded values
	for _, v := range encoded {
		binary.BigEndian.PutUint64(b[:], v)
		w.Write(b[:])
	}
	return min, max, nil
}

// UnsignedArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capactity to b.
//
// UnsignedArrayEncodeAll implements batch oriented versions of the three integer
// encoding types we support: uncompressed, simple8b and RLE.
//
// Important: IntegerArrayEncodeAll modifies the contents of src by using it as
// scratch space for delta encoded values. It is NOT SAFE to use src after
// passing it into IntegerArrayEncodeAll.
func UnsignedArrayEncodeAll(src []uint64, w io.Writer) (uint64, uint64, error) {
	srcint := ReintepretUint64ToInt64Slice(src)
	min, max, err := integerArrayEncodeAll(srcint, w, true)
	return uint64(min), uint64(max), err
}

var (
	integerBatchDecoderFunc = [...]func(b []byte, dst []int64) ([]int64, error){
		integerBatchDecodeAllUncompressed,
		integerBatchDecodeAllSimple,
		integerBatchDecodeAllRLE,
		integerBatchDecodeAllInvalid,
	}
)

func IntegerArrayDecodeAll(b []byte, dst []int64) ([]int64, error) {
	if len(b) == 0 {
		return []int64{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	return integerBatchDecoderFunc[encoding&3](b, dst)
}

func UnsignedArrayDecodeAll(b []byte, dst []uint64) ([]uint64, error) {
	if len(b) == 0 {
		return []uint64{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	res, err := integerBatchDecoderFunc[encoding&3](b, ReintepretUint64ToInt64Slice(dst))
	return ReintepretInt64ToUint64Slice(res), err
}

func integerBatchDecodeAllUncompressed(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b)&0x7 != 0 {
		return []int64{}, fmt.Errorf("pack: IntegerArrayDecodeAll expected multiple of 8 bytes")
	}

	count := len(b) / 8
	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	prev := int64(0)
	for i := range dst {
		prev += ZigZagDecode(binary.BigEndian.Uint64(b[i*8:]))
		dst[i] = prev
	}

	return dst, nil
}

func integerBatchDecodeAllSimple(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int64{}, fmt.Errorf("pack: IntegerArrayDecodeAll not enough data to decode packed value")
	}

	count, err := simple8b.CountBytes(b[8:])
	if err != nil {
		return []int64{}, err
	}

	count += 1
	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	// first value
	dst[0] = ZigZagDecode(binary.BigEndian.Uint64(b))

	// decode compressed values
	buf := ReintepretInt64ToUint64Slice(dst)
	n, err := simple8b.DecodeBytesBigEndian(buf[1:], b[8:])
	if err != nil {
		return []int64{}, err
	}
	if n != count-1 {
		return []int64{}, fmt.Errorf("pack: IntegerArrayDecodeAll unexpected number of values decoded; got=%d, exp=%d", n, count-1)
	}

	// calculate prefix sum
	prev := dst[0]
	for i := 1; i < len(dst); i++ {
		prev += ZigZagDecode(uint64(dst[i]))
		dst[i] = prev
	}

	return dst, nil
}

func integerBatchDecodeAllRLE(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int64{}, fmt.Errorf("pack: IntegerArrayDecodeAll not enough data to decode RLE starting value")
	}

	var k, n int

	// Next 8 bytes is the starting value
	first := ZigZagDecode(binary.BigEndian.Uint64(b[k : k+8]))
	k += 8

	// Next 1-10 bytes is the delta value
	value, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("pack: IntegerArrayDecodeAll invalid RLE delta value")
	}
	k += n
	delta := ZigZagDecode(value)

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("pack: IntegerArrayDecodeAll invalid RLE repeat value")
	}
	count += 1

	if cap(dst) < int(count) {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	if delta == 0 {
		for i := range dst {
			dst[i] = first
		}
	} else {
		acc := first
		for i := range dst {
			dst[i] = acc
			acc += delta
		}
	}

	return dst, nil
}

func integerBatchDecodeAllInvalid(b []byte, _ []int64) ([]int64, error) {
	return []int64{}, fmt.Errorf("pack: unknown integer encoding %v", b[0]>>4)
}
