// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// Original from: InfluxData, MIT
// https://github.com/influxdata/influxdb
package compress

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"blockwatch.cc/knoxdb/encoding/s8b"
	"blockwatch.cc/knoxdb/vec"
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

var uint64Pool = &sync.Pool{
	New: func() interface{} { return make([]uint64, 0, DefaultMaxPointsPerBlock) },
}

// upper bound, may store uncompressed 64bit strides
func Int256ArrayEncodedSize(src vec.Int256LLSlice) int {
	return src.Len()*32 + 1
}

// upper bound, may store uncompressed 64bit strides
func Int128ArrayEncodedSize(src vec.Int128LLSlice) int {
	return src.Len()*16 + 1
}

// upper bound, may store uncompressed 64bit
func Int64ArrayEncodedSize(src []int64) int {
	return len(src)*8 + 1
}

// upper bound, s8b may inflate
func Int32ArrayEncodedSize(src []int32) int {
	return len(src)*8 + 1
}

// upper bound, s8b may inflate
func Int16ArrayEncodedSize(src []int16) int {
	return len(src)*8/3 + 6 + 1
}

// upper bound
func Int8ArrayEncodedSize(src []int8) int {
	return len(src)*8/7 + 7 + 1
}

// upper bound, may store uncompressed 64bit
func Uint64ArrayEncodedSize(src []uint64) int {
	return len(src)*8 + 1
}

// upper bound
func Uint32ArrayEncodedSize(src []uint32) int {
	return len(src)*8 + 1
}

// upper bound
func Uint16ArrayEncodedSize(src []uint16) int {
	return len(src)*8/3 + 6 + 1
}

// upper bound
func Uint8ArrayEncodedSize(src []uint8) int {
	return len(src)*8/7 + 7 + 1
}

func ArrayEncodeAllInt64(src []int64, w io.Writer) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}

	deltas := ReintepretInt64ToUint64Slice(src)
	maxdelta := zzDeltaEncodeUint64(deltas)

	return integerArrayEncodeAll(deltas, maxdelta, w)
}

func ArrayEncodeAllInt32(src []int32, w io.Writer) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}

	deltas := ReintepretInt32ToUint32Slice(src)
	maxdelta := uint64(zzDeltaEncodeUint32(deltas))

	var (
		cp []uint64
		v  interface{}
	)
	if len(deltas) <= DefaultMaxPointsPerBlock {
		v = uint64Pool.Get()
		cp = v.([]uint64)[:len(deltas)]
	} else {
		cp = make([]uint64, len(deltas))
	}
	for i, v := range deltas {
		cp[i] = uint64(v)
	}

	n, err := integerArrayEncodeAll(cp, maxdelta, w)
	if v != nil {
		uint64Pool.Put(v)
	}

	return n, err
}

func ArrayEncodeAllInt16(src []int16, w io.Writer) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}

	deltas := ReintepretInt16ToUint16Slice(src)
	maxdelta := uint64(zzDeltaEncodeUint16(deltas))

	var (
		cp []uint64
		v  interface{}
	)
	if len(deltas) <= DefaultMaxPointsPerBlock {
		v = uint64Pool.Get()
		cp = v.([]uint64)[:len(deltas)]
	} else {
		cp = make([]uint64, len(deltas))
	}
	for i, v := range deltas {
		cp[i] = uint64(v)
	}

	n, err := integerArrayEncodeAll(cp, maxdelta, w)
	if v != nil {
		uint64Pool.Put(v)
	}

	return n, err
}

func ArrayEncodeAllInt8(src []int8, w io.Writer) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}

	deltas := ReintepretInt8ToUint8Slice(src)
	maxdelta := uint64(zzDeltaEncodeUint8(deltas))

	var (
		cp []uint64
		v  interface{}
	)
	if len(deltas) <= DefaultMaxPointsPerBlock {
		v = uint64Pool.Get()
		cp = v.([]uint64)[:len(deltas)]
	} else {
		cp = make([]uint64, len(deltas))
	}
	for i, v := range deltas {
		cp[i] = uint64(v)
	}

	n, err := integerArrayEncodeAll(cp, maxdelta, w)
	if v != nil {
		uint64Pool.Put(v)
	}

	return n, err
}

func ArrayEncodeAllUint64(src []uint64, w io.Writer) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}

	maxdelta := zzDeltaEncodeUint64(src)

	return integerArrayEncodeAll(src, maxdelta, w)
}

func ArrayEncodeAllUint32(src []uint32, w io.Writer) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}

	deltas := src
	maxdelta := uint64(zzDeltaEncodeUint32(deltas))

	var (
		cp []uint64
		v  interface{}
	)
	if len(deltas) <= DefaultMaxPointsPerBlock {
		v = uint64Pool.Get()
		cp = v.([]uint64)[:len(deltas)]
	} else {
		cp = make([]uint64, len(deltas))
	}
	for i, v := range deltas {
		cp[i] = uint64(v)
	}

	n, err := integerArrayEncodeAll(cp, maxdelta, w)
	if v != nil {
		uint64Pool.Put(v)
	}

	return n, err
}

func ArrayEncodeAllUint16(src []uint16, w io.Writer) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}

	deltas := src
	maxdelta := uint64(zzDeltaEncodeUint16(deltas))

	var (
		cp []uint64
		v  interface{}
	)
	if len(deltas) <= DefaultMaxPointsPerBlock {
		v = uint64Pool.Get()
		cp = v.([]uint64)[:len(deltas)]
	} else {
		cp = make([]uint64, len(deltas))
	}
	for i, v := range deltas {
		cp[i] = uint64(v)
	}

	n, err := integerArrayEncodeAll(cp, maxdelta, w)
	if v != nil {
		uint64Pool.Put(v)
	}

	return n, err
}

func ArrayEncodeAllUint8(src []uint8, w io.Writer) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}

	deltas := src
	maxdelta := uint64(zzDeltaEncodeUint8(deltas))

	var (
		cp []uint64
		v  interface{}
	)
	if len(deltas) <= DefaultMaxPointsPerBlock {
		v = uint64Pool.Get()
		cp = v.([]uint64)[:len(deltas)]
	} else {
		cp = make([]uint64, len(deltas))
	}
	for i, v := range deltas {
		cp[i] = uint64(v)
	}

	n, err := integerArrayEncodeAll(cp, maxdelta, w)
	if v != nil {
		uint64Pool.Put(v)
	}

	return n, err
}

func integerArrayEncodeAll(deltas []uint64, maxdelta uint64, w io.Writer) (int, error) {
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
			count := 1
			// The first value
			binary.BigEndian.PutUint64(b[:8], deltas[0])
			w.Write(b[:8])
			count += 8
			// The first delta
			n := binary.PutUvarint(b[:], deltas[1])
			w.Write(b[:n])
			count += n
			// The number of times the delta is repeated
			n = binary.PutUvarint(b[:], uint64(len(deltas)-1))
			w.Write(b[:n])
			count += n
			return count, nil
		}
	}

	if maxdelta > s8b.MaxValue {
		// There is an encoded value that's too big to simple8b encode, so
		// encode uncompressed.

		// 4 high bits of first byte store the encoding type for the block
		w.Write([]byte{intUncompressed << 4})
		count := 1
		for _, v := range deltas {
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(v))
			w.Write(b[:])
			count += 8
		}
		return count, nil
	}

	// Encode with simple8b - fist value is written unencoded using 8 bytes.
	encoded, err := s8b.EncodeAll(deltas[1:])
	if err != nil {
		return 0, err
	}

	// 4 high bits of first byte store the encoding type for the block
	w.Write([]byte{intCompressedSimple << 4})
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
		count += 8
	}
	return count, nil
}

var (
	decoderFuncInt64 = [...]func(b []byte, dst []int64) ([]int64, error){
		integerBatchDecodeAllUncompressed,
		decodeAllSimpleInt64,
		decodeAllRLEInt64,
		decodeAllInvalidInt64,
	}
	decoderFuncInt32 = [...]func(b []byte, dst []int32) ([]int32, error){
		decodeAllInvalidInt32,
		decodeAllSimpleInt32,
		decodeAllRLEInt32,
		decodeAllInvalidInt32,
	}
	decoderFuncInt16 = [...]func(b []byte, dst []int16) ([]int16, error){
		decodeAllInvalidInt16,
		decodeAllSimpleInt16,
		decodeAllRLEInt16,
		decodeAllInvalidInt16,
	}
	decoderFuncInt8 = [...]func(b []byte, dst []int8) ([]int8, error){
		decodeAllInvalidInt8,
		decodeAllSimpleInt8,
		decodeAllRLEInt8,
		decodeAllInvalidInt8,
	}
)

func ArrayDecodeAllInt64(b []byte, dst []int64) ([]int64, error) {
	if len(b) == 0 {
		return []int64{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	return decoderFuncInt64[encoding&3](b, dst)
}

func ArrayDecodeAllInt32(b []byte, dst []int32) ([]int32, error) {
	if len(b) == 0 {
		return []int32{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	return decoderFuncInt32[encoding&3](b, dst)
}

func ArrayDecodeAllInt16(b []byte, dst []int16) ([]int16, error) {
	if len(b) == 0 {
		return []int16{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	return decoderFuncInt16[encoding&3](b, dst)
}

func ArrayDecodeAllInt8(b []byte, dst []int8) ([]int8, error) {
	if len(b) == 0 {
		return []int8{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	return decoderFuncInt8[encoding&3](b, dst)
}

func ArrayDecodeAllUint64(b []byte, dst []uint64) ([]uint64, error) {
	if len(b) == 0 {
		return []uint64{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	res, err := decoderFuncInt64[encoding&3](b, ReintepretUint64ToInt64Slice(dst))
	return ReintepretInt64ToUint64Slice(res), err
}

func ArrayDecodeAllUint32(b []byte, dst []uint32) ([]uint32, error) {
	if len(b) == 0 {
		return []uint32{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	res, err := decoderFuncInt32[encoding&3](b, ReintepretUint32ToInt32Slice(dst))
	return ReintepretInt32ToUint32Slice(res), err
}

func ArrayDecodeAllUint16(b []byte, dst []uint16) ([]uint16, error) {
	if len(b) == 0 {
		return []uint16{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	res, err := decoderFuncInt16[encoding&3](b, ReintepretUint16ToInt16Slice(dst))
	return ReintepretInt16ToUint16Slice(res), err
}

func ArrayDecodeAllUint8(b []byte, dst []uint8) ([]uint8, error) {
	if len(b) == 0 {
		return []uint8{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	res, err := decoderFuncInt8[encoding&3](b, ReintepretUint8ToInt8Slice(dst))
	return ReintepretInt8ToUint8Slice(res), err
}

func integerBatchDecodeAllUncompressed(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b)&0x7 != 0 {
		return []int64{}, fmt.Errorf("compress: IntegerArrayDecodeAll expected multiple of 8 bytes")
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

func decodeAllSimpleInt64(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int64{}, fmt.Errorf("compress: decodeAllSimpleInt64 not enough data to decode packed value")
	}

	count, err := s8b.CountValues(b[8:])
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
	buf[0] = binary.LittleEndian.Uint64(b)
	// decode compressed values
	n, err := s8b.DecodeAllUint64(buf[1:], b[8:])
	if err != nil {
		return []int64{}, err
	}
	if n != count-1 {
		return []int64{}, fmt.Errorf("compress: decodeAllSimpleInt64 unexpected number of values decoded; got=%d, exp=%d", n, count-1)
	}

	zzDeltaDecodeInt64(dst)

	return dst, nil
}

func decodeAllSimpleInt32(b []byte, dst []int32) ([]int32, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int32{}, fmt.Errorf("compress: decodeAllSimpleInt32 not enough data to decode packed value")
	}

	count, err := s8b.CountValues(b[8:])
	if err != nil {
		return []int32{}, err
	}
	count += 1

	if cap(dst) < count {
		dst = make([]int32, count)
	} else {
		dst = dst[:count]
	}

	buf := ReintepretInt32ToUint32Slice(dst)

	// first value
	buf[0] = uint32(binary.LittleEndian.Uint64(b))
	// decode compressed values
	n, err := s8b.DecodeAllUint32(buf[1:], b[8:])
	if err != nil {
		return []int32{}, err
	}
	if n != count-1 {
		return []int32{}, fmt.Errorf("compress: decodeAllSimpleInt32 unexpected number of values decoded; got=%d, exp=%d", n, count-1)
	}

	zzDeltaDecodeInt32(dst)

	return dst, nil
}

func decodeAllSimpleInt16(b []byte, dst []int16) ([]int16, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int16{}, fmt.Errorf("compress: decodeAllSimpleInt16 not enough data to decode packed value")
	}

	count, err := s8b.CountValues(b[8:])
	if err != nil {
		return []int16{}, err
	}
	count += 1

	if cap(dst) < count {
		dst = make([]int16, count)
	} else {
		dst = dst[:count]
	}

	buf := ReintepretInt16ToUint16Slice(dst)

	// first value
	buf[0] = uint16(binary.LittleEndian.Uint64(b))
	// decode compressed values
	n, err := s8b.DecodeAllUint16(buf[1:], b[8:])
	if err != nil {
		return []int16{}, err
	}
	if n != count-1 {
		return []int16{}, fmt.Errorf("compress: decodeAllSimpleInt16 unexpected number of values decoded; got=%d, exp=%d", n, count-1)
	}

	zzDeltaDecodeInt16(dst)

	return dst, nil
}

func decodeAllSimpleInt8(b []byte, dst []int8) ([]int8, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int8{}, fmt.Errorf("compress: decodeAllSimpleInt8 not enough data to decode packed value")
	}

	count, err := s8b.CountValues(b[8:])
	if err != nil {
		return []int8{}, err
	}
	count += 1

	if cap(dst) < count {
		dst = make([]int8, count)
	} else {
		dst = dst[:count]
	}

	buf := ReintepretInt8ToUint8Slice(dst)

	// first value
	buf[0] = uint8(binary.LittleEndian.Uint64(b))
	// decode compressed values
	n, err := s8b.DecodeAllUint8(buf[1:], b[8:])
	if err != nil {
		return []int8{}, err
	}
	if n != count-1 {
		return []int8{}, fmt.Errorf("compress: decodeAllSimpleInt8 unexpected number of values decoded; got=%d, exp=%d", n, count-1)
	}

	zzDeltaDecodeInt8(dst)

	return dst, nil
}

func decodeAllRLEInt64(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int64{}, fmt.Errorf("compress: IntegerArrayDecodeAll not enough data to decode RLE starting value")
	}

	var k, n int

	// Next 8 bytes is the starting value
	first := ZigZagDecode(binary.BigEndian.Uint64(b[k : k+8]))
	k += 8

	// Next 1-10 bytes is the delta value
	value, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("compress: IntegerArrayDecodeAll invalid RLE delta value")
	}
	k += n
	delta := ZigZagDecode(value)

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("compress: IntegerArrayDecodeAll invalid RLE repeat value")
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

func decodeAllRLEInt32(b []byte, dst []int32) ([]int32, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int32{}, fmt.Errorf("compress: decodeAllRLEInt32 not enough data to decode RLE starting value")
	}

	var k, n int

	// Next 8 bytes is the starting value
	first := ZigZagDecode(binary.BigEndian.Uint64(b[k : k+8]))
	k += 8

	// Next 1-10 bytes is the delta value
	value, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int32{}, fmt.Errorf("compress: decodeAllRLEInt32 invalid RLE delta value")
	}
	k += n
	delta := ZigZagDecode(value)

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int32{}, fmt.Errorf("compress: decodeAllRLEInt32 invalid RLE repeat value")
	}
	count += 1

	if cap(dst) < int(count) {
		dst = make([]int32, count)
	} else {
		dst = dst[:count]
	}

	if delta == 0 {
		for i := range dst {
			dst[i] = int32(first)
		}
	} else {
		acc := first
		for i := range dst {
			dst[i] = int32(acc)
			acc += delta
		}
	}

	return dst, nil
}

func decodeAllRLEInt16(b []byte, dst []int16) ([]int16, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int16{}, fmt.Errorf("compress: decodeAllRLEInt16 not enough data to decode RLE starting value")
	}

	var k, n int

	// Next 8 bytes is the starting value
	first := ZigZagDecode(binary.BigEndian.Uint64(b[k : k+8]))
	k += 8

	// Next 1-10 bytes is the delta value
	value, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int16{}, fmt.Errorf("compress: decodeAllRLEInt16 invalid RLE delta value")
	}
	k += n
	delta := ZigZagDecode(value)

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int16{}, fmt.Errorf("compress: decodeAllRLEInt16 invalid RLE repeat value")
	}
	count += 1

	if cap(dst) < int(count) {
		dst = make([]int16, count)
	} else {
		dst = dst[:count]
	}

	if delta == 0 {
		for i := range dst {
			dst[i] = int16(first)
		}
	} else {
		acc := first
		for i := range dst {
			dst[i] = int16(acc)
			acc += delta
		}
	}

	return dst, nil
}

func decodeAllRLEInt8(b []byte, dst []int8) ([]int8, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int8{}, fmt.Errorf("compress: decodeAllRLEInt8 not enough data to decode RLE starting value")
	}

	var k, n int

	// Next 8 bytes is the starting value
	first := ZigZagDecode(binary.BigEndian.Uint64(b[k : k+8]))
	k += 8

	// Next 1-10 bytes is the delta value
	value, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int8{}, fmt.Errorf("compress: decodeAllRLEInt8 invalid RLE delta value")
	}
	k += n
	delta := ZigZagDecode(value)

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int8{}, fmt.Errorf("compress: decodeAllRLEInt8 invalid RLE repeat value")
	}
	count += 1

	if cap(dst) < int(count) {
		dst = make([]int8, count)
	} else {
		dst = dst[:count]
	}

	if delta == 0 {
		for i := range dst {
			dst[i] = int8(first)
		}
	} else {
		acc := first
		for i := range dst {
			dst[i] = int8(acc)
			acc += delta
		}
	}

	return dst, nil
}

func integerBatchDecodeAllInvalid(b []byte, _ []int64) ([]int64, error) {
	return []int64{}, fmt.Errorf("compress: unknown integer encoding %v", b[0]>>4)
}

func decodeAllInvalidInt64(b []byte, _ []int64) ([]int64, error) {
	return []int64{}, fmt.Errorf("compress: unknown integer encoding %v", b[0]>>4)
}
func decodeAllInvalidInt32(b []byte, _ []int32) ([]int32, error) {
	return []int32{}, fmt.Errorf("compress: unknown integer encoding %v", b[0]>>4)
}
func decodeAllInvalidInt16(b []byte, _ []int16) ([]int16, error) {
	return []int16{}, fmt.Errorf("compress: unknown integer encoding %v", b[0]>>4)
}

func decodeAllInvalidInt8(b []byte, _ []int8) ([]int8, error) {
	return []int8{}, fmt.Errorf("compress: unknown integer encoding %v", b[0]>>4)
}
