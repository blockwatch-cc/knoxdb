// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// Inspired by InfluxData, MIT, https://github.com/influxdata/influxdb
package zip

import (
	"bytes"
	"encoding/binary"
	"io"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/s8b"
	"blockwatch.cc/knoxdb/pkg/num"
)

// Integer encoding uses delta and zig-zag encoding as pre-processing steps.
// Delte reduces the general size of numeric values which is beneficial for subsequent
// algorithms, but can introduce additional negative values. Negative values are
// undesired because of their leading 1 bits, hence zig-zag is used to interleave
// positive and negative integers across a range of positive integers.
//
// For example, [-2,-1,0,1] becomes [3,1,0,2]. See
// https://developers.google.com/protocol-buffers/docs/encoding?hl=en#signed-integers
//
// Depending on the result, either RLE (when all deltas are equal) or simple8b are
// used to further compress integers.  If all the zig zag encoded values are less
// than 1 << 60 - 1, they are compressed using simple8b encoding.  If any value is
// larger than 1 << 60 - 1, the original values (before delta and zigzag) are stored
// uncompressed.
//
// The serialization format contains a 1 byte header signalling the encoding type in
// the 4 high bits (the remaining 4 bits are reserved for future extension) followed
// by multiple 8 byte packed integers or 8 byte uncompressed integers.
//
// Future improvements to consider are to use a patched encoding such as PFOR
// if only a small number of values exceed the max compressed value range and
// an uncompressed 32bit format in case of 32bit (or 16bit) numbers with high
// likelihood of MSB bits being set (e.g. like for IPv4 addresses) and pairs of
// 32bit ints do not pack into a 2^60 bit integer.

const (
	// intUncompressed is an uncompressed format using 8 bytes per point
	intUncompressed64 = 0
	// intCompressedPacked is a bit-packed format using simple8b encoding
	intCompressedPacked = 1
	// intCompressedRLE is a run-length encoding format
	intCompressedRLE = 2
	// intUncompressed16 is an uncompressed format using 2 bytes per point
	intUncompressed16 = 3
	// intUncompressed32 is an uncompressed format using 4 bytes per point
	intUncompressed32 = 4
)

// upper bound, may store uncompressed 64bit
func Int64EncodedSize(n int) int {
	return n*8 + 1
}

// upper bound, s8b may inflate
func Int32EncodedSize(n int) int {
	return n*8 + 1
}

// upper bound, s8b may inflate
func Int16EncodedSize(n int) int {
	return n*8/3 + 6 + 1
}

// upper bound
func Int8EncodedSize(n int) int {
	return n*8/7 + 7 + 1
}

// upper bound, may store uncompressed 64bit strides
func Int256EncodedSize(n int) int {
	return n*32 + 1
}

// upper bound, may store uncompressed 64bit strides
func Int128EncodedSize(n int) int {
	return n*16 + 1
}

func EncodeInt128(src num.Int128Stride, w io.Writer) (int, error) {
	if src.Len() == 0 {
		return 0, nil
	}

	// prepare scratch space for each stride of int64 data
	scratch := arena.Alloc(arena.AllocBytes, Int64EncodedSize(src.Len()))
	buf := bytes.NewBuffer(scratch.([]byte)[:0])

	// write int128 as 2x int64 strides
	n, err := EncodeUint64(asU64(src.X0), buf)
	if err != nil {
		return 0, err
	}

	// write first stride data len
	_ = binary.Write(w, binary.LittleEndian, uint32(n))
	n += 4

	// write stride data
	w.Write(buf.Bytes())

	// encode second stride
	buf.Reset()
	m, err := EncodeUint64(src.X1, buf)
	if err != nil {
		return n, err
	}

	// write second stride data len
	_ = binary.Write(w, binary.LittleEndian, uint32(m))
	n += 4

	// write second stride data
	_, err = w.Write(buf.Bytes())
	n += m

	// release scratch buffer
	arena.Free(arena.AllocBytes, scratch)

	return n, err
}

func EncodeInt256(src num.Int256Stride, w io.Writer) (int, error) {
	if src.Len() == 0 {
		return 0, nil
	}

	// prepare scratch space for each stride of int64 data
	scratch := arena.Alloc(arena.AllocBytes, Int64EncodedSize(src.Len()))
	buf := bytes.NewBuffer(scratch.([]byte)[:0])

	// write int256 as 4x int64 strides
	n, err := EncodeUint64(asU64(src.X0), buf)
	if err != nil {
		return 0, err
	}

	// write first stride data len
	_ = binary.Write(w, binary.LittleEndian, uint32(n))
	n += 4

	// write stride data
	w.Write(buf.Bytes())

	for _, stride := range [][]uint64{src.X1, src.X2, src.X3} {
		// encode next stride
		buf.Reset()
		m, err := EncodeUint64(stride, buf)
		if err != nil {
			return n, err
		}

		// write next stride data len
		_ = binary.Write(w, binary.LittleEndian, uint32(m))
		n += 4 + m

		// write next stride data
		_, err = w.Write(buf.Bytes())
		if err != nil {
			return n, err
		}
	}

	// release scratch buffer
	arena.Free(arena.AllocBytes, scratch)

	return n, nil

}

func EncodeInt64(src []int64, w io.Writer) (int, error) {
	return EncodeUint64(asU64(src), w)
}

func EncodeUint64(src []uint64, w io.Writer) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}

	// alloc scratch space for in-place encoders
	buf := arena.Alloc(arena.AllocUint64, len(src))
	scratch := buf.([]uint64)[:len(src)]
	defer arena.Free(arena.AllocUint64, buf)

	// zzdelta writes to uint64 required by simple8b encoder
	maxdelta := zzDeltaEncodeUint64(scratch, src)

	if maxdelta > s8b.MaxValue {
		// There is an encoded value that's too big to simple8b encode, so
		// encode uncompressed.

		// 4 high bits of first byte store the encoding type for the block
		w.Write([]byte{intUncompressed64 << 4})
		count := 1
		for _, v := range src {
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], v)
			w.Write(b[:])
			count += 8
		}
		return count, nil
	}

	return encodeInteger(scratch, w)
}

func EncodeInt32(src []int32, w io.Writer) (int, error) {
	return EncodeUint32(asU32(src), w)
}

func EncodeUint32(src []uint32, w io.Writer) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}

	// alloc scratch space for in-place encoders
	buf := arena.Alloc(arena.AllocUint64, len(src))
	scratch := buf.([]uint64)[:len(src)]
	defer arena.Free(arena.AllocUint64, buf)

	// zzdelta writes to uint64 required by simple8b encoder
	maxdelta := zzDeltaEncodeUint32(scratch, src)
	if maxdelta > s8b.MaxValue32 {
		// There is an encoded value that's too big to efficiently
		// simple8b encode (the final result may be larger than uncompressed 32bit),
		// so encode uncompressed right away.

		// 4 high bits of first byte store the encoding type for the block
		w.Write([]byte{intUncompressed32 << 4})
		count := 1
		for _, v := range src {
			var b [4]byte
			binary.LittleEndian.PutUint32(b[:], v)
			w.Write(b[:])
			count += 4
		}
		return count, nil
	}

	return encodeInteger(scratch, w)
}

func EncodeInt16(src []int16, w io.Writer) (int, error) {
	return EncodeUint16(asU16(src), w)
}

func EncodeUint16(src []uint16, w io.Writer) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}

	// alloc scratch space for in-place encoders
	buf := arena.Alloc(arena.AllocUint64, len(src))
	scratch := buf.([]uint64)[:len(src)]
	defer arena.Free(arena.AllocUint64, buf)

	// zzdelta writes to uint64 required by simple8b encoder
	maxdelta := zzDeltaEncodeUint16(scratch, src)
	if maxdelta > s8b.MaxValue16 {
		// There is an encoded value that's too big to efficiently
		// simple8b encode (the final result may be larger than uncompressed 16bit),
		// so encode uncompressed right away.

		// 4 high bits of first byte store the encoding type for the block
		w.Write([]byte{intUncompressed16 << 4})
		count := 1
		for _, v := range src {
			var b [2]byte
			binary.LittleEndian.PutUint16(b[:], v)
			w.Write(b[:])
			count += 2
		}
		return count, nil
	}

	return encodeInteger(scratch, w)
}

func EncodeInt8(src []int8, w io.Writer) (int, error) {
	return EncodeUint8(asU8(src), w)
}

func EncodeUint8(src []uint8, w io.Writer) (int, error) {
	if len(src) == 0 {
		return 0, nil
	}

	// alloc scratch space for in-place encoders
	buf := arena.Alloc(arena.AllocUint64, len(src))
	scratch := buf.([]uint64)[:len(src)]
	defer arena.Free(arena.AllocUint64, buf)

	// zzdelta writes to uint64 required by simple8b encoder
	zzDeltaEncodeUint8(scratch, src)
	return encodeInteger(scratch, w)
}

func encodeInteger(deltas []uint64, w io.Writer) (int, error) {
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
			binary.LittleEndian.PutUint64(b[:8], deltas[0])
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

	// Encode with simple8b - fist value is written unencoded using 8 bytes.
	encoded, err := s8b.EncodeUint64(deltas[1:])
	if err != nil {
		return 0, err
	}

	// 4 high bits of first byte store the encoding type for the block
	w.Write([]byte{intCompressedPacked << 4})
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
