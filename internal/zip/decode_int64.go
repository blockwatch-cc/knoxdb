// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package zip

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/s8b"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	errInvalidIntEncoding = errors.New("zip: invalid integer encoding")

	decoderFuncUint64 = [...]func(dst []uint64, buf []byte) (int, error){
		decodeUncompressedUint64,
		decodePackedUint64,
		decodeRleUint64,
	}
)

// DecodeUint64 is a memory efficient (u)int64 integer decoder.
// This is the preferred method for decoding compressed data.
// It returns the number of decoded elements and an error.
var DecodeUint64 = decodeUint64

// DecodeInt64 is the int64 version of an 8byte integer decoder.
// Its a simple wrapper around the uint variant.
func DecodeInt64(dst []int64, buf []byte) (int, error) {
	return DecodeUint64(util.ReinterpretSlice[int64, uint64](dst), buf)
}

// ReadUint64 is an io.Reader wrapper for decoding (u)int64 data.
// Since there is no framing (length) present, we assume the reader
// contains exactly the data that is required to decode a single block.
//
// Use this in combination with an entropy decoder like snappy or
// a bytes.Buffer().
//
// Internally ReadUint64 will first read all data into a scratch
// buffer and then forward to the related block decoder. This is because
// both generic and vector s8b and zigzag-delta decoders require all data
// to be present.
func ReadUint64(dst []uint64, r io.Reader) (int, int64, error) {
	// assume max length of encoded data
	scratch := arena.Alloc(arena.AllocBytes, Int64EncodedSize(cap(dst)))
	defer arena.Free(arena.AllocBytes, scratch)
	b := bytes.NewBuffer(scratch.([]byte)[:0])
	_, err := io.Copy(b, r)
	if err != nil {
		return 0, 0, err
	}
	l, err := DecodeUint64(dst, b.Bytes())
	return l, int64(b.Len()), err
}

// ReadInt64 is the int64 version of an 8 byte integer decoder.
// Its a simple wrapper around the uint variant.
var ReadInt64 = ReadUint64

// main dispatcher function
func decodeUint64(dst []uint64, buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	typ := (buf[0] >> 4) & 0xf
	switch typ {
	case intCompressedPacked, intCompressedRLE, intUncompressed64:
		// ok
	default:
		return 0, fmt.Errorf("zip: unsupported int64 encoding %d", typ)
	}
	return decoderFuncUint64[typ](dst, buf[1:])
}

func decodeUncompressedUint64(dst []uint64, buf []byte) (int, error) {
	// check data size requirement
	if len(buf)&7 != 0 {
		return 0, fmt.Errorf("zip: decodeUncompressedInt64 buffer size is not multiple of 8")
	}

	// uncompressed is src values in little endian byte order
	// without zigzag or delta encoding
	dst = dst[:len(buf)>>3]
	for i := range dst {
		dst[i] = binary.LittleEndian.Uint64(buf[i<<3:])
	}
	return len(dst), nil
}

func decodePackedUint64(dst []uint64, buf []byte) (int, error) {
	// decode simple8 in 2 steps (count values to size dst slice, then decode)
	if len(buf) < 8 {
		return 0, fmt.Errorf("zip: decodeSimpleInt64 not enough data")
	}

	n := s8b.CountValues(buf[8:])
	if n < 0 {
		return 0, fmt.Errorf("zip: decodeSimpleInt64 bad count")
	}
	n += 1

	// pre-dimension target
	if cap(dst) < n {
		return 0, fmt.Errorf("zip: decodeSimpleInt64 target slice too small for %d values (max=%d)", n, cap(dst))
	}
	dst = dst[:n]

	// first value
	dst[0] = binary.LittleEndian.Uint64(buf)

	// decode compressed values
	c, err := s8b.DecodeUint64(dst[1:], buf[8:])
	if err != nil {
		return 0, fmt.Errorf("zip: decodeSimpleInt64 decode: %v", err)
	}
	if c != n-1 {
		return 0, fmt.Errorf("zip: decodeSimpleInt64 unexpected number of decoded values, got=%d, exp=%d", c, n-1)
	}

	// reverse zigzag and delta
	zzDeltaDecodeInt64(asI64(dst))

	return len(dst), nil
}

func decodeRleUint64(dst []uint64, buf []byte) (int, error) {
	if len(buf) < 10 {
		return 0, fmt.Errorf("zip: decodeRleInt64 not enough data")
	}

	// first value is stored uncompressed (just zig zagged)
	first := uint64(zigZagDecodeUint64(binary.LittleEndian.Uint64(buf)))
	buf = buf[8:]

	// read RLE delta value
	value, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("zip: decodeRleInt64 invalid delta value")
	}
	delta := uint64(zigZagDecodeUint64(value))
	buf = buf[n:]

	// read RLE count
	count, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("zip: decodeRleInt64 invalid count value")
	}

	// pre-dimension target
	n = int(count) + 1
	if cap(dst) < n {
		return 0, fmt.Errorf("zip: decodeRleInt64 target slice too small for %d values (max=%d)", n, cap(dst))
	}
	dst = dst[:n]

	// fill slice
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

	return len(dst), nil
}
