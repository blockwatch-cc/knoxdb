// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package zip

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/s8b"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	decoderFuncUint8 = [...]func(dst []uint8, buf []byte) (int, error){
		decodeInvalidInt8,
		decodePackedInt8,
		decodeRLEInt8,
	}
)

func decodeInvalidInt8(_ []uint8, _ []byte) (int, error) {
	return 0, errInvalidIntEncoding
}

// DecodeUint8 is a memory efficient (u)int8 integer decoder.
// This is the preferred method for decoding compressed data.
// It returns the number of decoded elements and an error.
var DecodeUint8 = decodeUint8

// DecodeInt8 is the int8 version of a 1 byte integer decoder.
// Its a simple wrapper around the uint variant.
func DecodeInt8(dst []int8, buf []byte) (int, error) {
	return DecodeUint8(util.ReinterpretSlice[int8, uint8](dst), buf)
}

// ReadUint8 is an io.Reader wrapper for decoding (u)int8 data.
// Since there is no framing (length) present, we assume the reader
// contains exactly the data that is required to decode a single block.
//
// Use this in combination with an entropy decoder like snappy or
// a bytes.Buffer().
//
// Internally ReadUint8 will first read all data into a scratch
// buffer and then forward to the related block decoder. This is because
// both generic and vector s8b and zigzag-delta decoders require all data
// to be present.
func ReadUint8(dst []uint8, r io.Reader) (int, int64, error) {
	// assume max length of encoded data
	scratch := arena.Alloc(arena.AllocBytes, Int8EncodedSize(cap(dst)))
	defer arena.Free(arena.AllocBytes, scratch)
	b := bytes.NewBuffer(scratch.([]byte)[:0])
	_, err := io.Copy(b, r)
	if err != nil {
		return 0, 0, err
	}
	l, err := DecodeUint8(dst, b.Bytes())
	return l, int64(b.Len()), err
}

// ReadInt8 is the int8 version of an byte integer decoder.
// Its a simple wrapper around the uint variant.
var ReadInt8 = ReadUint8

// main dispatcher function
func decodeUint8(dst []uint8, buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	typ := (buf[0] >> 4) & 0xf
	switch typ {
	case intCompressedPacked, intCompressedRLE:
		// ok
	default:
		return 0, fmt.Errorf("zip: unsupported int8 encoding %d", typ)
	}
	return decoderFuncUint8[typ](dst, buf[1:])
}

func decodePackedInt8(dst []uint8, buf []byte) (int, error) {
	// decode simple8 in 2 steps (count values to size dst slice, then decode)
	if len(buf) < 8 {
		return 0, fmt.Errorf("zip: decodePackedInt8 not enough data to decode packed value")
	}

	n, err := s8b.CountValues(buf[8:])
	if err != nil {
		return 0, fmt.Errorf("zip: decodePackedInt8 count: %v", err)
	}
	n += 1

	// pre-dimension target
	if cap(dst) < n {
		return 0, fmt.Errorf("zip: decodePackedInt8 target slice too small for %d values (max=%d)", n, cap(dst))
	}
	dst = dst[:n]

	// first value (simple8 stores 64bit)
	dst[0] = uint8(binary.LittleEndian.Uint64(buf))

	// decode compressed values
	c, err := s8b.DecodeUint8(dst[1:], buf[8:])
	if err != nil {
		return 0, fmt.Errorf("zip: decodePackedInt8 decode: %v", err)
	}
	if c != n-1 {
		return 0, fmt.Errorf("zip: decodePackedInt8 unexpected number of decoded values, got=%d, exp=%d", c, n-1)
	}

	// reverse zigzag and delta
	zzDeltaDecodeInt8(asI8(dst))

	return n, nil
}

func decodeRLEInt8(dst []uint8, buf []byte) (int, error) {
	if len(buf) < 10 {
		return 0, fmt.Errorf("zip: decodeRleInt8 not enough data")
	}

	// first value is stored uncompressed (just zig zagged)
	first := uint8(zigZagDecodeUint64(binary.LittleEndian.Uint64(buf)))
	buf = buf[8:]

	// read RLE delta value
	value, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("zip: decodeRleInt8 invalid delta value")
	}
	delta := uint8(zigZagDecodeUint64(value))
	buf = buf[n:]

	// read RLE count
	count, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("zip: decodeRleInt8 invalid count value")
	}

	// pre-dimension target
	n = int(count) + 1
	if cap(dst) < n {
		return 0, fmt.Errorf("zip: decodeRleInt8 target slice too small for %d values (max=%d)", n, cap(dst))
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

	return n, nil
}
