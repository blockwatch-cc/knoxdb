// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package zip

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/s8b"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	decoderFuncUint16 = [...]func(dst []uint16, buf []byte) (int, error){
		decodeInvalidInt16,
		decodePackedInt16,
		decodeRLEInt16,
		decodeUncompressedUint16,
	}
)

func decodeInvalidInt16(_ []uint16, _ []byte) (int, error) {
	return 0, errInvalidIntEncoding
}

// DecodeUint16 is a memory efficient (u)int16 integer decoder.
// This is the preferred method for decoding compressed data.
var DecodeUint16 = decodeUint16

// DecodeInt16 is the int16 version of a 2 byte integer decoder.
// Its a simple wrapper around the uint variant.
func DecodeInt16(dst []int16, buf []byte) (int, error) {
	return DecodeUint16(util.ReinterpretSlice[int16, uint16](dst), buf)
}

// ReadUint16 is an io.Reader wrapper for decoding (u)int16 data.
// Since there is no framing (length) present, we assume the reader
// contains exactly the data that is required to decode a single block.
//
// Use this in combination with an entropy decoder like snappy or
// a bytes.Buffer().
//
// Internally ReadUint16 will first read all data into a scratch
// buffer and then forward to the related block decoder. This is because
// both generic and vector s8b and zigzag-delta decoders require all data
// to be present.
func ReadUint16(dst []uint16, r io.Reader) (int, int64, error) {
	// assume max length of encoded data
	scratch := arena.AllocBytes(Int16EncodedSize(cap(dst)))
	defer arena.Free(scratch)
	b := bytes.NewBuffer(scratch)
	_, err := io.Copy(b, r)
	if err != nil {
		return 0, 0, err
	}
	l, err := DecodeUint16(dst, b.Bytes())
	return l, int64(b.Len()), err
}

// ReadInt16 is the int16 version of an 2 byte integer decoder.
// Its a simple wrapper around the uint variant.
var ReadInt16 = ReadUint16

// main dispatcher function
func decodeUint16(dst []uint16, buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	typ := (buf[0] >> 4) & 0xf
	switch typ {
	case intCompressedPacked, intCompressedRLE, intUncompressed16:
		// ok
	default:
		return 0, fmt.Errorf("zip: unsupported int16 encoding %d", typ)
	}
	return decoderFuncUint16[typ](dst, buf[1:])
}

func decodeUncompressedUint16(dst []uint16, buf []byte) (int, error) {
	// check data size requirement
	if len(buf)&1 != 0 {
		return 0, fmt.Errorf("zip: decodeUncompressedInt16 buffer size is not multiple of 2")
	}

	// uncompressed is src values in little endian byte order
	// without zigzag or delta encoding
	dst = dst[:len(buf)>>1]
	for i := range dst {
		dst[i] = binary.LittleEndian.Uint16(buf[i<<1:])
	}
	return len(dst), nil
}

func decodePackedInt16(dst []uint16, buf []byte) (int, error) {
	// decode simple8 in 2 steps (count values to size dst slice, then decode)
	if len(buf) < 8 {
		return 0, fmt.Errorf("zip: decodePackedInt16 not enough data to decode packed value")
	}

	n := s8b.CountValues(buf[8:])
	if n < 0 {
		return 0, fmt.Errorf("zip: decodePackedInt16 bad count")
	}
	n += 1

	// pre-dimension target
	if cap(dst) < n {
		return 0, fmt.Errorf("zip: decodePackedInt16 target slice too small for %d values (max=%d)", n, cap(dst))
	}
	dst = dst[:n]

	// first value (simple8 stores 64bit)
	dst[0] = uint16(binary.LittleEndian.Uint64(buf))

	// decode compressed values
	c, err := s8b.DecodeUint16(dst[1:], buf[8:])
	if err != nil {
		return 0, fmt.Errorf("zip: decodePackedInt16 decode: %v", err)
	}
	if c != n-1 {
		return 0, fmt.Errorf("zip: decodePackedInt16 unexpected number of decoded values, got=%d, exp=%d", c, n-1)
	}

	// reverse zigzag and delta
	zzDeltaDecodeInt16(asI16(dst))

	return n, nil
}

func decodeRLEInt16(dst []uint16, buf []byte) (int, error) {
	if len(buf) < 10 {
		return 0, fmt.Errorf("zip: decodeRleInt16 not enough data")
	}

	// first value is stored uncompressed (just zig zagged)
	first := uint16(zigZagDecodeUint64(binary.LittleEndian.Uint64(buf)))
	buf = buf[8:]

	// read RLE delta value
	value, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("zip: decodeRleInt16 invalid delta value")
	}
	delta := uint16(zigZagDecodeUint64(value))
	buf = buf[n:]

	// read RLE count
	count, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("zip: decodeRleInt16 invalid count value")
	}

	// pre-dimension target
	n = int(count) + 1
	if cap(dst) < n {
		return 0, fmt.Errorf("zip: decodeRleInt16 target slice too small for %d values (max=%d)", n, cap(dst))
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
