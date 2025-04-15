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
	decoderFuncUint32 = [...]func(dst []uint32, buf []byte) (int, error){
		decodeInvalidInt32,
		decodePackedInt32,
		decodeRLEInt32,
		decodeInvalidInt32,
		decodeUncompressedUint32,
	}
)

func decodeInvalidInt32(_ []uint32, _ []byte) (int, error) {
	return 0, errInvalidIntEncoding
}

// DecodeUint32 is a memory efficient (u)int32 integer decoder.
// This is the preferred method for decoding compressed data.
// It returns the number of decoded elements and an error.
var DecodeUint32 = decodeUint32

// DecodeInt32 is the int32 version of a 4 byte integer decoder.
// Its a simple wrapper around the uint variant.
func DecodeInt32(dst []int32, buf []byte) (int, error) {
	return DecodeUint32(util.ReinterpretSlice[int32, uint32](dst), buf)
}

// ReadUint32 is an io.Reader wrapper for decoding (u)int32 data.
// Since there is no framing (length) present, we assume the reader
// contains exactly the data that is required to decode a single block.
//
// Use this in combination with an entropy decoder like snappy or
// a bytes.Buffer().
//
// Internally ReadUint32 will first read all data into a scratch
// buffer and then forward to the related block decoder. This is because
// both generic and vector s8b and zigzag-delta decoders require all data
// to be present.
func ReadUint32(dst []uint32, r io.Reader) (int, int64, error) {
	// assume max length of encoded data
	scratch := arena.AllocBytes(Int32EncodedSize(cap(dst)))
	defer arena.Free(scratch)
	b := bytes.NewBuffer(scratch)
	_, err := io.Copy(b, r)
	if err != nil {
		return 0, 0, err
	}
	l, err := DecodeUint32(dst, b.Bytes())
	return l, int64(b.Len()), err
}

// ReadInt32 is the int32 version of an 4 byte integer decoder.
// Its a simple wrapper around the uint variant.
var ReadInt32 = ReadUint32

// main dispatcher function
func decodeUint32(dst []uint32, buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	typ := (buf[0] >> 4) & 0xf
	switch typ {
	case intCompressedPacked, intCompressedRLE, intUncompressed32:
		// ok
	default:
		return 0, fmt.Errorf("zip: unsupported int32 encoding %d", typ)
	}
	return decoderFuncUint32[typ](dst, buf[1:])
}

func decodeUncompressedUint32(dst []uint32, buf []byte) (int, error) {
	// check data size requirement
	if len(buf)&3 != 0 {
		return 0, fmt.Errorf("zip: decodeUncompressedInt32 buffer size is not multiple of 4")
	}

	// uncompressed is src values in little endian byte order
	// without zigzag or delta encoding
	dst = dst[:len(buf)>>2]
	for i := range dst {
		dst[i] = binary.LittleEndian.Uint32(buf[i<<2:])
	}
	return len(dst), nil
}

func decodePackedInt32(dst []uint32, buf []byte) (int, error) {
	// decode simple8 in 2 steps (count values to size dst slice, then decode)
	if len(buf) < 8 {
		return 0, fmt.Errorf("zip: decodePackedInt32 not enough data to decode packed value")
	}

	n := s8b.CountValues(buf[8:])
	if n < 0 {
		return 0, fmt.Errorf("zip: decodePackedInt32 bad count")
	}
	n += 1

	// pre-dimension target
	if cap(dst) < n {
		return 0, fmt.Errorf("zip: decodePackedInt32 target slice too small for %d values (max=%d)", n, cap(dst))
	}
	dst = dst[:n]

	// first value (simple8 stores 64bit)
	dst[0] = uint32(binary.LittleEndian.Uint64(buf))

	// decode compressed values
	c, err := s8b.DecodeUint32(dst[1:], buf[8:])
	if err != nil {
		return 0, fmt.Errorf("zip: decodePackedInt32 decode: %v", err)
	}
	if c != n-1 {
		return 0, fmt.Errorf("zip: decodePackedInt32 unexpected number of decoded values, got=%d, exp=%d", c, n-1)
	}

	// reverse zigzag and delta
	zzDeltaDecodeInt32(asI32(dst))

	return n, nil
}

func decodeRLEInt32(dst []uint32, buf []byte) (int, error) {
	if len(buf) < 10 {
		return 0, fmt.Errorf("zip: decodeRleInt32 not enough data")
	}

	// first value is stored uncompressed (just zig zagged)
	first := uint32(zigZagDecodeUint64(binary.LittleEndian.Uint64(buf)))
	buf = buf[8:]

	// read RLE delta value
	value, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("zip: decodeRleInt32 invalid delta value")
	}
	delta := uint32(zigZagDecodeUint64(value))
	buf = buf[n:]

	// read RLE count
	count, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("zip: decodeRleInt32 invalid count value")
	}

	// pre-dimension target
	n = int(count) + 1
	if cap(dst) < n {
		return 0, fmt.Errorf("zip: decodeRleInt32 target slice too small for %d values (max=%d)", n, cap(dst))
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
