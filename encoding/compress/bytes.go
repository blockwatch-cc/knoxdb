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
)

const (
	// bytesUncompressed is a an uncompressed format encoding as raw bytes.
	// Default
	bytesUncompressed = 0
)

var (
	errBytesBatchDecodeInvalidLength  = fmt.Errorf("compress: BytesArrayDecodeAll invalid encoded length")
	errBytesBatchDecodeLengthOverflow = fmt.Errorf("compress: BytesArrayDecodeAll length overflow")
	errBytesBatchDecodeShortBuffer    = fmt.Errorf("compress: BytesArrayDecodeAll short buffer")
)

func BytesArrayEncodedSize(src [][]byte) int {
	var sz int
	for _, v := range src {
		l := len(v)
		sz += l + uvarIntLen(l)
	}
	return sz + 1
}

// BytesArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capactity to b.
//
// Currently only the string compression scheme used snappy.
func BytesArrayEncodeAll(src [][]byte, w io.Writer) error {
	w.Write([]byte{bytesUncompressed << 4})
	if len(src) == 0 {
		return nil
	}

	var buf [binary.MaxVarintLen64]byte
	for i := range src {
		l := binary.PutUvarint(buf[:], uint64(len(src[i])))
		w.Write(buf[:l])
		w.Write(src[i])
	}

	return nil
}

func BytesArrayDecodeAll(b []byte, dst [][]byte) ([][]byte, error) {
	if len(b) == 0 {
		return [][]byte{}, nil
	}

	// skip the encoding type
	b = b[1:]
	var i, l int

	sz := cap(dst)
	if sz == 0 {
		sz = DefaultMaxPointsPerBlock
		dst = make([][]byte, sz)
	} else {
		dst = dst[:sz]
	}

	j := 0

	for i < len(b) {
		length, n := binary.Uvarint(b[i:])
		if n <= 0 {
			return [][]byte{}, errBytesBatchDecodeInvalidLength
		}

		// The length of this string plus the length of the variable byte encoded length
		l = int(length) + n

		lower := i + n
		upper := lower + int(length)
		if upper < lower {
			return [][]byte{}, errBytesBatchDecodeLengthOverflow
		}
		if upper > len(b) {
			return [][]byte{}, errBytesBatchDecodeShortBuffer
		}

		val := b[lower:upper]
		if j < len(dst) {
			dst[j] = val
		} else {
			dst = append(dst, val) // force a resize
			dst = dst[:cap(dst)]
		}
		i += l
		j++
	}

	return dst[:j], nil
}
