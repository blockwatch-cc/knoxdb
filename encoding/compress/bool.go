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

	"blockwatch.cc/knoxdb/encoding/bitset"
)

const (
	// booleanUncompressed is an uncompressed boolean format.
	// Not yet implemented.
	booleanUncompressed = 0

	// booleanCompressedBitPacked is an bit packed format using 1 bit per boolean
	booleanCompressedBitPacked = 1

	// TODO
	// integer compressions simple8, ?
)

func BitsetEncodedSize(b *bitset.Bitset) int {
	return b.EncodedSize() + 1 + binary.MaxVarintLen64
}

func BitsetEncodeAll(src *bitset.Bitset, w io.Writer) (int, error) {
	// Store the encoding type in the 4 high bits of the first byte
	w.Write([]byte{booleanCompressedBitPacked << 4})

	// Encode the number of bits written.
	var b [binary.MaxVarintLen64]byte
	i := binary.PutUvarint(b[:], uint64(src.Len()))
	w.Write(b[:i])

	// write raw bitset data
	w.Write(src.Bytes())
	return 1 + i + len(src.Bytes()), nil
}

func BitsetDecodeAll(b []byte, dst *bitset.Bitset) (*bitset.Bitset, error) {
	if len(b) == 0 {
		return dst, nil
	}

	// First byte stores the encoding type, only have 1 bit-packet format
	// currently ignore for now.
	b = b[1:]
	val, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, fmt.Errorf("compress: BitsetDecoder invalid count")
	}

	if dst.Cap() < int(val) {
		dst.Close()
		dst = nil
	}

	if dst == nil {
		dst = bitset.NewBitsetFromBytes(b[n:], int(val))
	} else {
		dst.SetFromBytes(b[n:], int(val))
	}
	return dst, nil
}
