// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package zip

import (
	"encoding/binary"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/internal/bitset"
)

const (
	// booleanUncompressed is an uncompressed boolean format.
	// Invalid: Not implemented.
	booleanUncompressed = 0

	// booleanCompressedBitPacked is an bit packed format using 1 bit per boolean
	booleanCompressedBitPacked = 1

	// TODO
	// xroar
)

func readByte(r io.Reader) byte {
	var typ [1]byte
	_, _ = r.Read(typ[:])
	return typ[0]
}

func BitsetEncodedSize(b *bitset.Bitset) int {
	return b.EncodedSize() + 1 + binary.MaxVarintLen64
}

func EncodeBitset(src *bitset.Bitset, w io.Writer) (int, error) {
	// Store the encoding type in the 4 high bits of the first byte
	w.Write([]byte{booleanCompressedBitPacked << 4})

	// Encode the number of bits written.
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(src.Len()))
	w.Write(b[:])

	// write raw bitset data
	w.Write(src.Bytes())
	return 1 + 8 + len(src.Bytes()), nil
}

// ReadBitset is the io.Reader version of the bitset block decoder.
// It efficiently reads data into the target memory.
func ReadBitset(dst *bitset.Bitset, r io.Reader) (int64, error) {
	// read, but ignore type
	_ = readByte(r)

	// we need a byte reader for uvarints
	var sz uint64
	err := binary.Read(r, binary.LittleEndian, &sz)
	if err != nil {
		return 1, fmt.Errorf("zip: bitset decode: invalid size value: %v", err)
	}

	// resize bitset
	dst.Resize(int(sz))

	// and have it read the remainder
	_, err = dst.ReadFrom(r)
	if err != nil {
		return 9, fmt.Errorf("zip: bitset decode: %v", err)
	}

	return int64(9 + sz), nil
}

// DecodeBitset is a block decoder for bitset data.
func DecodeBitset(dst *bitset.Bitset, buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	// skip type byte
	buf = buf[1:]

	// read size
	sz, n := binary.Uvarint(buf)
	if n <= 0 {
		return fmt.Errorf("zip: decodeBitset invalid size value")
	}

	// resize bitset and copy data
	dst.SetFromBytes(buf[n:], int(sz))
	return nil
}
