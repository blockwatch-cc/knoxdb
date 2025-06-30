// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"io"
)

// A variable length integer is an encoding of 64-bit unsigned integers
// into between 1 and 9 bytes. The encoding has the following properties:
//
// - Smaller (and more common) values use fewer bytes and take up less space
//   than larger (and less common) values.
// - The length of any varint can be determined by looking at just the first
//   byte of the encoding.
// - Lexicographical and numeric ordering for varints are the same. Hence if
//   a group of varints are order lexicographically (that is to say, if they
//   are order by bytes.Cmp() with shorter varints coming first) then those
//   varints will also be in numeric order. This property means that varints
//   can be used as keys in the key/value backend storage and the records will
//   occur in numerical order of the keys.
// - The equality between lexicographical and numeric order also applies to
//   nested/concatenated varints.
// - Loop unrolled and optimized for smaller values.
//
// For spec see https://sqlite.org/src4/doc/trunk/www/varint.wiki

const (
	MaxVarintLen16 = 3
	MaxVarintLen32 = 5
	MaxVarintLen64 = 9
)

type Int interface {
	int | uint | uint64 | uint32 | uint16 | uint8 | int64 | int32 | int16 | int8
}

func UvarintLen[T Int](x T) int {
	var v [MaxVarintLen64]byte
	return PutUvarint(v[:], uint64(x))
}

func EncodeUvarint(x uint64) []byte {
	var v [MaxVarintLen64]byte
	n := PutUvarint(v[:], x)
	return v[:n]
}

func AppendUvarint(b []byte, x uint64) []byte {
	var v [MaxVarintLen64]byte
	n := PutUvarint(v[:], x)
	return append(b, v[:n]...)
}

func PutUvarint(b []byte, x uint64) int {
	if x <= 240 {
		b[0] = byte(x)
		return 1
	}
	if x <= 2287 {
		y := x - 240
		b[0] = byte(y>>8 + 241)
		b[1] = byte(y)
		return 2
	}
	if x <= 67823 {
		y := x - 2288
		b[0] = 249
		b[1] = byte(y >> 8)
		b[2] = byte(y)
		return 3
	}
	if x <= 0xffffff { // 16777215
		b[0] = 250
		b[1] = byte(x >> 16)
		b[2] = byte(x >> 8)
		b[3] = byte(x)
		return 4
	}
	w := x >> 32
	if w == 0 { // 32-bit values
		b[0] = 251
		b[1] = byte(x >> 24)
		b[2] = byte(x >> 16)
		b[3] = byte(x >> 8)
		b[4] = byte(x)
		return 5
	}
	if w <= 0xff {
		b[0] = 252
		b[1] = byte(w)
		b[2] = byte(x >> 24)
		b[3] = byte(x >> 16)
		b[4] = byte(x >> 8)
		b[5] = byte(x)
		return 6
	}
	if w <= 0xffff {
		b[0] = 253
		b[1] = byte(w >> 8)
		b[2] = byte(w)
		b[3] = byte(x >> 24)
		b[4] = byte(x >> 16)
		b[5] = byte(x >> 8)
		b[6] = byte(x)
		return 7
	}
	if w <= 0xffffff {
		b[0] = 254
		b[1] = byte(w >> 16)
		b[2] = byte(w >> 8)
		b[3] = byte(w)
		b[4] = byte(x >> 24)
		b[5] = byte(x >> 16)
		b[6] = byte(x >> 8)
		b[7] = byte(x)
		return 8
	}
	b[0] = 255
	b[1] = byte(w >> 24)
	b[2] = byte(w >> 16)
	b[3] = byte(w >> 8)
	b[4] = byte(w)
	b[5] = byte(x >> 24)
	b[6] = byte(x >> 16)
	b[7] = byte(x >> 8)
	b[8] = byte(x)
	return 9
}

func Uvarint(b []byte) (uint64, int) {
	b0 := b[0]
	if b0 <= 240 {
		return uint64(b0), 1
	}
	if b0 <= 248 {
		return 240 + uint64(b0-241)<<8 + uint64(b[1]), 2
	}
	if b0 == 249 {
		return 2288 + uint64(b[1])<<8 + uint64(b[2]), 3
	}
	if b0 == 250 {
		return uint64(b[1])<<16 + uint64(b[2])<<8 + uint64(b[3]), 4
	}
	x := uint64(b[1])<<24 + uint64(b[2])<<16 + uint64(b[3])<<8 + uint64(b[4])
	if b0 == 251 {
		return x, 5
	}
	if b0 == 252 {
		return x<<8 + uint64(b[5]), 6
	}
	if b0 == 253 {
		return x<<16 + uint64(b[5])<<8 + uint64(b[6]), 7
	}
	if b0 == 254 {
		return x<<24 + uint64(b[5])<<16 + uint64(b[6])<<8 + uint64(b[7]), 8
	}
	if b0 == 255 {
		return x<<32 + uint64(b[5])<<24 + uint64(b[6])<<16 + uint64(b[7])<<8 + uint64(b[8]), 9
	}
	return 0, 0 // Invalid b0
}

func WriteUvarint(w io.Writer, x uint64) (int, error) {
	var v [MaxVarintLen64]byte
	n := PutUvarint(v[:], x)
	return w.Write(v[:n])
}

func ReadUvarint(r io.ByteReader) (uint64, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch {
	case b <= 240:
		// If A0 is between 0 and 240 inclusive, then the result is the value of A0.
		return uint64(b), nil
	case b <= 248:
		// If A0 is between 241 and 248 inclusive, then the result is 240+256*(A0-241)+A1.
		x := 240 + 256*uint64(b-241)
		b, err = r.ReadByte()
		if err != nil {
			return 0, err
		}
		return x + uint64(b), nil
	case b == 249:
		// If A0 is 249 then the result is 2288+256*A1+A2.
		b, err = r.ReadByte()
		if err != nil {
			return 0, err
		}
		x := 2288 + 256*uint64(b)
		b, err = r.ReadByte()
		if err != nil {
			return 0, err
		}
		return x + uint64(b), nil
	default:
		// read big endian (3..8 bytes)
		var x uint64
		n := int(b - 246)
		for i := 1; i < n; i++ {
			b, err := r.ReadByte()
			if err != nil {
				return 0, err
			}
			x <<= 8
			x |= uint64(b)
		}
		return x, nil
	}
}
