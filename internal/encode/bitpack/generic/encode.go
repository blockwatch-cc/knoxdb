// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"math/bits"

	"blockwatch.cc/knoxdb/internal/types"
)

// Encode packs a vector of unsigned values of type uint8, uint16, uint32 or
// uint64 into buffer at the mimimal bit width calculated from minv/maxv range.
// Before packing minv is subtracted from each value (MinFOR scheme).
// This makes all values positive and we can handle them as unsigned ints.
func Encode[T types.Integer](buf []byte, vals []T, minv, maxv T) ([]byte, int, error) {
	log2 := byte(bits.Len64(uint64(maxv - minv)))
	var n int
	if log2 < 59 {
		n = encode1(buf, vals, minv, max(log2, 1))
	} else {
		n = encode2(buf, vals, minv, max(log2, 1))
	}
	return buf[:n], int(log2), nil
}

// all packings < 59bit
func encode1[T types.Integer](buf []byte, src []T, minv T, log2 byte) int {
	var (
		i     int    // src pos
		n     int    // write pos
		vsh   byte   // value shift
		l     int    = len(src)
		shift byte   = (64 - log2) & 7 // 0..7
		mask  uint64 = 1<<log2 - 1
	)

	for i < l {
		vsh += shift
		vsh &= 7
		msb := (log2 + vsh - 1) >> 3

		// convert value, mask and shift
		val := uint64(src[i]-minv) & mask << vsh

		// merge top byte and write non-overlapping bytes
		switch msb {
		case 0:
			buf[n] |= byte(val)
		case 1:
			buf[n] |= byte(val >> 8)
			buf[n+1] = byte(val)
		case 2:
			buf[n] |= byte(val >> 16)
			buf[n+1] = byte(val >> 8)
			buf[n+2] = byte(val)
		case 3:
			buf[n] |= byte(val >> 24)
			buf[n+1] = byte(val >> 16)
			buf[n+2] = byte(val >> 8)
			buf[n+3] = byte(val)
		case 4:
			buf[n] |= byte(val >> 32)
			buf[n+1] = byte(val >> 24)
			buf[n+2] = byte(val >> 16)
			buf[n+3] = byte(val >> 8)
			buf[n+4] = byte(val)
		case 5:
			buf[n] |= byte(val >> 40)
			buf[n+1] = byte(val >> 32)
			buf[n+2] = byte(val >> 24)
			buf[n+3] = byte(val >> 16)
			buf[n+4] = byte(val >> 8)
			buf[n+5] = byte(val)
		case 6:
			buf[n] |= byte(val >> 48)
			buf[n+1] = byte(val >> 40)
			buf[n+2] = byte(val >> 32)
			buf[n+3] = byte(val >> 24)
			buf[n+4] = byte(val >> 16)
			buf[n+5] = byte(val >> 8)
			buf[n+6] = byte(val)
		case 7:
			buf[n] |= byte(val >> 56)
			buf[n+1] = byte(val >> 48)
			buf[n+2] = byte(val >> 40)
			buf[n+3] = byte(val >> 32)
			buf[n+4] = byte(val >> 24)
			buf[n+5] = byte(val >> 16)
			buf[n+6] = byte(val >> 8)
			buf[n+7] = byte(val)
		}

		// advance src and dst (with adjustment)
		i++
		n += int(msb + b2b(vsh == 0 || i%8 == 0))
	}

	// adjust buf len
	if shift > 0 && l%8 > 0 {
		n++
	}

	return n
}

// packings >= 59bit
func encode2[T types.Integer](buf []byte, src []T, minv T, log2 byte) int {
	var (
		i     int    // src pos
		n     int    // write pos
		vsh   byte   // value shift
		l     int    = len(src)
		shift byte   = (64 - log2) & 7 // 0..7
		mask  uint64 = 1<<log2 - 1
	)

	for i < l {
		vsh = (vsh + shift) & 7
		msb := (log2 + vsh - 1) >> 3

		if msb < 8 {
			// convert value, mask and shift
			val := uint64(src[i]-minv) & mask << vsh

			// merge top byte
			buf[n] |= byte(val >> 56)

			// write non-overlapping bytes
			buf[n+1] = byte(val >> 48)
			buf[n+2] = byte(val >> 40)
			buf[n+3] = byte(val >> 32)
			buf[n+4] = byte(val >> 24)
			buf[n+5] = byte(val >> 16)
			buf[n+6] = byte(val >> 8)
			buf[n+7] = byte(val)

		} else {
			// convert value, mask and shift
			val := uint64(src[i]-minv) & mask

			// merge top byte
			buf[n] |= byte(val >> (64 - vsh))

			// shift for correct remaining byte positions
			val <<= vsh

			// write non-overlapping bytes
			buf[n+1] = byte(val >> 56)
			buf[n+2] = byte(val >> 48)
			buf[n+3] = byte(val >> 40)
			buf[n+4] = byte(val >> 32)
			buf[n+5] = byte(val >> 24)
			buf[n+6] = byte(val >> 16)
			buf[n+7] = byte(val >> 8)
			buf[n+8] = byte(val)
		}

		// adjust src and write pos
		i++
		n += int(msb + b2b(vsh == 0 || i%8 == 0))
	}

	// adjust buf len
	if shift > 0 && l%8 > 0 {
		n++
	}

	return n
}

// Decode unpacks a vector of len(dst) integer values of type uint8,
// uint16, uint32 or uint64 into vals from bit-width log2 and adds minv.
// Dst must be allocated, have the desired length and src must contain
// necessary bits for. The function panics on any mismatch.
func Decode[T types.Integer](dst []T, buf []byte, log2 int, minv T) (int, error) {
	if log2 < 59 {
		return decode1(dst, buf, byte(log2), minv)
	} else {
		return decode2(dst, buf, byte(log2), minv)
	}
}

func decode1[T types.Integer](dst []T, buf []byte, log2 byte, minv T) (int, error) {
	var (
		i     int    // write pos
		n     int    // read pos
		vsh   byte   // value shift
		l     int    = len(dst)
		shift byte   = (64 - log2) & 7 // 0..7
		mask  uint64 = 1<<log2 - 1
	)

	for i < l {
		vsh += shift
		vsh &= 7
		msb := (log2 + vsh - 1) >> 3

		var val uint64
		switch msb {
		case 0:
			val = uint64(buf[n])
		case 1:
			val = uint64(buf[n])<<8 | uint64(buf[n+1])
		case 2:
			val = uint64(buf[n])<<16 | uint64(buf[n+1])<<8 | uint64(buf[n+2])
		case 3:
			val = uint64(buf[n])<<24 |
				uint64(buf[n+1])<<16 |
				uint64(buf[n+2])<<8 |
				uint64(buf[n+3])
		case 4:
			val = uint64(buf[n])<<32 |
				uint64(buf[n+1])<<24 |
				uint64(buf[n+2])<<16 |
				uint64(buf[n+3])<<8 |
				uint64(buf[n+4])
		case 5:
			val = uint64(buf[n])<<40 |
				uint64(buf[n+1])<<32 |
				uint64(buf[n+2])<<24 |
				uint64(buf[n+3])<<16 |
				uint64(buf[n+4])<<8 |
				uint64(buf[n+5])
		case 6:
			val = uint64(buf[n])<<48 |
				uint64(buf[n+1])<<40 |
				uint64(buf[n+2])<<32 |
				uint64(buf[n+3])<<24 |
				uint64(buf[n+4])<<16 |
				uint64(buf[n+5])<<8 |
				uint64(buf[n+6])
		case 7:
			val = uint64(buf[n])<<56 |
				uint64(buf[n+1])<<48 |
				uint64(buf[n+2])<<40 |
				uint64(buf[n+3])<<32 |
				uint64(buf[n+4])<<24 |
				uint64(buf[n+5])<<16 |
				uint64(buf[n+6])<<8 |
				uint64(buf[n+7])
		}

		// shift and mask output
		dst[i] = T(val>>vsh&mask) + minv
		i++
		n += int(msb + b2b(vsh == 0 || i%8 == 0))
	}

	return i, nil
}

func decode2[T types.Integer](dst []T, buf []byte, log2 byte, minv T) (int, error) {
	var (
		i     int    // write pos
		n     int    // read pos
		vsh   byte   // value shift
		l     int    = len(dst)
		shift byte   = (64 - log2) & 7 // 0..7
		mask  uint64 = 1<<log2 - 1
	)

	for i < l {
		vsh += shift
		vsh &= 7
		msb := (log2 + vsh - 1) >> 3

		if msb < 8 {
			// assemble from encoding
			val := uint64(buf[n])<<56 |
				uint64(buf[n+1])<<48 |
				uint64(buf[n+2])<<40 |
				uint64(buf[n+3])<<32 |
				uint64(buf[n+4])<<24 |
				uint64(buf[n+5])<<16 |
				uint64(buf[n+6])<<8 |
				uint64(buf[n+7])

			// shift, mask & add min-FOR
			dst[i] = T(val>>vsh&mask) + minv

		} else {
			// assemble from encoding
			val := uint64(buf[n+1])<<56 |
				uint64(buf[n+2])<<48 |
				uint64(buf[n+3])<<40 |
				uint64(buf[n+4])<<32 |
				uint64(buf[n+5])<<24 |
				uint64(buf[n+6])<<16 |
				uint64(buf[n+7])<<8 |
				uint64(buf[n+8])

			// shift into position
			val >>= vsh

			// patch top byte
			val |= uint64(buf[n]) << (64 - vsh)

			// mask & add min-FOR
			dst[i] = T(val&mask) + minv
		}

		i++
		n += int(msb + b2b(vsh == 0 || i%8 == 0))
	}

	return i, nil
}

// type packFunc func([]byte, uint64)

// var packFuncs = [8]packFunc{
// 	pack0, pack1, pack2, pack3, pack4, pack5, pack6, pack7,
// }

// func pack0(b []byte, v uint64) {
// 	b[0] |= byte(v) // merge top byte
// }

// func pack1(b []byte, v uint64) {
// 	_ = b[1]
// 	b[0] |= byte(v >> 8) // merge top byte
// 	b[1] = byte(v)
// }

// func pack2(b []byte, v uint64) {
// 	_ = b[2]
// 	b[0] |= byte(v >> 16) // merge top byte
// 	b[1] = byte(v >> 8)
// 	b[2] = byte(v)
// }

// func pack3(b []byte, v uint64) {
// 	_ = b[3]
// 	b[0] |= byte(v >> 24) // merge top byte
// 	b[1] = byte(v >> 16)
// 	b[2] = byte(v >> 8)
// 	b[3] = byte(v)
// }

// func pack4(b []byte, v uint64) {
// 	_ = b[4]
// 	b[0] |= byte(v >> 32) // merge top byte
// 	b[1] = byte(v >> 24)
// 	b[2] = byte(v >> 16)
// 	b[3] = byte(v >> 8)
// 	b[4] = byte(v)
// }

// func pack5(b []byte, v uint64) {
// 	_ = b[5]
// 	b[0] |= byte(v >> 40) // merge top byte
// 	b[1] = byte(v >> 32)
// 	b[2] = byte(v >> 24)
// 	b[3] = byte(v >> 16)
// 	b[4] = byte(v >> 8)
// 	b[5] = byte(v)
// }

// func pack6(b []byte, v uint64) {
// 	_ = b[6]
// 	b[0] |= byte(v >> 48) // merge top byte
// 	b[1] = byte(v >> 40)
// 	b[2] = byte(v >> 32)
// 	b[3] = byte(v >> 24)
// 	b[4] = byte(v >> 16)
// 	b[5] = byte(v >> 8)
// 	b[6] = byte(v)
// }

// func pack7(b []byte, v uint64) {
// 	_ = b[7]
// 	b[0] |= byte(v >> 56) // merge top byte
// 	b[1] = byte(v >> 48)
// 	b[2] = byte(v >> 40)
// 	b[3] = byte(v >> 32)
// 	b[4] = byte(v >> 24)
// 	b[5] = byte(v >> 16)
// 	b[6] = byte(v >> 8)
// 	b[7] = byte(v)
// }
