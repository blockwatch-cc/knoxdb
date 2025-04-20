// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
// Legacy code adapted from github.com/jwilder/encoding
//
// Compatibility Notice
//
// This legacy version is no longer binary compatible with the optimized
// encoder/decoder due to re-purposing of selectors 0 and 1!

package generic

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
)

var (
	numBits = [...][2]byte{
		// { number of values, max bits per value }
		{60, 1}, // code 2
		{30, 2}, // code 3
		{20, 3}, // code 4
		{15, 4}, // code 5
		{12, 5}, // code 6
		{10, 6}, // code 7
		{8, 7},  // code 8
		{7, 8},  // code 9
		{6, 10}, // code 10
		{5, 12}, // code 11
		{4, 15}, // code 12
		{3, 20}, // code 13
		{2, 30}, // code 14
		{1, 60}, // code 15
	}
)

type packing struct {
	n, bit int
	unpack func(uint64, unsafe.Pointer)
	pack   func(unsafe.Pointer, uint64) uint64
}

var selectorLegacy64 [16]packing = [16]packing{
	{240, 0, unpack_240[uint64], pack_240[uint64]},
	{120, 0, unpack_120[uint64], pack_120[uint64]},
	{60, 1, unpack_60[uint64], pack_60[uint64]},
	{30, 2, unpack_30[uint64], pack_30[uint64]},
	{20, 3, unpack_20[uint64], pack_20[uint64]},
	{15, 4, unpack_15[uint64], pack_15[uint64]},
	{12, 5, unpack_12[uint64], pack_12[uint64]},
	{10, 6, unpack_10[uint64], pack_10[uint64]},
	{8, 7, unpack_8[uint64], pack_8[uint64]},
	{7, 8, unpack_7[uint64], pack_7[uint64]},
	{6, 10, unpack_6[uint64], pack_6[uint64]},
	{5, 12, unpack_5[uint64], pack_5[uint64]},
	{4, 15, unpack_4[uint64], pack_4[uint64]},
	{3, 20, unpack_3[uint64], pack_3[uint64]},
	{2, 30, unpack_2[uint64], pack_2[uint64]},
	{1, 60, unpack_1[uint64], pack_1[uint64]},
}

var selectorLegacy32 [16]packing = [16]packing{
	{240, 0, unpack_240[uint32], pack_240[uint32]},
	{120, 0, unpack_120[uint32], pack_120[uint32]},
	{60, 1, unpack_60[uint32], pack_60[uint32]},
	{30, 2, unpack_30[uint32], pack_30[uint32]},
	{20, 3, unpack_20[uint32], pack_20[uint32]},
	{15, 4, unpack_15[uint32], pack_15[uint32]},
	{12, 5, unpack_12[uint32], pack_12[uint32]},
	{10, 6, unpack_10[uint32], pack_10[uint32]},
	{8, 7, unpack_8[uint32], pack_8[uint32]},
	{7, 8, unpack_7[uint32], pack_7[uint32]},
	{6, 10, unpack_6[uint32], pack_6[uint32]},
	{5, 12, unpack_5[uint32], pack_5[uint32]},
	{4, 15, unpack_4[uint32], pack_4[uint32]},
	{3, 20, unpack_3[uint32], pack_3[uint32]},
	{2, 30, unpack_2[uint32], pack_2[uint32]},
	{1, 60, unpack_1[uint32], pack_1[uint32]},
}

var selectorLegacy16 [16]packing = [16]packing{
	{240, 0, unpack_240[uint16], pack_240[uint16]},
	{120, 0, unpack_120[uint16], pack_120[uint16]},
	{60, 1, unpack_60[uint16], pack_60[uint16]},
	{30, 2, unpack_30[uint16], pack_30[uint16]},
	{20, 3, unpack_20[uint16], pack_20[uint16]},
	{15, 4, unpack_15[uint16], pack_15[uint16]},
	{12, 5, unpack_12[uint16], pack_12[uint16]},
	{10, 6, unpack_10[uint16], pack_10[uint16]},
	{8, 7, unpack_8[uint16], pack_8[uint16]},
	{7, 8, unpack_7[uint16], pack_7[uint16]},
	{6, 10, unpack_6[uint16], pack_6[uint16]},
	{5, 12, unpack_5[uint16], pack_5[uint16]},
	{4, 15, unpack_4[uint16], pack_4[uint16]},
	{3, 20, unpack_3[uint16], pack_3[uint16]},
	{2, 30, unpack_2[uint16], pack_2[uint16]},
	{1, 60, unpack_1[uint16], pack_1[uint16]},
}

var selectorLegacy8 [16]packing = [16]packing{
	{240, 0, unpack_240[uint8], pack_240[uint8]},
	{120, 0, unpack_120[uint8], pack_120[uint8]},
	{60, 1, unpack_60[uint8], pack_60[uint8]},
	{30, 2, unpack_30[uint8], pack_30[uint8]},
	{20, 3, unpack_20[uint8], pack_20[uint8]},
	{15, 4, unpack_15[uint8], pack_15[uint8]},
	{12, 5, unpack_12[uint8], pack_12[uint8]},
	{10, 6, unpack_10[uint8], pack_10[uint8]},
	{8, 7, unpack_8[uint8], pack_8[uint8]},
	{7, 8, unpack_7[uint8], pack_7[uint8]},
	{6, 10, unpack_6[uint8], pack_6[uint8]},
	{5, 12, unpack_5[uint8], pack_5[uint8]},
	{4, 15, unpack_4[uint8], pack_4[uint8]},
	{3, 20, unpack_3[uint8], pack_3[uint8]},
	{2, 30, unpack_2[uint8], pack_2[uint8]},
	{1, 60, unpack_1[uint8], pack_1[uint8]},
}

// legacy use only
func unpack_240[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[240]T)(p)
	for i := range dst {
		dst[i] = 1
	}
}

func unpack_120[T types.Integer](v uint64, p unsafe.Pointer) {
	dst := (*[120]T)(p)
	for i := range dst {
		dst[i] = 1
	}
}

// pack240 packs 240 ones from in using 1 bit each
func pack_240[T types.Integer](_ unsafe.Pointer, _ uint64) uint64 {
	return 0
}

// pack120 packs 120 ones from in using 1 bit each
func pack_120[T types.Integer](_ unsafe.Pointer, _ uint64) uint64 {
	return 1 << 60
}

// EncodeLegacy returns a packed slice of the values from src.  If a value is over
// 1 << 60, an error is returned.  The input src is modified to avoid extra
// allocations.  If you need to re-use, use a copy.
func EncodeLegacy(src []uint64) ([]uint64, error) {
	i := 0

	// Re-use the input slice and write encoded values back in place
	dst := src
	j := 0

NEXTVALUE:
	for i < len(src) {
		remaining := src[i:]

		// try to pack run of 240 or 120 1s
		if len(remaining) >= 120 {
			// Invariant: len(a) is fixed to 120 or 240 values
			var a []uint64
			if len(remaining) >= 240 {
				a = remaining[:240]
			} else {
				a = remaining[:120]
			}

			// search for the longest sequence of 1s in a
			// Postcondition: k equals the index of the last 1 or -1
			k := 0
			for k = range a {
				if a[k] != 1 {
					k--
					break
				}
			}

			v := uint64(0)
			switch {
			case k == 239:
				// 240 1s
				i += 240

			case k >= 119:
				// at least 120 1s
				v = 1 << 60
				i += 120

			default:
				goto CODES
			}

			dst[j] = v
			j++
			continue
		}

	CODES:
		for code := range numBits {
			intN := int(numBits[code][0])
			bitN := numBits[code][1]
			if intN > len(remaining) {
				continue
			}

			maxVal := uint64(1 << (bitN & 0x3f))
			val := uint64(code+2) << S8B_BIT_SIZE

			for k, inV := range remaining {
				if k < intN {
					if inV >= maxVal {
						continue CODES
					}
					val |= inV << ((byte(k) * bitN) & 0x3f)
				} else {
					break
				}
			}
			dst[j] = val
			j += 1
			i += intN
			continue NEXTVALUE
		}
		return nil, ErrValueOutOfBounds
	}
	return dst[:j], nil
}

// Decode writes the uncompressed values from src to dst.  It returns the number
// of values written or an error.
// nocheckptr while the underlying struct layout doesn't change
//
//go:nocheckptr
func DecodeLegacy(dst, src []uint64) (value int, err error) {
	j := 0
	for _, v := range src {
		sel := (v >> 60) & 0xf
		selectorLegacy64[sel].unpack(v, unsafe.Pointer(&dst[j]))
		j += selectorLegacy64[sel].n
	}
	return j, nil
}

func CountLegacy(b []byte) (int, error) {
	var count int
	for len(b) >= 8 {
		v := binary.LittleEndian.Uint64(b[:8])
		b = b[8:]

		sel := v >> 60
		if sel >= 16 {
			return 0, fmt.Errorf("invalid selector value: %v", sel)
		}
		count += selectorLegacy64[sel].n
	}

	if len(b) > 0 {
		return 0, fmt.Errorf("invalid slice len remaining: %v", len(b))
	}
	return count, nil
}

// use nocheckptr because the underlying struct layout doesn't change
//
//go:nocheckptr
func DecodeLegacyUint64(dst []uint64, src []byte) (value int, err error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}

	i := 0
	j := 0
	l := len(src)
	for i < l {
		v := binary.LittleEndian.Uint64(src[i:])
		sel := (v >> 60) & 0xf
		selectorLegacy64[sel].unpack(v, unsafe.Pointer(&dst[j]))
		j += selectorLegacy64[sel].n
		i += 8
	}
	return j, nil
}

// use nocheckptr because the underlying struct layout doesn't change
//
//go:nocheckptr
func DecodeLegacyUint32(dst []uint32, src []byte) (value int, err error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}

	i := 0
	j := 0
	l := len(src)
	for i < l {
		v := binary.LittleEndian.Uint64(src[i:])
		sel := (v >> 60) & 0xf
		selectorLegacy32[sel].unpack(v, unsafe.Pointer(&dst[j]))
		j += selectorLegacy32[sel].n
		i += 8
	}
	return j, nil
}

// use nocheckptr because the underlying struct layout doesn't change
//
//go:nocheckptr
func DecodeLegacyUint16(dst []uint16, src []byte) (value int, err error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}

	i := 0
	j := 0
	l := len(src)
	for i < l {
		v := binary.LittleEndian.Uint64(src[i:])
		sel := (v >> 60) & 0xf
		selectorLegacy16[sel].unpack(v, unsafe.Pointer(&dst[j]))
		j += selectorLegacy16[sel].n
		i += 8
	}
	return j, nil
}

// use nocheckptr because the underlying struct layout doesn't change
//
//go:nocheckptr
func DecodeLegacyUint8(dst []uint8, src []byte) (value int, err error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}

	i := 0
	j := 0
	l := len(src)
	for i < l {
		v := binary.LittleEndian.Uint64(src[i:])
		sel := (v >> 60) & 0xf
		selectorLegacy8[sel].unpack(v, unsafe.Pointer(&dst[j]))
		j += selectorLegacy8[sel].n
		i += 8
	}
	return j, nil
}
