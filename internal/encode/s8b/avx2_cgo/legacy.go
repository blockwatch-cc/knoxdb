package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"
)

const (
	MaxValue     = (1 << 60) - 1
	S8B_BIT_SIZE = 60
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

	ErrValueOutOfBounds = errors.New("value out of bounds")
)

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
		selector64[sel].unpack(v, unsafe.Pointer(&dst[j]))
		j += selector64[sel].n
	}
	return j, nil
}

func CountValues(b []byte) (int, error) {
	var count int
	for len(b) >= 8 {
		v := binary.LittleEndian.Uint64(b[:8])
		b = b[8:]

		sel := v >> 60
		if sel >= 16 {
			return 0, fmt.Errorf("invalid selector value: %v", sel)
		}
		count += selector64[sel].n
	}

	if len(b) > 0 {
		return 0, fmt.Errorf("invalid slice len remaining: %v", len(b))
	}
	return count, nil
}

// use nocheckptr because the underlying struct layout doesn't change
//
//go:nocheckptr
func DecodeUint64(dst []uint64, src []byte) (value int, err error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}

	i := 0
	j := 0
	l := len(src)
	for i < l {
		v := binary.LittleEndian.Uint64(src[i:])
		sel := (v >> 60) & 0xf
		selector64[sel].unpack(v, unsafe.Pointer(&dst[j]))
		j += selector64[sel].n
		i += 8
	}
	return j, nil
}

// use nocheckptr because the underlying struct layout doesn't change
//
//go:nocheckptr
func DecodeUint32(dst []uint32, src []byte) (value int, err error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}

	i := 0
	j := 0
	l := len(src)
	for i < l {
		v := binary.LittleEndian.Uint64(src[i:])
		sel := (v >> 60) & 0xf
		selector32[sel].unpack(v, unsafe.Pointer(&dst[j]))
		j += selector32[sel].n
		i += 8
	}
	return j, nil
}

// use nocheckptr because the underlying struct layout doesn't change
//
//go:nocheckptr
func DecodeUint16(dst []uint16, src []byte) (value int, err error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}

	i := 0
	j := 0
	l := len(src)
	for i < l {
		v := binary.LittleEndian.Uint64(src[i:])
		sel := (v >> 60) & 0xf
		selector16[sel].unpack(v, unsafe.Pointer(&dst[j]))
		j += selector16[sel].n
		i += 8
	}
	return j, nil
}

// use nocheckptr because the underlying struct layout doesn't change
//
//go:nocheckptr
func DecodeUint8(dst []uint8, src []byte) (value int, err error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}

	i := 0
	j := 0
	l := len(src)
	for i < l {
		v := binary.LittleEndian.Uint64(src[i:])
		sel := (v >> 60) & 0xf
		selector8[sel].unpack(v, unsafe.Pointer(&dst[j]))
		j += selector8[sel].n
		i += 8
	}
	return j, nil
}
