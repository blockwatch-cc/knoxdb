// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// Original from: InfluxData, MIT
// https://github.com/influxdata/influxdb
package zip

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/bits"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
)

// bitMask contains a lookup table where the index is the number of bits
// and the value is a mask. The table is always read by ANDing the index
// with 0x3f, such that if the index is 64, position 0 will be read, which
// is a 0xffffffffffffffff, thus returning all bits.
//
// 00 = 0xffffffffffffffff
// 01 = 0x0000000000000001
// 02 = 0x0000000000000003
// 03 = 0x0000000000000007
// ...
// 62 = 0x3fffffffffffffff
// 63 = 0x7fffffffffffffff
var bitMask [64]uint64

func init() {
	v := uint64(1)
	for i := 1; i <= 64; i++ {
		bitMask[i&0x3f] = v
		v = v<<1 | 1
	}
}

// DecodeFloat32 is a memory efficient float32 decoder.
// This is the preferred method for decoding compressed data.
// It returns the number of decoded elements and an error.
func DecodeFloat32(dst []float32, buf []byte) (int, error) {
	// call decoder with slice arg and return new slice dimension
	return decodeFloat32(dst, buf[1:])
}

// DecodeFloat64 is the morst efficient version for decoding 32bit float data.
func DecodeFloat64(dst []float64, buf []byte) (int, error) {
	// call decoder with slice arg and store new slice dimension
	return decodeFloat64(dst, buf[1:])
}

// ReadFloat32 is the io.Reader version for decoding 32bit float data.
func ReadFloat32(dst []float32, r io.Reader) (int, int64, error) {
	f64 := arena.AllocFloat64(cap(dst))
	defer arena.Free(f64)
	l, n, err := ReadFloat64(f64, r)
	if err != nil {
		return l, n, err
	}
	dst = dst[:l]
	for i, v := range f64 {
		dst[i] = float32(v)
	}
	return l, n, nil
}

// ReadFloat64 is the io.Reader version for decoding 64bit float data.
func ReadFloat64(dst []float64, r io.Reader) (int, int64, error) {
	// read, but discard type; always Gorilla
	_ = readByte(r)

	// we need the full data for gorilla decoding, its max size cannot be larger than
	// the target slice plus encoder data
	scratch := arena.AllocBytes(FloatEncodedSize(cap(dst)))
	defer arena.Free(scratch)
	buf := bytes.NewBuffer(scratch[:0])
	_, err := io.Copy(buf, r)
	if err != nil {
		return 0, 0, err
	}
	l, err := decodeFloat64(dst, buf.Bytes())
	return l, int64(buf.Len()), err
}

func decodeFloat32(dst []float32, buf []byte) (int, error) {
	f64 := arena.AllocFloat64(cap(dst))
	defer arena.Free(f64)
	n, err := decodeFloat64(f64, buf)
	if err != nil {
		return 0, err
	}
	f64 = f64[:n]
	dst = dst[:n]
	for i, v := range f64 {
		dst[i] = float32(v)
	}
	return n, nil
}

func decodeFloat64(dst []float64, b []byte) (int, error) {
	var (
		val         uint64      // current value
		trailingN   uint8       // trailing zero count
		meaningfulN uint8  = 64 // meaningful bit count
	)

	if len(b) < 8 {
		return 0, nil
	}

	dst = dst[:0]
	val = binary.BigEndian.Uint64(b)
	if val == uvnan {
		// special case: there were no values to decode
		return 0, nil
	}

	// convert the []float64 to []uint64 to avoid calling math.Float64Frombits,
	// which results in unnecessary moves between Xn registers before moving
	// the value into the float64 slice. This change increased performance from
	// 320 MB/s to 340 MB/s on an Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz
	buf := *(*[]uint64)(unsafe.Pointer(&dst))
	buf = append(buf, val)

	b = b[8:]

	// The bit reader code uses brCachedVal to store up to the next 8 bytes
	// of MSB data read from b. brValidBits stores the number of remaining unread
	// bits starting from the MSB. Before N bits are read from brCachedVal,
	// they are left-rotated N bits, such that they end up in the left-most position.
	// Using bits.RotateLeft64 results in a single instruction on many CPU architectures.
	// This approach permits simple tests, such as for the two control bits:
	//
	//    brCachedVal&1 > 0
	//
	// The alternative was to leave brCachedValue alone and perform shifts and
	// masks to read specific bits. The original approach looked like the
	// following:
	//
	//    brCachedVal&(1<<(brValidBits&0x3f)) > 0
	//
	var (
		brCachedVal uint64 // a buffer of up to the next 8 bytes read from b in MSB order
		brValidBits uint8  // the number of unread bits remaining in brCachedVal
	)

	// Refill brCachedVal, reading up to 8 bytes from b
	switch {
	case len(b) >= 8:
		// fast path reads 8 bytes directly
		brCachedVal = binary.BigEndian.Uint64(b)
		brValidBits = 64
		b = b[8:]
	case len(b) > 0:
		brCachedVal = 0
		brValidBits = uint8(len(b) * 8)
		for i := range b {
			brCachedVal = (brCachedVal << 8) | uint64(b[i])
		}
		brCachedVal = bits.RotateLeft64(brCachedVal, -int(brValidBits))
		b = b[:0]
	default:
		goto ERROR
	}

	// The expected exit condition is for a uvnan to be decoded.
	// Any other error (EOF) indicates a truncated stream.
	for {
		if brValidBits > 0 {
			// brValidBits > 0 is impossible to predict, so we place the
			// most likely case inside the if and immediately jump, keeping
			// the instruction pipeline consistently full.
			// This is a similar approach to using the GCC __builtin_expect
			// intrinsic, which modifies the order of branches such that the
			// likely case follows the conditional jump.
			//
			// Written as if brValidBits == 0 and placing the Refill brCachedVal
			// code inside reduces benchmarks from 318 MB/s to 260 MB/s on an
			// Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz
			goto READ0
		}

		// Refill brCachedVal, reading up to 8 bytes from b
		switch {
		case len(b) >= 8:
			brCachedVal = binary.BigEndian.Uint64(b)
			brValidBits = 64
			b = b[8:]
		case len(b) > 0:
			brCachedVal = 0
			brValidBits = uint8(len(b) * 8)
			for i := range b {
				brCachedVal = (brCachedVal << 8) | uint64(b[i])
			}
			brCachedVal = bits.RotateLeft64(brCachedVal, -int(brValidBits))
			b = b[:0]
		default:
			goto ERROR
		}

	READ0:
		// read control bit 0
		brValidBits -= 1
		brCachedVal = bits.RotateLeft64(brCachedVal, 1)
		if brCachedVal&1 > 0 {
			if brValidBits > 0 {
				goto READ1
			}

			// Refill brCachedVal, reading up to 8 bytes from b
			switch {
			case len(b) >= 8:
				brCachedVal = binary.BigEndian.Uint64(b)
				brValidBits = 64
				b = b[8:]
			case len(b) > 0:
				brCachedVal = 0
				brValidBits = uint8(len(b) * 8)
				for i := range b {
					brCachedVal = (brCachedVal << 8) | uint64(b[i])
				}
				brCachedVal = bits.RotateLeft64(brCachedVal, -int(brValidBits))
				b = b[:0]
			default:
				goto ERROR
			}

		READ1:
			// read control bit 1
			brValidBits -= 1
			brCachedVal = bits.RotateLeft64(brCachedVal, 1)
			if brCachedVal&1 > 0 {
				// read 5 bits for leading zero count and 6 bits for the meaningful data count
				const leadingTrailingBitCount = 11
				var lmBits uint64 // leading + meaningful data counts
				if brValidBits >= leadingTrailingBitCount {
					// decode 5 bits leading + 6 bits meaningful for a total of 11 bits
					brValidBits -= leadingTrailingBitCount
					brCachedVal = bits.RotateLeft64(brCachedVal, leadingTrailingBitCount)
					lmBits = brCachedVal
				} else {
					bits01 := uint8(11)
					if brValidBits > 0 {
						bits01 -= brValidBits
						lmBits = bits.RotateLeft64(brCachedVal, 11)
					}

					// Refill brCachedVal, reading up to 8 bytes from b
					switch {
					case len(b) >= 8:
						brCachedVal = binary.BigEndian.Uint64(b)
						brValidBits = 64
						b = b[8:]
					case len(b) > 0:
						brCachedVal = 0
						brValidBits = uint8(len(b) * 8)
						for i := range b {
							brCachedVal = (brCachedVal << 8) | uint64(b[i])
						}
						brCachedVal = bits.RotateLeft64(brCachedVal, -int(brValidBits))
						b = b[:0]
					default:
						goto ERROR
					}
					brCachedVal = bits.RotateLeft64(brCachedVal, int(bits01))
					brValidBits -= bits01
					lmBits &^= bitMask[bits01&0x3f]
					lmBits |= brCachedVal & bitMask[bits01&0x3f]
				}

				lmBits &= 0x7ff
				leadingN := uint8((lmBits >> 6) & 0x1f) // 5 bits leading
				meaningfulN = uint8(lmBits & 0x3f)      // 6 bits meaningful
				if meaningfulN > 0 {
					trailingN = 64 - leadingN - meaningfulN
				} else {
					// meaningfulN == 0 is a special case, such that all bits
					// are meaningful
					trailingN = 0
					meaningfulN = 64
				}
			}

			var sBits uint64 // significant bits
			if brValidBits >= meaningfulN {
				brValidBits -= meaningfulN
				brCachedVal = bits.RotateLeft64(brCachedVal, int(meaningfulN))
				sBits = brCachedVal
			} else {
				mBits := meaningfulN
				if brValidBits > 0 {
					mBits -= brValidBits
					sBits = bits.RotateLeft64(brCachedVal, int(meaningfulN))
				}

				// Refill brCachedVal, reading up to 8 bytes from b
				switch {
				case len(b) >= 8:
					brCachedVal = binary.BigEndian.Uint64(b)
					brValidBits = 64
					b = b[8:]
				case len(b) > 0:
					brCachedVal = 0
					brValidBits = uint8(len(b) * 8)
					for i := range b {
						brCachedVal = (brCachedVal << 8) | uint64(b[i])
					}
					brCachedVal = bits.RotateLeft64(brCachedVal, -int(brValidBits))
					b = b[:0]
				default:
					goto ERROR
				}
				brCachedVal = bits.RotateLeft64(brCachedVal, int(mBits))
				brValidBits -= mBits
				sBits &^= bitMask[mBits&0x3f]
				sBits |= brCachedVal & bitMask[mBits&0x3f]
			}
			sBits &= bitMask[meaningfulN&0x3f]

			val ^= sBits << (trailingN & 0x3f)
			if val == uvnan {
				// IsNaN, eof
				break
			}
		}

		buf = append(buf, val)
	}

	return len(buf), nil

ERROR:
	return 0, errFloatBatchDecodeShortBuffer
}
