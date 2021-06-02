// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

// Benchmarks using Go 1.10.5 on OSX 10.14
// Intel Core i7 2.5GHz 4-core 256k L2, 6M L3
//
// go test ./vec/... -bench=.

package vec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"reflect"
	"testing"
)

type BitsetTest struct {
	name      string
	source    []byte
	sourceStr string
	result    []byte
	resultStr string
	size      int
	count     int
}

type bitsetBenchmarkSize struct {
	name string
	l    int
}

var bitsetSizes = []int{
	7, 8, 9, 15, 16, 17, 23, 24, 25, 31, 32, 33,
	63, 64, 65, 127,
	128,   // min AVX size
	129,   // AVX + 1bit
	160,   // AVX + i32
	161,   // AVX + i32 + 1
	255,   // AVX + i32 + 7
	256,   // 2x AVX
	257,   // 2x AVX + 1
	512,   // 4x AVX
	1024,  // 8x AVX
	2048,  // min AVX2 size
	2176,  // AVX2 + AVX size
	2208,  // AVX2 + AVX + i32 size
	2216,  // AVX2 + AVX + i32 + i8 size
	2217,  // AVX2 + AVX + i32 + i8 size + 1 bit
	4096,  // 2x AVX2
	4224,  // 2x AVX2 + AVX
	4256,  // 2x AVX2 + AVX + i32
	4264,  // 2x AVX2 + AVX + i32 +i8
	4265,  // 2x AVX2 + AVX + i32 +i8 + 1 bit
	8192,  // 4x AVX2
	16384, // 16x AVX2
}

var bitsetBenchmarkSizes = []bitsetBenchmarkSize{
	//	{"32", 32},
	//	{"128", 128},
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"64K", 64 * 1024},
	{"128K", 128 * 1024},
	{"1M", 1024 * 1024},
	//	{"16M", 16 * 1024 * 1024},
	{"128M", 128 * 1024 * 1024},
	//	{"512M", 512 * 1024 * 1024},
	{"1K-8", 1*1024 - 8},
	{"16K-8", 16*1024 - 8},
	{"64K-8", 64*1024 - 8},
	{"128K-8", 128*1024 - 8},
	{"1M-8", 1024*1024 - 8},
	{"128M-8", 128*1024*1024 - 8},
}

type bitsetBenchmarkDensity struct {
	name string
	d    float64
}

var bitsetBenchmarkDensities = []bitsetBenchmarkDensity{
	{"1/2", 1.0 / 2},
	{"1/4", 1.0 / 4},
	{"1/8", 1.0 / 8},
	{"1/16", 1.0 / 16},
	{"1/32", 1.0 / 32},
	{"1/64", 1.0 / 64},
	{"1/128", 1.0 / 128},
	{"1/256", 1.0 / 256},
	{"1/512", 1.0 / 512},
	{"1/1024", 1.0 / 1024},
	{"1/2048", 1.0 / 2048},
	{"1/4096", 1.0 / 4096},
}

var bitsetPatterns = []byte{
	0xfa,
	0x08,
	0x11,
	0x01,
	0x80,
}

var bitsetCases = []BitsetTest{
	BitsetTest{
		name:   "zeros_7",
		source: []byte{0x0},
		result: []byte{0x0},
		size:   7,
		count:  0,
	},
	BitsetTest{
		name:   "ones_7",
		source: []byte{0x7f},
		result: []byte{0x7f},
		size:   7,
		count:  7,
	},
	BitsetTest{
		name:   "fa_7",
		source: []byte{0xfa},
		result: []byte{0x7a},
		size:   7,
		count:  5,
	},
	BitsetTest{
		name:   "f9_7",
		source: []byte{0xf9},
		result: []byte{0x79},
		size:   7,
		count:  5,
	},
}

func fillBitset(buf []byte, size int, val byte) []byte {
	if len(buf) == 0 {
		buf = make([]byte, bitFieldLen(size))
	}
	buf[0] = val
	for bp := 1; bp < len(buf); bp *= 2 {
		copy(buf[bp:], buf[:bp])
	}
	buf[len(buf)-1] &= bytemask(size)
	return buf
}

func fillBitsetSaw(buf []byte, size int) []byte {
	if len(buf) == 0 {
		buf = make([]byte, bitFieldLen(size))
	}
	// generate the first sawtooth
	for i := 0; i < 256 && i < len(buf); i++ {
		buf[i] = byte(i)
	}
	// concat again and again, we make it one shorter to avoid a symetric vector
	for bp := 256; bp < len(buf); bp = 2*bp - 1 {
		copy(buf[bp:], buf[:bp])
	}
	buf[len(buf)-1] &= bytemask(size)
	return buf
}

func fillBitsetRand(buf []byte, size int, dense float64) []byte {
	if len(buf) == 0 {
		buf = make([]byte, bitFieldLen(size))
	} else {
		for i := range buf {
			buf[i] = 0
		}
	}
	appbitcount := int(math.Ceil(dense * float64(size)))
	for ccount := 0; ccount < appbitcount; {
		bit := rand.Intn(size)
		bef := buf[bit/8]
		aft := bef | 0x01<<(bit%8)
		if bef != aft {
			ccount++
		}
		buf[bit/8] = aft
	}
	if appbitcount != int(bitsetPopCount(buf, size)) {
		panic("fillBitsetRand: wrong number of bits")
	}

	return buf
}

func popcount(buf []byte) int {
	var cnt int
	for _, c := range buf {
		cnt += bits.OnesCount8(uint8(c))
	}
	return cnt
}

func f(s string, args ...interface{}) string {
	return fmt.Sprintf(s, args...)
}

func checkCleanTail(T *testing.T, buf []byte) {
	tail := len(buf)
	buf = buf[:cap(buf)]
	for i := range buf[tail:] {
		if buf[tail+i] != 0 {
			T.Errorf("unclean memory %x at pos %d+%d: %x", buf[i], tail, i, buf)
			T.FailNow()
		}
	}
}

// Test low-level slice handlers
func TestBitsetPopCountGeneric(T *testing.T) {
	for _, c := range bitsetCases {
		T.Run(c.name, func(t *testing.T) {
			cnt := bitsetPopCountGeneric(c.source, c.size)
			if got, want := int(cnt), c.count; got != want {
				T.Errorf("unexpected count %d, expected %d", got, want)
			}
		})
	}
	for _, sz := range bitsetSizes {
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				buf := fillBitset(nil, sz, pt)
				if got, want := int(bitsetPopCountGeneric(buf, sz)), popcount(buf); got != want {
					T.Errorf("unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitsetPopCountAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range bitsetCases {
		// call the function selector to do proper last byte masking!
		T.Run(c.name, func(t *testing.T) {
			cnt := bitsetPopCount(c.source, c.size)
			if got, want := int(cnt), c.count; got != want {
				T.Errorf("unexpected count %d, expected %d", got, want)
			}
		})
	}
	for _, sz := range bitsetSizes {
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				buf := fillBitset(nil, sz, pt)
				// call the function selector to do proper last byte masking!
				if got, want := int(bitsetPopCount(buf, sz)), popcount(buf); got != want {
					T.Errorf("unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitAndGeneric(T *testing.T) {
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			src := fillBitset(nil, sz, pt)
			dst := fillBitset(nil, sz, pt)

			// same value, same slice
			bitsetAndGeneric(dst, dst, sz)
			if bytes.Compare(dst, src) != 0 {
				T.Errorf("%d_%x_dst===src: unexpected result %x, expected %x", sz, pt, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				T.Errorf("%d_%x_dst===src: unexpected count %d, expected %d", sz, pt, got, want)
			}

			// same value, other slice
			copy(dst, src)
			bitsetAndGeneric(dst, src, sz)
			if bytes.Compare(dst, src) != 0 {
				T.Errorf("%d_%x_dst==src: unexpected result %x, expected %x", sz, pt, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				T.Errorf("%d_%x_dst==src: unexpected count %d, expected %d", sz, pt, got, want)
			}

			// all zeros
			copy(dst, src)
			bitsetAndGeneric(dst, zeros, sz)
			if bytes.Compare(dst, zeros) != 0 {
				T.Errorf("%d_%x_zeros: unexpected result %x, expected %x", sz, pt, dst, zeros)
			}
			if got, want := popcount(dst), 0; got != want {
				T.Errorf("%d_%x_zeros: unexpected count %d, expected %d", sz, pt, got, want)
			}

			// all ones
			copy(dst, src)
			bitsetAndGeneric(dst, ones, sz)
			if bytes.Compare(dst, src) != 0 {
				T.Errorf("%d_%x_ones: unexpected result %x, expected %x", sz, pt, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				T.Errorf("%d_%x_ones: unexpected count %d, expected %d", sz, pt, got, want)
			}
		}
	}
}

func TestBitAndGenericFlag1(T *testing.T) {
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				ret := bitsetAndGenericFlag1(dst, dst, sz)
				if pt == 0x80 && sz == 7 {
					if ret != 0 {
						T.Errorf("dst===src: unexpected return value %x, expected 0", ret)
					}
				} else {
					if ret == 0 {
						T.Errorf("dst===src: unexpected return value %x, expected !=0", ret)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				ret = bitsetAndGenericFlag1(dst, src, sz)
				if pt == 0x80 && sz == 7 {
					if ret != 0 {
						T.Errorf("dst==src: unexpected return value %x, expected 0", ret)
					}
				} else {
					if ret == 0 {
						T.Errorf("dst==src: unexpected return value %x, expected !=0", ret)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// all zeros
				copy(dst, src)
				ret = bitsetAndGenericFlag1(dst, zeros, sz)
				if ret != 0 {
					T.Errorf("zeros: unexpected return value %x, expected %x", ret, 0)
				}
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all ones
				copy(dst, src)
				ret = bitsetAndGenericFlag1(dst, ones, sz)
				if pt == 0x80 && sz == 7 {
					if ret != 0 {
						T.Errorf("ones: unexpected return value %x, expected 0", ret)
					}
				} else {
					if ret == 0 {
						T.Errorf("ones: unexpected return value %x, expected !=0", ret)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitAndAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}

	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				bitsetAndAVX2(dst, dst)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				bitsetAndAVX2(dst, src)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// all zeros
				copy(dst, src)
				bitsetAndAVX2(dst, zeros)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all ones
				copy(dst, src)
				bitsetAndAVX2(dst, ones)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitAndAVX2Flag1(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				ret := bitsetAndAVX2Flag1(dst, dst)
				if pt == 0x80 && sz == 7 {
					if ret != 0 {
						T.Errorf("dst===src: unexpected return value %x, expected 0", ret)
					}
				} else {
					if ret == 0 {
						T.Errorf("dst===src: unexpected return value %x, expected !=0", ret)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				ret = bitsetAndAVX2Flag1(dst, src)
				if pt == 0x80 && sz == 7 {
					if ret != 0 {
						T.Errorf("dst==src: unexpected return value %x, expected 0", ret)
					}
				} else {
					if ret == 0 {
						T.Errorf("dst==src: unexpected return value %x, expected !=0", ret)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// all zeros
				copy(dst, src)
				ret = bitsetAndAVX2Flag1(dst, zeros)
				if ret != 0 {
					T.Errorf("zeros: unexpected return value %x, expected %x", ret, 0)
				}
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all ones
				copy(dst, src)
				ret = bitsetAndAVX2Flag1(dst, ones)
				if pt == 0x80 && sz == 7 {
					if ret != 0 {
						T.Errorf("ones: unexpected return value %x, expected 0", ret)
					}
				} else {
					if ret == 0 {
						T.Errorf("ones: unexpected return value %x, expected !=0", ret)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitAndNotGeneric(T *testing.T) {
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := make([]byte, len(src))

				// same value, same slice
				bitsetAndNotGeneric(dst, dst, sz)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				bitsetAndNotGeneric(dst, src, sz)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// val AND NOT zeros == val
				copy(dst, src)
				bitsetAndNotGeneric(dst, zeros, sz)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all AND NOT ones == zero
				copy(dst, src)
				bitsetAndNotGeneric(dst, ones, sz)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitAndNotAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := make([]byte, len(src))

				// same value, same slice
				bitsetAndNot(dst, dst, sz)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				bitsetAndNot(dst, src, sz)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// val AND NOT zeros == val
				copy(dst, src)
				bitsetAndNot(dst, zeros, sz)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all AND NOT ones == zero
				copy(dst, src)
				bitsetAndNot(dst, ones, sz)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitOrGeneric(T *testing.T) {
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				bitsetOrGeneric(dst, dst, sz)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				bitsetOrGeneric(dst, src, sz)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// val OR zeros == val
				copy(dst, src)
				bitsetOrGeneric(dst, zeros, sz)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all OR ones == ones
				copy(dst, src)
				bitsetOrGeneric(dst, ones, sz)
				if bytes.Compare(dst, ones) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, ones)
				}
				if got, want := popcount(dst), popcount(ones); got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitOrGenericFlag1(T *testing.T) {
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				ret := bitsetOrGenericFlag1(dst, dst, sz)
				if pt == 0x80 && sz == 7 {
					if ret != 0 {
						T.Errorf("dst===src: unexpected return value %x, expected 0", ret)
					}
				} else {
					if ret == 0 {
						T.Errorf("dst===src: unexpected return value %x, expected !=0", ret)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				ret = bitsetOrGenericFlag1(dst, src, sz)
				if pt == 0x80 && sz == 7 {
					if ret != 0 {
						T.Errorf("dst==src: unexpected return value %x, expected 0", ret)
					}
				} else {
					if ret == 0 {
						T.Errorf("dst==src: unexpected return value %x, expected !=0", ret)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// val OR zeros == val
				copy(dst, src)
				ret = bitsetOrGenericFlag1(dst, zeros, sz)
				if pt == 0x80 && sz == 7 {
					if ret != 0 {
						T.Errorf("zeros: unexpected return value %x, expected 0", ret)
					}
				} else {
					if ret == 0 {
						T.Errorf("zeros: unexpected return value %x, expected !=0", ret)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all OR ones == ones
				copy(dst, src)
				ret = bitsetOrGenericFlag1(dst, ones, sz)
				if ret == 0 {
					T.Errorf("ones: unexpected return value %x, expected !=0", ret)
				}
				if bytes.Compare(dst, ones) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, ones)
				}
				if got, want := popcount(dst), popcount(ones); got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitOrAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				bitsetOr(dst, dst, sz)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				bitsetOr(dst, src, sz)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// val OR zeros == val
				copy(dst, src)
				bitsetOr(dst, zeros, sz)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all OR ones == ones
				copy(dst, src)
				bitsetOr(dst, ones, sz)
				if bytes.Compare(dst, ones) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, ones)
				}
				if got, want := popcount(dst), popcount(ones); got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitOrAVX2Flag1(T *testing.T) {
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				ret := bitsetOrAVX2Flag1(dst, dst)
				if pt == 0x80 && sz == 7 {
					if ret != 0 {
						T.Errorf("dst===src: unexpected return value %x, expected 0", ret)
					}
				} else {
					if ret == 0 {
						T.Errorf("dst===src: unexpected return value %x, expected !=0", ret)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				ret = bitsetOrAVX2Flag1(dst, src)
				if pt == 0x80 && sz == 7 {
					if ret != 0 {
						T.Errorf("dst==src: unexpected return value %x, expected 0", ret)
					}
				} else {
					if ret == 0 {
						T.Errorf("dst==src: unexpected return value %x, expected !=0", ret)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// val OR zeros == val
				copy(dst, src)
				ret = bitsetOrAVX2Flag1(dst, zeros)
				if pt == 0x80 && sz == 7 {
					if ret != 0 {
						T.Errorf("zeros: unexpected return value %x, expected 0", ret)
					}
				} else {
					if ret == 0 {
						T.Errorf("zeros: unexpected return value %x, expected !=0", ret)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all OR ones == ones
				copy(dst, src)
				ret = bitsetOrAVX2Flag1(dst, ones)
				if ret == 0 {
					T.Errorf("ones: unexpected return value %x, expected !=0", ret)
				}
				if bytes.Compare(dst, ones) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, ones)
				}
				if got, want := popcount(dst), popcount(ones); got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitXorGeneric(T *testing.T) {
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				bitsetXorGeneric(dst, dst, sz)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				bitsetXorGeneric(dst, src, sz)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// val XOR zeros == val
				copy(dst, src)
				bitsetXorGeneric(dst, zeros, sz)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// val XOR ones == neg(val)
				copy(dst, src)
				bitsetXorGeneric(dst, ones, sz)
				cmp := fillBitset(nil, sz, ^pt)
				if bytes.Compare(dst, cmp) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, cmp)
				}
				if got, want := popcount(dst), popcount(cmp); got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitXorAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				bitsetXor(dst, dst, sz)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("dst===src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				bitsetXor(dst, src, sz)
				if bytes.Compare(dst, zeros) != 0 {
					T.Errorf("dst==src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					T.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// val XOR zeros == val
				copy(dst, src)
				bitsetXor(dst, zeros, sz)
				if bytes.Compare(dst, src) != 0 {
					T.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					T.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// val XOR ones == neg(val)
				copy(dst, src)
				bitsetXor(dst, ones, sz)
				cmp := fillBitset(nil, sz, ^pt)
				if bytes.Compare(dst, cmp) != 0 {
					T.Errorf("ones: unexpected result %x, expected %x", dst, cmp)
				}
				if got, want := popcount(dst), popcount(cmp); got != want {
					T.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitNegGeneric(T *testing.T) {
	for _, sz := range bitsetSizes {
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				cmp := fillBitset(nil, sz, ^pt)

				bitsetNegGeneric(src, sz)
				if bytes.Compare(src, cmp) != 0 {
					T.Errorf("unexpected result %x, expected %x", src, cmp)
				}
				if got, want := popcount(src), popcount(cmp); got != want {
					T.Errorf("unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitNegAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				cmp := fillBitset(nil, sz, ^pt)

				bitsetNeg(src, sz)
				if bytes.Compare(src, cmp) != 0 {
					T.Errorf("unexpected result %x, expected %x", src, cmp)
				}
				if got, want := popcount(src), popcount(cmp); got != want {
					T.Errorf("unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

// Test high-level bitset API
//
func TestBitsetNew(T *testing.T) {
	for _, c := range bitsetCases {
		T.Run(c.name, func(t *testing.T) {
			bits := NewBitset(c.size)
			if got, want := len(bits.Bytes()), len(c.source); got != want {
				T.Errorf("unexpected buf length %d, expected %d", got, want)
			}
			if got, want := bits.Len(), c.size; got != want {
				T.Errorf("unexpected size %d, expected %d", got, want)
			}
			if got, want := bits.Count(), 0; got != want {
				T.Errorf("unexpected count %d, expected %d", got, want)
			}
			checkCleanTail(T, bits.Bytes())
		})
	}
}

func TestBitsetFromBytes(T *testing.T) {
	for _, c := range bitsetCases {
		T.Run(c.name, func(t *testing.T) {
			bits := NewBitsetFromBytes(c.source, c.size)
			if got, want := len(bits.Bytes()), len(c.source); got != want {
				T.Errorf("unexpected buf length %d, expected %d", got, want)
			}
			if got, want := bits.Len(), c.size; got != want {
				T.Errorf("unexpected size %d, expected %d", got, want)
			}
			if got, want := bits.Count(), c.count; got != want {
				T.Errorf("unexpected count %d, expected %d", got, want)
			}
			if bytes.Compare(bits.Bytes(), c.result) != 0 {
				T.Errorf("unexpected result %x, expected %x", bits.Bytes(), c.source)
			}
		})
	}
}

func TestBitsetOne(T *testing.T) {
	for _, sz := range bitsetSizes {
		T.Run(f("%d", sz), func(t *testing.T) {
			bits := NewBitset(sz)
			bits.One()
			if got, want := len(bits.Bytes()), bitFieldLen(sz); got != want {
				T.Errorf("unexpected buf length %d, expected %d", got, want)
			}
			if got, want := bits.Len(), sz; got != want {
				T.Errorf("unexpected size %d, expected %d", got, want)
			}
			if got, want := bits.Count(), sz; got != want {
				T.Errorf("unexpected count %d, expected %d", got, want)
			}
			buf := bytes.Repeat([]byte{0xff}, bitFieldLen(sz)-1)
			buf = append(buf, byte(0xff>>((8-uint(sz)&0x7)&0x7)&0xff))
			if bytes.Compare(bits.Bytes(), buf) != 0 {
				T.Errorf("unexpected result %x, expected %x", bits.Bytes(), buf)
			}
		})
	}
}

func TestBitsetZero(T *testing.T) {
	for _, c := range bitsetCases {
		T.Run(c.name, func(t *testing.T) {
			bits := NewBitsetFromBytes(c.source, c.size)
			bits.Zero()
			if got, want := len(bits.Bytes()), len(c.source); got != want {
				T.Errorf("unexpected buf length %d, expected %d", got, want)
			}
			if got, want := bits.Len(), c.size; got != want {
				T.Errorf("unexpected size %d, expected %d", got, want)
			}
			if got, want := bits.Count(), 0; got != want {
				T.Errorf("unexpected count %d, expected %d", got, want)
			}
			buf := bytes.Repeat([]byte{0}, bitFieldLen(c.size))
			if bytes.Compare(bits.Bytes(), buf) != 0 {
				T.Errorf("unexpected result %x, expected %x", bits.Bytes(), buf)
			}
		})
	}
}

func TestBitsetGrow(T *testing.T) {
	for _, sz := range bitsetSizes {
		for _, sznew := range bitsetSizes {
			T.Run(f("%d_%d", sz, sznew), func(t *testing.T) {
				bits := NewBitset(sz)
				bits.One()
				bits.Grow(sznew)
				if got, want := len(bits.Bytes()), bitFieldLen(sznew); got != want {
					T.Errorf("unexpected buf length %d, expected %d", got, want)
					T.FailNow()
				}
				if got, want := bits.Len(), sznew; got != want {
					T.Errorf("unexpected size %d, expected %d", got, want)
					T.FailNow()
				}
				if got, want := bits.Count(), min(sz, sznew); got != want {
					T.Errorf("unexpected count %d, expected %d", got, want)
					T.FailNow()
				}
				if got, want := bits.Count(), popcount(bits.Bytes()); got != want {
					T.Errorf("unexpected real count %d, expected %d", got, want)
					T.FailNow()
				}
				lena := bitFieldLen(sz)
				lenb := bitFieldLen(sznew)
				diff := lena - lenb
				buf := bytes.Repeat([]byte{0xff}, min(lena, lenb))
				buf[len(buf)-1] &= byte(0xff >> (7 - uint(min(sz, sznew)-1)&0x7))
				if diff < 0 {
					buf = append(buf, bytes.Repeat([]byte{0x0}, -diff)...)
				}
				if bytes.Compare(bits.Bytes(), buf) != 0 {
					T.Errorf("unexpected result %x, expected %x", bits.Bytes(), buf)
					T.FailNow()
				}
				checkCleanTail(T, bits.Bytes())
			})
		}
	}
	// clear/reset bitset to zero
	for _, sz := range bitsetSizes {
		T.Run(f("%d_grow_0", sz), func(t *testing.T) {
			bits := NewBitset(sz)
			bits.One()
			bits.Grow(0)
			if got, want := len(bits.Bytes()), 0; got != want {
				T.Errorf("unexpected buf length %d, expected %d", got, want)
				T.FailNow()
			}
			if got, want := bits.Len(), 0; got != want {
				T.Errorf("unexpected size %d, expected %d", got, want)
				T.FailNow()
			}
			if got, want := bits.Count(), 0; got != want {
				T.Errorf("unexpected count %d, expected %d", got, want)
				T.FailNow()
			}
			checkCleanTail(T, bits.Bytes())
		})
	}
	// grow + 1
	for _, sz := range bitsetSizes {
		T.Run(f("%d_grow+1", sz), func(t *testing.T) {
			bits := NewBitset(sz)
			bits.One()
			bits.Grow(bits.Len() + 1)
			bits.Set(bits.Len() - 1)
			if got, want := len(bits.Bytes()), bitFieldLen(sz+1); got != want {
				T.Errorf("unexpected buf length %d, expected %d", got, want)
				T.FailNow()
			}
			if got, want := bits.Len(), sz+1; got != want {
				T.Errorf("unexpected size %d, expected %d", got, want)
				T.FailNow()
			}
			if got, want := bits.Count(), sz+1; got != want {
				T.Errorf("unexpected count %d, expected %d", got, want)
				T.FailNow()
			}
			if got, want := bits.Count(), popcount(bits.Bytes()); got != want {
				T.Errorf("unexpected real count %d, expected %d", got, want)
				T.FailNow()
			}
			checkCleanTail(T, bits.Bytes())
		})
	}
}

func TestBitsetFill(T *testing.T) {
	for _, sz := range bitsetSizes {
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				cmp := fillBitset(nil, sz, pt)
				bits := NewBitset(sz)
				bits.Fill(pt)

				if got, want := len(bits.Bytes()), bitFieldLen(sz); got != want {
					T.Errorf("unexpected buf length %d, expected %d", got, want)
				}
				if got, want := bits.Len(), sz; got != want {
					T.Errorf("unexpected size %d, expected %d", got, want)
				}
				if got, want := bits.Count(), popcount(cmp); got != want {
					T.Errorf("unexpected count %d, expected %d", got, want)
				}
				if bytes.Compare(bits.Bytes(), cmp) != 0 {
					T.Errorf("unexpected result %x, expected %x", bits.Bytes(), cmp)
				}
			})
		}
	}
}

func TestBitsetSet(T *testing.T) {
	for _, sz := range bitsetSizes {
		T.Run(f("%d", sz), func(t *testing.T) {
			bits := NewBitset(sz)
			cmp := fillBitset(nil, sz, 0)

			// set first bit
			bits.Set(0)
			cmp[0] |= 0x01
			if got, want := bits.Count(), 1; got != want {
				T.Errorf("unexpected count %d, expected %d", got, want)
			}
			if !bits.IsSet(0) {
				T.Errorf("unexpected IsSet=false")
			}
			if bytes.Compare(bits.Bytes(), cmp) != 0 {
				T.Errorf("unexpected result %x, expected %x", bits.Bytes(), cmp)
			}

			// set last bit
			bits.Set(sz - 1)
			cmp[(sz-1)>>3] |= 1 << uint((sz-1)&0x7)
			if got, want := bits.Count(), 2; got != want {
				T.Errorf("unexpected count %d, expected %d", got, want)
			}
			if !bits.IsSet(sz - 1) {
				T.Errorf("unexpected IsSet=false")
			}
			if bytes.Compare(bits.Bytes(), cmp) != 0 {
				T.Errorf("unexpected result %x, expected %x", bits.Bytes(), cmp)
			}

			// set invalid bit
			bits.Set(-1)
			if got, want := bits.Count(), 2; got != want {
				T.Errorf("unexpected count %d, expected %d", got, want)
			}
			if bits.IsSet(-1) {
				T.Errorf("unexpected IsSet=true")
			}
			if bytes.Compare(bits.Bytes(), cmp) != 0 {
				T.Errorf("unexpected result %x, expected %x", bits.Bytes(), cmp)
			}

			bits.Set(sz)
			if got, want := bits.Count(), 2; got != want {
				T.Errorf("unexpected count %d, expected %d", got, want)
			}
			if bits.IsSet(sz) {
				T.Errorf("unexpected IsSet=true")
			}
			if bytes.Compare(bits.Bytes(), cmp) != 0 {
				T.Errorf("unexpected result %x, expected %x", bits.Bytes(), cmp)
			}
			checkCleanTail(T, bits.Bytes())
		})
	}
}

func TestBitsetClear(T *testing.T) {
	for _, sz := range bitsetSizes {
		T.Run(f("%d", sz), func(t *testing.T) {
			bits := NewBitset(sz)
			bits.One()
			cmp := fillBitset(nil, sz, 0xff)

			// clear first bit
			bits.Clear(0)
			cmp[0] &= 0xfe
			if got, want := bits.Count(), popcount(cmp); got != want {
				T.Errorf("first: unexpected count %d, expected %d", got, want)
			}
			if bits.IsSet(0) {
				T.Errorf("unexpected IsSet=true")
			}
			if bytes.Compare(bits.Bytes(), cmp) != 0 {
				T.Errorf("first: unexpected result %x, expected %x", bits.Bytes(), cmp)
			}

			// clear last bit
			bits.Clear(sz - 1)
			cmp[(sz-1)>>3] &^= 1 << uint((sz-1)&0x7)
			if got, want := bits.Count(), popcount(cmp); got != want {
				T.Errorf("last: unexpected count %d, expected %d", got, want)
			}
			if bits.IsSet(sz - 1) {
				T.Errorf("unexpected IsSet=true")
			}
			if bytes.Compare(bits.Bytes(), cmp) != 0 {
				T.Errorf("last: unexpected result %x, expected %x", bits.Bytes(), cmp)
			}

			// clear invalid bit
			bits.Clear(-1)
			if got, want := bits.Count(), popcount(cmp); got != want {
				T.Errorf("invalid-: unexpected count %d, expected %d", got, want)
			}
			if bits.IsSet(-1) {
				T.Errorf("unexpected IsSet=true")
			}
			if bytes.Compare(bits.Bytes(), cmp) != 0 {
				T.Errorf("invalid-: unexpected result %x, expected %x", bits.Bytes(), cmp)
			}

			bits.Clear(sz)
			if got, want := bits.Count(), popcount(cmp); got != want {
				T.Errorf("invalid+: unexpected count %d, expected %d", got, want)
			}
			if bits.IsSet(sz) {
				T.Errorf("unexpected IsSet=true")
			}
			if bytes.Compare(bits.Bytes(), cmp) != 0 {
				T.Errorf("invalid+: unexpected result %x, expected %x", bits.Bytes(), cmp)
			}
		})
	}
}

func TestBitsetReverseAVX2(T *testing.T) {
	for _, sz := range bitsetSizes {
		bits := fillBitsetSaw(nil, sz)
		cmp := make([]byte, len(bits))
		copy(cmp, bits)
		bitsetReverseGeneric(cmp)
		bitsetReverseAVX2(bits, bitsetReverseLut256)

		if got, want := len(bits), len(cmp); got != want {
			T.Errorf("%d: unexpected buf length %d, expected %d", sz, got, want)
		}
		if got, want := popcount(bits), popcount(cmp); got != want {
			T.Errorf("%d: unexpected count %d, expected %d", sz, got, want)
		}
		if bytes.Compare(bits, cmp) != 0 {
			T.Errorf("%d: unexpected result %x, expected %x", sz, bits, cmp)
		}
	}
}

func TestBitsetIndexGeneric(T *testing.T) {
	for _, c := range runTestcases {
		idx := make([]uint32, len(c.idx))
		T.Run(c.name, func(t *testing.T) {
			var ret = bitsetIndexesGeneric(c.buf, c.size, idx)
			if got, want := ret, len(c.idx); got != want {
				T.Errorf("unexpected return value %d, expected %d", got, want)
			}
			if !reflect.DeepEqual(idx, c.idx) {
				T.Errorf("unexpected result %d, expected %d", idx, c.idx)
			}
		})
	}
}

func TestBitsetIndexAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range runTestcases {
		idx := make([]uint32, len(c.idx)+8)
		T.Run(c.name, func(t *testing.T) {
			var ret = bitsetIndexesAVX2(c.buf, c.size, idx)
			idx = idx[:ret]
			if got, want := ret, len(c.idx); got != want {
				T.Errorf("unexpected return value %d, expected %d", got, want)
			}
			if !reflect.DeepEqual(idx, c.idx) {
				T.Errorf("unexpected result %d, expected %d", idx, c.idx)
			}
		})
	}
}

func TestBitsetIndexAVX2New(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range runTestcases {
		c.buf = append(c.buf, 0xff)
		c.buf = c.buf[:len(c.buf)-1]
		idx := make([]uint32, len(c.idx)+8)
		T.Run(c.name, func(t *testing.T) {
			var ret = bitsetIndexesAVX2New(c.buf, c.size, idx)
			idx = idx[:ret]
			if got, want := ret, len(c.idx); got != want {
				T.Errorf("unexpected return value %d, expected %d", got, want)
			}
			if !reflect.DeepEqual(idx, c.idx) {
				T.Errorf("unexpected result %d, expected %d", idx, c.idx)
			}
		})
	}
}

type bitsetRunTestcase struct {
	// source data
	name string
	buf  []byte
	size int
	// results for run algos
	runs  [][2]int
	rruns [][2]int // reverse
	// results for index algos
	idx []uint32
}

func fillIndex(start, length int) []uint32 {
	result := make([]uint32, length)
	for i := range result {
		result[i] = uint32(start + i)
	}
	return result
}

var runTestcases = []bitsetRunTestcase{
	bitsetRunTestcase{
		name: "first_7",
		buf:  []byte{0xff},
		size: 7,
		runs: [][2]int{
			[2]int{0, 7},
		},
		rruns: [][2]int{
			[2]int{6, 7},
		},
		idx: fillIndex(0, 7),
	},
	bitsetRunTestcase{
		name: "first_9",
		buf:  []byte{0xff, 0xff},
		size: 9,
		runs: [][2]int{
			[2]int{0, 9},
		},
		rruns: [][2]int{
			[2]int{8, 9},
		},
		idx: fillIndex(0, 9),
	},
	bitsetRunTestcase{
		name: "first_15",
		buf:  []byte{0xff, 0xff},
		size: 15,
		runs: [][2]int{
			[2]int{0, 15},
		},
		rruns: [][2]int{
			[2]int{14, 15},
		},
		idx: fillIndex(0, 15),
	},
	bitsetRunTestcase{
		name: "first_17",
		buf:  []byte{0xff, 0xff, 0xff},
		size: 17,
		runs: [][2]int{
			[2]int{0, 17},
		},
		rruns: [][2]int{
			[2]int{16, 17},
		},
		idx: fillIndex(0, 17),
	},
	bitsetRunTestcase{
		name: "first_7_srl_1",
		buf:  []byte{0xfe},
		size: 7,
		runs: [][2]int{
			[2]int{1, 6},
		},
		rruns: [][2]int{
			[2]int{6, 6},
		},
		idx: fillIndex(1, 6),
	},
	bitsetRunTestcase{
		name: "first_15_srl_1",
		buf:  []byte{0xfe, 0xff},
		size: 15,
		runs: [][2]int{
			[2]int{1, 14},
		},
		rruns: [][2]int{
			[2]int{14, 14},
		},
		idx: fillIndex(1, 14),
	},
	bitsetRunTestcase{
		name: "first_ff_srl_4",
		buf:  []byte{0xf0, 0x0f},
		size: 16,
		runs: [][2]int{
			[2]int{4, 8},
		},
		rruns: [][2]int{
			[2]int{11, 8},
		},
		idx: fillIndex(4, 8),
	},
	bitsetRunTestcase{
		name: "first_33_srl_3",
		buf:  []byte{0xf8, 0xff, 0xff, 0xff, 0x01},
		size: 33,
		runs: [][2]int{
			[2]int{3, 30},
		},
		rruns: [][2]int{
			[2]int{32, 30},
		},
		idx: fillIndex(3, 30),
	},
	bitsetRunTestcase{
		name: "second_15",
		buf:  []byte{0x0, 0xff},
		size: 15,
		runs: [][2]int{
			[2]int{8, 7},
		},
		rruns: [][2]int{
			[2]int{14, 7},
		},
		idx: fillIndex(8, 7),
	},
	bitsetRunTestcase{
		name: "second_33_srl_3",
		buf:  []byte{0x0, 0xf8, 0xff, 0xff, 0x01},
		size: 33,
		runs: [][2]int{
			[2]int{11, 22},
		},
		rruns: [][2]int{
			[2]int{32, 22},
		},
		idx: fillIndex(11, 22),
	},
	bitsetRunTestcase{
		name: "two_fe_33",
		buf:  []byte{0x7f, 0x00, 0x7f, 0x00, 0x00},
		size: 33,
		runs: [][2]int{
			[2]int{0, 7},
			[2]int{16, 7},
		},
		rruns: [][2]int{
			[2]int{22, 7},
			[2]int{6, 7},
		},
		idx: append(fillIndex(0, 7), fillIndex(16, 7)...),
	},
	bitsetRunTestcase{
		name: "four_0e_31",
		buf:  []byte{0x70, 0x70, 0x70, 0x70},
		size: 31,
		runs: [][2]int{
			[2]int{4, 3},
			[2]int{12, 3},
			[2]int{20, 3},
			[2]int{28, 3},
		},
		rruns: [][2]int{
			[2]int{30, 3},
			[2]int{22, 3},
			[2]int{14, 3},
			[2]int{6, 3},
		},
		idx: []uint32{4, 5, 6, 12, 13, 14, 20, 21, 22, 28, 29, 30},
	},
	bitsetRunTestcase{
		name: "every_aa_15",
		buf:  []byte{0x55, 0x55},
		size: 15,
		runs: [][2]int{
			[2]int{0, 1},
			[2]int{2, 1},
			[2]int{4, 1},
			[2]int{6, 1},
			[2]int{8, 1},
			[2]int{10, 1},
			[2]int{12, 1},
			[2]int{14, 1},
		},
		rruns: [][2]int{
			[2]int{14, 1},
			[2]int{12, 1},
			[2]int{10, 1},
			[2]int{8, 1},
			[2]int{6, 1},
			[2]int{4, 1},
			[2]int{2, 1},
			[2]int{0, 1},
		},
		idx: []uint32{0, 2, 4, 6, 8, 10, 12, 14},
	},
	bitsetRunTestcase{
		name: "every_cc_15",
		buf:  []byte{0x33, 0x33},
		size: 15,
		runs: [][2]int{
			[2]int{0, 2},
			[2]int{4, 2},
			[2]int{8, 2},
			[2]int{12, 2},
		},
		rruns: [][2]int{
			[2]int{13, 2},
			[2]int{9, 2},
			[2]int{5, 2},
			[2]int{1, 2},
		},
		idx: []uint32{0, 1, 4, 5, 8, 9, 12, 13},
	},
	bitsetRunTestcase{
		name: "every_55_15",
		buf:  []byte{0xaa, 0xaa},
		size: 15,
		runs: [][2]int{
			[2]int{1, 1},
			[2]int{3, 1},
			[2]int{5, 1},
			[2]int{7, 1},
			[2]int{9, 1},
			[2]int{11, 1},
			[2]int{13, 1},
		},
		rruns: [][2]int{
			[2]int{13, 1},
			[2]int{11, 1},
			[2]int{9, 1},
			[2]int{7, 1},
			[2]int{5, 1},
			[2]int{3, 1},
			[2]int{1, 1},
		},
		idx: []uint32{1, 3, 5, 7, 9, 11, 13},
	},
	bitsetRunTestcase{
		name: "every_88_17",
		buf:  []byte{0x11, 0x11, 0x11},
		size: 17,
		runs: [][2]int{
			[2]int{0, 1},
			[2]int{4, 1},
			[2]int{8, 1},
			[2]int{12, 1},
			[2]int{16, 1},
		},
		rruns: [][2]int{
			[2]int{16, 1},
			[2]int{12, 1},
			[2]int{8, 1},
			[2]int{4, 1},
			[2]int{0, 1},
		},
		idx: []uint32{0, 4, 8, 12, 16},
	},
	bitsetRunTestcase{
		name: "last_0e_32",
		buf:  []byte{0x0, 0x0, 0x0, 0x70},
		size: 32,
		runs: [][2]int{
			[2]int{28, 3},
		},
		rruns: [][2]int{
			[2]int{30, 3},
		},
		idx: []uint32{28, 29, 30},
	},
	bitsetRunTestcase{
		name: "last_16",
		buf:  []byte{0x0, 0x80},
		size: 16,
		runs: [][2]int{
			[2]int{15, 1},
		},
		rruns: [][2]int{
			[2]int{15, 1},
		},
		idx: []uint32{15},
	},
	bitsetRunTestcase{
		name: "last_256",
		buf:  append(fillBitset(nil, 256-8, 0), byte(0x80)),
		size: 256,
		runs: [][2]int{
			[2]int{255, 1},
		},
		rruns: [][2]int{
			[2]int{255, 1},
		},
		idx: []uint32{255},
	},
	bitsetRunTestcase{
		name: "last_16k",
		buf:  append(fillBitset(nil, 16*1024-8, 0), byte(0x80)),
		size: 16 * 1024,
		runs: [][2]int{
			[2]int{16*1024 - 1, 1},
		},
		rruns: [][2]int{
			[2]int{16*1024 - 1, 1},
		},
		idx: []uint32{16*1024 - 1},
	},
	bitsetRunTestcase{
		name: "empty",
		buf:  []byte{},
		size: 0,
		runs: [][2]int{
			[2]int{-1, 0},
		},
		rruns: [][2]int{
			[2]int{-1, 0},
		},
		idx: []uint32{},
	},
	bitsetRunTestcase{
		name: "nil",
		buf:  nil,
		size: 0,
		runs: [][2]int{
			[2]int{-1, 0},
		},
		rruns: [][2]int{
			[2]int{-1, 0},
		},
		idx: []uint32{},
	},
	bitsetRunTestcase{
		name: "zeros_8",
		buf:  fillBitset(nil, 8, 0),
		size: 8,
		runs: [][2]int{
			[2]int{-1, 0},
		},
		rruns: [][2]int{
			[2]int{-1, 0},
		},
		idx: []uint32{},
	},
	bitsetRunTestcase{
		name: "zeros_32",
		buf:  fillBitset(nil, 32, 0),
		size: 32,
		runs: [][2]int{
			[2]int{-1, 0},
		},
		rruns: [][2]int{
			[2]int{-1, 0},
		},
		idx: []uint32{},
	},
	bitsetRunTestcase{
		name: "ones_32",
		buf:  fillBitset(nil, 32, 0xff),
		size: 32,
		runs: [][2]int{
			[2]int{0, 32},
		},
		rruns: [][2]int{
			[2]int{31, 32},
		},
		idx: fillIndex(0, 32),
	},
	bitsetRunTestcase{
		name: "ones_64",
		buf:  fillBitset(nil, 64, 0xff),
		size: 64,
		runs: [][2]int{
			[2]int{0, 64},
		},
		rruns: [][2]int{
			[2]int{63, 64},
		},
		idx: fillIndex(0, 64),
	},
	bitsetRunTestcase{
		name: "ones_32_zeros_32",
		buf:  []byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff},
		size: 96,
		runs: [][2]int{
			[2]int{0, 32},
			[2]int{64, 32},
		},
		rruns: [][2]int{
			[2]int{95, 32},
			[2]int{31, 32},
		},
		idx: append(fillIndex(0, 32), fillIndex(64, 32)...),
	},
	bitsetRunTestcase{
		name: "ones_64_zeros_64",
		buf: []byte{
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0, 0, 0, 0, 0, 0, 0, 0,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		size: 192,
		runs: [][2]int{
			[2]int{0, 64},
			[2]int{128, 64},
		},
		rruns: [][2]int{
			[2]int{191, 64},
			[2]int{63, 64},
		},
		idx: append(fillIndex(0, 64), fillIndex(128, 64)...),
	},
	bitsetRunTestcase{
		name: "64k_and_cd",
		buf:  append(bytes.Repeat([]byte{0x0}, 8*1024-1), byte(0xcd)),
		size: 64 * 1024,
		runs: [][2]int{
			[2]int{64*1024 - 8, 1},
			[2]int{64*1024 - 6, 2},
			[2]int{64*1024 - 2, 2},
		},
		rruns: [][2]int{
			[2]int{64*1024 - 1, 2},
			[2]int{64*1024 - 5, 2},
			[2]int{64*1024 - 8, 1},
		},
		idx: []uint32{64*1024 - 8, 64*1024 - 6, 64*1024 - 5, 64*1024 - 2, 64*1024 - 1},
	},
	bitsetRunTestcase{
		name: "64k_and_8d",
		buf:  append(bytes.Repeat([]byte{0x0}, 8*1024-1), byte(0x8d)),
		size: 64 * 1024,
		runs: [][2]int{
			[2]int{64*1024 - 8, 1},
			[2]int{64*1024 - 6, 2},
			[2]int{64*1024 - 1, 1},
		},
		rruns: [][2]int{
			[2]int{64*1024 - 1, 1},
			[2]int{64*1024 - 5, 2},
			[2]int{64*1024 - 8, 1},
		},
		idx: []uint32{64*1024 - 8, 64*1024 - 6, 64*1024 - 5, 64*1024 - 1},
	},
}

func TestBitsetRunGeneric(T *testing.T) {
	for _, c := range runTestcases {
		bits := NewBitsetFromBytes(c.buf, c.size)
		var idx, length int
		for i, r := range c.runs {
			T.Run(f("%s_%d", c.name, i), func(t *testing.T) {
				idx, length = bitsetRunGeneric(bits.Bytes(), idx+length, bits.Len())
				if got, want := idx, r[0]; got != want {
					T.Errorf("unexpected index %d, expected %d", got, want)
				}
				if got, want := length, r[1]; got != want {
					T.Errorf("unexpected length %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitsetRunReverse(T *testing.T) {
	for _, c := range runTestcases {
		if c.rruns == nil {
			continue
		}
		bits := NewBitsetFromBytes(c.buf, c.size)
		rev := bits.Reverse()
		var length int
		idx := bits.Len() - 1
		for i, r := range c.rruns {
			T.Run(f("%s_%d", c.name, i), func(t *testing.T) {
				idx, length = rev.Run(idx - length)
				if got, want := idx, r[0]; got != want {
					// fmt.Printf("%d - %s: Reverse Bitfield %08b\n", x, c.name, rev.Bytes())
					// fmt.Printf("%d - %s: Runs %#v\n", x, c.name, c.rruns)
					T.Errorf("unexpected index %d, expected %d", got, want)
				}
				if got, want := length, r[1]; got != want {
					// fmt.Printf("%d - %s: Reverse Bitfield %08b\n", x, c.name, rev.Bytes())
					// fmt.Printf("%d - %s: Runs %#v\n", x, c.name, c.rruns)
					T.Errorf("unexpected length %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitsetRunAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range runTestcases {
		bits := NewBitsetFromBytes(c.buf, c.size)
		var idx, length int
		for i, r := range c.runs {
			T.Run(f("%s_%d", c.name, i), func(t *testing.T) {
				idx, length = bitsetRunAVX2Wrapper(bits.Bytes(), idx+length, bits.Len())
				if got, want := idx, r[0]; got != want {
					T.Errorf("unexpected index %d, expected %d", got, want)
				}
				if got, want := length, r[1]; got != want {
					T.Errorf("unexpected length %d, expected %d", got, want)
				}
			})
		}
	}
}

// bits generates sequence of n numbers with max bits,
// ensuring max bit is set for 50% of the values.
func randBits(n int) []byte {
	c := (n + 7) / 8
	out := make([]byte, c+3)
	for i := 0; i < (c+3)/4; i++ {
		binary.BigEndian.PutUint32(out[4*i:4*i+4], uint32(rand.Int31()))
	}
	return out[:c]
}

func randBitsets(n, sz int) []*Bitset {
	res := make([]*Bitset, n)
	for i := range res {
		res[i] = NewBitsetFromBytes(randBits(sz), sz)
	}
	return res
}

func TestBitsetSlice(T *testing.T) {
	rand.Seed(0)
	for _, sz := range bitsetSizes {
		for i, b := range randBitsets(100, sz) {
			T.Run(f("%d_%d", sz, i), func(t *testing.T) {
				slice := b.Slice()
				if got, want := len(slice), sz; got != want {
					T.Errorf("unexpected length %d, expected %d", got, want)
					// T.FailNow()
				}
				for k, v := range slice {
					if got, want := v, b.IsSet(k); got != want {
						T.Errorf("unexpected bit %d: got %t, expected %t", k, got, want)
						T.FailNow()
					}
				}
			})
		}
	}
}

func TestBitsetSubSlice(T *testing.T) {
	rand.Seed(0)
	for _, sz := range bitsetSizes {
		for i, b := range randBitsets(100, sz) {
			T.Run(f("%d_%d", sz, i), func(t *testing.T) {
				start := int(rand.Int31n(int32(b.Len())))
				n := int(rand.Int31n(int32(b.Len() - start)))
				slice := b.SubSlice(start, n)
				if got, want := len(slice), n; got != want {
					T.Errorf("unexpected length %d, expected %d", got, want)
					T.FailNow()
				}
				for k, v := range slice {
					if got, want := v, b.IsSet(start+k); got != want {
						T.Errorf("unexpected bit %d: got %t, expected %t", k, got, want)
						T.FailNow()
					}
				}
			})
		}
	}
}

func TestBitsetFromSlice(T *testing.T) {
	rand.Seed(0)
	for _, sz := range bitsetSizes {
		for i, b := range randBitsets(100, sz) {
			T.Run(f("%d_%d", sz, i), func(t *testing.T) {
				slice := b.Slice()
				bits := NewBitsetFromSlice(slice)
				if got, want := len(bits.Bytes()), len(b.Bytes()); got != want {
					T.Errorf("unexpected buf length %d, expected %d", got, want)
					T.FailNow()
				}
				if got, want := bits.Len(), b.Len(); got != want {
					T.Errorf("unexpected size %d, expected %d", got, want)
					T.FailNow()
				}
				if got, want := bits.Count(), b.Count(); got != want {
					T.Errorf("unexpected count %d, expected %d", got, want)
					T.FailNow()
				}
				if bytes.Compare(bits.Bytes(), b.Bytes()) != 0 {
					T.Fatalf("unexpected result %x, expected %x", bits.Bytes(), b.Bytes())
					T.FailNow()
				}
			})
		}
	}
}

// TODO: edge cases
// - dstPos < 0
// - srcPos + srcLen > size
func TestBitsetInsert(T *testing.T) {
	var fast, fasthead, slow int
	rand.Seed(0)
	for _, sz := range bitsetSizes {
		for i, src := range randBitsets(100, sz) {
			dst := NewBitset(1024)
			for _, pat := range bitsetPatterns {
				T.Run(f("%d_%d_%x", sz, i, pat), func(t *testing.T) {
					dst.Fill(pat)
					srcPos := int(rand.Int31n(int32(src.Len())))
					srcLen := int(rand.Int31n(int32(src.Len() - srcPos)))
					dstPos := int(rand.Int31n(int32(dst.Len())))

					if dstPos&0x7+srcLen&0x7 == 0 {
						fasthead++
					}

					if srcPos&0x7+dstPos&0x7+srcLen&0x7 == 0 {
						fast++
					} else {
						slow++
					}

					lbefore := dst.Len()
					cbefore := dst.Count()
					dst.Insert(src, srcPos, srcLen, dstPos)

					dstSlice := dst.SubSlice(dstPos, srcLen)
					srcSlice := src.SubSlice(srcPos, srcLen)
					var srcSet int
					for i := range srcSlice {
						if srcSlice[i] {
							srcSet++
						}
					}

					T.Logf("SRC=%x DST=%x srcPos=%d dstPos=%d n=%d srcBits=%d\n",
						src.Bytes(), dst.Bytes(), srcPos, dstPos, srcLen, srcSet)
					if got, want := lbefore+srcLen, dst.Len(); got != want {
						T.Errorf("unexpected dst bitset len %d, expected %d", got, want)
						T.FailNow()
					}
					if got, want := dst.Count(), cbefore+srcSet; got != want {
						T.Errorf("unexpected count %d, expected %d", got, want)
						T.FailNow()
					}
					if got, want := dst.Count(), popcount(dst.Bytes()); got != want {
						T.Errorf("unexpected real count %d, expected %d", got, want)
						T.FailNow()
					}
					if got, want := len(dstSlice), len(srcSlice); got != want {
						T.Errorf("unexpected []bool size %d, expected %d", got, want)
						T.FailNow()
					}
					for j := range dstSlice {
						if got, want := dstSlice[j], srcSlice[j]; got != want {
							T.Errorf("unexpected bit %d: %t, expected %t", j, got, want)
							T.FailNow()
						}
					}
				})
			}
		}
	}
	if fast == 0 || fasthead == 0 {
		T.Errorf("%d slow, %d fast, %d fast head/tail path hits – try increasing random sample size\n", slow, fast, fasthead)
	}
}

// TODO: edge cases
func TestBitsetReplace(T *testing.T) {
	var fast, slow int
	rand.Seed(0)
	for _, sz := range bitsetSizes {
		for i, src := range randBitsets(100, sz) {
			dst := NewBitset(sz)
			for _, pat := range bitsetPatterns {
				T.Run(f("%d_%d_%x", sz, i, pat), func(t *testing.T) {
					dst.Fill(pat)
					srcPos := int(rand.Int31n(int32(src.Len())))
					srcLen := int(rand.Int31n(int32(src.Len() - srcPos)))
					// dstPos := int(rand.Int31n(int32(dst.Len() - srcLen)))
					dstPos := int(rand.Int31n(int32(dst.Len())))

					if srcPos&0x7+dstPos&0x7+srcLen&0x7 == 0 {
						fast++
					} else {
						slow++
					}

					lbefore := dst.Len()
					dst.Replace(src, srcPos, srcLen, dstPos)

					dstSlice := dst.SubSlice(dstPos, srcLen)
					srcSlice := src.SubSlice(srcPos, min(srcLen, dst.Len()-dstPos))
					// T.Logf("SRC=%x DST=%x srcPos=%d dstPos=%d n=%d\n",
					// 	src.Bytes(), dst.Bytes(), srcPos, dstPos, srcLen)
					if got, want := dst.Len(), lbefore; got != want {
						T.Errorf("unexpected bitset len %d, expected %d", got, want)
						T.FailNow()
					}
					if got, want := len(dstSlice), len(srcSlice); got != want {
						T.Errorf("unexpected []bool size %d, expected %d", got, want)
						T.FailNow()
					}
					for j := range dstSlice {
						if got, want := dstSlice[j], srcSlice[j]; got != want {
							T.Errorf("unexpected bit %d: %t, expected %t", j, got, want)
							T.FailNow()
						}
					}
				})
			}
		}
	}
	if fast == 0 {
		T.Errorf("%d slow, %d fast path hits – try increasing random sample size\n", slow, fast)
	}
}

func TestBitsetAppend(T *testing.T) {
	var fast, slow int
	rand.Seed(0)
	for _, sz := range bitsetSizes {
		for i, src := range randBitsets(100, sz) {
			dst := NewBitset(sz)
			for _, pat := range bitsetPatterns {
				T.Run(f("%d_%d_%x", sz, i, pat), func(t *testing.T) {
					dst.Fill(pat)
					srcPos := int(rand.Int31n(int32(src.Len())))
					srcLen := int(rand.Int31n(int32(src.Len() - srcPos)))

					if dst.size&0x7+srcPos&0x7+srcLen&0x7 == 0 {
						fast++
					} else {
						slow++
					}

					lbefore := dst.Len()
					cbefore := dst.Count()
					dst.Append(src, srcPos, srcLen)

					dstSlice := dst.SubSlice(lbefore, srcLen)
					srcSlice := src.SubSlice(srcPos, srcLen)
					var srcSet int
					for i := range srcSlice {
						if srcSlice[i] {
							srcSet++
						}
					}

					T.Logf("SRC=%x DST=%x srcPos=%d dstPos=%d n=%d\n",
						src.Bytes(), dst.Bytes(), srcPos, lbefore, srcLen)
					if got, want := lbefore+srcLen, dst.Len(); got != want {
						T.Errorf("unexpected dst bitset len %d, expected %d", got, want)
						T.FailNow()
					}
					if got, want := dst.Count(), cbefore+srcSet; got != want {
						T.Errorf("unexpected count %d, expected %d", got, want)
						T.FailNow()
					}
					if got, want := dst.Count(), popcount(dst.Bytes()); got != want {
						T.Errorf("unexpected real count %d, expected %d", got, want)
						T.FailNow()
					}
					if got, want := len(dstSlice), len(srcSlice); got != want {
						T.Errorf("unexpected []bool size %d, expected %d", got, want)
						T.FailNow()
					}
					for j := range dstSlice {
						if got, want := dstSlice[j], srcSlice[j]; got != want {
							T.Errorf("unexpected bit %d: %t, expected %t", j, got, want)
							T.FailNow()
						}
					}
				})
			}
		}
	}
	if fast == 0 {
		T.Errorf("%d slow, %d fast path hits – try increasing random sample size\n", slow, fast)
	}
}

func TestBitsetDelete(T *testing.T) {
	var fast, slow int
	rand.Seed(0)
	for _, sz := range bitsetSizes {
		for i, src := range randBitsets(100, sz) {
			dst := NewBitset(sz)
			for _, pat := range bitsetPatterns {
				T.Run(f("%d_%d_%x", sz, i, pat), func(t *testing.T) {
					// strategy:
					// - create a defined bitset with poison data
					// - insert random data (requires the insert test to succeed)
					// - delete the inserted data
					// - check original poison is unchanged
					dst.Fill(pat)
					srcPos := int(rand.Int31n(int32(src.Len())))
					srcLen := int(rand.Int31n(int32(src.Len() - srcPos)))
					dstPos := int(rand.Int31n(int32(dst.Len())))

					if dstPos&0x7+srcLen&0x7 == 0 {
						fast++
					} else {
						slow++
					}

					before := dst.Clone()
					dst.Insert(src, srcPos, srcLen, dstPos)
					dst.Delete(dstPos, srcLen)

					T.Logf("BEFORE(%d/%d)=%x AFTER(%d/%d)=%x delPos=%d n=%d fast=%t\n",
						before.Count(), before.Len(), before.Bytes(),
						dst.Count(), dst.Len(), dst.Bytes(),
						dstPos, srcLen,
						dstPos&0x7+srcLen&0x7 == 0,
					)
					if got, want := dst.Len(), before.Len(); got != want {
						T.Errorf("unexpected dst bitset len %d, expected %d", got, want)
						T.FailNow()
					}
					if got, want := dst.Count(), before.Count(); got != want {
						T.Errorf("unexpected count %d, expected %d", got, want)
						T.FailNow()
					}
					if got, want := dst.Count(), popcount(dst.Bytes()); got != want {
						T.Errorf("unexpected real count %d, expected %d", got, want)
						T.FailNow()
					}
					if got, want := len(dst.Bytes()), len(before.Bytes()); got != want {
						T.Fatalf("unexpected bitset buf len %d, expected %d", got, want)
						T.FailNow()
					}
					if bytes.Compare(dst.Bytes(), before.Bytes()) != 0 {
						T.Fatalf("unexpected memory contents %x, expected %x", dst.Bytes(), before.Bytes())
						T.FailNow()
					}
					checkCleanTail(T, dst.Bytes())
				})
			}
		}
	}
	if fast == 0 {
		T.Errorf("%d slow, %d fast path hits – try increasing random sample size\n", slow, fast)
	}
}

func TestBitsetSwap(T *testing.T) {
	rand.Seed(0)
	for _, sz := range bitsetSizes {
		for i, src := range randBitsets(100, sz) {
			T.Run(f("%d_%d", sz, i), func(t *testing.T) {
				i := int(rand.Int31n(int32(src.Len())))
				j := int(rand.Int31n(int32(src.Len())))

				ibefore := src.IsSet(i)
				jbefore := src.IsSet(j)
				cbefore := src.Count()
				lbefore := src.Len()
				src.Swap(i, j)

				T.Logf("SWAP(%d/%d)=%t/%t AFTER(%d/%d)=%t/%t cnt=%d len=%d\n",
					i, j, ibefore, jbefore,
					i, j, src.IsSet(i), src.IsSet(j),
					cbefore, lbefore,
				)
				if got, want := src.Len(), lbefore; got != want {
					T.Errorf("unexpected bitset len %d, expected %d", got, want)
					T.FailNow()
				}
				if got, want := src.Count(), cbefore; got != want {
					T.Errorf("unexpected count %d, expected %d", got, want)
					T.FailNow()
				}
				if got, want := src.Count(), popcount(src.Bytes()); got != want {
					T.Errorf("unexpected real count %d, expected %d", got, want)
					T.FailNow()
				}
				if got, want := src.IsSet(j), ibefore; got != want {
					T.Fatalf("unexpected bit i=%d: got %t, expected %t", i, got, want)
					T.FailNow()
				}
				if got, want := src.IsSet(i), jbefore; got != want {
					T.Fatalf("unexpected bit j=%d: got %t, expected %t", j, got, want)
					T.FailNow()
				}
				checkCleanTail(T, src.Bytes())
			})
		}
	}
}

// Bitset low-level benchmarks
//
func BenchmarkBitsetSwap(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			bs := NewBitsetFromBytes(bits, n.l)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bs.Swap(0, n.l/2)
			}
		})
	}
}

func BenchmarkBitsetSwapBool(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			bs := NewBitsetFromBytes(bits, n.l)
			slice := bs.Slice()
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				slice[0], slice[n.l/2] = slice[n.l/2], slice[0]
			}
		})
	}
}

/*func BenchmarkBitsetIndexHighDensity(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/3; i++ {
				bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
			}
			bs := NewBitsetFromBytes(bits, n.l)
			slice := make([]int, int(bs.Count()))
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				_ = bs.Indexes(slice)
			}
		})
	}
}*/

func BenchmarkBitsetIndexGenericHighDensity(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/3; i++ {
				bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
			}*/
			bits := fillBitsetRand(nil, n.l, 0.3333)
			slice := make([]uint32, int(bitsetPopCountGeneric(bits, n.l)))
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetIndexesGeneric(bits, n.l, slice)
			}
		})
	}
}

func BenchmarkBitsetIndexGeneric64K(B *testing.B) {
	for _, n := range bitsetBenchmarkDensities {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/3; i++ {
				bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
			}*/
			var l int = 64 * 1024
			bits := fillBitsetRand(nil, l, n.d)
			slice := make([]uint32, int(bitsetPopCountGeneric(bits, l)))
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(l)))
			for i := 0; i < B.N; i++ {
				bitsetIndexesGeneric(bits, l, slice)
			}
		})
	}
}

func BenchmarkBitsetIndexAVX264K(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkDensities {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/3; i++ {
				bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
			}*/
			var l int = 64 * 1024
			bits := fillBitsetRand(nil, l, n.d)
			slice := make([]uint32, int(bitsetPopCountGeneric(bits, l))+8)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(l)))
			for i := 0; i < B.N; i++ {
				bitsetIndexesAVX2(bits, l, slice)
			}
		})
	}
}

func BenchmarkBitsetIndexAVX2New64K(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkDensities {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/3; i++ {
				bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
			}*/
			var l int = 4 * 1024
			bits := fillBitsetRand(nil, l, n.d)
			pc := int(bitsetPopCountGeneric(bits, l))
			slice := make([]uint32, int(pc)+8)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(l)))
			for i := 0; i < B.N; i++ {
				if ret := bitsetIndexesAVX2New(bits, l, slice); pc != ret {
					B.Errorf("Bad popcount %d != %d", ret, pc)
					fmt.Print(bits)
					fmt.Print(slice)
				}
			}
		})
	}
}

func BenchmarkBitsetIndexAVX2HighDensity(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			/*			bits := fillBitset(nil, n.l, 0)
						l := uint64(bitFieldLen(n.l))
						var rnd = rand.NewSource(0).(rand.Source64)
						for i := 0; i < n.l/3; i++ {
							bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
						}*/
			bits := fillBitsetRand(nil, n.l, 0.3333)
			slice := make([]uint32, int(bitsetPopCountAVX2(bits)+8))
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetIndexesAVX2(bits, n.l, slice)
			}
		})
	}
}

func BenchmarkBitsetIndexGenericMedDensity(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/20; i++ {
				bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
			}*/
			bits := fillBitsetRand(nil, n.l, 0.05)
			slice := make([]uint32, int(bitsetPopCountGeneric(bits, n.l)))
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetIndexesGeneric(bits, n.l, slice)
			}
		})
	}
}

func BenchmarkBitsetIndexAVX2MedDensity(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/20; i++ {
				bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
			}*/
			bits := fillBitsetRand(nil, n.l, 0.05)
			slice := make([]uint32, int(bitsetPopCountAVX2(bits)+8))
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetIndexesAVX2(bits, n.l, slice)
			}
		})
	}
}

func BenchmarkBitsetIndexGenericLowDensity(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/1250; i++ {
				bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
			}*/
			bits := fillBitsetRand(nil, n.l, 0.0008)
			slice := make([]uint32, int(bitsetPopCountGeneric(bits, n.l)))
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetIndexesGeneric(bits, n.l, slice)
			}
		})
	}
}

func BenchmarkBitsetIndexAVX2LowDensity(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/1000; i++ {
				bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
			}*/
			bits := fillBitsetRand(nil, n.l, 0.0008)
			slice := make([]uint32, int(bitsetPopCountAVX2(bits)+8))
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetIndexesAVX2(bits, n.l, slice)
			}
		})
	}
}

/*func BenchmarkBitsetIndexLowDensity(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/1280; i++ {
				bits[rnd.Uint64()%l] = 1
			}
			bs := NewBitsetFromBytes(bits, n.l)
			slice := make([]int, int(bs.Count()))
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				_ = bs.Indexes(slice)
			}
		})
	}
}*/

func BenchmarkBitsetRunGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0)
			bits[len(bits)-1] |= 1
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetRunGeneric(bits, 0, n.l)
			}
		})
	}
}

func BenchmarkBitsetRunAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0)
			bits[len(bits)-1] |= 1
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetRunAVX2Wrapper(bits, 0, n.l)
			}
		})
	}
}

func BenchmarkBitsetRunGenericMean(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0)
			bits[len(bits)/2] |= 1
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetRunGeneric(bits, 0, n.l)
			}
		})
	}
}

func BenchmarkBitsetRunAVX2Mean(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0)
			bits[len(bits)/2] |= 1
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetRunAVX2Wrapper(bits, 0, n.l)
			}
		})
	}
}

// 0.0008%
// BenchmarkBitsetRunGenericLowDensity/32-8    	100000000	        11.1 ns/op	 360.04 MB/s
// BenchmarkBitsetRunGenericLowDensity/128-8   	89182185	        14.0 ns/op	1138.98 MB/s
// BenchmarkBitsetRunGenericLowDensity/1K-8    	39659865	        31.8 ns/op	4021.96 MB/s
// BenchmarkBitsetRunGenericLowDensity/16K-8   	 2192918	       549 ns/op	3731.56 MB/s
// BenchmarkBitsetRunGenericLowDensity/128K-8  	  267871	      4397 ns/op	3726.08 MB/s
// BenchmarkBitsetRunGenericLowDensity/1M-8    	   26887	     44459 ns/op	2948.15 MB/s
// BenchmarkBitsetRunGenericLowDensity/16M-8   	    1504	    760248 ns/op	2758.51 MB/s
// BenchmarkBitsetRunGenericLowDensity/128M-8  	     189	   5986233 ns/op	2802.63 MB/s
// BenchmarkBitsetRunGenericLowDensity/512M-8  	      39	  28992549 ns/op	2314.69 MB/s
func BenchmarkBitsetRunGenericLowDensity(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/1250; i++ {
				bits[rnd.Uint64()%l] = 1
			}*/
			bits := fillBitsetRand(nil, n.l, 0.0008)
			B.ResetTimer()
			B.SetBytes(int64(n.l / 8))
			for i := 0; i < B.N; i++ {
				var idx, length int
				for idx > -1 {
					idx, length = bitsetRunGeneric(bits, idx+length, n.l)
				}
			}
		})
	}
}

// 5%
// BenchmarkBitsetRunGenericMedDensity/32-8   	60871117	        20.8 ns/op	 192.39 MB/s
// BenchmarkBitsetRunGenericMedDensity/128-8  	16443476	        72.4 ns/op	 221.08 MB/s
// BenchmarkBitsetRunGenericMedDensity/1K-8   	 1775103	       649 ns/op	 197.22 MB/s
// BenchmarkBitsetRunGenericMedDensity/16K-8  	  115305	     10708 ns/op	 191.25 MB/s
// BenchmarkBitsetRunGenericMedDensity/128K-8 	    9120	    133947 ns/op	 122.32 MB/s
// BenchmarkBitsetRunGenericMedDensity/1M-8   	    1041	   1106326 ns/op	 118.48 MB/s
// BenchmarkBitsetRunGenericMedDensity/16M-8  	      63	  19047267 ns/op	 110.10 MB/s
// BenchmarkBitsetRunGenericMedDensity/128M-8 	       8	 139005274 ns/op	 120.69 MB/s
// BenchmarkBitsetRunGenericMedDensity/512M-8 	       2	 546642168 ns/op	 122.77 MB/s
func BenchmarkBitsetRunGenericMedDensity(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/20; i++ {
				bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
			}*/
			bits := fillBitsetRand(nil, n.l, 0.05)
			B.ResetTimer()
			B.SetBytes(int64(n.l / 8))
			for i := 0; i < B.N; i++ {
				var idx, length int
				for idx > -1 {
					idx, length = bitsetRunGeneric(bits, idx+length, n.l)
				}
			}
		})
	}
}

// 33%
// BenchmarkBitsetRunGenericHighDensity/32-8   	13744953	        89.0 ns/op	  44.93 MB/s
// BenchmarkBitsetRunGenericHighDensity/128-8  	 3384679	       362 ns/op	  44.20 MB/s
// BenchmarkBitsetRunGenericHighDensity/1K-8   	  508273	      2371 ns/op	  53.98 MB/s
// BenchmarkBitsetRunGenericHighDensity/16K-8  	   24044	     50633 ns/op	  40.45 MB/s
// BenchmarkBitsetRunGenericHighDensity/128K-8 	    2713	    426425 ns/op	  38.42 MB/s
// BenchmarkBitsetRunGenericHighDensity/1M-8   	     346	   3409036 ns/op	  38.45 MB/s
// BenchmarkBitsetRunGenericHighDensity/16M-8  	      21	  56076283 ns/op	  37.40 MB/s
// BenchmarkBitsetRunGenericHighDensity/128M-8 	       3	 452572946 ns/op	  37.07 MB/s
// BenchmarkBitsetRunGenericHighDensity/512M-8 	       1	1787180770 ns/op	  37.55 MB/s
func BenchmarkBitsetRunGenericHighDensity(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/3; i++ {
				bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
			}*/
			bits := fillBitsetRand(nil, n.l, 0.3333)
			B.ResetTimer()
			B.SetBytes(int64(n.l / 8))
			for i := 0; i < B.N; i++ {
				var idx, length int
				for idx > -1 {
					idx, length = bitsetRunGeneric(bits, idx+length, n.l)
				}
			}
		})
	}
}

func BenchmarkBitsetRunGeneric64K(B *testing.B) {
	for _, n := range bitsetBenchmarkDensities {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/3; i++ {
				bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
			}*/
			var l int = 64 * 1024
			bits := fillBitsetRand(nil, l, n.d)
			B.ResetTimer()
			B.SetBytes(int64(l / 8))
			for i := 0; i < B.N; i++ {
				var idx, length int
				for idx > -1 {
					idx, length = bitsetRunGeneric(bits, idx+length, l)
				}
			}
		})
	}
}

// 0.08%
// BenchmarkBitsetRunAVX2LowDensity/32-8   	100000000	        10.9 ns/op	 365.95 MB/s
// BenchmarkBitsetRunAVX2LowDensity/128-8  	63099681	        18.2 ns/op	 880.15 MB/s
// BenchmarkBitsetRunAVX2LowDensity/1K-8   	53690181	        22.4 ns/op	5703.72 MB/s
// BenchmarkBitsetRunAVX2LowDensity/16K-8  	 2441510	       442 ns/op	4629.37 MB/s
// BenchmarkBitsetRunAVX2LowDensity/128K-8 	  325444	      3573 ns/op	4586.05 MB/s
// BenchmarkBitsetRunAVX2LowDensity/1M-8   	   37729	     32338 ns/op	4053.14 MB/s
// BenchmarkBitsetRunAVX2LowDensity/16M-8  	    1993	    596405 ns/op	3516.32 MB/s
// BenchmarkBitsetRunAVX2LowDensity/128M-8 	     244	   4634930 ns/op	3619.73 MB/s
// BenchmarkBitsetRunAVX2LowDensity/512M-8 	      62	  18705419 ns/op	3587.67 MB/s
func BenchmarkBitsetRunAVX2LowDensity(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/1250; i++ {
				bits[rnd.Uint64()%l] = 1
			}*/
			bits := fillBitsetRand(nil, n.l, 0.0008)
			B.ResetTimer()
			B.SetBytes(int64(n.l / 8))
			for i := 0; i < B.N; i++ {
				var idx, length int
				for idx > -1 {
					idx, length = bitsetRunAVX2Wrapper(bits, idx+length, n.l)
				}
			}
		})
	}
}

// 5%
// BenchmarkBitsetRunAVX2MedDensity/32-8  	51144909	        24.5 ns/op	 163.43 MB/s
// BenchmarkBitsetRunAVX2MedDensity/128-8 	13814709	        92.3 ns/op	 173.28 MB/s
// BenchmarkBitsetRunAVX2MedDensity/1K-8  	 1362798	       881 ns/op	 145.26 MB/s
// BenchmarkBitsetRunAVX2MedDensity/16K-8 	   71910	     16210 ns/op	 126.34 MB/s
// BenchmarkBitsetRunAVX2MedDensity/128K-8	    7946	    157822 ns/op	 103.81 MB/s
// BenchmarkBitsetRunAVX2MedDensity/1M-8  	    1003	   1214049 ns/op	 107.96 MB/s
// BenchmarkBitsetRunAVX2MedDensity/16M-8 	      55	  22018695 ns/op	  95.24 MB/s
// BenchmarkBitsetRunAVX2MedDensity/128M-8	       7	 152491717 ns/op	 110.02 MB/s
// BenchmarkBitsetRunAVX2MedDensity/512M-8	       2	 617679541 ns/op	 108.65 MB/s
func BenchmarkBitsetRunAVX2MedDensity(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/20; i++ {
				bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
			}*/
			bits := fillBitsetRand(nil, n.l, 0.05)
			B.ResetTimer()
			B.SetBytes(int64(n.l / 8))
			for i := 0; i < B.N; i++ {
				var idx, length int
				for idx > -1 {
					idx, length = bitsetRunAVX2Wrapper(bits, idx+length, n.l)
				}
			}
		})
	}
}

// 33%
// BenchmarkBitsetRunAVX2HighDensity/32-8  	13101159	        88.5 ns/op	  45.22 MB/s
// BenchmarkBitsetRunAVX2HighDensity/128-8 	 3330346	       371 ns/op	  43.15 MB/s
// BenchmarkBitsetRunAVX2HighDensity/1K-8  	  405645	      2894 ns/op	  44.23 MB/s
// BenchmarkBitsetRunAVX2HighDensity/16K-8 	   16256	     67881 ns/op	  30.17 MB/s
// BenchmarkBitsetRunAVX2HighDensity/128K-8	    2247	    528475 ns/op	  31.00 MB/s
// BenchmarkBitsetRunAVX2HighDensity/1M-8  	     276	   4290119 ns/op	  30.55 MB/s
// BenchmarkBitsetRunAVX2HighDensity/16M-8 	      18	  66521837 ns/op	  31.53 MB/s
// BenchmarkBitsetRunAVX2HighDensity/128M-8	       2	 542901150 ns/op	  30.90 MB/s
// BenchmarkBitsetRunAVX2HighDensity/512M-8	       1	2212268406 ns/op	  30.33 MB/s
func BenchmarkBitsetRunAVX2HighDensity(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/3; i++ {
				bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
			}*/
			bits := fillBitsetRand(nil, n.l, 0.3333)
			B.ResetTimer()
			B.SetBytes(int64(n.l / 8))
			for i := 0; i < B.N; i++ {
				var idx, length int
				for idx > -1 {
					idx, length = bitsetRunAVX2Wrapper(bits, idx+length, n.l)
				}
			}
		})
	}
}

func BenchmarkBitsetRunAVX264K(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkDensities {
		B.Run(n.name, func(B *testing.B) {
			/*bits := fillBitset(nil, n.l, 0)
			l := uint64(bitFieldLen(n.l))
			var rnd = rand.NewSource(0).(rand.Source64)
			for i := 0; i < n.l/3; i++ {
				bits[rnd.Uint64()%l] |= 1 << byte(rnd.Uint64()%8)
			}*/
			var l int = 64 * 1024
			bits := fillBitsetRand(nil, l, n.d)
			B.ResetTimer()
			B.SetBytes(int64(l / 8))
			for i := 0; i < B.N; i++ {
				var idx, length int
				for idx > -1 {
					idx, length = bitsetRunAVX2Wrapper(bits, idx+length, l)
				}
			}
		})
	}
}

// 1100 1100
// BenchmarkBitsetRunGenericCC/32-8         	13196994	        89.7 ns/op	  44.60 MB/s
// BenchmarkBitsetRunGenericCC/128-8        	 3101284	       376 ns/op	  42.56 MB/s
// BenchmarkBitsetRunGenericCC/1K-8         	  393585	      2949 ns/op	  43.40 MB/s
// BenchmarkBitsetRunGenericCC/16K-8        	   25574	     46632 ns/op	  43.92 MB/s
// BenchmarkBitsetRunGenericCC/128K-8       	    3075	    414731 ns/op	  39.51 MB/s
// BenchmarkBitsetRunGenericCC/1M-8         	     390	   3055885 ns/op	  42.89 MB/s
// BenchmarkBitsetRunGenericCC/16M-8        	      22	  48163218 ns/op	  43.54 MB/s
// BenchmarkBitsetRunGenericCC/128M-8       	       3	 402005366 ns/op	  41.73 MB/s
// BenchmarkBitsetRunGenericCC/512M-8       	       1	1561153801 ns/op	  42.99 MB/s
func BenchmarkBitsetRunGenericCC(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xcc)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				var idx, length int
				for idx > -1 {
					idx, length = bitsetRunGeneric(bits, idx+length, n.l)
				}
			}
		})
	}
}

// 1100 1100
// BenchmarkBitsetRunAVX2CC/32-8         	10091468	       105 ns/op	  37.94 MB/s
// BenchmarkBitsetRunAVX2CC/128-8        	 2755395	       462 ns/op	  34.65 MB/s
// BenchmarkBitsetRunAVX2CC/1K-8         	  348259	      3415 ns/op	  37.48 MB/s
// BenchmarkBitsetRunAVX2CC/16K-8        	   21565	     54885 ns/op	  37.31 MB/s
// BenchmarkBitsetRunAVX2CC/128K-8       	    2641	    445564 ns/op	  36.77 MB/s
// BenchmarkBitsetRunAVX2CC/1M-8         	     343	   3388466 ns/op	  38.68 MB/s
// BenchmarkBitsetRunAVX2CC/16M-8        	      20	  54911479 ns/op	  38.19 MB/s
// BenchmarkBitsetRunAVX2CC/128M-8       	       3	 453276596 ns/op	  37.01 MB/s
// BenchmarkBitsetRunAVX2CC/512M-8       	       1	1790669109 ns/op	  37.48 MB/s
func BenchmarkBitsetRunAVX2CC(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xcc)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				var idx, length int
				for idx > -1 {
					idx, length = bitsetRunAVX2Wrapper(bits, idx+length, n.l)
				}
			}
		})
	}
}

// BenchmarkBitsetRunGenericAA/32-8         	 6444073	       190 ns/op	  21.05 MB/s
// BenchmarkBitsetRunGenericAA/128-8        	 1513558	       766 ns/op	  20.89 MB/s
// BenchmarkBitsetRunGenericAA/1K-8         	  199784	      6045 ns/op	  21.17 MB/s
// BenchmarkBitsetRunGenericAA/16K-8        	   12344	     94062 ns/op	  21.77 MB/s
// BenchmarkBitsetRunGenericAA/128K-8       	    1560	    750240 ns/op	  21.84 MB/s
// BenchmarkBitsetRunGenericAA/1M-8         	     198	   6235992 ns/op	  21.02 MB/s
// BenchmarkBitsetRunGenericAA/16M-8        	      12	  97889100 ns/op	  21.42 MB/s
// BenchmarkBitsetRunGenericAA/128M-8       	       2	 773090222 ns/op	  21.70 MB/s
// BenchmarkBitsetRunGenericAA/512M-8       	       1	3162817860 ns/op	  21.22 MB/s
func BenchmarkBitsetRunGenericAA(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xaa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				var idx, length int
				for idx > -1 {
					idx, length = bitsetRunGeneric(bits, idx+length, n.l)
				}
			}
		})
	}
}

// 1010 1010
// BenchmarkBitsetRunAVX2AA/32-8         	 5843056	       201 ns/op	  19.95 MB/s
// BenchmarkBitsetRunAVX2AA/128-8        	 1487444	       794 ns/op	  20.15 MB/s
// BenchmarkBitsetRunAVX2AA/1K-8         	  176836	      6421 ns/op	  19.93 MB/s
// BenchmarkBitsetRunAVX2AA/16K-8        	   10000	    113820 ns/op	  17.99 MB/s
// BenchmarkBitsetRunAVX2AA/128K-8       	    1398	    827598 ns/op	  19.80 MB/s
// BenchmarkBitsetRunAVX2AA/1M-8         	     164	   6824297 ns/op	  19.21 MB/s
// BenchmarkBitsetRunAVX2AA/16M-8        	      10	 106722016 ns/op	  19.65 MB/s
// BenchmarkBitsetRunAVX2AA/128M-8       	       2	 829955647 ns/op	  20.21 MB/s
// BenchmarkBitsetRunAVX2AA/512M-8       	       1	3276270639 ns/op	  20.48 MB/s
func BenchmarkBitsetRunAVX2AA(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xaa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				var idx, length int
				for idx > -1 {
					idx, length = bitsetRunAVX2Wrapper(bits, idx+length, n.l)
				}
			}
		})
	}
}

// BenchmarkBitsetPopCountGeneric/32-8         	200000000	         7.45 ns/op	 537.09 MB/s
// BenchmarkBitsetPopCountGeneric/128-8        	100000000	        13.4 ns/op	1190.32 MB/s
// BenchmarkBitsetPopCountGeneric/1K-8         	30000000	        42.9 ns/op	2986.69 MB/s
// BenchmarkBitsetPopCountGeneric/16K-8        	 3000000	       540 ns/op	3788.38 MB/s
// BenchmarkBitsetPopCountGeneric/128K-8       	  300000	      4235 ns/op	3867.94 MB/s
// BenchmarkBitsetPopCountGeneric/1M-8         	   50000	     34329 ns/op	3818.10 MB/s
// BenchmarkBitsetPopCountGeneric/16M-8        	    3000	    560950 ns/op	3738.57 MB/s
// BenchmarkBitsetPopCountGeneric/128M-8       	     300	   4358409 ns/op	3849.39 MB/s
// BenchmarkBitsetPopCountGeneric/512M-8       	     100	  18061159 ns/op	3715.65 MB/s
func BenchmarkBitsetPopCountGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetPopCountGeneric(bits, n.l)
			}
		})
	}
}

// BenchmarkBitsetPopCountAVX2/32-8            	300000000	         6.12 ns/op	 653.36 MB/s
// BenchmarkBitsetPopCountAVX2/128-8           	200000000	         9.16 ns/op	1746.30 MB/s
// BenchmarkBitsetPopCountAVX2/1K-8            	100000000	        10.5 ns/op	12173.29 MB/s
// BenchmarkBitsetPopCountAVX2/16K-8           	30000000	        62.6 ns/op	32699.70 MB/s
// BenchmarkBitsetPopCountAVX2/128K-8          	 3000000	       358 ns/op	45673.24 MB/s
// BenchmarkBitsetPopCountAVX2/1M-8            	  500000	      3008 ns/op	43568.68 MB/s
// BenchmarkBitsetPopCountAVX2/16M-8           	   30000	     59189 ns/op	35431.28 MB/s
// BenchmarkBitsetPopCountAVX2/128M-8          	    2000	    894400 ns/op	18758.06 MB/s
// BenchmarkBitsetPopCountAVX2/512M-8          	     500	   3709751 ns/op	18089.85 MB/s
func BenchmarkBitsetPopCountAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetPopCountAVX2(bits)
			}
		})
	}
}

// BenchmarkBitsetAndGeneric/32-8         	200000000	         7.34 ns/op	 544.76 MB/s
// BenchmarkBitsetAndGeneric/128-8        	100000000	        21.6 ns/op	 740.72 MB/s
// BenchmarkBitsetAndGeneric/1K-8         	20000000	        87.1 ns/op	1468.98 MB/s
// BenchmarkBitsetAndGeneric/16K-8        	 1000000	      1225 ns/op	1671.20 MB/s
// BenchmarkBitsetAndGeneric/128K-8       	  200000	      9985 ns/op	1640.75 MB/s
// BenchmarkBitsetAndGeneric/1M-8         	   20000	     80136 ns/op	1635.60 MB/s
// BenchmarkBitsetAndGeneric/16M-8        	    1000	   1265940 ns/op	1656.60 MB/s
// BenchmarkBitsetAndGeneric/128M-8       	     100	  10242328 ns/op	1638.03 MB/s
// BenchmarkBitsetAndGeneric/512M-8       	      30	  44840644 ns/op	1496.61 MB/s
func BenchmarkBitsetAndGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetAndGeneric(bits, cmp, n.l)
			}
		})
	}
}

func BenchmarkBitsetAndGenericFlag1(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetAndGenericFlag1(bits, cmp, n.l)
			}
		})
	}
}

// BenchmarkBitsetAndAVX2/32-8         	200000000	         6.19 ns/op	 646.11 MB/s
// BenchmarkBitsetAndAVX2/128-8        	200000000	         8.85 ns/op	1808.15 MB/s
// BenchmarkBitsetAndAVX2/1K-8         	200000000	         8.12 ns/op	15767.05 MB/s
// BenchmarkBitsetAndAVX2/16K-8        	50000000	        25.2 ns/op	81304.13 MB/s
// BenchmarkBitsetAndAVX2/128K-8       	10000000	       195 ns/op	83982.76 MB/s
// BenchmarkBitsetAndAVX2/1M-8         	  200000	      6065 ns/op	21607.81 MB/s
// BenchmarkBitsetAndAVX2/16M-8        	   10000	    149202 ns/op	14055.70 MB/s
// BenchmarkBitsetAndAVX2/128M-8       	    1000	   1845298 ns/op	9091.87 MB/s
// BenchmarkBitsetAndAVX2/512M-8       	     200	  12771249 ns/op	5254.68 MB/s
func BenchmarkBitsetAndAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetAndAVX2(bits, cmp)
			}
		})
	}
}

func BenchmarkBitsetAndAVX2Flag1(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetAndAVX2Flag1(bits, cmp)
			}
		})
	}
}

// BenchmarkBitsetAndNotGeneric/32-8         	200000000	         8.04 ns/op	 497.60 MB/s
// BenchmarkBitsetAndNotGeneric/128-8        	100000000	        15.3 ns/op	1046.45 MB/s
// BenchmarkBitsetAndNotGeneric/1K-8         	20000000	        89.4 ns/op	1432.12 MB/s
// BenchmarkBitsetAndNotGeneric/16K-8        	 1000000	      1268 ns/op	1614.63 MB/s
// BenchmarkBitsetAndNotGeneric/128K-8       	  200000	     10361 ns/op	1581.21 MB/s
// BenchmarkBitsetAndNotGeneric/1M-8         	   20000	     81666 ns/op	1604.97 MB/s
// BenchmarkBitsetAndNotGeneric/16M-8        	    1000	   1384304 ns/op	1514.95 MB/s
// BenchmarkBitsetAndNotGeneric/128M-8       	     100	  11017526 ns/op	1522.78 MB/s
// BenchmarkBitsetAndNotGeneric/512M-8       	      30	  45853262 ns/op	1463.56 MB/s
func BenchmarkBitsetAndNotGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetAndNotGeneric(bits, cmp, n.l)
			}
		})
	}
}

// BenchmarkBitsetAndNotAVX2/32-8         	200000000	         6.67 ns/op	 599.59 MB/s
// BenchmarkBitsetAndNotAVX2/128-8        	200000000	         8.81 ns/op	1816.07 MB/s
// BenchmarkBitsetAndNotAVX2/1K-8         	200000000	         8.24 ns/op	15528.55 MB/s
// BenchmarkBitsetAndNotAVX2/16K-8        	50000000	        27.2 ns/op	75205.08 MB/s
// BenchmarkBitsetAndNotAVX2/128K-8       	10000000	       190 ns/op	86011.87 MB/s
// BenchmarkBitsetAndNotAVX2/1M-8         	  200000	      5680 ns/op	23075.02 MB/s
// BenchmarkBitsetAndNotAVX2/16M-8        	   10000	    133204 ns/op	15743.80 MB/s
// BenchmarkBitsetAndNotAVX2/128M-8       	    1000	   1844008 ns/op	9098.23 MB/s
// BenchmarkBitsetAndNotAVX2/512M-8       	     100	  10232017 ns/op	6558.71 MB/s
func BenchmarkBitsetAndNotAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetAndNotAVX2(bits, cmp)
			}
		})
	}
}

// BenchmarkBitsetOrGeneric/32-8         	200000000	         7.36 ns/op	 543.27 MB/s
// BenchmarkBitsetOrGeneric/128-8        	100000000	        15.0 ns/op	1063.83 MB/s
// BenchmarkBitsetOrGeneric/1K-8         	20000000	        88.0 ns/op	1454.75 MB/s
// BenchmarkBitsetOrGeneric/16K-8        	 1000000	      1211 ns/op	1689.95 MB/s
// BenchmarkBitsetOrGeneric/128K-8       	  200000	     10110 ns/op	1620.47 MB/s
// BenchmarkBitsetOrGeneric/1M-8         	   20000	     83328 ns/op	1572.96 MB/s
// BenchmarkBitsetOrGeneric/16M-8        	    1000	   1264415 ns/op	1658.59 MB/s
// BenchmarkBitsetOrGeneric/128M-8       	     100	  10283848 ns/op	1631.41 MB/s
// BenchmarkBitsetOrGeneric/512M-8       	      30	  42982470 ns/op	1561.31 MB/s
func BenchmarkBitsetOrGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetOrGeneric(bits, cmp, n.l)
			}
		})
	}
}

// BenchmarkBitsetOrAVX2/32-8            	200000000	         6.31 ns/op	 633.87 MB/s
// BenchmarkBitsetOrAVX2/128-8           	200000000	         8.61 ns/op	1858.88 MB/s
// BenchmarkBitsetOrAVX2/1K-8            	200000000	         9.36 ns/op	13678.46 MB/s
// BenchmarkBitsetOrAVX2/16K-8           	50000000	        32.5 ns/op	63010.45 MB/s
// BenchmarkBitsetOrAVX2/128K-8          	10000000	       189 ns/op	86453.95 MB/s
// BenchmarkBitsetOrAVX2/1M-8            	  300000	      5502 ns/op	23822.10 MB/s
// BenchmarkBitsetOrAVX2/16M-8           	   10000	    135032 ns/op	15530.76 MB/s
// BenchmarkBitsetOrAVX2/128M-8          	    1000	   1886645 ns/op	8892.62 MB/s
// BenchmarkBitsetOrAVX2/512M-8          	     200	  10585586 ns/op	6339.65 MB/s
func BenchmarkBitsetOrAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetOrAVX2(bits, cmp)
			}
		})
	}
}

// BenchmarkBitsetXorGeneric/32-8         	200000000	         7.88 ns/op	 507.62 MB/s
// BenchmarkBitsetXorGeneric/128-8        	100000000	        15.3 ns/op	1042.87 MB/s
// BenchmarkBitsetXorGeneric/1K-8         	20000000	        88.1 ns/op	1452.42 MB/s
// BenchmarkBitsetXorGeneric/16K-8        	 1000000	      1201 ns/op	1704.00 MB/s
// BenchmarkBitsetXorGeneric/128K-8       	  200000	     10056 ns/op	1629.13 MB/s
// BenchmarkBitsetXorGeneric/1M-8         	   20000	     79915 ns/op	1640.14 MB/s
// BenchmarkBitsetXorGeneric/16M-8        	    1000	   1307923 ns/op	1603.42 MB/s
// BenchmarkBitsetXorGeneric/128M-8       	     100	  10600042 ns/op	1582.75 MB/s
// BenchmarkBitsetXorGeneric/512M-8       	      30	  44760594 ns/op	1499.28 MB/s
func BenchmarkBitsetXorGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetXorGeneric(bits, cmp, n.l)
			}
		})
	}
}

// BenchmarkBitsetXorAVX2/32-8            	200000000	         6.25 ns/op	 639.86 MB/s
// BenchmarkBitsetXorAVX2/128-8           	200000000	         8.37 ns/op	1911.01 MB/s
// BenchmarkBitsetXorAVX2/1K-8            	200000000	         9.09 ns/op	14087.81 MB/s
// BenchmarkBitsetXorAVX2/16K-8           	50000000	        25.9 ns/op	79163.49 MB/s
// BenchmarkBitsetXorAVX2/128K-8          	10000000	       188 ns/op	86805.89 MB/s
// BenchmarkBitsetXorAVX2/1M-8            	  300000	      5619 ns/op	23323.86 MB/s
// BenchmarkBitsetXorAVX2/16M-8           	   10000	    138406 ns/op	15152.13 MB/s
// BenchmarkBitsetXorAVX2/128M-8          	    1000	   2075723 ns/op	8082.59 MB/s
// BenchmarkBitsetXorAVX2/512M-8          	     100	  12700923 ns/op	5283.78 MB/s
func BenchmarkBitsetXorAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			cmp := fillBitset(nil, n.l, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetXorAVX2(bits, cmp)
			}
		})
	}
}

// BenchmarkBitsetNotGeneric/32-8         	200000000	         7.42 ns/op	 538.80 MB/s
// BenchmarkBitsetNotGeneric/128-8        	100000000	        13.6 ns/op	1175.29 MB/s
// BenchmarkBitsetNotGeneric/1K-8         	20000000	        66.8 ns/op	1916.45 MB/s
// BenchmarkBitsetNotGeneric/16K-8        	 2000000	       824 ns/op	2484.28 MB/s
// BenchmarkBitsetNotGeneric/128K-8       	  200000	      6269 ns/op	2613.18 MB/s
// BenchmarkBitsetNotGeneric/1M-8         	   30000	     50854 ns/op	2577.39 MB/s
// BenchmarkBitsetNotGeneric/16M-8        	    2000	    836395 ns/op	2507.37 MB/s
// BenchmarkBitsetNotGeneric/128M-8       	     200	   6627973 ns/op	2531.27 MB/s
// BenchmarkBitsetNotGeneric/512M-8       	      50	  27560713 ns/op	2434.95 MB/s
func BenchmarkBitsetNotGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetNegGeneric(bits, n.l)
			}
		})
	}
}

// BenchmarkBitsetNotAVX2/32-8            	300000000	         5.24 ns/op	 763.87 MB/s
// BenchmarkBitsetNotAVX2/128-8           	200000000	         7.79 ns/op	2054.36 MB/s
// BenchmarkBitsetNotAVX2/1K-8            	200000000	         8.05 ns/op	15897.01 MB/s
// BenchmarkBitsetNotAVX2/16K-8           	100000000	        23.4 ns/op	87516.27 MB/s
// BenchmarkBitsetNotAVX2/128K-8          	10000000	       159 ns/op	102570.09 MB/s
// BenchmarkBitsetNotAVX2/1M-8            	  300000	      3931 ns/op	33338.47 MB/s
// BenchmarkBitsetNotAVX2/16M-8           	   20000	     81274 ns/op	25803.45 MB/s
// BenchmarkBitsetNotAVX2/128M-8          	    1000	   1072039 ns/op	15649.81 MB/s
// BenchmarkBitsetNotAVX2/512M-8          	     300	   4580533 ns/op	14650.88 MB/s
func BenchmarkBitsetNotAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetNegAVX2(bits)
			}
		})
	}
}

// BenchmarkBitsetReverse/32-8         	241114566	         4.96 ns/op	 806.48 MB/s
// BenchmarkBitsetReverse/128-8        	100000000	        11.6 ns/op	1378.02 MB/s
// BenchmarkBitsetReverse/1K-8         	16729281	        72.6 ns/op	1761.96 MB/s
// BenchmarkBitsetReverse/16K-8        	 1262490	       961 ns/op	2131.93 MB/s
// BenchmarkBitsetReverse/128K-8       	  158128	      7567 ns/op	2165.05 MB/s
// BenchmarkBitsetReverse/1M-8         	   19689	     62003 ns/op	2113.95 MB/s
// BenchmarkBitsetReverse/16M-8        	    1216	    979544 ns/op	2140.95 MB/s
// BenchmarkBitsetReverse/128M-8       	     147	   7965933 ns/op	2106.12 MB/s
// BenchmarkBitsetReverse/512M-8       	      33	  33553682 ns/op	2000.04 MB/s
func BenchmarkBitsetReverseGeneric(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetReverseGeneric(bits)
			}
		})
	}
}

func BenchmarkBitsetReverseAVX2(B *testing.B) {
	for _, n := range bitsetBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			bits := fillBitset(nil, n.l, 0xfa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.l)))
			for i := 0; i < B.N; i++ {
				bitsetReverseAVX2(bits, bitsetReverseLut256)
			}
		})
	}
}
