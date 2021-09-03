// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

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
			if got, want := ret, popcount(c.buf); got != want {
				T.Errorf("unexpected index vector length %d, expected %d", got, want)
			}
			if got, want := ret, len(c.idx); got != want {
				T.Errorf("unexpected return value %d, expected %d", got, want)
			}
			if !reflect.DeepEqual(idx, c.idx) {
				T.Errorf("unexpected result %d, expected %d", idx, c.idx)
			}
		})
	}
}

func TestBitsetIndexGenericSkip16(T *testing.T) {
	for _, c := range runTestcases {
		idx := make([]uint32, len(c.idx))
		T.Run(c.name, func(t *testing.T) {
			var ret = bitsetIndexesGenericSkip16(c.buf, c.size, idx)
			if got, want := ret, popcount(c.buf); got != want {
				T.Errorf("unexpected index vector length %d, expected %d", got, want)
			}
			if got, want := ret, len(c.idx); got != want {
				T.Errorf("unexpected return value %d, expected %d", got, want)
			}
			if !reflect.DeepEqual(idx, c.idx) {
				T.Errorf("unexpected result %d, expected %d", idx, c.idx)
			}
		})
	}
}

func TestBitsetIndexGenericSkip64(T *testing.T) {
	for _, c := range runTestcases {
		idx := make([]uint32, len(c.idx))
		T.Run(c.name, func(t *testing.T) {
			var ret = bitsetIndexesGenericSkip64(c.buf, c.size, idx)
			if got, want := ret, popcount(c.buf); got != want {
				T.Errorf("unexpected index vector length %d, expected %d", got, want)
			}
			if got, want := ret, len(c.idx); got != want {
				T.Errorf("unexpected return value %d, expected %d", got, want)
			}
			if !reflect.DeepEqual(idx, c.idx) {
				T.Errorf("unexpected result %d, expected %d", idx, c.idx)
			}
		})
	}
}

func TestBitsetIndexAVX2Full(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range runTestcases {
		idx := make([]uint32, len(c.idx)+8)
		T.Run(c.name, func(t *testing.T) {
			var ret = bitsetIndexesAVX2Full(c.buf, c.size, idx)
			if got, want := ret, popcount(c.buf); got != want {
				T.Errorf("unexpected index vector length %d, expected %d", got, want)
			}
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

func TestBitsetIndexAVX2Skip(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range runTestcases {
		c.buf = append(c.buf, 0xff)
		c.buf = c.buf[:len(c.buf)-1]
		idx := make([]uint32, len(c.idx)+8)
		T.Run(c.name, func(t *testing.T) {
			var ret = bitsetIndexesAVX2Skip(c.buf, c.size, idx)
			if got, want := ret, popcount(c.buf); got != want {
				T.Errorf("unexpected index vector length %d, expected %d", got, want)
			}
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
		name: "128_and_cd",
		buf:  append(bytes.Repeat([]byte{0x0}, 15), byte(0xcd)),
		size: 128,
		runs: [][2]int{
			[2]int{128 - 8, 1},
			[2]int{128 - 6, 2},
			[2]int{128 - 2, 2},
		},
		rruns: [][2]int{
			[2]int{128 - 1, 2},
			[2]int{128 - 5, 2},
			[2]int{128 - 8, 1},
		},
		idx: []uint32{128 - 8, 128 - 6, 128 - 5, 128 - 2, 128 - 1},
	},
	bitsetRunTestcase{
		name: "136_and_cd",
		buf:  append(bytes.Repeat([]byte{0x0}, 16), byte(0xcd)),
		size: 136,
		runs: [][2]int{
			[2]int{136 - 8, 1},
			[2]int{136 - 6, 2},
			[2]int{136 - 2, 2},
		},
		rruns: [][2]int{
			[2]int{136 - 1, 2},
			[2]int{136 - 5, 2},
			[2]int{136 - 8, 1},
		},
		idx: []uint32{136 - 8, 136 - 6, 136 - 5, 136 - 2, 136 - 1},
	},
	bitsetRunTestcase{
		name: "2048_and_cd",
		buf:  append(bytes.Repeat([]byte{0x0}, 255), byte(0xcd)),
		size: 2048,
		runs: [][2]int{
			[2]int{2048 - 8, 1},
			[2]int{2048 - 6, 2},
			[2]int{2048 - 2, 2},
		},
		rruns: [][2]int{
			[2]int{2048 - 1, 2},
			[2]int{2048 - 5, 2},
			[2]int{2048 - 8, 1},
		},
		idx: []uint32{2048 - 8, 2048 - 6, 2048 - 5, 2048 - 2, 2048 - 1},
	},
	bitsetRunTestcase{
		name: "2056_and_cd",
		buf:  append(bytes.Repeat([]byte{0x0}, 256), byte(0xcd)),
		size: 2056,
		runs: [][2]int{
			[2]int{2056 - 8, 1},
			[2]int{2056 - 6, 2},
			[2]int{2056 - 2, 2},
		},
		rruns: [][2]int{
			[2]int{2056 - 1, 2},
			[2]int{2056 - 5, 2},
			[2]int{2056 - 8, 1},
		},
		idx: []uint32{2056 - 8, 2056 - 6, 2056 - 5, 2056 - 2, 2056 - 1},
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
