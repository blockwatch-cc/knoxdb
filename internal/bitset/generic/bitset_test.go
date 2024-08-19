// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package generic

import (
	"bytes"
	"reflect"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset/tests"
)

var (
	bitsetPatterns = tests.Patterns
	popcount       = tests.Popcount
	fillBitset     = tests.FillBitset
	fillBitsetRand = tests.FillBitsetRand
	popCases       = tests.PopCases
	runTestcases   = tests.RunTestcases
	f              = tests.F
)

var bitsetSizes = []int{
	// only non-avx sizes
	7, 8, 9, 15, 16, 17, 23, 24, 25, 31, 32, 33,
	63, 64, 65, 127,
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
	for _, c := range popCases {
		T.Run(c.Name, func(t *testing.T) {
			cnt := PopCount(c.Source, c.Size)
			if got, want := int(cnt), c.Count; got != want {
				t.Errorf("unexpected count %d, expected %d", got, want)
			}
		})
	}
	for _, sz := range bitsetSizes {
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				buf := fillBitset(nil, sz, pt)
				if got, want := int(PopCount(buf, sz)), popcount(buf); got != want {
					t.Errorf("unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitAndGeneric(T *testing.T) {
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				And(dst, dst, sz)
				if bytes.Compare(dst, src) != 0 {
					t.Errorf("dst===src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					t.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				And(dst, src, sz)
				if bytes.Compare(dst, src) != 0 {
					t.Errorf("dst==src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					t.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// all zeros
				copy(dst, src)
				And(dst, zeros, sz)
				if bytes.Compare(dst, zeros) != 0 {
					t.Errorf("zeros: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					t.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all ones
				copy(dst, src)
				And(dst, ones, sz)
				if bytes.Compare(dst, src) != 0 {
					t.Errorf("ones: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					t.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitAndGenericFlag(T *testing.T) {
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				any, all := AndFlag(dst, dst, sz)
				if pt == 0x80 && sz == 7 {
					if any {
						t.Errorf("dst===src: unexpected return value %v, expected false", any)
					}
				} else {
					if !any {
						t.Errorf("dst===src: unexpected return value %v, expected true", any)
					}
				}
				if pt == 0xff {
					if !all {
						t.Errorf("dst===src: unexpected return value %v, expected true", all)
					}
				} else {
					if all {
						t.Errorf("dst===src: unexpected return value %v, expected false", all)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					t.Errorf("dst===src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					t.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				any, all = AndFlag(dst, src, sz)
				if pt == 0x80 && sz == 7 {
					if any {
						t.Errorf("dst==src: unexpected return value %v, expected false", any)
					}
				} else {
					if !any {
						t.Errorf("dst==src: unexpected return value %v, expected true", any)
					}
				}
				if pt == 0xff {
					if !all {
						t.Errorf("dst==src: unexpected return value %v, expected true", all)
					}
				} else {
					if all {
						t.Errorf("dst==src: unexpected return value %v, expected false", all)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					t.Errorf("dst==src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					t.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// all zeros
				copy(dst, src)
				any, all = AndFlag(dst, zeros, sz)
				if any {
					t.Errorf("zeros: unexpected return value %v, expected false", any)
				}
				if all {
					t.Errorf("zeros: unexpected return value %v, expected false", all)
				}
				if bytes.Compare(dst, zeros) != 0 {
					t.Errorf("zeros: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					t.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all ones
				copy(dst, src)
				any, all = AndFlag(dst, ones, sz)
				if pt == 0x80 && sz == 7 {
					if any {
						t.Errorf("ones: unexpected return value %v, expected false", any)
					}
				} else {
					if !any {
						t.Errorf("ones: unexpected return value %v, expected true", any)
					}
				}
				if pt == 0xff {
					if !all {
						t.Errorf("ones: unexpected return value %v, expected true", all)
					}
				} else {
					if all {
						t.Errorf("ones: unexpected return value %v, expected 0", all)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					t.Errorf("ones: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					t.Errorf("ones: unexpected count %d, expected %d", got, want)
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
				AndNot(dst, dst, sz)
				if bytes.Compare(dst, zeros) != 0 {
					t.Errorf("dst===src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					t.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				AndNot(dst, src, sz)
				if bytes.Compare(dst, zeros) != 0 {
					t.Errorf("dst==src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					t.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// val AND NOT zeros == val
				copy(dst, src)
				AndNot(dst, zeros, sz)
				if bytes.Compare(dst, src) != 0 {
					t.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					t.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all AND NOT ones == zero
				copy(dst, src)
				AndNot(dst, ones, sz)
				if bytes.Compare(dst, zeros) != 0 {
					t.Errorf("ones: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					t.Errorf("ones: unexpected count %d, expected %d", got, want)
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
				Or(dst, dst, sz)
				if bytes.Compare(dst, src) != 0 {
					t.Errorf("dst===src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					t.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				Or(dst, src, sz)
				if bytes.Compare(dst, src) != 0 {
					t.Errorf("dst==src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					t.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// val OR zeros == val
				copy(dst, src)
				Or(dst, zeros, sz)
				if bytes.Compare(dst, src) != 0 {
					t.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					t.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all OR ones == ones
				copy(dst, src)
				Or(dst, ones, sz)
				if bytes.Compare(dst, ones) != 0 {
					t.Errorf("ones: unexpected result %x, expected %x", dst, ones)
				}
				if got, want := popcount(dst), popcount(ones); got != want {
					t.Errorf("ones: unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitOrGenericFlag(T *testing.T) {
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			T.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				src := fillBitset(nil, sz, pt)
				dst := fillBitset(nil, sz, pt)

				// same value, same slice
				any, all := OrFlag(dst, dst, sz)
				if pt == 0x80 && sz == 7 {
					if any {
						t.Errorf("dst===src: unexpected return value %v, expected false", any)
					}
				} else {
					if !any {
						t.Errorf("dst===src: unexpected return value %v, expected true", any)
					}
				}
				if pt == 0xff {
					if !all {
						t.Errorf("dst===src: unexpected return value %v, expected true", all)
					}
				} else {
					if all {
						t.Errorf("dst===src: unexpected return value %v, expected false", all)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					t.Errorf("dst===src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					t.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				any, all = OrFlag(dst, src, sz)
				if pt == 0x80 && sz == 7 {
					if any {
						t.Errorf("dst==src: unexpected return value %v, expected false", any)
					}
				} else {
					if !any {
						t.Errorf("dst==src: unexpected return value %v, expected true", any)
					}
				}
				if pt == 0xff {
					if !all {
						t.Errorf("dst==src: unexpected return value %v, expected true", all)
					}
				} else {
					if all {
						t.Errorf("dst==src: unexpected return value %v, expected false", all)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					t.Errorf("dst==src: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					t.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// all zeros
				copy(dst, src)
				any, all = OrFlag(dst, zeros, sz)
				if pt == 0x80 && sz == 7 {
					if any {
						t.Errorf("zeros: unexpected return value %v, expected false", any)
					}
				} else {
					if !any {
						t.Errorf("zeros: unexpected return value %v, expected true", any)
					}
				}
				if pt == 0xff {
					if !all {
						t.Errorf("zeros: unexpected return value %v, expected true", all)
					}
				} else {
					if all {
						t.Errorf("zeros: unexpected return value %v, expected 0", all)
					}
				}
				if bytes.Compare(dst, src) != 0 {
					t.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					t.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// all ones
				copy(dst, src)
				any, all = OrFlag(dst, ones, sz)
				if !any {
					t.Errorf("ones: unexpected return value %v, expected true", any)
				}
				if !all {
					t.Errorf("ones: unexpected return value %v, expected true", all)
				}
				if bytes.Compare(dst, ones) != 0 {
					t.Errorf("ones: unexpected result %x, expected %x", dst, ones)
				}
				if got, want := popcount(dst), popcount(ones); got != want {
					t.Errorf("ones: unexpected count %d, expected %d", got, want)
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
				Xor(dst, dst, sz)
				if bytes.Compare(dst, zeros) != 0 {
					t.Errorf("dst===src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					t.Errorf("dst===src: unexpected count %d, expected %d", got, want)
				}

				// same value, other slice
				copy(dst, src)
				Xor(dst, src, sz)
				if bytes.Compare(dst, zeros) != 0 {
					t.Errorf("dst==src: unexpected result %x, expected %x", dst, zeros)
				}
				if got, want := popcount(dst), 0; got != want {
					t.Errorf("dst==src: unexpected count %d, expected %d", got, want)
				}

				// val XOR zeros == val
				copy(dst, src)
				Xor(dst, zeros, sz)
				if bytes.Compare(dst, src) != 0 {
					t.Errorf("zeros: unexpected result %x, expected %x", dst, src)
				}
				if got, want := popcount(dst), popcount(src); got != want {
					t.Errorf("zeros: unexpected count %d, expected %d", got, want)
				}

				// val XOR ones == neg(val)
				copy(dst, src)
				Xor(dst, ones, sz)
				cmp := fillBitset(nil, sz, ^pt)
				if bytes.Compare(dst, cmp) != 0 {
					t.Errorf("ones: unexpected result %x, expected %x", dst, cmp)
				}
				if got, want := popcount(dst), popcount(cmp); got != want {
					t.Errorf("ones: unexpected count %d, expected %d", got, want)
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

				Neg(src, sz)
				if bytes.Compare(src, cmp) != 0 {
					t.Errorf("unexpected result %x, expected %x", src, cmp)
				}
				if got, want := popcount(src), popcount(cmp); got != want {
					t.Errorf("unexpected count %d, expected %d", got, want)
				}
			})
		}
	}
}

func TestBitsetIndexGenericSkip64(T *testing.T) {
	for _, c := range runTestcases {
		idx := make([]uint32, len(c.Idx))
		T.Run(c.Name, func(t *testing.T) {
			var ret = Indexes(c.Buf, c.Size, idx)
			if got, want := ret, popcount(c.Buf); got != want {
				t.Errorf("unexpected index vector length %d, expected %d", got, want)
			}
			if got, want := ret, len(c.Idx); got != want {
				t.Errorf("unexpected return value %d, expected %d", got, want)
			}
			if !reflect.DeepEqual(idx, c.Idx) {
				t.Errorf("unexpected result %d, expected %d", idx, c.Idx)
			}
		})
	}
}

func TestBitsetRunGeneric(T *testing.T) {
	for _, c := range runTestcases {
		var idx, length int
		for i, r := range c.Runs {
			T.Run(f("%s_%d", c.Name, i), func(t *testing.T) {
				idx, length = Run(c.Buf, idx+length, c.Size)
				if got, want := idx, r[0]; got != want {
					t.Errorf("unexpected index %d, expected %d", got, want)
				}
				if got, want := length, r[1]; got != want {
					t.Errorf("unexpected length %d, expected %d", got, want)
				}
			})
		}
	}
}
