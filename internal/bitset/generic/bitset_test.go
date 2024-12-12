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

// Test low-level slice handlers
func TestBitsetPopCountGeneric(t *testing.T) {
	for _, c := range popCases {
		cnt := PopCount(c.Source, c.Size)
		if got, want := int(cnt), c.Count; got != want {
			t.Errorf("%s: unexpected count %d, expected %d", c.Name, got, want)
		}
	}
	for _, sz := range bitsetSizes {
		for _, pt := range bitsetPatterns {
			n := f("%d_%x", sz, pt)
			buf := fillBitset(nil, sz, pt)
			if got, want := int(PopCount(buf, sz)), popcount(buf); got != want {
				t.Errorf("%s: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestBitAndGeneric(t *testing.T) {
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			n := f("%d_%x", sz, pt)
			src := fillBitset(nil, sz, pt)
			dst := fillBitset(nil, sz, pt)

			// same value, same slice
			And(dst, dst, sz)
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: dst===src: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				t.Errorf("%s: dst===src: unexpected count %d, expected %d", n, got, want)
			}

			// same value, other slice
			copy(dst, src)
			And(dst, src, sz)
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: dst==src: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				t.Errorf("%s: dst==src: unexpected count %d, expected %d", n, got, want)
			}

			// all zeros
			copy(dst, src)
			And(dst, zeros, sz)
			if !bytes.Equal(dst, zeros) {
				t.Errorf("%s: zeros: unexpected result %x, expected %x", n, dst, zeros)
			}
			if got, want := popcount(dst), 0; got != want {
				t.Errorf("%s: zeros: unexpected count %d, expected %d", n, got, want)
			}

			// all ones
			copy(dst, src)
			And(dst, ones, sz)
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: ones: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				t.Errorf("%s: ones: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestBitAndGenericFlag(t *testing.T) {
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			n := f("%d_%x", sz, pt)
			src := fillBitset(nil, sz, pt)
			dst := fillBitset(nil, sz, pt)

			// same value, same slice
			any, all := AndFlag(dst, dst, sz)
			if pt == 0x80 && sz == 7 {
				if any {
					t.Errorf("%s: dst===src: unexpected return value %v, expected false", n, any)
				}
			} else {
				if !any {
					t.Errorf("%s: dst===src: unexpected return value %v, expected true", n, any)
				}
			}
			if pt == 0xff {
				if !all {
					t.Errorf("%s: dst===src: unexpected return value %v, expected true", n, all)
				}
			} else {
				if all {
					t.Errorf("%s: dst===src: unexpected return value %v, expected false", n, all)
				}
			}
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: dst===src: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				t.Errorf("%s: dst===src: unexpected count %d, expected %d", n, got, want)
			}

			// same value, other slice
			copy(dst, src)
			any, all = AndFlag(dst, src, sz)
			if pt == 0x80 && sz == 7 {
				if any {
					t.Errorf("%s: dst==src: unexpected return value %v, expected false", n, any)
				}
			} else {
				if !any {
					t.Errorf("%s: dst==src: unexpected return value %v, expected true", n, any)
				}
			}
			if pt == 0xff {
				if !all {
					t.Errorf("%s: dst==src: unexpected return value %v, expected true", n, all)
				}
			} else {
				if all {
					t.Errorf("%s: dst==src: unexpected return value %v, expected false", n, all)
				}
			}
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: dst==src: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				t.Errorf("%s: dst==src: unexpected count %d, expected %d", n, got, want)
			}

			// all zeros
			copy(dst, src)
			any, all = AndFlag(dst, zeros, sz)
			if any {
				t.Errorf("%s: zeros: unexpected return value %v, expected false", n, any)
			}
			if all {
				t.Errorf("%s: zeros: unexpected return value %v, expected false", n, all)
			}
			if !bytes.Equal(dst, zeros) {
				t.Errorf("%s: zeros: unexpected result %x, expected %x", n, dst, zeros)
			}
			if got, want := popcount(dst), 0; got != want {
				t.Errorf("%s: zeros: unexpected count %d, expected %d", n, got, want)
			}

			// all ones
			copy(dst, src)
			any, all = AndFlag(dst, ones, sz)
			if pt == 0x80 && sz == 7 {
				if any {
					t.Errorf("%s: ones: unexpected return value %v, expected false", n, any)
				}
			} else {
				if !any {
					t.Errorf("%s: ones: unexpected return value %v, expected true", n, any)
				}
			}
			if pt == 0xff {
				if !all {
					t.Errorf("%s: ones: unexpected return value %v, expected true", n, all)
				}
			} else {
				if all {
					t.Errorf("%s: ones: unexpected return value %v, expected 0", n, all)
				}
			}
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: ones: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				t.Errorf("%s: ones: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestBitAndNotGeneric(t *testing.T) {
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			n := f("%d_%x", sz, pt)
			src := fillBitset(nil, sz, pt)
			dst := make([]byte, len(src))

			// same value, same slice
			AndNot(dst, dst, sz)
			if !bytes.Equal(dst, zeros) {
				t.Errorf("%s: dst===src: unexpected result %x, expected %x", n, dst, zeros)
			}
			if got, want := popcount(dst), 0; got != want {
				t.Errorf("%s: dst===src: unexpected count %d, expected %d", n, got, want)
			}

			// same value, other slice
			copy(dst, src)
			AndNot(dst, src, sz)
			if !bytes.Equal(dst, zeros) {
				t.Errorf("%s: dst==src: unexpected result %x, expected %x", n, dst, zeros)
			}
			if got, want := popcount(dst), 0; got != want {
				t.Errorf("%s: dst==src: unexpected count %d, expected %d", n, got, want)
			}

			// val AND NOT zeros == val
			copy(dst, src)
			AndNot(dst, zeros, sz)
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: zeros: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				t.Errorf("%s: zeros: unexpected count %d, expected %d", n, got, want)
			}

			// all AND NOT ones == zero
			copy(dst, src)
			AndNot(dst, ones, sz)
			if !bytes.Equal(dst, zeros) {
				t.Errorf("%s: ones: unexpected result %x, expected %x", n, dst, zeros)
			}
			if got, want := popcount(dst), 0; got != want {
				t.Errorf("%s: ones: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestBitOrGeneric(t *testing.T) {
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			n := f("%d_%x", sz, pt)
			src := fillBitset(nil, sz, pt)
			dst := fillBitset(nil, sz, pt)

			// same value, same slice
			Or(dst, dst, sz)
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: dst===src: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				t.Errorf("%s: dst===src: unexpected count %d, expected %d", n, got, want)
			}

			// same value, other slice
			copy(dst, src)
			Or(dst, src, sz)
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: dst==src: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				t.Errorf("%s: dst==src: unexpected count %d, expected %d", n, got, want)
			}

			// val OR zeros == val
			copy(dst, src)
			Or(dst, zeros, sz)
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: zeros: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				t.Errorf("%s: zeros: unexpected count %d, expected %d", n, got, want)
			}

			// all OR ones == ones
			copy(dst, src)
			Or(dst, ones, sz)
			if !bytes.Equal(dst, ones) {
				t.Errorf("%s: ones: unexpected result %x, expected %x", n, dst, ones)
			}
			if got, want := popcount(dst), popcount(ones); got != want {
				t.Errorf("%s: ones: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestBitOrGenericFlag(t *testing.T) {
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			n := f("%d_%x", sz, pt)
			src := fillBitset(nil, sz, pt)
			dst := fillBitset(nil, sz, pt)

			// same value, same slice
			any, all := OrFlag(dst, dst, sz)
			if pt == 0x80 && sz == 7 {
				if any {
					t.Errorf("%s: dst===src: unexpected return value %v, expected false", n, any)
				}
			} else {
				if !any {
					t.Errorf("%s: dst===src: unexpected return value %v, expected true", n, any)
				}
			}
			if pt == 0xff {
				if !all {
					t.Errorf("%s: dst===src: unexpected return value %v, expected true", n, all)
				}
			} else {
				if all {
					t.Errorf("%s: dst===src: unexpected return value %v, expected false", n, all)
				}
			}
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: dst===src: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				t.Errorf("%s: dst===src: unexpected count %d, expected %d", n, got, want)
			}

			// same value, other slice
			copy(dst, src)
			any, all = OrFlag(dst, src, sz)
			if pt == 0x80 && sz == 7 {
				if any {
					t.Errorf("%s: dst==src: unexpected return value %v, expected false", n, any)
				}
			} else {
				if !any {
					t.Errorf("%s: dst==src: unexpected return value %v, expected true", n, any)
				}
			}
			if pt == 0xff {
				if !all {
					t.Errorf("%s: dst==src: unexpected return value %v, expected true", n, all)
				}
			} else {
				if all {
					t.Errorf("%s: dst==src: unexpected return value %v, expected false", n, all)
				}
			}
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: dst==src: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				t.Errorf("%s: dst==src: unexpected count %d, expected %d", n, got, want)
			}

			// all zeros
			copy(dst, src)
			any, all = OrFlag(dst, zeros, sz)
			if pt == 0x80 && sz == 7 {
				if any {
					t.Errorf("%s: zeros: unexpected return value %v, expected false", n, any)
				}
			} else {
				if !any {
					t.Errorf("%s: zeros: unexpected return value %v, expected true", n, any)
				}
			}
			if pt == 0xff {
				if !all {
					t.Errorf("%s: zeros: unexpected return value %v, expected true", n, all)
				}
			} else {
				if all {
					t.Errorf("%s: zeros: unexpected return value %v, expected 0", n, all)
				}
			}
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: zeros: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				t.Errorf("%s: zeros: unexpected count %d, expected %d", n, got, want)
			}

			// all ones
			copy(dst, src)
			any, all = OrFlag(dst, ones, sz)
			if !any {
				t.Errorf("%s: ones: unexpected return value %v, expected true", n, any)
			}
			if !all {
				t.Errorf("%s: ones: unexpected return value %v, expected true", n, all)
			}
			if !bytes.Equal(dst, ones) {
				t.Errorf("%s: ones: unexpected result %x, expected %x", n, dst, ones)
			}
			if got, want := popcount(dst), popcount(ones); got != want {
				t.Errorf("%s: ones: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestBitXorGeneric(t *testing.T) {
	for _, sz := range bitsetSizes {
		zeros := fillBitset(nil, sz, 0)
		ones := fillBitset(nil, sz, 0xff)
		for _, pt := range bitsetPatterns {
			n := f("%d_%x", sz, pt)
			src := fillBitset(nil, sz, pt)
			dst := fillBitset(nil, sz, pt)

			// same value, same slice
			Xor(dst, dst, sz)
			if !bytes.Equal(dst, zeros) {
				t.Errorf("%s: dst===src: unexpected result %x, expected %x", n, dst, zeros)
			}
			if got, want := popcount(dst), 0; got != want {
				t.Errorf("%s: dst===src: unexpected count %d, expected %d", n, got, want)
			}

			// same value, other slice
			copy(dst, src)
			Xor(dst, src, sz)
			if !bytes.Equal(dst, zeros) {
				t.Errorf("%s: dst==src: unexpected result %x, expected %x", n, dst, zeros)
			}
			if got, want := popcount(dst), 0; got != want {
				t.Errorf("%s: dst==src: unexpected count %d, expected %d", n, got, want)
			}

			// val XOR zeros == val
			copy(dst, src)
			Xor(dst, zeros, sz)
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: zeros: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount(dst), popcount(src); got != want {
				t.Errorf("%s: zeros: unexpected count %d, expected %d", n, got, want)
			}

			// val XOR ones == neg(val)
			copy(dst, src)
			Xor(dst, ones, sz)
			cmp := fillBitset(nil, sz, ^pt)
			if !bytes.Equal(dst, cmp) {
				t.Errorf("%s: ones: unexpected result %x, expected %x", n, dst, cmp)
			}
			if got, want := popcount(dst), popcount(cmp); got != want {
				t.Errorf("%s: ones: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestBitNegGeneric(t *testing.T) {
	for _, sz := range bitsetSizes {
		for _, pt := range bitsetPatterns {
			n := f("%d_%x", sz, pt)
			src := fillBitset(nil, sz, pt)
			cmp := fillBitset(nil, sz, ^pt)

			Neg(src, sz)
			if !bytes.Equal(src, cmp) {
				t.Errorf("%s: unexpected result %x, expected %x", n, src, cmp)
			}
			if got, want := popcount(src), popcount(cmp); got != want {
				t.Errorf("%s: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestBitsetIndexGenericSkip64(t *testing.T) {
	for _, c := range runTestcases {
		idx := make([]uint32, len(c.Idx))
		var ret = Indexes(c.Buf, c.Size, idx)
		if got, want := ret, popcount(c.Buf); got != want {
			t.Errorf("%s: unexpected index vector length %d, expected %d", c.Name, got, want)
		}
		if got, want := ret, len(c.Idx); got != want {
			t.Errorf("%s: unexpected return value %d, expected %d", c.Name, got, want)
		}
		if !reflect.DeepEqual(idx, c.Idx) {
			t.Errorf("%s: unexpected result %d, expected %d", c.Name, idx, c.Idx)
		}
	}
}

func TestBitsetRunGeneric(t *testing.T) {
	for _, c := range runTestcases {
		var idx, length int
		for i, r := range c.Runs {
			n := f("%s_%d", c.Name, i)
			idx, length = Run(c.Buf, idx+length, c.Size)
			if got, want := idx, r[0]; got != want {
				t.Errorf("%s: unexpected index %d, expected %d", n, got, want)
			}
			if got, want := length, r[1]; got != want {
				t.Errorf("%s: unexpected length %d, expected %d", n, got, want)
			}
		}
	}
}
