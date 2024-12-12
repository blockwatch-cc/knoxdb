// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx2

import (
	"bytes"
	"reflect"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset/generic"
	"blockwatch.cc/knoxdb/internal/bitset/tests"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	bitsetPatterns = tests.Patterns
	popcount_ref   = tests.Popcount
	fillBitset     = tests.FillBitset
	fillBitsetSaw  = tests.FillBitsetSaw
	fillBitsetRand = tests.FillBitsetRand
	popCases       = tests.PopCases
	runTestcases   = tests.RunTestcases
	f              = tests.F
)

// mostly AVX sizes
var bitsetSizes = []int{
	7, 8, 127, // some non-avx
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

func TestAndAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}

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
			if got, want := popcount_ref(dst), popcount_ref(src); got != want {
				t.Errorf("%s: dst===src: unexpected count %d, expected %d", n, got, want)
			}

			// same value, other slice
			copy(dst, src)
			And(dst, src, sz)
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: dst==src: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount_ref(dst), popcount_ref(src); got != want {
				t.Errorf("%s: dst==src: unexpected count %d, expected %d", n, got, want)
			}

			// all zeros
			copy(dst, src)
			And(dst, zeros, sz)
			if !bytes.Equal(dst, zeros) {
				t.Errorf("%s: zeros: unexpected result %x, expected %x", n, dst, zeros)
			}
			if got, want := popcount_ref(dst), 0; got != want {
				t.Errorf("%s: zeros: unexpected count %d, expected %d", n, got, want)
			}

			// all ones
			copy(dst, src)
			And(dst, ones, sz)
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: ones: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount_ref(dst), popcount_ref(src); got != want {
				t.Errorf("%s: ones: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestAndAVX2Flag(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
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
			if got, want := popcount_ref(dst), popcount_ref(src); got != want {
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
			if got, want := popcount_ref(dst), popcount_ref(src); got != want {
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
			if got, want := popcount_ref(dst), 0; got != want {
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
					t.Errorf("%s: ones: unexpected return value %v, expected false", n, all)
				}
			}
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: ones: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount_ref(dst), popcount_ref(src); got != want {
				t.Errorf("%s: ones: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestBitAndNotAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	// calls use the function selector to do proper last byte masking!
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
			if got, want := popcount_ref(dst), 0; got != want {
				t.Errorf("%s: dst===src: unexpected count %d, expected %d", n, got, want)
			}

			// same value, other slice
			copy(dst, src)
			AndNot(dst, src, sz)
			if !bytes.Equal(dst, zeros) {
				t.Errorf("%s: dst==src: unexpected result %x, expected %x", n, dst, zeros)
			}
			if got, want := popcount_ref(dst), 0; got != want {
				t.Errorf("%s: dst==src: unexpected count %d, expected %d", n, got, want)
			}

			// val AND NOT zeros == val
			copy(dst, src)
			AndNot(dst, zeros, sz)
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: zeros: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount_ref(dst), popcount_ref(src); got != want {
				t.Errorf("%s: zeros: unexpected count %d, expected %d", n, got, want)
			}

			// all AND NOT ones == zero
			copy(dst, src)
			AndNot(dst, ones, sz)
			if !bytes.Equal(dst, zeros) {
				t.Errorf("%s: ones: unexpected result %x, expected %x", n, dst, zeros)
			}
			if got, want := popcount_ref(dst), 0; got != want {
				t.Errorf("%s: ones: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestBitOrAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	// calls use the function selector to do proper last byte masking!
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
			if got, want := popcount_ref(dst), popcount_ref(src); got != want {
				t.Errorf("%s: dst===src: unexpected count %d, expected %d", n, got, want)
			}

			// same value, other slice
			copy(dst, src)
			Or(dst, src, sz)
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: dst==src: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount_ref(dst), popcount_ref(src); got != want {
				t.Errorf("%s: dst==src: unexpected count %d, expected %d", n, got, want)
			}

			// val OR zeros == val
			copy(dst, src)
			Or(dst, zeros, sz)
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: zeros: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount_ref(dst), popcount_ref(src); got != want {
				t.Errorf("%s: zeros: unexpected count %d, expected %d", n, got, want)
			}

			// all OR ones == ones
			copy(dst, src)
			Or(dst, ones, sz)
			if !bytes.Equal(dst, ones) {
				t.Errorf("%s: ones: unexpected result %x, expected %x", n, dst, ones)
			}
			if got, want := popcount_ref(dst), popcount_ref(ones); got != want {
				t.Errorf("%s: ones: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestBitOrAVX2Flag(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
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
			if got, want := popcount_ref(dst), popcount_ref(src); got != want {
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
			if got, want := popcount_ref(dst), popcount_ref(src); got != want {
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
			if got, want := popcount_ref(dst), popcount_ref(src); got != want {
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
			if got, want := popcount_ref(dst), popcount_ref(ones); got != want {
				t.Errorf("%s: ones: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestBitXorAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	// calls use the function selector to do proper last byte masking!
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
			if got, want := popcount_ref(dst), 0; got != want {
				t.Errorf("%s: dst===src: unexpected count %d, expected %d", n, got, want)
			}

			// same value, other slice
			copy(dst, src)
			Xor(dst, src, sz)
			if !bytes.Equal(dst, zeros) {
				t.Errorf("%s: dst==src: unexpected result %x, expected %x", n, dst, zeros)
			}
			if got, want := popcount_ref(dst), 0; got != want {
				t.Errorf("%s: dst==src: unexpected count %d, expected %d", n, got, want)
			}

			// val XOR zeros == val
			copy(dst, src)
			Xor(dst, zeros, sz)
			if !bytes.Equal(dst, src) {
				t.Errorf("%s: zeros: unexpected result %x, expected %x", n, dst, src)
			}
			if got, want := popcount_ref(dst), popcount_ref(src); got != want {
				t.Errorf("%s: zeros: unexpected count %d, expected %d", n, got, want)
			}

			// val XOR ones == neg(val)
			copy(dst, src)
			Xor(dst, ones, sz)
			cmp := fillBitset(nil, sz, ^pt)
			if !bytes.Equal(dst, cmp) {
				t.Errorf("%s: ones: unexpected result %x, expected %x", n, dst, cmp)
			}
			if got, want := popcount_ref(dst), popcount_ref(cmp); got != want {
				t.Errorf("%s: ones: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestBitNegAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	// calls use the function selector to do proper last byte masking!
	for _, sz := range bitsetSizes {
		for _, pt := range bitsetPatterns {
			n := f("%d_%x", sz, pt)
			src := fillBitset(nil, sz, pt)
			cmp := fillBitset(nil, sz, ^pt)

			Neg(src, sz)
			if !bytes.Equal(src, cmp) {
				t.Errorf("%s: unexpected result %x, expected %x", n, src, cmp)
			}
			if got, want := popcount_ref(src), popcount_ref(cmp); got != want {
				t.Errorf("%s: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestBitsetPopCountAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range popCases {
		// call the function selector to do proper last byte masking!
		cnt := PopCount(c.Source, c.Size)
		if got, want := int(cnt), c.Count; got != want {
			t.Errorf("%s: unexpected count %d, expected %d", c.Name, got, want)
		}
	}
	for _, sz := range bitsetSizes {
		for _, pt := range bitsetPatterns {
			n := f("%d_%x", sz, pt)
			buf := fillBitset(nil, sz, pt)
			// call the function selector to do proper last byte masking!
			if got, want := int(PopCount(buf, sz)), popcount_ref(buf); got != want {
				t.Errorf("%s: unexpected count %d, expected %d", n, got, want)
			}
		}
	}
}

func TestBitsetReverseAVX2(t *testing.T) {
	for _, sz := range bitsetSizes {
		bits := fillBitsetSaw(nil, sz)
		cmp := make([]byte, len(bits))
		copy(cmp, bits)
		generic.Reverse(cmp)
		Reverse(bits)

		if got, want := len(bits), len(cmp); got != want {
			t.Errorf("%d: unexpected buf length %d, expected %d", sz, got, want)
		}
		if got, want := popcount_ref(bits), popcount_ref(cmp); got != want {
			t.Errorf("%d: unexpected count %d, expected %d", sz, got, want)
		}
		if !bytes.Equal(bits, cmp) {
			t.Errorf("%d: unexpected result %x, expected %x", sz, bits, cmp)
		}
	}
}

func TestBitsetIndexAVX2Skip(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range runTestcases {
		c.Buf = append(c.Buf, 0xff)
		c.Buf = c.Buf[:len(c.Buf)-1]
		idx := make([]uint32, len(c.Idx)+8)
		var ret = Indexes(c.Buf, c.Size, idx)
		if got, want := ret, popcount_ref(c.Buf); got != want {
			t.Errorf("%s: unexpected index vector length %d, expected %d", c.Name, got, want)
		}
		idx = idx[:ret]
		if got, want := ret, len(c.Idx); got != want {
			t.Errorf("%s: unexpected return value %d, expected %d", c.Name, got, want)
		}
		if !reflect.DeepEqual(idx, c.Idx) {
			t.Errorf("%s: unexpected result %d, expected %d", c.Name, idx, c.Idx)
		}
	}
}

func TestBitsetRunAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
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
