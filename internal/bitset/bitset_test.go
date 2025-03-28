// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package bitset

import (
	"bytes"
	"encoding/binary"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset/tests"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	bitsetPatterns = tests.Patterns
	popcount       = tests.Popcount
	fillBitset     = tests.FillBitset
	fillBitsetRand = tests.FillBitsetRand
	popCases       = tests.PopCases
	f              = tests.F
)

var bitsetSizes = []int{
	// only non-avx sizes
	7, 8, 9, 15, 16, 17, 23, 24, 25, 31, 32, 33,
	63, 64, 65, 127,
}

func checkCleanTail(t *testing.T, name string, buf []byte) {
	tail := len(buf)
	buf = buf[:cap(buf)]
	for i := range buf[tail:] {
		if buf[tail+i] != 0 {
			t.Errorf("%s: unclean memory %x at pos %d+%d: %x", name, buf[i], tail, i, buf)
			t.FailNow()
		}
	}
}

// Test high-level bitset API
func TestBitsetNew(t *testing.T) {
	for _, c := range popCases {
		bits := NewBitset(c.Size)
		if got, want := len(bits.Bytes()), len(c.Source); got != want {
			t.Errorf("%s: unexpected buf length %d, expected %d", c.Name, got, want)
		}
		if got, want := bits.Len(), c.Size; got != want {
			t.Errorf("%s: unexpected size %d, expected %d", c.Name, got, want)
		}
		if got, want := bits.Count(), 0; got != want {
			t.Errorf("%s: unexpected count %d, expected %d", c.Name, got, want)
		}
		checkCleanTail(t, c.Name, bits.Bytes())
	}
}

func TestBitsetFromBytes(t *testing.T) {
	for _, c := range popCases {
		bits := FromBuffer(c.Source, c.Size)
		if got, want := len(bits.Bytes()), len(c.Source); got != want {
			t.Errorf("%s: unexpected buf length %d, expected %d", c.Name, got, want)
		}
		if got, want := bits.Len(), c.Size; got != want {
			t.Errorf("%s: unexpected size %d, expected %d", c.Name, got, want)
		}
		if got, want := bits.Count(), c.Count; got != want {
			t.Errorf("%s: unexpected count %d, expected %d", c.Name, got, want)
		}
		if !bytes.Equal(bits.Bytes(), c.Result) {
			t.Errorf("%s: unexpected result %x, expected %x", c.Name, bits.Bytes(), c.Result)
		}
	}
}

func TestBitsetOne(t *testing.T) {
	for _, sz := range bitsetSizes {
		n := f("%d", sz)
		bits := NewBitset(sz)
		bits.One()
		if got, want := len(bits.Bytes()), bitFieldLen(sz); got != want {
			t.Errorf("%s: unexpected buf length %d, expected %d", n, got, want)
		}
		if got, want := bits.Len(), sz; got != want {
			t.Errorf("%s: unexpected size %d, expected %d", n, got, want)
		}
		if got, want := bits.Count(), sz; got != want {
			t.Errorf("%s: unexpected count %d, expected %d", n, got, want)
		}
		buf := bytes.Repeat([]byte{0xff}, bitFieldLen(sz)-1)
		buf = append(buf, byte(0xff>>((8-uint(sz)&0x7)&0x7)&0xff))
		if !bytes.Equal(bits.Bytes(), buf) {
			t.Errorf("%s: unexpected result %x, expected %x", n, bits.Bytes(), buf)
		}
	}
}

func TestBitsetZero(t *testing.T) {
	for _, c := range popCases {
		bits := FromBuffer(c.Source, c.Size)
		bits.Zero()
		if got, want := len(bits.Bytes()), len(c.Source); got != want {
			t.Errorf("%s: unexpected buf length %d, expected %d", c.Name, got, want)
		}
		if got, want := bits.Len(), c.Size; got != want {
			t.Errorf("%s: unexpected size %d, expected %d", c.Name, got, want)
		}
		if got, want := bits.Count(), 0; got != want {
			t.Errorf("%s: unexpected count %d, expected %d", c.Name, got, want)
		}
		buf := bytes.Repeat([]byte{0}, bitFieldLen(c.Size))
		if !bytes.Equal(bits.Bytes(), buf) {
			t.Errorf("%s: unexpected result %x, expected %x", c.Name, bits.Bytes(), buf)
		}
	}
}

func TestBitsetResize(t *testing.T) {
	for _, sz := range bitsetSizes {
		for _, sznew := range bitsetSizes {
			n := f("%d_%d", sz, sznew)
			bits := NewBitset(sz)
			bits.One()
			bits.Resize(sznew)
			if got, want := len(bits.Bytes()), bitFieldLen(sznew); got != want {
				t.Errorf("%s: unexpected buf length %d, expected %d", n, got, want)
				t.FailNow()
			}
			if got, want := bits.Len(), sznew; got != want {
				t.Errorf("%s: unexpected size %d, expected %d", n, got, want)
				t.FailNow()
			}
			if got, want := bits.Count(), util.Min(sz, sznew); got != want {
				t.Errorf("%s: unexpected count %d, expected %d", n, got, want)
				t.FailNow()
			}
			if got, want := bits.Count(), popcount(bits.Bytes()); got != want {
				t.Errorf("%s: unexpected real count %d, expected %d", n, got, want)
				t.FailNow()
			}
			lena := bitFieldLen(sz)
			lenb := bitFieldLen(sznew)
			diff := lena - lenb
			buf := bytes.Repeat([]byte{0xff}, util.Min(lena, lenb))
			buf[len(buf)-1] &= byte(0xff >> (7 - uint(util.Min(sz, sznew)-1)&0x7))
			if diff < 0 {
				buf = append(buf, bytes.Repeat([]byte{0x0}, -diff)...)
			}
			if !bytes.Equal(bits.Bytes(), buf) {
				t.Errorf("%s: unexpected result %x, expected %x", n, bits.Bytes(), buf)
				t.FailNow()
			}
			checkCleanTail(t, n, bits.Bytes())
		}
	}
	// clear/reset bitset to zero
	for _, sz := range bitsetSizes {
		n := f("%d_resize_0", sz)
		bits := NewBitset(sz)
		bits.One()
		bits.Resize(0)
		if got, want := len(bits.Bytes()), 0; got != want {
			t.Errorf("%s: unexpected buf length %d, expected %d", n, got, want)
			t.FailNow()
		}
		if got, want := bits.Len(), 0; got != want {
			t.Errorf("%s: unexpected size %d, expected %d", n, got, want)
			t.FailNow()
		}
		if got, want := bits.Count(), 0; got != want {
			t.Errorf("%s: unexpected count %d, expected %d", n, got, want)
			t.FailNow()
		}
		checkCleanTail(t, n, bits.Bytes())
	}
	// grow + 1
	for _, sz := range bitsetSizes {
		n := f("%d_resize+1", sz)
		bits := NewBitset(sz)
		bits.One()
		bits.Resize(bits.Len() + 1)
		bits.Set(bits.Len() - 1)
		if got, want := len(bits.Bytes()), bitFieldLen(sz+1); got != want {
			t.Errorf("%s: unexpected buf length %d, expected %d", n, got, want)
			t.FailNow()
		}
		if got, want := bits.Len(), sz+1; got != want {
			t.Errorf("%s: unexpected size %d, expected %d", n, got, want)
			t.FailNow()
		}
		if got, want := bits.Count(), sz+1; got != want {
			t.Errorf("%s: unexpected count %d, expected %d", n, got, want)
			t.FailNow()
		}
		if got, want := bits.Count(), popcount(bits.Bytes()); got != want {
			t.Errorf("%s: unexpected real count %d, expected %d", n, got, want)
			t.FailNow()
		}
		checkCleanTail(t, n, bits.Bytes())
	}
}

func TestBitsetFill(t *testing.T) {
	for _, sz := range bitsetSizes {
		for _, pt := range bitsetPatterns {
			cmp := fillBitset(nil, sz, pt)
			bits := NewBitset(sz)
			bits.Fill(pt)
			n := f("%d_%x", sz, pt)
			if got, want := len(bits.Bytes()), bitFieldLen(sz); got != want {
				t.Errorf("%s: unexpected buf length %d, expected %d", n, got, want)
			}
			if got, want := bits.Len(), sz; got != want {
				t.Errorf("%s: unexpected size %d, expected %d", n, got, want)
			}
			if got, want := bits.Count(), popcount(cmp); got != want {
				t.Errorf("%s: unexpected count %d, expected %d", n, got, want)
			}
			if !bytes.Equal(bits.Bytes(), cmp) {
				t.Errorf("%s: unexpected result %x, expected %x", n, bits.Bytes(), cmp)
			}
		}
	}
}

func TestBitsetSet(t *testing.T) {
	for _, sz := range bitsetSizes {
		n := f("%d", sz)
		bits := NewBitset(sz)
		cmp := fillBitset(nil, sz, 0)

		// set first bit
		bits.Set(0)
		cmp[0] |= 0x01
		if got, want := bits.Count(), 1; got != want {
			t.Errorf("%s: unexpected count %d, expected %d", n, got, want)
		}
		if !bits.IsSet(0) {
			t.Errorf("%s: unexpected IsSet=false", n)
		}
		if !bytes.Equal(bits.Bytes(), cmp) {
			t.Errorf("%s: unexpected result %x, expected %x", n, bits.Bytes(), cmp)
		}

		// set last bit
		bits.Set(sz - 1)
		cmp[(sz-1)>>3] |= 1 << uint((sz-1)&0x7)
		if got, want := bits.Count(), 2; got != want {
			t.Errorf("%s: unexpected count %d, expected %d", n, got, want)
		}
		if !bits.IsSet(sz - 1) {
			t.Errorf("%s: unexpected IsSet=false", n)
		}
		if !bytes.Equal(bits.Bytes(), cmp) {
			t.Errorf("%s: unexpected result %x, expected %x", n, bits.Bytes(), cmp)
		}

		// set invalid bit
		bits.Set(-1)
		if got, want := bits.Count(), 2; got != want {
			t.Errorf("%s: unexpected count %d, expected %d", n, got, want)
		}
		if bits.IsSet(-1) {
			t.Errorf("%s: unexpected IsSet=true", n)
		}
		if !bytes.Equal(bits.Bytes(), cmp) {
			t.Errorf("%s: unexpected result %x, expected %x", n, bits.Bytes(), cmp)
		}

		bits.Set(sz)
		if got, want := bits.Count(), 2; got != want {
			t.Errorf("%s: unexpected count %d, expected %d", n, got, want)
		}
		if bits.IsSet(sz) {
			t.Errorf("%s: unexpected IsSet=true", n)
		}
		if !bytes.Equal(bits.Bytes(), cmp) {
			t.Errorf("%s: unexpected result %x, expected %x", n, bits.Bytes(), cmp)
		}
		checkCleanTail(t, n, bits.Bytes())
	}
}

func TestBitsetClear(t *testing.T) {
	for _, sz := range bitsetSizes {
		n := f("%d", sz)
		bits := NewBitset(sz)
		bits.One()
		cmp := fillBitset(nil, sz, 0xff)

		// clear first bit
		bits.Clear(0)
		cmp[0] &= 0xfe
		if got, want := bits.Count(), popcount(cmp); got != want {
			t.Errorf("%s: first: unexpected count %d, expected %d", n, got, want)
		}
		if bits.IsSet(0) {
			t.Errorf("%s: unexpected IsSet=true", n)
		}
		if !bytes.Equal(bits.Bytes(), cmp) {
			t.Errorf("%s: first: unexpected result %x, expected %x", n, bits.Bytes(), cmp)
		}

		// clear last bit
		bits.Clear(sz - 1)
		cmp[(sz-1)>>3] &^= 1 << uint((sz-1)&0x7)
		if got, want := bits.Count(), popcount(cmp); got != want {
			t.Errorf("%s: last: unexpected count %d, expected %d", n, got, want)
		}
		if bits.IsSet(sz - 1) {
			t.Errorf("%s: unexpected IsSet=true", n)
		}
		if !bytes.Equal(bits.Bytes(), cmp) {
			t.Errorf("%s: last: unexpected result %x, expected %x", n, bits.Bytes(), cmp)
		}

		// clear invalid bit
		bits.Clear(-1)
		if got, want := bits.Count(), popcount(cmp); got != want {
			t.Errorf("%s: invalid-: unexpected count %d, expected %d", n, got, want)
		}
		if bits.IsSet(-1) {
			t.Errorf("%s: unexpected IsSet=true", n)
		}
		if !bytes.Equal(bits.Bytes(), cmp) {
			t.Errorf("%s: invalid-: unexpected result %x, expected %x", n, bits.Bytes(), cmp)
		}

		bits.Clear(sz)
		if got, want := bits.Count(), popcount(cmp); got != want {
			t.Errorf("%s: invalid+: unexpected count %d, expected %d", n, got, want)
		}
		if bits.IsSet(sz) {
			t.Errorf("%s: unexpected IsSet=true", n)
		}
		if !bytes.Equal(bits.Bytes(), cmp) {
			t.Errorf("%s: invalid+: unexpected result %x, expected %x", n, bits.Bytes(), cmp)
		}
	}
}

// bits generates sequence of n numbers with max bits,
// ensuring max bit is set for 50% of the values.
func randBits(n int) []byte {
	c := (n + 7) / 8
	out := make([]byte, c+3)
	for i := 0; i < (c+3)/4; i++ {
		binary.BigEndian.PutUint32(out[4*i:4*i+4], util.RandUint32())
	}
	return out[:c]
}

func randBitsets(sz int) []*Bitset {
	res := make([]*Bitset, 100)
	for i := range res {
		res[i] = NewBitset(sz).SetFromBytes(randBits(sz), sz, false)
	}
	return res
}

func TestBitsetSlice(t *testing.T) {
	for _, sz := range bitsetSizes {
		for i, b := range randBitsets(sz) {
			n := f("%d_%d", sz, i)
			slice := b.Slice()
			if got, want := len(slice), sz; got != want {
				t.Errorf("%s: unexpected length %d, expected %d", n, got, want)
				// t.FailNow()
			}
			for k, v := range slice {
				if got, want := v, b.IsSet(k); got != want {
					t.Errorf("%s: unexpected bit %d: got %t, expected %t", n, k, got, want)
					t.FailNow()
				}
			}
		}
	}
}

func TestBitsetSubSlice(t *testing.T) {
	for _, sz := range bitsetSizes {
		for i, b := range randBitsets(sz) {
			name := f("%d_%d", sz, i)
			start := int(util.RandInt32n(int32(b.Len())))
			n := int(util.RandInt32n(int32(b.Len() - start)))
			slice := b.SubSlice(start, n)
			if got, want := len(slice), n; got != want {
				t.Errorf("%s: unexpected length %d, expected %d", name, got, want)
				t.FailNow()
			}
			for k, v := range slice {
				if got, want := v, b.IsSet(start+k); got != want {
					t.Errorf("%s: unexpected bit %d: got %t, expected %t", name, k, got, want)
					t.FailNow()
				}
			}

		}
	}
}

// TODO: edge cases
// - dstPos < 0
// - srcPos + srcLen > size
func TestBitsetInsert(t *testing.T) {
	var fast, fasthead, slow int
	for _, sz := range bitsetSizes {
		for i, src := range randBitsets(sz) {
			dst := NewBitset(1024)
			for _, pat := range bitsetPatterns {
				name := f("%d_%d_%x", sz, i, pat)
				dst.Fill(pat)
				srcPos := int(util.RandInt32n(int32(src.Len())))
				srcLen := int(util.RandInt32n(int32(src.Len() - srcPos)))
				dstPos := int(util.RandInt32n(int32(dst.Len())))

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
				dst.InsertFrom(src, srcPos, srcLen, dstPos)

				dstSlice := dst.SubSlice(dstPos, srcLen)
				srcSlice := src.SubSlice(srcPos, srcLen)
				var srcSet int
				for i := range srcSlice {
					if srcSlice[i] {
						srcSet++
					}
				}

				// T.Logf("SRC=%x DST=%x srcPos=%d dstPos=%d n=%d srcBits=%d\n",
				// src.Bytes(), dst.Bytes(), srcPos, dstPos, srcLen, srcSet)
				if got, want := lbefore+srcLen, dst.Len(); got != want {
					t.Errorf("%s: unexpected dst bitset len %d, expected %d", name, got, want)
					t.FailNow()
				}
				if got, want := dst.Count(), cbefore+srcSet; got != want {
					t.Errorf("%s: unexpected count %d, expected %d", name, got, want)
					t.FailNow()
				}
				if got, want := dst.Count(), popcount(dst.Bytes()); got != want {
					t.Errorf("%s: unexpected real count %d, expected %d", name, got, want)
					t.FailNow()
				}
				if got, want := len(dstSlice), len(srcSlice); got != want {
					t.Errorf("%s: unexpected []bool size %d, expected %d", name, got, want)
					t.FailNow()
				}
				for j := range dstSlice {
					if got, want := dstSlice[j], srcSlice[j]; got != want {
						t.Errorf("%s: unexpected bit %d: %t, expected %t", name, j, got, want)
						t.FailNow()
					}
				}
			}
		}
	}
	if fast == 0 || fasthead == 0 {
		t.Errorf("%d slow, %d fast, %d fast head/tail path hits – try increasing random sample size\n", slow, fast, fasthead)
	}
}

// TODO: edge cases
func TestBitsetReplace(t *testing.T) {
	var fast, slow int
	for _, sz := range bitsetSizes {
		for i, src := range randBitsets(sz) {
			dst := NewBitset(sz)
			for _, pat := range bitsetPatterns {
				name := f("%d_%d_%x", sz, i, pat)
				dst.Fill(pat)
				srcPos := int(util.RandInt32n(int32(src.Len())))
				srcLen := int(util.RandInt32n(int32(src.Len() - srcPos)))
				dstPos := int(util.RandInt32n(int32(dst.Len())))

				if srcPos&0x7+dstPos&0x7+srcLen&0x7 == 0 {
					fast++
				} else {
					slow++
				}

				lbefore := dst.Len()
				dst.ReplaceFrom(src, srcPos, srcLen, dstPos)

				dstSlice := dst.SubSlice(dstPos, srcLen)
				srcSlice := src.SubSlice(srcPos, util.Min(srcLen, dst.Len()-dstPos))
				// T.Logf("SRC=%x DST=%x srcPos=%d dstPos=%d n=%d\n",
				// 	src.Bytes(), dst.Bytes(), srcPos, dstPos, srcLen)
				if got, want := dst.Len(), lbefore; got != want {
					t.Errorf("%s: unexpected bitset len %d, expected %d", name, got, want)
					t.FailNow()
				}
				if got, want := len(dstSlice), len(srcSlice); got != want {
					t.Errorf("%s: unexpected []bool size %d, expected %d", name, got, want)
					t.FailNow()
				}
				for j := range dstSlice {
					if got, want := dstSlice[j], srcSlice[j]; got != want {
						t.Errorf("%s: unexpected bit %d: %t, expected %t", name, j, got, want)
						t.FailNow()
					}
				}
			}
		}
	}
	if fast == 0 {
		t.Errorf("%d slow, %d fast path hits – try increasing random sample size\n", slow, fast)
	}
}

func TestBitsetAppend(t *testing.T) {
	var fast, slow int
	for _, sz := range bitsetSizes {
		for i, src := range randBitsets(sz) {
			dst := NewBitset(sz)
			for _, pat := range bitsetPatterns {
				name := f("%d_%d_%x", sz, i, pat)
				dst.Fill(pat)
				srcPos := int(util.RandInt32n(int32(src.Len())))
				srcLen := int(util.RandInt32n(int32(src.Len() - srcPos)))

				if dst.size&0x7+srcPos&0x7+srcLen&0x7 == 0 {
					fast++
				} else {
					slow++
				}

				lbefore := dst.Len()
				cbefore := dst.Count()
				dst.AppendFrom(src, srcPos, srcLen)

				dstSlice := dst.SubSlice(lbefore, srcLen)
				srcSlice := src.SubSlice(srcPos, srcLen)
				var srcSet int
				for i := range srcSlice {
					if srcSlice[i] {
						srcSet++
					}
				}

				// T.Logf("SRC=%x DST=%x srcPos=%d dstPos=%d n=%d\n",
				// src.Bytes(), dst.Bytes(), srcPos, lbefore, srcLen)
				if got, want := lbefore+srcLen, dst.Len(); got != want {
					t.Errorf("%s: unexpected dst bitset len %d, expected %d", name, got, want)
					t.FailNow()
				}
				if got, want := dst.Count(), cbefore+srcSet; got != want {
					t.Errorf("%s: unexpected count %d, expected %d", name, got, want)
					t.FailNow()
				}
				if got, want := dst.Count(), popcount(dst.Bytes()); got != want {
					t.Errorf("%s: unexpected real count %d, expected %d", name, got, want)
					t.FailNow()
				}
				if got, want := len(dstSlice), len(srcSlice); got != want {
					t.Errorf("%s: unexpected []bool size %d, expected %d", name, got, want)
					t.FailNow()
				}
				for j := range dstSlice {
					if got, want := dstSlice[j], srcSlice[j]; got != want {
						t.Errorf("%s: unexpected bit %d: %t, expected %t", name, j, got, want)
						t.FailNow()
					}
				}
			}
		}
	}
	if fast == 0 {
		t.Errorf("%d slow, %d fast path hits – try increasing random sample size\n", slow, fast)
	}
}

func TestBitsetDelete(t *testing.T) {
	var fast, slow int
	for _, sz := range bitsetSizes {
		for i, src := range randBitsets(sz) {
			dst := NewBitset(sz)
			for _, pat := range bitsetPatterns {
				name := f("%d_%d_%x", sz, i, pat)
				// strategy:
				// - create a defined bitset with poison data
				// - insert random data (requires the insert test to succeed)
				// - delete the inserted data
				// - check original poison is unchanged
				dst.Fill(pat)
				srcPos := int(util.RandInt32n(int32(src.Len())))
				srcLen := int(util.RandInt32n(int32(src.Len() - srcPos)))
				dstPos := int(util.RandInt32n(int32(dst.Len())))

				if dstPos&0x7+srcLen&0x7 == 0 {
					fast++
				} else {
					slow++
				}

				before := dst.Clone()
				dst.InsertFrom(src, srcPos, srcLen, dstPos)
				dst.Delete(dstPos, srcLen)

				// T.Logf("BEFORE(%d/%d)=%x AFTER(%d/%d)=%x delPos=%d n=%d fast=%t\n",
				// 	before.Count(), before.Len(), before.Bytes(),
				// 	dst.Count(), dst.Len(), dst.Bytes(),
				// 	dstPos, srcLen,
				// 	dstPos&0x7+srcLen&0x7 == 0,
				// )
				if got, want := dst.Len(), before.Len(); got != want {
					t.Errorf("%s: unexpected dst bitset len %d, expected %d", name, got, want)
					t.FailNow()
				}
				if got, want := dst.Count(), before.Count(); got != want {
					t.Errorf("%s: unexpected count %d, expected %d", name, got, want)
					t.FailNow()
				}
				if got, want := dst.Count(), popcount(dst.Bytes()); got != want {
					t.Errorf("%s: unexpected real count %d, expected %d", name, got, want)
					t.FailNow()
				}
				if got, want := len(dst.Bytes()), len(before.Bytes()); got != want {
					t.Fatalf("%s: unexpected bitset buf len %d, expected %d", name, got, want)
					t.FailNow()
				}
				if !bytes.Equal(dst.Bytes(), before.Bytes()) {
					t.Fatalf("%s: unexpected memory contents %x, expected %x", name, dst.Bytes(), before.Bytes())
					t.FailNow()
				}
				checkCleanTail(t, name, dst.Bytes())
			}
		}
	}
	if fast == 0 {
		t.Errorf("%d slow, %d fast path hits – try increasing random sample size\n", slow, fast)
	}
}

func TestBitsetSwap(t *testing.T) {
	for _, sz := range bitsetSizes {
		for i, src := range randBitsets(sz) {
			name := f("%d_%d", sz, i)
			i := int(util.RandInt32n(int32(src.Len())))
			j := int(util.RandInt32n(int32(src.Len())))

			ibefore := src.IsSet(i)
			jbefore := src.IsSet(j)
			cbefore := src.Count()
			lbefore := src.Len()
			src.Swap(i, j)

			// t.Logf("SWAP(%d/%d)=%t/%t AFTER(%d/%d)=%t/%t cnt=%d len=%d\n",
			// 	i, j, ibefore, jbefore,
			// 	i, j, src.IsSet(i), src.IsSet(j),
			// 	cbefore, lbefore,
			// )

			if got, want := src.Len(), lbefore; got != want {
				t.Errorf("%s: unexpected bitset len %d, expected %d", name, got, want)
				t.FailNow()
			}
			if got, want := src.Count(), cbefore; got != want {
				t.Errorf("%s: unexpected count %d, expected %d", name, got, want)
				t.FailNow()
			}
			if got, want := src.Count(), popcount(src.Bytes()); got != want {
				t.Errorf("%s: unexpected real count %d, expected %d", name, got, want)
				t.FailNow()
			}
			if got, want := src.IsSet(j), ibefore; got != want {
				t.Fatalf("%s: unexpected bit i=%d: got %t, expected %t", name, i, got, want)
				t.FailNow()
			}
			if got, want := src.IsSet(i), jbefore; got != want {
				t.Fatalf("%s: unexpected bit j=%d: got %t, expected %t", name, j, got, want)
				t.FailNow()
			}
			checkCleanTail(t, name, src.Bytes())
		}
	}
}
