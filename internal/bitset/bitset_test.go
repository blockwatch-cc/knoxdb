// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package bitset

import (
	"bytes"
	"encoding/binary"
	"math/rand"
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

// Test high-level bitset API
func TestBitsetNew(T *testing.T) {
	for _, c := range popCases {
		T.Run(c.Name, func(t *testing.T) {
			bits := NewBitset(c.Size)
			if got, want := len(bits.Bytes()), len(c.Source); got != want {
				T.Errorf("unexpected buf length %d, expected %d", got, want)
			}
			if got, want := bits.Len(), c.Size; got != want {
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
	for _, c := range popCases {
		T.Run(c.Name, func(t *testing.T) {
			bits := NewBitsetFromBytes(c.Source, c.Size)
			if got, want := len(bits.Bytes()), len(c.Source); got != want {
				T.Errorf("unexpected buf length %d, expected %d", got, want)
			}
			if got, want := bits.Len(), c.Size; got != want {
				T.Errorf("unexpected size %d, expected %d", got, want)
			}
			if got, want := bits.Count(), c.Count; got != want {
				T.Errorf("unexpected count %d, expected %d", got, want)
			}
			if bytes.Compare(bits.Bytes(), c.Result) != 0 {
				T.Errorf("unexpected result %x, expected %x", bits.Bytes(), c.Source)
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
	for _, c := range popCases {
		T.Run(c.Name, func(t *testing.T) {
			bits := NewBitsetFromBytes(c.Source, c.Size)
			bits.Zero()
			if got, want := len(bits.Bytes()), len(c.Source); got != want {
				T.Errorf("unexpected buf length %d, expected %d", got, want)
			}
			if got, want := bits.Len(), c.Size; got != want {
				T.Errorf("unexpected size %d, expected %d", got, want)
			}
			if got, want := bits.Count(), 0; got != want {
				T.Errorf("unexpected count %d, expected %d", got, want)
			}
			buf := bytes.Repeat([]byte{0}, bitFieldLen(c.Size))
			if bytes.Compare(bits.Bytes(), buf) != 0 {
				T.Errorf("unexpected result %x, expected %x", bits.Bytes(), buf)
			}
		})
	}
}

func TestBitsetResize(T *testing.T) {
	for _, sz := range bitsetSizes {
		for _, sznew := range bitsetSizes {
			T.Run(f("%d_%d", sz, sznew), func(t *testing.T) {
				bits := NewBitset(sz)
				bits.One()
				bits.Resize(sznew)
				if got, want := len(bits.Bytes()), bitFieldLen(sznew); got != want {
					T.Errorf("unexpected buf length %d, expected %d", got, want)
					T.FailNow()
				}
				if got, want := bits.Len(), sznew; got != want {
					T.Errorf("unexpected size %d, expected %d", got, want)
					T.FailNow()
				}
				if got, want := bits.Count(), util.Min(sz, sznew); got != want {
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
				buf := bytes.Repeat([]byte{0xff}, util.Min(lena, lenb))
				buf[len(buf)-1] &= byte(0xff >> (7 - uint(util.Min(sz, sznew)-1)&0x7))
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
		T.Run(f("%d_resize_0", sz), func(t *testing.T) {
			bits := NewBitset(sz)
			bits.One()
			bits.Resize(0)
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
		T.Run(f("%d_resize+1", sz), func(t *testing.T) {
			bits := NewBitset(sz)
			bits.One()
			bits.Resize(bits.Len() + 1)
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
					srcSlice := src.SubSlice(srcPos, util.Min(srcLen, dst.Len()-dstPos))
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
