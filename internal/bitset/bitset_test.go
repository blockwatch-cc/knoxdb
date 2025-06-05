// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package bitset

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"
	"testing/quick"

	"blockwatch.cc/knoxdb/internal/bitset/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	7, 8, 9, 15, 16, 17, 22, 23, 24, 25, 31, 32, 33,
	63, 64, 65, 127,
}

func checkCleanTail(t *testing.T, buf []byte) {
	t.Helper()
	tail := len(buf)
	buf = buf[:cap(buf)]
	for i := range buf[tail:] {
		assert.Equal(t, uint8(0), buf[tail+i], "unclean memory %x at pos %d+%d: %x", buf[i], tail, i, buf)
	}
}

// Test high-level bitset API
func TestBitsetNew(t *testing.T) {
	for _, c := range popCases {
		t.Run(c.Name, func(t *testing.T) {
			bits := New(c.Size)
			assert.Len(t, bits.Bytes(), len(c.Source), "length")
			assert.Equal(t, c.Size, bits.Len(), "size")
			assert.Equal(t, 0, bits.Count(), "count")
			assert.False(t, bits.Any(), "any")
			assert.False(t, bits.All(), "all")
			assert.True(t, bits.None(), "none")
			checkCleanTail(t, bits.Bytes())
		})
	}
}

func TestBitsetFromBytes(t *testing.T) {
	for _, c := range popCases {
		t.Run(c.Name, func(t *testing.T) {
			bits := NewFromBytes(c.Source, c.Size)
			assert.Len(t, bits.Bytes(), len(c.Source), "length")
			assert.Equal(t, c.Size, bits.Len(), "size")
			assert.Equal(t, c.Count, bits.Count(), "count")
			assert.Equal(t, c.Result, bits.Bytes(), "bytes")
		})
	}
}

func TestBitsetOne(t *testing.T) {
	for _, sz := range bitsetSizes {
		t.Run(f("sz_%d", sz), func(t *testing.T) {
			bits := New(sz)
			bits.One()
			assert.Len(t, bits.Bytes(), bitFieldLen(sz), "length")
			assert.Equal(t, sz, bits.Len(), "size")
			assert.Equal(t, sz, bits.Count(), "count")
			assert.True(t, bits.Any(), "any")
			assert.True(t, bits.All(), "all")
			assert.False(t, bits.None(), "none")
			buf := bytes.Repeat([]byte{0xff}, bitFieldLen(sz)-1)
			buf = append(buf, byte(0xff>>((8-uint(sz)&0x7)&0x7)&0xff))
			assert.Equal(t, buf, bits.Bytes(), "bytes")
		})
	}
}

func TestBitsetZero(t *testing.T) {
	for _, c := range popCases {
		t.Run(c.Name, func(t *testing.T) {
			bits := NewFromBytes(bytes.Clone(c.Source), c.Size)
			bits.Zero()
			assert.Len(t, bits.Bytes(), len(c.Source), "length")
			assert.Equal(t, c.Size, bits.Len(), "size")
			assert.Equal(t, 0, bits.Count(), "count")
			assert.False(t, bits.Any(), "any")
			assert.False(t, bits.All(), "all")
			assert.True(t, bits.None(), "none")
			buf := bytes.Repeat([]byte{0}, bitFieldLen(c.Size))
			assert.Equal(t, buf, bits.Bytes(), "bytes")
		})
	}
}

func TestBitsetResize(t *testing.T) {
	for _, sz := range bitsetSizes {
		for _, sznew := range bitsetSizes {
			t.Run(f("%d_to_%d", sz, sznew), func(t *testing.T) {
				bits := New(sz)
				bits.One()
				bits.Resize(sznew)

				assert.Len(t, bits.Bytes(), bitFieldLen(sznew), "length")
				assert.Equal(t, sznew, bits.Len(), "size")
				assert.Equal(t, min(sz, sznew), bits.Count(), "count")
				assert.Equal(t, popcount(bits.Bytes()), bits.Count(), "popcount")

				lena := bitFieldLen(sz)
				lenb := bitFieldLen(sznew)
				diff := lena - lenb
				buf := bytes.Repeat([]byte{0xff}, util.Min(lena, lenb))
				buf[len(buf)-1] &= byte(0xff >> (7 - uint(util.Min(sz, sznew)-1)&0x7))
				if diff < 0 {
					buf = append(buf, bytes.Repeat([]byte{0x0}, -diff)...)
				}
				assert.Equal(t, buf, bits.Bytes(), "bytes")
				checkCleanTail(t, bits.Bytes())
			})
		}
	}
	// clear/reset bitset to zero
	for _, sz := range bitsetSizes {
		t.Run(f("%d_resize_0", sz), func(t *testing.T) {
			bits := New(sz)
			bits.One()
			bits.Resize(0)

			assert.Len(t, bits.Bytes(), 0, "length")
			assert.Equal(t, 0, bits.Len(), "size")
			assert.Equal(t, 0, bits.Count(), "count")
			checkCleanTail(t, bits.Bytes())
		})
	}
	// grow + 1
	for _, sz := range bitsetSizes {
		t.Run(f("%d_resize+1", sz), func(t *testing.T) {
			bits := New(sz)
			bits.One()
			bits.Resize(bits.Len() + 1)
			bits.Set(bits.Len() - 1)

			assert.Len(t, bits.Bytes(), bitFieldLen(sz+1), "length")
			assert.Equal(t, sz+1, bits.Len(), "size")
			assert.Equal(t, sz+1, bits.Count(), "count")
			assert.Equal(t, popcount(bits.Bytes()), bits.Count(), "popcount")
			checkCleanTail(t, bits.Bytes())
		})
	}
}

func TestBitsetFill(t *testing.T) {
	for _, sz := range bitsetSizes {
		for _, pt := range bitsetPatterns {
			t.Run(f("%d_%x", sz, pt), func(t *testing.T) {
				cmp := fillBitset(nil, sz, pt)
				bits := New(sz)
				bits.Fill(pt)

				assert.Len(t, bits.Bytes(), bitFieldLen(sz), "length")
				assert.Equal(t, sz, bits.Len(), "size")
				assert.Equal(t, popcount(cmp), bits.Count(), "popcount")
				assert.Equal(t, cmp, bits.Bytes(), "bytes")
			})
		}
	}
}

func TestBitsetSet(t *testing.T) {
	for _, sz := range bitsetSizes {
		t.Run(f("%d", sz), func(t *testing.T) {
			bits := New(sz)
			cmp := fillBitset(nil, sz, 0)

			// set first bit
			bits.Set(0)
			cmp[0] |= 0x01
			assert.Equal(t, 1, bits.Count(), "count")
			assert.True(t, bits.Contains(0), "isset(0)")
			assert.True(t, bits.Any(), "any")
			assert.False(t, bits.All(), "all")
			assert.False(t, bits.None(), "none")
			assert.Equal(t, cmp, bits.Bytes(), "bytes")

			// set last bit
			bits.Set(sz - 1)
			cmp[(sz-1)>>3] |= 1 << uint((sz-1)&0x7)
			assert.Equal(t, 2, bits.Count(), "count")
			assert.True(t, bits.Contains(sz-1), "isset(sz-1)")
			assert.True(t, bits.Any(), "any")
			assert.False(t, bits.All(), "all")
			assert.False(t, bits.None(), "none")
			assert.Equal(t, cmp, bits.Bytes(), "bytes")

			// set invalid bit
			bits.Set(-1)
			assert.Equal(t, 2, bits.Count(), "count")
			assert.False(t, bits.Contains(-1), "isset(sz-1)")
			assert.Equal(t, cmp, bits.Bytes(), "bytes")

			bits.Set(sz)
			assert.Equal(t, 2, bits.Count(), "count")
			assert.False(t, bits.Contains(sz), "isset(sz)")
			assert.Equal(t, cmp, bits.Bytes(), "bytes")

			checkCleanTail(t, bits.Bytes())
		})
	}
}

func TestBitsetSetRange(t *testing.T) {
	for _, sz := range bitsetSizes {
		t.Run(f("%d", sz), func(t *testing.T) {
			err := quick.Check(func(rg [2]uint8) bool {
				bits := New(sz)
				start := int(rg[0])
				end := int(rg[1])
				if start > end {
					start, end = end, start
				}
				// t.Logf("SetRange(%d, %d)", start, end)
				bits.SetRange(start, end)
				for i := range sz {
					require.Equal(t, i >= start && i <= end, bits.Contains(i), "isset(%d) in %x", i, bits.Bytes())
				}
				return true
			}, nil)
			require.NoError(t, err)
		})
	}
}

func TestBitsetUnset(t *testing.T) {
	for _, sz := range bitsetSizes {
		t.Run(f("%d", sz), func(t *testing.T) {
			bits := New(sz)
			bits.One()
			cmp := fillBitset(nil, sz, 0xff)

			// clear first bit
			bits.Unset(0)
			cmp[0] &= 0xfe
			assert.Equal(t, popcount(cmp), bits.Count(), "popcount")
			assert.False(t, bits.Contains(0), "isset(0)")
			assert.Equal(t, cmp, bits.Bytes(), "bytes")

			// clear last bit
			bits.Unset(sz - 1)
			cmp[(sz-1)>>3] &^= 1 << uint((sz-1)&0x7)
			assert.Equal(t, popcount(cmp), bits.Count(), "popcount")
			assert.False(t, bits.Contains(sz-1), "isset(sz-1)")
			assert.Equal(t, cmp, bits.Bytes(), "bytes")

			// clear invalid bit
			bits.Unset(-1)
			assert.Equal(t, popcount(cmp), bits.Count(), "popcount")
			assert.False(t, bits.Contains(-1), "isset(-1)")
			assert.Equal(t, cmp, bits.Bytes(), "bytes")

			bits.Unset(sz)
			assert.Equal(t, popcount(cmp), bits.Count(), "count")
			assert.False(t, bits.Contains(sz), "isset(sz)")
			assert.Equal(t, cmp, bits.Bytes(), "bytes")
		})
	}
}

// func TestBitsetSlice(t *testing.T) {
// 	for _, sz := range bitsetSizes {
// 		t.Run(f("%d", sz), func(t *testing.T) {
// 			for _, b := range randBitsets(sz) {
// 				slice := b.Slice()
// 				require.Len(t, slice, sz, "length")
// 				for k, v := range slice {
// 					assert.Equal(t, b.Contains(k), v, "bit %d in %x", k, b.Bytes())
// 				}
// 			}
// 		})
// 	}
// }

func TestBitsetSlice(t *testing.T) {
	for _, sz := range bitsetSizes {
		t.Run(f("%d", sz), func(t *testing.T) {
			for _, b := range randBitsets(sz) {
				start := int(util.RandInt32n(int32(b.Len())))
				n := int(util.RandInt32n(int32(b.Len() - start)))
				slice := b.Slice(start, start+n)
				require.Len(t, slice, n, "length")
				for k, v := range slice {
					require.Equal(t, b.Contains(start+k), v, "bit %d in %x", start+k, b.Bytes())
				}
			}
		})
	}
}

func TestBitsetAppend(t *testing.T) {
	var fast, slow int
	for _, sz := range bitsetSizes {
		for _, pat := range bitsetPatterns {
			t.Run(f("%d_%x", sz, pat), func(t *testing.T) {
				for _, src := range randBitsets(sz) {
					dst := New(sz)
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
					dst.AppendRange(src, srcPos, srcPos+srcLen)

					dstSlice := dst.Slice(lbefore, lbefore+srcLen)
					srcSlice := src.Slice(srcPos, srcPos+srcLen)
					var srcSet int
					for i := range srcSlice {
						if srcSlice[i] {
							srcSet++
						}
					}

					// T.Logf("SRC=%x DST=%x srcPos=%d dstPos=%d n=%d\n",
					// src.Bytes(), dst.Bytes(), srcPos, lbefore, srcLen)
					require.Equal(t, dst.Len(), lbefore+srcLen, "length")
					require.Equal(t, dst.Count(), cbefore+srcSet, "count")
					require.Equal(t, dst.Count(), popcount(dst.Bytes()), "popcount")
					require.Len(t, dstSlice, len(srcSlice), "slice length")

					for j := range dstSlice {
						require.Equal(t, dstSlice[j], srcSlice[j], "bit %d in %x", j, dst.Bytes())
					}
				}
			})
		}
	}
	if fast == 0 {
		t.Errorf("%d slow, %d fast path hits – try increasing random sample size\n", slow, fast)
	}
}

func TestBitsetDelete(t *testing.T) {
	var fast, slow int
	for _, sz := range bitsetSizes {
		for _, pat := range bitsetPatterns {
			t.Run(f("%d_%x", sz, pat), func(t *testing.T) {
				for _, src := range randBitsets(sz) {
					dst := New(sz)
					dst.Fill(pat)
					cmp := dst.Slice(0, sz)
					delPos := int(util.RandInt32n(int32(src.Len())))
					delLen := int(util.RandInt32n(int32(src.Len() - delPos)))

					if delPos&0x7+delLen&0x7 == 0 {
						fast++
					} else {
						slow++
					}

					dst.Delete(delPos, delPos+delLen)
					cmp = slices.Delete(cmp, delPos, delPos+delLen)

					require.Equal(t, dst.Len(), len(cmp), "length")
					require.Equal(t, dst.Count(), popcount(dst.Bytes()), "popcount")

					for i, v := range cmp {
						assert.Equal(t, dst.Contains(i), v, "bit %d in %x", i, dst.Bytes())
					}

					checkCleanTail(t, dst.Bytes())
				}
			})
		}
	}
	if fast == 0 {
		t.Errorf("%d slow, %d fast path hits – try increasing random sample size\n", slow, fast)
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
		res[i] = New(sz)
		res[i].SetFromBytes(randBits(sz), sz, false)
	}
	return res
}
