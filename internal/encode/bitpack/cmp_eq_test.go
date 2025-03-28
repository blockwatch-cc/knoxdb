// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitpack

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/encode/tests"
	"github.com/stretchr/testify/require"
)

var (
	testSizes = []int{1, 7, 15, 16} // algorithm boundaries (8x loop unrolled, 7x tail)
	chars     = "abcdefgh"
)

func TestCmpEqual(t *testing.T) {
	for _, n := range testSizes {
		for d := range 64 {
			t.Run(fmt.Sprintf("%d_bits/n_%d", d, n), func(t *testing.T) {
				data, val := tests.GenEqual[uint64](n, 10)
				mask := uint64(1<<d - 1)
				buf := make([]byte, (d+7)/8*n)
				PackVec(buf, data, d)
				t.Logf("Data %v", data)

				bits1 := bitset.NewBitset(n)
				bits2 := bitset.NewBitset(n)

				// function under test
				Equal[d](buf, val&mask, n, bits1)

				// check
				maskedVal := val & mask
				for i := range n {
					v := Unpack(buf, i, d) & mask
					t.Logf("Val %s=%d unpacked %d test %d", string(chars[i%8]), data[i]&mask, v, maskedVal)
					if v == maskedVal {
						bits2.Set(i)
					}
				}

				require.Equal(t, bits2.Count(), bits1.Count(), "count for %x", bits1.Bytes())
				require.Equal(t, bits2.Bytes(), bits1.Bytes(), "bytes")

				bits1.Close()
				bits2.Close()
			})
		}
	}
}

// Kernel fusion (23-50 µs = 2.3 cycles/value)
func BenchmarkCmpEqual(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, p := range tests.BenchmarkPatterns {
			data, val := tests.GenEqual[uint32](c.N, p.Pct)
			bits := bitset.NewBitset(c.N)
			for d := range 64 {
				buf := make([]byte, (d+7)/8*c.N)
				mask := uint64(1<<d - 1)
				PackVec(buf, data, d)
				b.Run(fmt.Sprintf("%s/%s/%d_bits", c.Name, p.Name, d), func(b *testing.B) {
					b.ResetTimer()
					b.SetBytes(int64(2 * c.N))
					for i := 0; i < b.N; i++ {
						Equal[d](buf, uint64(val)&mask, c.N, bits)
					}
				})
			}
		}
	}
}

// 5-7x slower (unpack: 90-150 µs, match: 25 µs, total: 115-180 µs = 8.2 cycles/value)
func BenchmarkCmpEqualUnpacked(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, p := range tests.BenchmarkPatterns {
			data, val := tests.GenEqual[uint16](c.N, p.Pct)
			bits := bitset.NewBitset(c.N)
			for d := range 64 {
				buf := make([]byte, (d+7)/8*c.N)
				PackVec(buf, data, d)
				unpacked := make([]uint16, c.N)
				b.Run(fmt.Sprintf("%s/%s/%d_bits", c.Name, p.Name, d), func(b *testing.B) {
					b.ResetTimer()
					b.SetBytes(int64(2 * c.N))
					for i := 0; i < b.N; i++ {
						UnpackVec(buf, unpacked, d)
						cmp.MatchUint16Equal(unpacked, val, bits, nil)
					}
				})
			}
		}
	}
}

// same as vectorized (119-181 µs = 8.2 cycles/value)
func BenchmarkCmpEqualLoop(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, p := range tests.BenchmarkPatterns {
			data, val := tests.GenEqual[uint16](c.N, p.Pct)
			bits := bitset.NewBitset(c.N)
			for d := range 64 {
				buf := make([]byte, (d+7)/8*c.N)
				PackVec(buf, data, d)
				unpack := Unpacker(d)
				b.Run(fmt.Sprintf("%s/%s/%d_bits", c.Name, p.Name, d), func(b *testing.B) {
					b.ResetTimer()
					b.SetBytes(int64(2 * c.N))
					for i := 0; i < b.N; i++ {
						for i := range c.N {
							if uint16(unpack(buf, i)) == val {
								bits.Set(i)
							}
						}
					}
				})
			}
		}
	}
}
