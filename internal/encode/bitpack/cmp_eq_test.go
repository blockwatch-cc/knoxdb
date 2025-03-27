// Copyright (c) 2018-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitpack

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/encode/tests"
)

// Kernel fusion (23-50 µs = 2.3 cycles/value)
func BenchmarkCmpEqual(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, p := range tests.BenchmarkPatterns {
			buf := make([]byte, 2*c.N)
			data, val := tests.GenEqual[uint16](c.N, p.Pct)
			bits := bitset.NewBitset(c.N)
			for d := range 16 {
				PackVec(buf, data, d)
				b.Run(fmt.Sprintf("%s/%s/%d_bits", c.Name, p.Name, d), func(b *testing.B) {
					b.ResetTimer()
					b.SetBytes(int64(2 * c.N))
					for i := 0; i < b.N; i++ {
						Equal[d](buf, uint64(val), c.N, bits)
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
			buf := make([]byte, 2*c.N)
			data, val := tests.GenEqual[uint16](c.N, p.Pct)
			bits := bitset.NewBitset(c.N)
			for d := range 16 {
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

func BenchmarkCmpEqualLoop(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, p := range tests.BenchmarkPatterns {
			buf := make([]byte, 2*c.N)
			data, val := tests.GenEqual[uint16](c.N, p.Pct)
			bits := bitset.NewBitset(c.N)
			for d := range 16 {
				PackVec(buf, data, d)
				b.Run(fmt.Sprintf("%s/%s/%d_bits", c.Name, p.Name, d), func(b *testing.B) {
					b.ResetTimer()
					b.SetBytes(int64(2 * c.N))
					for i := 0; i < b.N; i++ {
						for i := range c.N {
							if uint16(Unpack(buf, i, d)) == val {
								bits.Set(i)
							}
						}
					}
				})
			}
		}
	}
}
