// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// go test ./internal/bitset/... -bench=.
package bitset

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset/tests"
)

var (
	sizes     = tests.BenchmarkSizes
	densities = tests.BenchmarkDensities
	ranges    = tests.BenchmarkRanges
)

func BenchmarkBitsetSet(b *testing.B) {
	for _, n := range sizes {
		bits := New(n.L)
		b.Run(n.Name, func(b *testing.B) {
			for i := range b.N {
				bits.Set(i % n.L)
			}
		})
	}
}

func BenchmarkBitsetSetRange(b *testing.B) {
	for _, n := range sizes {
		for _, r := range ranges {
			bits := New(n.L)
			b.Run(n.Name+"_"+r.Name, func(b *testing.B) {
				for i := range b.N {
					var a, b int
					if i%n.L >= n.L-r.Range {
						a, b = 0, r.Range
					} else {
						a, b = i%n.L, i%n.L+r.Range
					}
					bits.SetRange(a, b)
				}
			})
		}
	}
}

func BenchmarkBitsetIndexes(b *testing.B) {
	for _, n := range sizes {
		for _, d := range densities {
			buf := fillBitsetRand(nil, n.L, d.D)
			cnt := popcount(buf)
			slice := make([]uint32, cnt, n.L)
			bits := NewFromBuffer(buf, n.L)
			b.Run(n.Name+"/"+d.Name, func(b *testing.B) {
				b.SetBytes(int64(bits.Len()))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = bits.Indexes(slice)
				}
			})
		}
	}
}

// see https://lemire.me/blog/2016/09/22/swift-versus-java-the-bitset-performance-test/
func BenchmarkBitsetIterate(b *testing.B) {
	for _, n := range sizes {
		for _, d := range densities {
			buf := fillBitsetRand(nil, n.L, d.D)
			bits := NewFromBuffer(buf, n.L)

			buffer := make([]int, 256)
			b.Run(n.Name+"/"+d.Name, func(b *testing.B) {
				b.ResetTimer()
				b.SetBytes(int64(bits.Len()))
				sum := int(0)
				for i := 0; i < b.N; i++ {
					j := int(0)
					j, buffer = bits.Iterate(j, buffer)
					for ; len(buffer) > 0; j, buffer = bits.Iterate(j, buffer) {
						for k := range buffer {
							sum += buffer[k]
						}
						j++
					}
				}

				if sum == 0 { // added just to fool ineffassign
					return
				}
			})
		}
	}
}
