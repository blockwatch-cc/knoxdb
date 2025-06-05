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

func BenchmarkSet(b *testing.B) {
	for _, n := range sizes {
		bits := New(n.L)
		b.Run(n.Name, func(b *testing.B) {
			for i := range b.N {
				bits.Set(i % n.L)
			}
		})
	}
}

func BenchmarkSetRange(b *testing.B) {
	for _, n := range sizes {
		for _, r := range ranges {
			bits := New(n.L)
			b.Run(n.Name+"/"+r.Name, func(b *testing.B) {
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

func BenchmarkIndexes(b *testing.B) {
	for _, n := range sizes {
		for _, d := range densities {
			buf := fillBitsetRand(nil, n.L, d.D)
			cnt := popcount(buf)
			slice := make([]uint32, cnt, n.L)
			bits := NewFromBytes(buf, n.L)
			b.Run(n.Name+"/"+d.Name, func(b *testing.B) {
				b.SetBytes(int64(bits.Len()))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = bits.Indexes(slice)
				}
				b.ReportMetric(float64(n.L*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

// see https://lemire.me/blog/2016/09/22/swift-versus-java-the-bitset-performance-test/
func BenchmarkIterate(b *testing.B) {
	for _, n := range sizes {
		for _, d := range densities {
			bits := NewFromBytes(fillBitsetRand(nil, n.L, d.D), n.L)
			b.Run(n.Name+"/"+d.Name, func(b *testing.B) {
				b.ResetTimer()
				b.SetBytes(int64(bits.Len()))
				for b.Loop() {
					var (
						buf  [128]int // alloc once and reuse
						last int      = -1
					)
					for {
						vals, ok := bits.Iterate(last, buf[:])
						if !ok {
							break
						}
						last = vals[len(vals)-1]
					}
				}
				b.ReportMetric(float64(n.L*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

func BenchmarkChunk(b *testing.B) {
	for _, n := range sizes {
		for _, d := range densities {
			bits := NewFromBytes(fillBitsetRand(nil, n.L, d.D), n.L)
			var sum int
			b.Run(n.Name+"/"+d.Name, func(b *testing.B) {
				b.ResetTimer()
				b.SetBytes(int64(bits.Len()))
				for b.Loop() {
					it := bits.Chunks()
					for {
						idxs, ok := it.Next()
						if !ok {
							break
						}
						sum += len(idxs)
						// for _, idx := range idxs {
						// 	sum += idx
						// }
					}
					it.Close()
				}
				b.ReportMetric(float64(n.L*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
			_ = sum
		}
	}
}

func BenchmarkIterator(b *testing.B) {
	for _, n := range sizes {
		for _, d := range densities {
			buf := fillBitsetRand(nil, n.L, d.D)
			src := NewFromBytes(buf, n.L)
			var x int
			b.Run(n.Name+"/"+d.Name, func(b *testing.B) {
				b.SetBytes(int64(src.Len()))
				for b.Loop() {
					for v := range src.Iterator() {
						x += v
					}
				}
				b.ReportMetric(float64(n.L*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
			_ = x
		}
	}
}
