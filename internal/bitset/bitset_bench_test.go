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
	benchmarkSizes     = tests.BenchmarkSizes
	benchmarkDensities = tests.BenchmarkDensities
)

// Bitset high-level benchmarks
func BenchmarkBitsetSwap(b *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		bs := FromBuffer(bits, n.L)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				bs.Swap(i, n.L-i)
			}
		})
	}
}

func BenchmarkBitsetIndexes(b *testing.B) {
	for _, n := range benchmarkSizes {
		for _, d := range benchmarkDensities {
			buf := fillBitsetRand(nil, n.L, d.D)
			cnt := popcount(buf)
			slice := make([]uint32, cnt, n.L)
			bits := FromBuffer(buf, n.L)
			b.Run(n.Name+"-"+d.Name, func(b *testing.B) {
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
	for _, n := range benchmarkSizes {
		for _, d := range benchmarkDensities {
			buf := fillBitsetRand(nil, n.L, d.D)
			bits := FromBuffer(buf, n.L)

			buffer := make([]int, 256)
			b.Run(n.Name+"-"+d.Name, func(b *testing.B) {
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
