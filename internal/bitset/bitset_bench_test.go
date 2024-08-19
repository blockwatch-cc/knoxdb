// Copyright (c) 2023 Blockwatch Data Inc.
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
func BenchmarkBitsetSwap(B *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		bs := NewBitsetFromBytes(bits, n.L)
		B.Run(n.Name, func(B *testing.B) {
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				bs.Swap(i, n.L-i)
			}
		})
	}
}

func BenchmarkBitsetIndexNative(B *testing.B) {
	for _, n := range benchmarkSizes {
		for _, d := range benchmarkDensities {
			buf := fillBitsetRand(nil, n.L, d.D)
			cnt := int(popcount(buf))
			slice := make([]int, cnt, n.L)
			bits := NewBitsetFromBytes(buf, n.L)
			B.Run(n.Name+"-"+d.Name, func(B *testing.B) {
				// we count hits in a bitset instead of raw throughput
				B.SetBytes(int64(cnt))
				B.ResetTimer()
				for i := 0; i < B.N; i++ {
					_ = bits.Indexes(slice)
				}
			})
		}
	}
}

func BenchmarkBitsetIndexOpt(B *testing.B) {
	for _, n := range benchmarkSizes {
		for _, d := range benchmarkDensities {
			buf := fillBitsetRand(nil, n.L, d.D)
			cnt := int(popcount(buf))
			slice := make([]uint32, cnt, n.L)
			bits := NewBitsetFromBytes(buf, n.L)
			B.Run(n.Name+"-"+d.Name, func(B *testing.B) {
				// we count hits in a bitset instead of raw throughput
				B.SetBytes(int64(cnt))
				B.ResetTimer()
				for i := 0; i < B.N; i++ {
					_ = bits.IndexesU32(slice)
				}
			})
		}
	}
}
