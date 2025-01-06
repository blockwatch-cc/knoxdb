// Copyright (c) 2020-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx2

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset/generic"
	"blockwatch.cc/knoxdb/internal/bitset/tests"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	benchmarkSizes     = tests.BenchmarkSizes
	benchmarkDensities = tests.BenchmarkDensities
)

func BenchmarkBitsetIndexesAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range benchmarkSizes {
		for _, d := range benchmarkDensities {
			b.Run(n.Name+"-"+d.Name, func(b *testing.B) {
				bits := fillBitsetRand(nil, n.L, d.D)
				slice := make([]uint32, int(generic.PopCount(bits, n.L))+8)
				b.ResetTimer()
				b.SetBytes(int64(bitFieldLen(n.L)))
				for i := 0; i < b.N; i++ {
					_ = Indexes(bits, n.L, slice)
				}
			})
		}
	}
}

func BenchmarkBitsetPopCountAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range benchmarkSizes {
		b.Run(n.Name, func(b *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				PopCount(bits, n.L)
			}
		})
	}
}

func BenchmarkBitsetAndAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range benchmarkSizes {
		b.Run(n.Name, func(b *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			cmp := fillBitset(nil, n.L, 0xae)
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				And(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetAndAVX2Flag(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range benchmarkSizes {
		b.Run(n.Name, func(b *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			cmp := fillBitset(nil, n.L, 0xae)
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				AndFlag(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetAndNotAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range benchmarkSizes {
		b.Run(n.Name, func(b *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			cmp := fillBitset(nil, n.L, 0xae)
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				AndNot(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetOrAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range benchmarkSizes {
		b.Run(n.Name, func(b *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			cmp := fillBitset(nil, n.L, 0xae)
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				Or(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetOrAVX2Flag(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range benchmarkSizes {
		b.Run(n.Name, func(b *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			cmp := fillBitset(nil, n.L, 0xae)
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				OrFlag(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetXorAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range benchmarkSizes {
		b.Run(n.Name, func(b *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			cmp := fillBitset(nil, n.L, 0xae)
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				Xor(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetNotAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range benchmarkSizes {
		b.Run(n.Name, func(b *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				Neg(bits, n.L)
			}
		})
	}
}
