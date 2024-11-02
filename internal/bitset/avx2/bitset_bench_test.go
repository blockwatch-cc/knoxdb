// Copyright (c) 2020 Blockwatch Data Inc.
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

func BenchmarkBitsetIndexAVX2Skip(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		for _, d := range benchmarkDensities {
			B.Run(n.Name+"-"+d.Name, func(B *testing.B) {
				bits := fillBitsetRand(nil, n.L, d.D)
				slice := make([]uint32, int(generic.PopCount(bits, n.L))+8)
				B.ResetTimer()
				B.SetBytes(int64(bitFieldLen(n.L)))
				for i := 0; i < B.N; i++ {
					_ = Indexes(bits, n.L, slice)
				}
			})
		}
	}
}

func BenchmarkBitsetRunAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		for _, d := range benchmarkDensities {
			B.Run(n.Name+"-"+d.Name, func(B *testing.B) {
				bits := fillBitsetRand(nil, n.L, d.D)
				B.ResetTimer()
				B.SetBytes(int64(bitFieldLen(n.L)))
				for i := 0; i < B.N; i++ {
					var idx, length int
					for idx > -1 {
						idx, length = Run(bits, idx+length, n.L)
					}
				}
			})
		}
	}
}

func BenchmarkBitsetPopCountAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				PopCount(bits, n.L)
			}
		})
	}
}

func BenchmarkBitsetAndAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			cmp := fillBitset(nil, n.L, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				And(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetAndAVX2Flag(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			cmp := fillBitset(nil, n.L, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				AndFlag(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetAndNotAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			cmp := fillBitset(nil, n.L, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				AndNot(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetOrAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			cmp := fillBitset(nil, n.L, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				Or(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetOrAVX2Flag(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			cmp := fillBitset(nil, n.L, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				OrFlag(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetXorAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			cmp := fillBitset(nil, n.L, 0xae)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				Xor(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetNotAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				Neg(bits, n.L)
			}
		})
	}
}

func BenchmarkBitsetReverseAVX2(B *testing.B) {
	for _, n := range benchmarkSizes {
		B.Run(n.Name, func(B *testing.B) {
			bits := fillBitset(nil, n.L, 0xfa)
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				Reverse(bits)
			}
		})
	}
}
