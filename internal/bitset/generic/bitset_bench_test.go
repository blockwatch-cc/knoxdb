// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
// go test ./internal/bitset/generic/... -bench=.
package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset/tests"
)

var (
	benchmarkSizes     = tests.BenchmarkSizes
	benchmarkDensities = tests.BenchmarkDensities
)

// Bitset low-level benchmarks
func BenchmarkBitsetIndexGenericSkip64(B *testing.B) {
	for _, n := range benchmarkSizes {
		for _, d := range benchmarkDensities {
			bits := fillBitsetRand(nil, n.L, d.D)
			slice := make([]uint32, int(PopCount(bits, n.L)))
			B.Run(n.Name+"-"+d.Name, func(B *testing.B) {
				B.ResetTimer()
				B.SetBytes(int64(bitFieldLen(n.L)))
				for i := 0; i < B.N; i++ {
					_ = Indexes(bits, n.L, slice)
				}
			})
		}
	}
}

func BenchmarkBitsetRunGeneric(B *testing.B) {
	for _, n := range benchmarkSizes {
		for _, d := range benchmarkDensities {
			bits := fillBitsetRand(nil, n.L, d.D)
			B.Run(n.Name+"-"+d.Name, func(B *testing.B) {
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

func BenchmarkBitsetPopCountGeneric(B *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		B.Run(n.Name, func(B *testing.B) {
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				PopCount(bits, n.L)
			}
		})
	}
}

func BenchmarkBitsetAndGeneric(B *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		cmp := fillBitset(nil, n.L, 0xae)
		B.Run(n.Name, func(B *testing.B) {
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				And(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetAndGenericFlag(B *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		cmp := fillBitset(nil, n.L, 0xae)
		B.Run(n.Name, func(B *testing.B) {
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				AndFlag(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetAndNotGeneric(B *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		cmp := fillBitset(nil, n.L, 0xae)
		B.Run(n.Name, func(B *testing.B) {
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				AndNot(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetOrGeneric(B *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		cmp := fillBitset(nil, n.L, 0xae)
		B.Run(n.Name, func(B *testing.B) {
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				Or(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetOrGenericFlag(B *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		cmp := fillBitset(nil, n.L, 0xae)
		B.Run(n.Name, func(B *testing.B) {
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				OrFlag(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetXorGeneric(B *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		cmp := fillBitset(nil, n.L, 0xae)
		B.Run(n.Name, func(B *testing.B) {
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				Xor(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetNotGeneric(B *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		B.Run(n.Name, func(B *testing.B) {
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				Neg(bits, n.L)
			}
		})
	}
}

func BenchmarkBitsetReverseGeneric(B *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		B.Run(n.Name, func(B *testing.B) {
			B.ResetTimer()
			B.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < B.N; i++ {
				Reverse(bits)
			}
		})
	}
}
