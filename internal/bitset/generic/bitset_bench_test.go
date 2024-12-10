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
func BenchmarkBitsetIndexGenericSkip64(b *testing.B) {
	for _, n := range benchmarkSizes {
		for _, d := range benchmarkDensities {
			bits := fillBitsetRand(nil, n.L, d.D)
			slice := make([]uint32, int(PopCount(bits, n.L)))
			b.Run(n.Name+"-"+d.Name, func(b *testing.B) {
				b.ResetTimer()
				b.SetBytes(int64(bitFieldLen(n.L)))
				for i := 0; i < b.N; i++ {
					_ = Indexes(bits, n.L, slice)
				}
			})
		}
	}
}

func BenchmarkBitsetRunGeneric(b *testing.B) {
	for _, n := range benchmarkSizes {
		for _, d := range benchmarkDensities {
			bits := fillBitsetRand(nil, n.L, d.D)
			b.Run(n.Name+"-"+d.Name, func(b *testing.B) {
				b.ResetTimer()
				b.SetBytes(int64(bitFieldLen(n.L)))
				for i := 0; i < b.N; i++ {
					var idx, length int
					for idx > -1 {
						idx, length = Run(bits, idx+length, n.L)
					}
				}
			})
		}
	}
}

func BenchmarkBitsetPopCountGeneric(b *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				PopCount(bits, n.L)
			}
		})
	}
}

func BenchmarkBitsetAndGeneric(b *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		cmp := fillBitset(nil, n.L, 0xae)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				And(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetAndGenericFlag(b *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		cmp := fillBitset(nil, n.L, 0xae)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				AndFlag(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetAndNotGeneric(b *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		cmp := fillBitset(nil, n.L, 0xae)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				AndNot(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetOrGeneric(b *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		cmp := fillBitset(nil, n.L, 0xae)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				Or(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetOrGenericFlag(b *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		cmp := fillBitset(nil, n.L, 0xae)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				OrFlag(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetXorGeneric(b *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		cmp := fillBitset(nil, n.L, 0xae)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				Xor(bits, cmp, n.L)
			}
		})
	}
}

func BenchmarkBitsetNotGeneric(b *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				Neg(bits, n.L)
			}
		})
	}
}

func BenchmarkBitsetReverseGeneric(b *testing.B) {
	for _, n := range benchmarkSizes {
		bits := fillBitset(nil, n.L, 0xfa)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(bitFieldLen(n.L)))
			for i := 0; i < b.N; i++ {
				Reverse(bits)
			}
		})
	}
}
