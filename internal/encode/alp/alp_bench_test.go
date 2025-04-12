// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/tests"
)

func BenchmarkCompressAlpFloat64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		var exn, n, r int
		src := tests.GenRndBits[float64](c.N, 32)
		b.Run(c.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				enc := NewEncoder[float64]().Compress(src)
				exn += len(enc.State().Exceptions)
				n += c.N
				r++
				enc.Close()
			}
			b.ReportMetric(float64(exn)/float64(r), "except(mean)")
			b.ReportMetric(float64(exn*100)/float64(n), "except(pct)")
		})
	}
}

func BenchmarkCompressAlpFloat32(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		var exn, n, r int
		src := tests.GenRndBits[float32](c.N, 28)
		b.Run(c.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				enc := NewEncoder[float32]().Compress(src)
				exn += len(enc.State().Exceptions)
				n += c.N
				r++
				enc.Close()
				b.ReportMetric(float64(exn)/float64(r), "except(mean)")
				b.ReportMetric(float64(exn*100)/float64(n), "except(pct)")
			}
		})
	}
}

func BenchmarkCompressAlpRdFloat64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[float64](c.N, 49)
		b.Run(c.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				RDCompress[float64, uint64](src)
			}
		})
	}
}

func BenchmarkCompressAlpRdFloat32(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[float32](c.N, 49)
		b.Run(c.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				RDCompress[float32, uint32](src)
			}
		})
	}
}

func BenchmarkDecompressAlpFloat64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		enc := NewEncoder[float64]().Compress(tests.GenRndBits[float64](c.N, 32))
		e := enc.State()
		out := make([]float64, c.N)
		dec := NewDecoder[float64](e.Encoding.F, e.Encoding.E).
			WithExceptions(e.Exceptions, e.Positions)
		b.Run(c.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				dec.Decompress(out, e.Integers)
			}
		})
	}
}

func BenchmarkDecompressAlpFloat32(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		enc := NewEncoder[float32]().Compress(tests.GenRndBits[float32](c.N, 28))
		e := enc.State()
		out := make([]float32, c.N)
		dec := NewDecoder[float32](e.Encoding.F, e.Encoding.E).
			WithExceptions(e.Exceptions, e.Positions)
		b.Run(c.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				dec.Decompress(out, e.Integers)
			}
		})
	}
}

func BenchmarkDecompressAlpRdFloat64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		s := RDCompress[float64, uint64](tests.GenRndBits[float64](c.N, 32))
		b.Run(c.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				RDDecompress[float64, uint64](s)
			}
		})
	}
}

func BenchmarkDecompressAlpRdFloat32(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		s := RDCompress[float32, uint32](tests.GenRndBits[float32](c.N, 49))
		b.Run(c.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				RDDecompress[float32, uint32](s)
			}
		})
	}
}
