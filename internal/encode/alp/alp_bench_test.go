// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/util"
)

func BenchmarkCompressAlpFloat64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		var exn, n, r int
		src := util.RandFloatsn[float64](c.N, 10000000)
		// src := tests.GenConst[float64](c.N, 2.5)
		b.Run(c.Name, func(b *testing.B) {
			b.ResetTimer()
			// b.ReportAllocs()
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
		src := util.RandFloatsn[float32](c.N, 10000000)
		// src := tests.GenConst[float32](c.N, 2.5)
		b.Run(c.Name, func(b *testing.B) {
			b.ResetTimer()
			// b.ReportAllocs()
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
	for _, n := range tests.BenchmarkSizes {
		fl := util.RandFloats[float64](n.N)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(n.N * 8))
			for range b.N {
				RDCompress[float64, uint64](fl)
			}
		})
	}
}

func BenchmarkCompressAlpRdFloat32(b *testing.B) {
	for _, n := range tests.BenchmarkSizes {
		fl := util.RandFloats[float32](n.N)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(n.N * 4))
			for range b.N {
				RDCompress[float32, uint32](fl)
			}
		})
	}
}

func BenchmarkDecompressAlpFloat64(b *testing.B) {
	for _, n := range tests.BenchmarkSizes {
		enc := NewEncoder[float64]().Compress(util.RandFloats[float64](n.N))
		e := enc.State()
		out := make([]float64, n.N)
		dec := NewDecoder[float64](e.EncodingIndice.Factor, e.EncodingIndice.Exponent).
			WithExceptions(e.Exceptions, e.ExceptionPositions)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(n.N * 8))
			for range b.N {
				dec.Decompress(out, e.EncodedIntegers)
			}
		})
	}
}

func BenchmarkDecompressAlpFloat32(b *testing.B) {
	for _, n := range tests.BenchmarkSizes {
		enc := NewEncoder[float32]().Compress(util.RandFloats[float32](n.N))
		e := enc.State()
		out := make([]float32, n.N)
		dec := NewDecoder[float32](e.EncodingIndice.Factor, e.EncodingIndice.Exponent).
			WithExceptions(e.Exceptions, e.ExceptionPositions)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(n.N * 4))
			for range b.N {
				dec.Decompress(out, e.EncodedIntegers)
			}
		})
	}
}

func BenchmarkDecompressAlpRdFloat64(b *testing.B) {
	for _, n := range tests.BenchmarkSizes {
		s := RDCompress[float64, uint64](util.RandFloats[float64](n.N))
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(n.N * 8))
			for range b.N {
				RDDecompress[float64, uint64](s)
			}
		})
	}
}

func BenchmarkDecompressAlpRdFloat32(b *testing.B) {
	for _, n := range tests.BenchmarkSizes {
		s := RDCompress[float32, uint32](util.RandFloats[float32](n.N))
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(n.N * 4))
			for range b.N {
				RDDecompress[float32, uint32](s)
			}
		})
	}
}
