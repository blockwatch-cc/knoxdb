// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

import (
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
)

type BenchmarkSize struct {
	Name string
	L    int
}

var BenchmarkSizes = []BenchmarkSize{
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"64K", 64 * 1024},
	// {"128K", 128 * 1024},
	// {"1M", 1024 * 1024},
	// {"128M", 128 * 1024 * 1024},
}

func BenchmarkCompressAlpFloat64(b *testing.B) {
	for _, n := range BenchmarkSizes {
		b.Run(n.Name, func(b *testing.B) {
			fl := util.RandFloats[float64](n.L)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = Compress(fl)
			}
		})
	}
}

func BenchmarkCompressAlpFloat32(b *testing.B) {
	for _, n := range BenchmarkSizes {
		fl := util.RandFloats[float32](n.L)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = Compress(fl)
			}
		})
	}
}

func BenchmarkCompressAlpRdFloat64(b *testing.B) {
	for _, n := range BenchmarkSizes {
		fl := util.RandFloats[float64](n.L)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				RDCompress[float64, uint64](fl)
			}
		})
	}
}

func BenchmarkCompressAlpRdFloat32(b *testing.B) {
	for _, n := range BenchmarkSizes {
		fl := util.RandFloats[float32](n.L)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				RDCompress[float32, uint32](fl)
			}
		})
	}
}

func BenchmarkDecompressAlpFloat64(b *testing.B) {
	for _, n := range BenchmarkSizes {
		s := Compress(util.RandFloats[float64](n.L))

		out := make([]float64, n.L)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				Decompress(out, s)
			}
		})
	}
}

func BenchmarkDecompressAlpFloat32(b *testing.B) {
	for _, n := range BenchmarkSizes {
		s := Compress(util.RandFloats[float32](n.L))

		out := make([]float32, n.L)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				Decompress(out, s)
			}
		})
	}
}

func BenchmarkDecompressAlpRdFloat64(b *testing.B) {
	for _, n := range BenchmarkSizes {
		s := RDCompress[float64, uint64](util.RandFloats[float64](n.L))

		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				RDDecompress[float64, uint64](s)
			}
		})
	}
}

func BenchmarkDecompressAlpRdFloat32(b *testing.B) {
	for _, n := range BenchmarkSizes {
		s := RDCompress[float32, uint32](util.RandFloats[float32](n.L))

		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				RDDecompress[float32, uint32](s)
			}
		})
	}
}
