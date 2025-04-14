// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/tests"
)

// -------------------------------------
// ALP
//

func BenchmarkAlp_CompressFloat64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		var exn, n, r int
		src := tests.GenRndBits[float64](c.N, 24)
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
			b.ReportMetric(float64(exn)/float64(r), "ex/op")
			b.ReportMetric(float64(exn*100)/float64(n), "%ex")
		})
	}
}

func BenchmarkAlp_CompressFloat32(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		var exn, n, r int
		src := tests.GenRndBits[float32](c.N, 12)
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
				b.ReportMetric(float64(exn)/float64(r), "ex/op")
				b.ReportMetric(float64(exn*100)/float64(n), "%ex")
			}
		})
	}
}

func BenchmarkAlp_DecompressFloat64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[float64](c.N, 24)
		enc := NewEncoder[float64]().Compress(src)
		e := enc.State()
		out := make([]float64, c.N)
		dec := NewDecoder[float64](e.Encoding.F, e.Encoding.E).
			WithExceptions(e.Exceptions, e.Positions)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				dec.Decompress(out, e.Integers)
			}
		})
	}
}

func BenchmarkAlp_DecompressFloat32(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[float32](c.N, 12)
		enc := NewEncoder[float32]().Compress(src)
		e := enc.State()
		out := make([]float32, c.N)
		dec := NewDecoder[float32](e.Encoding.F, e.Encoding.E).
			WithExceptions(e.Exceptions, e.Positions)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				dec.Decompress(out, e.Integers)
			}
		})
	}
}

// -------------------------------------
// ALP-RD
//

func BenchmarkAlpRD_EstimateFloat64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[float64](c.N, 49)
		unique := make([]uint16, 1<<16)
		sample := make([]float64, MaxSampleLen(c.N))
		FirstLevelSample(sample, src)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				_ = EstimateRD(sample, unique)
			}
		})
	}
}

func BenchmarkAlpRD_EstimateFloat32(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[float32](c.N, 32)
		unique := make([]uint16, 1<<16)
		sample := make([]float32, MaxSampleLen(c.N))
		FirstLevelSample(sample, src)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				_ = EstimateRD(sample, unique)
			}
		})
	}
}

func BenchmarkAlpRD_SplitFloat64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[float64](c.N, 49)
		unique := make([]uint16, max(c.N, 1<<16))
		e := EstimateRD(FirstLevelSample(src, nil), unique)
		left := make([]uint16, c.N)
		right := make([]uint64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				SplitRD(src, left, right, e.Shift)
			}
		})
	}
}

func BenchmarkAlpRD_SplitFloat32(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[float32](c.N, 32)
		unique := make([]uint16, max(c.N, 1<<16))
		e := EstimateRD(FirstLevelSample(src, nil), unique)
		left := make([]uint16, c.N)
		right := make([]uint64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				SplitRD(src, left, right, e.Shift)
			}
		})
	}
}

func BenchmarkAlpRD_MergeFloat64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[float64](c.N, 49)
		unique := make([]uint16, max(c.N, 1<<16))
		e := EstimateRD(FirstLevelSample(src, nil), unique)
		left := make([]uint16, c.N)
		right := make([]uint64, c.N)
		dst := make([]float64, c.N)
		SplitRD(src, left, right, e.Shift)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				MergeRD(dst, left, right, e.Shift)
			}
		})
	}
}

func BenchmarkAlpRD_MergeFloat32(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[float32](c.N, 32)
		unique := make([]uint16, max(c.N, 1<<16))
		e := EstimateRD(FirstLevelSample(src, nil), unique)
		left := make([]uint16, c.N)
		right := make([]uint64, c.N)
		dst := make([]float32, c.N)
		SplitRD(src, left, right, e.Shift)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				MergeRD(dst, left, right, e.Shift)
			}
		})
	}
}
