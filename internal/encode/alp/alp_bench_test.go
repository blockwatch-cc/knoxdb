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

func BenchmarkAlp_CompressFloat32(b *testing.B) {
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

func BenchmarkAlp_DecompressFloat64(b *testing.B) {
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

func BenchmarkAlp_DecompressFloat32(b *testing.B) {
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
				_ = EstimateShift(sample, unique)
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
				_ = EstimateShift(sample, unique)
			}
		})
	}
}

func BenchmarkAlpRD_SplitFloat64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[float64](c.N, 49)
		unique := make([]uint16, max(c.N, 1<<16))
		shift := EstimateShift(FirstLevelSample(src, nil), unique)
		left := make([]uint16, c.N)
		right := make([]uint64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				Split(src, left, right, shift)
			}
		})
	}
}

func BenchmarkAlpRD_SplitFloat32(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[float32](c.N, 32)
		unique := make([]uint16, max(c.N, 1<<16))
		shift := EstimateShift(FirstLevelSample(src, nil), unique)
		left := make([]uint16, c.N)
		right := make([]uint64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				Split(src, left, right, shift)
			}
		})
	}
}

func BenchmarkAlpRD_MergeFloat64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[float64](c.N, 49)
		unique := make([]uint16, max(c.N, 1<<16))
		shift := EstimateShift(FirstLevelSample(src, nil), unique)
		left := make([]uint16, c.N)
		right := make([]uint64, c.N)
		dst := make([]float64, c.N)
		Split(src, left, right, shift)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				Merge(dst, left, right, shift)
			}
		})
	}
}

func BenchmarkAlpRD_MergeFloat32(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[float32](c.N, 32)
		unique := make([]uint16, max(c.N, 1<<16))
		shift := EstimateShift(FirstLevelSample(src, nil), unique)
		left := make([]uint16, c.N)
		right := make([]uint64, c.N)
		dst := make([]float32, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				Merge(dst, left, right, shift)
			}
		})
	}
}
