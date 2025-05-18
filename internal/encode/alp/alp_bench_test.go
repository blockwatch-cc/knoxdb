// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package alp

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/encode/bitpack"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

// -------------------------------------
// ALP benchmarks
//

func BenchmarkAnalyze(b *testing.B) {
	benchAnalyze[float64, int64](b)
	benchAnalyze[float32, int32](b)
}

func benchAnalyze[T Float, E Int](b *testing.B) {
	for _, c := range tests.MakeBenchmarks[T]() {
		src := c.Data
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * util.SizeOf[T]()))
			for b.Loop() {
				_ = Analyze[T, E](src)
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkEncodeFull(b *testing.B) {
	benchEncode[float64, int64](b, true)
	benchEncode[float32, int32](b, true)
}

func BenchmarkEncodeOnly(b *testing.B) {
	benchEncode[float64, int64](b, false)
	benchEncode[float32, int32](b, false)
}

func benchEncode[T Float, E Int](b *testing.B, withAnalysis bool) {
	for _, c := range tests.BenchmarkSizes {
		var exn int
		src := tests.GenRndBits[T](c.N, util.SizeOf[T]()*3)
		a := Analyze[T, E](src)
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * util.SizeOf[T]()))
			for b.Loop() {
				enc := NewEncoder[T, E]()
				if withAnalysis {
					a = Analyze[T, E](src)
				}
				res := enc.Encode(src, a.Exp)
				exn += len(res.PatchValues)
				res.Close()
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			b.ReportMetric(float64(exn)/float64(b.N), "ex/op")
			b.ReportMetric(float64(exn*100)/float64(c.N*b.N), "%ex")
		})
	}
}

func BenchmarkDecode(b *testing.B) {
	benchDecode[float64, int64](b)
	benchDecode[float32, int32](b)
}

func BenchmarkDecodeFused(b *testing.B) {
	benchDecodeFused[float64, int64](b)
	benchDecodeFused[float32, int32](b)
}

func benchDecode[T Float, E Int](b *testing.B) {
	for _, c := range tests.MakeBenchmarks[T]() {
		src := c.Data
		enc := NewEncoder[T, E]()
		a := Analyze[T, E](src)
		res := enc.Encode(src, a.Exp)
		out := make([]T, c.N)
		dec := NewDecoder[T, E](a.Exp.F, a.Exp.E).
			WithPatches(res.PatchValues, res.PatchIndices).
			WithSafeInt(res.IsSafeInt)
		dst, log2 := bitpack.Encode(make([]byte, c.N*8), res.Encoded, res.Min, res.Max)
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.SetBytes(int64(c.N * enc.WIDTH))
			for b.Loop() {
				bitpack.Decode(res.Encoded, dst, log2, res.Min)
				dec.Decode(out, res.Encoded)
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func benchDecodeFused[T Float, E Int](b *testing.B) {
	for _, c := range tests.MakeBenchmarks[T]() {
		src := c.Data
		enc := NewEncoder[T, E]()
		a := Analyze[T, E](src)
		res := enc.Encode(src, a.Exp)
		out1 := make([]T, c.N)
		out2 := make([]T, c.N)

		log2 := types.Log2Range(res.Min, res.Max)
		dst := make([]byte, c.N*8)
		dst, _ = bitpack.Encode(dst, res.Encoded, res.Min, res.Max)
		dec := NewDecoder[T, E](a.Exp.F, a.Exp.E).
			WithPatches(res.PatchValues, res.PatchIndices)
		bitpack.Decode(res.Encoded, dst, log2, res.Min)
		dec.Decode(out1, res.Encoded)
		dec.DecodeFused(out2, dst, log2, res.Min)

		require.Equal(b, src, out1, "src is not equal out1")
		require.Equal(b, src, out2, "src is not equal out2")

		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.SetBytes(int64(c.N * enc.WIDTH))
			for range b.N {
				dec.DecodeFused(out2, dst, log2, res.Min)
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

// -------------------------------------
// ALP-RD benchmarks
//

func BenchmarkAnalyzeRD(b *testing.B) {
	benchAnalyzeRD[float64, uint64](b)
	benchAnalyzeRD[float32, uint32](b)
}

func benchAnalyzeRD[T Float, U Uint](b *testing.B) {
	for _, c := range tests.MakeBenchmarks[T]() {
		src := c.Data
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * util.SizeOf[T]()))
			for b.Loop() {
				_ = AnalyzeRD[T, U](src)
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkEncodeRD(b *testing.B) {
	benchEncodeRD[float64, uint64](b, false)
	benchEncodeRD[float32, uint32](b, false)
}

func benchEncodeRD[T Float, U Uint](b *testing.B, withAnalysis bool) {
	for _, c := range tests.MakeBenchmarks[T]() {
		src := c.Data
		a := AnalyzeRD[T, U](src)
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * util.SizeOf[T]()))
			for b.Loop() {
				enc := NewEncoderRD[T, U]()
				if withAnalysis {
					a = AnalyzeRD[T, U](src)
				}
				res := enc.Encode(src, a.Split)
				res.Close()
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkDecodeRD(b *testing.B) {
	benchDecodeRD[float64, uint64](b)
	benchDecodeRD[float32, uint32](b)
}

func benchDecodeRD[T Float, U Uint](b *testing.B) {
	for _, c := range tests.MakeBenchmarks[T]() {
		src := c.Data
		enc := NewEncoderRD[T, U]()
		a := AnalyzeRD[T, U](src)
		res := enc.Encode(src, a.Split)
		dst := make([]T, c.N)
		dec := NewDecoderRD[T, U](a.Split)
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.SetBytes(int64(c.N * util.SizeOf[T]()))
			for b.Loop() {
				dec.Decode(dst, res.Left, res.Right)
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}
