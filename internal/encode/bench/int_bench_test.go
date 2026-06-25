// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode_bench

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	. "blockwatch.cc/knoxdb/internal/encode"
	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------
// Benchmarks
//

func BenchmarkIntAnalyze(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[uint64]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			for b.Loop() {
				ctx := AnalyzeInt(c.Data, true)
				ctx.Close()
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkIntEstimate(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[uint64]() {
		for _, scheme := range []ContainerType{
			TIntConstant,
			TIntDelta,
			TIntRunEnd,
			TIntBitpacked,
			TIntDictionary,
			TIntSimple8,
			TIntRaw,
		} {
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				ctx := AnalyzeInt(c.Data, true)
				b.ResetTimer()
				b.ReportAllocs()
				b.SetBytes(int64(len(c.Data) * 8))
				var n int
				for b.Loop() {
					_ = EstimateInt(ctx, scheme, c.Data)
					n++
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

func BenchmarkIntEncode(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []ContainerType{
			TIntConstant,
			TIntDelta,
			TIntRunEnd,
			TIntBitpacked,
			TIntDictionary,
			TIntSimple8,
			TIntRaw,
		} {
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				data := etests.GenForIntScheme[int64](int(scheme), c.N)
				ctx := AnalyzeInt(data, scheme == TIntDictionary)
				once := etests.ShowInfo
				b.ResetTimer()
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				var sz int
				for b.Loop() {
					enc := NewInt[int64](scheme).Encode(ctx, data)
					if once {
						b.Log(enc.Info())
						once = false
					}
					sz += enc.Size()
					enc.Close()
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
				b.ReportMetric(float64(sz*8/b.N/c.N), "bits/val")
				b.ReportMetric(100*float64(sz)/float64(b.N*c.N*8), "c(%)")
				ctx.Close()
			})
		}
	}
}

func BenchmarkIntEncodeAndStore(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []ContainerType{
			TIntConstant,
			TIntDelta,
			TIntRunEnd,
			TIntBitpacked,
			TIntDictionary,
			TIntSimple8,
			TIntRaw,
		} {
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				data := etests.GenForIntScheme[int64](int(scheme), c.N)
				once := etests.ShowInfo
				b.ResetTimer()
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				var sz int
				for b.Loop() {
					ctx := AnalyzeInt(data, scheme == TIntDictionary)
					enc := NewInt[int64](scheme).Encode(ctx, data)
					buf := enc.Store(make([]byte, 0, enc.Size()))
					require.LessOrEqual(b, len(buf), enc.Size())
					if once {
						b.Log(enc.Info())
						once = false
					}
					sz += enc.Size()
					enc.Close()
					ctx.Close()
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
				b.ReportMetric(float64(sz*8/b.N/c.N), "bits/val")
				b.ReportMetric(100*float64(sz)/float64(b.N*c.N*8), "c(%)")
			})
		}
	}
}

func BenchmarkIntEncodeBest(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[uint64]() {
		once := etests.ShowInfo
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			var sz int
			for b.Loop() {
				enc := EncodeInt(nil, c.Data)
				sz += enc.Size()
				if once {
					b.Log(enc.Info())
					once = false
				}
				enc.Close()
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			b.ReportMetric(float64(sz*8/b.N/c.N), "bits/val")
			b.ReportMetric(100*float64(sz)/float64(b.N*c.N*8), "c(%)")
		})
	}
}

func BenchmarkIntDecode(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []ContainerType{
			TIntConstant,
			TIntDelta,
			TIntRunEnd,
			TIntBitpacked,
			TIntDictionary,
			TIntSimple8,
			TIntRaw,
		} {
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				data := etests.GenForIntScheme[int64](int(scheme), c.N)
				ctx := AnalyzeInt(data, scheme == TIntDictionary)
				enc := NewInt[int64](scheme).Encode(ctx, data)
				buf := enc.Store(make([]byte, 0, enc.Size()))
				dst := make([]int64, 0, c.N)
				// b.Log(enc.Info())
				b.ResetTimer()
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for b.Loop() {
					enc2 := NewInt[int64](scheme)
					_, err := enc2.Load(buf)
					require.NoError(b, err)
					dst = enc2.AppendTo(dst, nil)
					dst = dst[:0]
					enc2.Close()
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
				enc.Close()
				ctx.Close()
			})
		}
	}
}

func BenchmarkIntMatchEqual(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []ContainerType{
			TIntConstant,
			TIntDelta,
			TIntRunEnd,
			TIntBitpacked,
			TIntDictionary,
			TIntSimple8,
			TIntRaw,
		} {
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				data := etests.GenForIntScheme[uint64](int(scheme), c.N)
				ctx := AnalyzeInt(data, true)
				enc := NewInt[uint64](scheme).Encode(ctx, data)
				bits := bitset.New(c.N)
				// b.Log(enc.Info())

				b.ResetTimer()
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for b.Loop() {
					enc.MatchEqual(data[0], bits, nil)
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
				enc.Close()
				ctx.Close()
			})
		}
	}
}

func BenchmarkIntIterator(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []ContainerType{
			TIntConstant,
			TIntDelta,
			TIntRunEnd,
			TIntBitpacked,
			TIntDictionary,
			TIntSimple8,
			TIntRaw,
		} {
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				data := etests.GenForIntScheme[int64](int(scheme), c.N)
				ctx := AnalyzeInt(data, true)
				enc := NewInt[int64](scheme).Encode(ctx, data)
				buf := enc.Store(make([]byte, 0, enc.Size()))
				// b.Log(enc.Info())

				b.ResetTimer()
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for b.Loop() {
					enc2 := NewInt[int64](scheme)
					_, err := enc2.Load(buf)
					require.NoError(b, err)
					it := enc2.Chunks()
					for {
						_, n := it.Next()
						if n == 0 {
							break
						}
					}
					it.Close()
					enc2.Close()
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
				enc.Close()
				ctx.Close()
			})
		}
	}
}

func BenchmarkIntAll(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []ContainerType{
			TIntConstant,
			TIntDelta,
			TIntRunEnd,
			TIntBitpacked,
			TIntDictionary,
			TIntSimple8,
			TIntRaw,
		} {
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				data := etests.GenForIntScheme[int64](int(scheme), c.N)
				ctx := AnalyzeInt(data, true)
				enc := NewInt[int64](scheme).Encode(ctx, data)
				buf := enc.Store(make([]byte, 0, enc.Size()))
				// b.Log(enc.Info())

				b.ResetTimer()
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for b.Loop() {
					enc2 := NewInt[int64](scheme)
					_, err := enc2.Load(buf)
					require.NoError(b, err)
					for _, v := range enc2.All() {
						_ = v
					}
					enc2.Close()
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
				enc.Close()
				ctx.Close()
			})
		}
	}
}

func BenchmarkDictArray(b *testing.B) {
	DictArrayBenchmark[uint16](b)
	DictArrayBenchmark[uint8](b)
}

func DictArrayBenchmark[T types.Integer](b *testing.B) {
	for _, p := range tests.BenchmarkPatterns {
		for _, c := range tests.BenchmarkSizes {
			data := tests.GenDups[T](c.N, min(c.N, p.Size), 30)
			ctx := AnalyzeInt(data, true)
			var card int
			b.Run(fmt.Sprintf("%T/%s/%s", T(0), c.Name, p.Name), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * arena.SizeFor[T]()))
				for range b.N {
					dict, codes := DictEncodeArray(ctx, data)
					card = len(dict)
					arena.Free(dict)
					arena.Free(codes)
				}
				_ = card
			})
		}
	}
}
