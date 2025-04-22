// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/zip"
	"github.com/stretchr/testify/require"
)

func BenchmarkFloatAnalyze(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[float64]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			for b.Loop() {
				ctx := AnalyzeFloat(c.Data, true, true)
				ctx.Close()
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkFloatEstimate(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[float64]() {
		for _, scheme := range []FloatContainerType{
			TFloatConstant,
			TFloatRunEnd,
			TFloatDictionary,
			TFloatAlp,
			TFloatAlpRd,
			TFloatRaw,
		} {
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(c.Data) * 8))
				for b.Loop() {
					ctx := AnalyzeFloat(c.Data, true, true)
					_ = EstimateFloat(scheme, ctx, c.Data, MAX_CASCADE)
					ctx.Close()
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

func BenchmarkFloatEncode(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []FloatContainerType{
			TFloatConstant,
			TFloatRunEnd,
			TFloatDictionary,
			TFloatAlp,
			TFloatAlpRd,
			TFloatRaw,
		} {
			data := etests.GenForFloatScheme[float64](int(scheme), c.N)
			once := etests.ShowInfo
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for b.Loop() {
					ctx := AnalyzeFloat(data, scheme == TFloatDictionary, scheme == TFloatAlp)
					enc := NewFloat[float64](scheme).Encode(ctx, data, MAX_CASCADE)
					if once {
						b.Log(enc.Info())
						once = false
					}
					enc.Close()
					ctx.Close()
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

func BenchmarkFloatEncodeAndStore(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []FloatContainerType{
			TFloatConstant,
			TFloatRunEnd,
			TFloatDictionary,
			TFloatAlp,
			TFloatAlpRd,
			TFloatRaw,
		} {
			data := etests.GenForFloatScheme[float64](int(scheme), c.N)
			once := etests.ShowInfo
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for b.Loop() {
					ctx := AnalyzeFloat(data, scheme == TFloatDictionary, scheme == TFloatAlp)
					enc := NewFloat[float64](scheme).Encode(ctx, data, MAX_CASCADE)
					_ = enc.Store(make([]byte, 0, enc.Size()))
					if once {
						b.Log(enc.Info())
						once = false
					}
					enc.Close()
					ctx.Close()
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

func BenchmarkFloatEncodeBest(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[float64]() {
		once := etests.ShowInfo
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			var sz int
			for b.Loop() {
				enc := EncodeFloat(nil, c.Data, MAX_CASCADE)
				sz += enc.Size()
				if once {
					b.Log(enc.Info())
					once = false
				}
				enc.Close()
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			b.ReportMetric(float64(sz/b.N), "c(B)")
			b.ReportMetric(100*float64(sz)/float64(b.N*c.N*8), "c(%)")
		})
	}
}

func BenchmarkFloatEncodeLegacy(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[float64]() {
		buf := bytes.NewBuffer(make([]byte, zip.Int64EncodedSize(len(c.Data))))
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			var sz int
			for b.Loop() {
				n, _ := zip.EncodeFloat64(c.Data, buf)
				sz += n
				buf.Reset()
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			b.ReportMetric(float64(sz/b.N), "c(B)")
			b.ReportMetric(100*float64(sz)/float64(b.N*c.N*8), "c(%)")
		})
	}
}

func BenchmarkFloatAppend(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []FloatContainerType{
			TFloatConstant,
			TFloatRunEnd,
			TFloatDictionary,
			TFloatAlp,
			TFloatAlpRd,
			TFloatRaw,
		} {
			data := etests.GenForFloatScheme[float64](int(scheme), c.N)
			ctx := AnalyzeFloat(data, scheme == TFloatDictionary, scheme == TFloatAlp)
			enc := NewFloat[float64](scheme).Encode(ctx, data, MAX_CASCADE)
			buf := enc.Store(make([]byte, 0, enc.Size()))
			dst := make([]float64, 0, c.N)
			once := etests.ShowInfo
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for b.Loop() {
					enc2 := NewFloat[float64](scheme)
					_, err := enc2.Load(buf)
					require.NoError(b, err)
					dst = enc2.AppendTo(nil, dst)
					dst = dst[:0]
					if once {
						b.Log(enc2.Info())
						once = false
					}
					enc2.Close()
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

func BenchmarkFloatCmp(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []FloatContainerType{
			TFloatConstant,
			TFloatRunEnd,
			TFloatDictionary,
			TFloatAlp,
			TFloatAlpRd,
			TFloatRaw,
		} {
			data := etests.GenForFloatScheme[float64](int(scheme), c.N)
			ctx := AnalyzeFloat(data, true, true)
			enc := NewFloat[float64](scheme).Encode(ctx, data, MAX_CASCADE)
			bits := bitset.NewBitset(c.N)

			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for b.Loop() {
					enc.MatchEqual(data[0], bits, nil)
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}
