// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/stringx"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------
// Benchmarks
//

func BenchmarkStringAnalyze(b *testing.B) {
	for _, c := range tests.MakeStringBenchmarks() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.Data.DataSize()))
			for b.Loop() {
				ctx := AnalyzeString(c.Data)
				ctx.Close()
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkStringEncode(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []ContainerType{
			TStringConstant,
			TStringFixed,
			TStringCompact,
			TStringDictionary,
		} {
			data := etests.GenForStringScheme(int(scheme), c.N)
			ctx := AnalyzeString(data)
			once := etests.ShowInfo
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(data.DataSize()))
				var sz int
				for b.Loop() {
					enc := NewString(scheme).Encode(ctx, data)
					if once {
						b.Log(enc.Info())
						once = false
					}
					sz += enc.Size()
					enc.Close()
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
				b.ReportMetric(float64(sz*8)/float64(b.N)/float64(c.N), "bits/val")
				b.ReportMetric(100*float64(sz)/float64(b.N*c.N*8), "c(%)")
			})
			ctx.Close()
		}
	}
}

func BenchmarkStringEncodeAndStore(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []ContainerType{
			TStringConstant,
			TStringFixed,
			TStringCompact,
			TStringDictionary,
		} {
			data := etests.GenForStringScheme(int(scheme), c.N)
			once := etests.ShowInfo
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(data.DataSize()))
				var sz int
				for b.Loop() {
					ctx := AnalyzeString(data)
					enc := NewString(scheme).Encode(ctx, data)
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
				b.ReportMetric(float64(sz*8)/float64(b.N)/float64(c.N), "bits/val")
				b.ReportMetric(100*float64(sz)/float64(b.N*c.N*8), "c(%)")
			})
		}
	}
}

func BenchmarkStringEncodeBest(b *testing.B) {
	for _, c := range tests.MakeStringBenchmarks() {
		once := etests.ShowInfo
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.Data.DataSize()))
			var sz int
			for b.Loop() {
				enc := EncodeString(nil, c.Data)
				sz += enc.Size()
				if once {
					b.Log(enc.Info())
					once = false
				}
				enc.Close()
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			b.ReportMetric(float64(sz*8)/float64(b.N)/float64(c.N), "bits/val")
			b.ReportMetric(100*float64(sz)/float64(b.N*c.N*8), "c(%)")
		})
	}
}

func BenchmarkStringDecode(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []ContainerType{
			TStringConstant,
			TStringFixed,
			TStringCompact,
			TStringDictionary,
		} {
			data := etests.GenForStringScheme(int(scheme), c.N)
			ctx := AnalyzeString(data)
			enc := NewString(scheme).Encode(ctx, data)
			buf := enc.Store(make([]byte, 0, enc.Size()))
			dst := stringx.NewStringPool(c.N)
			b.Log(enc.Info())
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				b.SetBytes(int64(data.Size()))
				for b.Loop() {
					enc2 := NewString(scheme)
					_, err := enc2.Load(buf)
					require.NoError(b, err)
					enc2.AppendTo(dst, nil)
					dst.Clear()
					enc2.Close()
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

func BenchmarkStringCmp(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []ContainerType{
			TStringConstant,
			TStringFixed,
			TStringCompact,
			TStringDictionary,
		} {
			data := etests.GenForStringScheme(int(scheme), c.N)
			ctx := AnalyzeString(data)
			enc := NewString(scheme).Encode(ctx, data)
			bits := bitset.New(c.N)
			b.Log(enc.Info())
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				b.SetBytes(int64(data.DataSize()))
				for b.Loop() {
					enc.MatchEqual(data.Get(0), bits, nil)
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

func BenchmarkStringIterator(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []ContainerType{
			TStringConstant,
			TStringFixed,
			TStringCompact,
			TStringDictionary,
		} {
			data := etests.GenForStringScheme(int(scheme), c.N)
			ctx := AnalyzeString(data)
			enc := NewString(scheme).Encode(ctx, data)
			buf := enc.Store(make([]byte, 0, enc.Size()))
			b.Log(enc.Info())
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				b.SetBytes(int64(data.Size()))
				for b.Loop() {
					enc2 := NewString(scheme)
					_, err := enc2.Load(buf)
					require.NoError(b, err)
					it := enc2.Chunks()
					for {
						_, n := it.NextChunk()
						if n == 0 {
							break
						}
					}
					it.Close()
					enc2.Close()
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}
