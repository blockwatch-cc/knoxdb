// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/tests"
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
				var sz int
				for b.Loop() {
					ctx := AnalyzeFloat(data, scheme == TFloatDictionary, scheme == TFloatAlp || scheme == TFloatAlpRd)
					enc := NewFloat[float64](scheme).Encode(ctx, data, MAX_CASCADE)
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
				var sz int
				for b.Loop() {
					ctx := AnalyzeFloat(data, scheme == TFloatDictionary, scheme == TFloatAlp || scheme == TFloatAlpRd)
					enc := NewFloat[float64](scheme).Encode(ctx, data, MAX_CASCADE)
					_ = enc.Store(make([]byte, 0, enc.Size()))
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
			b.ReportMetric(float64(sz*8/b.N/c.N), "bits/val")
			b.ReportMetric(100*float64(sz)/float64(b.N*c.N*8), "c(%)")
		})
	}
}

// run as
// GO_BENCH_PATH=path_to_alp_bench_files go test ./internal/encode/ -bench=File -cpu=1 -info
func BenchmarkFloatEncodeFile(b *testing.B) {
	tests.EnsureDataFiles(b)
	for _, sz := range tests.BenchmarkSizes {
		for _, c := range tests.MakeFileBenchmarks[float64](sz.N) {
			once := etests.ShowInfo
			b.Run(c.Name+"/"+sz.Name, func(b *testing.B) {
				var sz, n int
				for b.Loop() {
					sz, n = 0, 0
					for {
						src, ok := c.Next()
						if !ok {
							break
						}
						enc := EncodeFloat(nil, src, MAX_CASCADE)
						if once {
							b.Logf("%s %d => %d", enc.Info(), len(src)*8, enc.Size())
							once = false
						}
						n += len(src)
						sz += enc.Size()
						enc.Close()
					}
					c.F.Rewind()
				}
				b.ReportMetric(float64(c.F.Len()*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
				b.ReportMetric(float64(sz*8)/float64(n), "bits/val")
				b.SetBytes(int64(c.F.Size()))
			})
		}
	}
}

func BenchmarkFloatDecode(b *testing.B) {
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

// run as
// GO_DATA_PATH=path_to_alp_bench_files go test ./internal/encode/ -bench=File -cpu=1 -info
func BenchmarkFloatDecodeFile(b *testing.B) {
	tests.EnsureDataFiles(b)
	for _, sz := range tests.BenchmarkSizes {
		for _, c := range tests.MakeFileBenchmarks[float64](sz.N) {
			b.Run(c.Name+"/"+sz.Name, func(b *testing.B) {
				// prepare data
				once := etests.ShowInfo
				bufs := make([][]byte, 0)
				for {
					src, ok := c.Next()
					if !ok {
						break
					}
					enc := EncodeFloat(nil, src, MAX_CASCADE)
					if once {
						b.Logf("%s %d => %d", enc.Info(), len(src)*8, enc.Size())
						once = false
					}
					buf := make([]byte, 0, enc.Size())
					bufs = append(bufs, enc.Store(buf))
					enc.Close()
				}
				c.F.Rewind()
				dst := make([]float64, sz.N)
				b.ResetTimer()

				// the actual benchmark
				for b.Loop() {
					for _, buf := range bufs {
						dec, err := LoadFloat[float64](buf)
						if err != nil {
							b.Fatal(err)
						}
						dec.AppendTo(nil, dst)
						dst = dst[:0]
						dec.Close()
					}
				}
				b.ReportMetric(float64(c.F.Len()*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
				b.SetBytes(int64(c.F.Size()))
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
			// da, db := data[0], data[0]
			// if len(data) > 1 {
			// 	db = data[1]
			// }
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				b.SetBytes(int64(c.N * 8))
				for b.Loop() {
					enc.MatchEqual(data[0], bits, nil)
					// enc.MatchBetween(da, db, bits, nil)
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

func BenchmarkFloatIterator(b *testing.B) {
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
			buf := enc.Store(make([]byte, 0, enc.Size()))
			once := etests.ShowInfo
			b.Run(scheme.String()+"/"+c.Name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for b.Loop() {
					enc2 := NewFloat[float64](scheme)
					_, err := enc2.Load(buf)
					require.NoError(b, err)
					if once {
						b.Log(enc2.Info())
						once = false
					}
					it := enc2.Iterator()
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
