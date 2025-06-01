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

// ---------------------------------------------
// Benchmarks
//

type Benchmark struct {
	Name string
	N    int
	Data *bitset.Bitset
}

func MakeBenchmarks(n int) []Benchmark {
	return []Benchmark{
		{"dense", n, bitset.New(n).SetIndexes(seq(n/2, 2))},
		{"sparse", n, bitset.New(n).SetIndexes(seq(n/32, 32))},
	}
}

func BenchmarkBitmapEncode(b *testing.B) {
	for _, sz := range tests.BenchmarkSizes {
		for _, c := range MakeBenchmarks(sz.N) {
			once := etests.ShowInfo
			b.Run(c.Name+"/"+sz.Name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.Data.Len() / 8))
				var sz int
				for b.Loop() {
					enc := NewBitmap().Encode(nil, c.Data)
					if once {
						b.Log(enc.Info())
						once = false
					}
					sz += enc.Size()
					enc.Close()
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
				b.ReportMetric(float64(sz*8)/float64(b.N)/float64(c.N), "bits/val")
				b.ReportMetric(100*float64(sz)/float64(b.N*c.N*16), "c(%)")
			})
		}
	}
}

func BenchmarkBitmapEncodeAndStore(b *testing.B) {
	for _, sz := range tests.BenchmarkSizes {
		for _, c := range MakeBenchmarks(sz.N) {
			once := etests.ShowInfo
			b.Run(c.Name+"/"+sz.Name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.Data.Len() / 8))
				var sz int
				for b.Loop() {
					enc := NewBitmap().Encode(nil, c.Data)
					buf := enc.Store(make([]byte, 0, enc.Size()))
					require.LessOrEqual(b, len(buf), enc.Size())
					if once {
						b.Log(enc.Info())
						once = false
					}
					sz += enc.Size()
					enc.Close()
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
				b.ReportMetric(float64(sz*8)/float64(b.N)/float64(c.N), "bits/val")
				b.ReportMetric(100*float64(sz)/float64(b.N*c.N*16), "c(%)")
			})
		}
	}
}

func BenchmarkBitmapDecode(b *testing.B) {
	for _, sz := range tests.BenchmarkSizes {
		for _, c := range MakeBenchmarks(sz.N) {
			enc := NewBitmap().Encode(nil, c.Data)
			buf := enc.Store(make([]byte, 0, enc.Size()))
			dst := bitset.New(c.N)
			once := etests.ShowInfo
			b.Run(c.Name+"/"+sz.Name, func(b *testing.B) {
				b.SetBytes(int64(c.Data.Len() / 8))
				for b.Loop() {
					enc2, err := LoadBitmap(buf)
					require.NoError(b, err)
					enc2.AppendTo(dst, nil)
					if once {
						b.Log(enc2.Info())
						once = false
					}
					dst.Resize(0)
					enc2.Close()
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}

func BenchmarkBitmapCmp(b *testing.B) {
	for _, sz := range tests.BenchmarkSizes {
		for _, c := range MakeBenchmarks(sz.N) {
			enc := NewBitmap().Encode(nil, c.Data)
			bits := bitset.New(c.N)
			b.Log(enc.Info())
			b.Run(c.Name+"/"+sz.Name, func(b *testing.B) {
				b.SetBytes(int64(c.Data.Len() / 8))
				for b.Loop() {
					enc.MatchEqual(true, bits, nil)
				}
				b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			})
		}
	}
}
