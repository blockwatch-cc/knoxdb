// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"slices"
	"testing"
)

type BenchmarkSize struct {
	Name string
	N    int
}

var BenchmarkSizes = []BenchmarkSize{
	{"1k", 1024},
	{"16k", 16 * 1024},
	{"64k", 64 * 1024},
}

func BenchmarkStringPoolAppend(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		src := RandByteSlices(sz.N, 32)
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			for b.Loop() {
				pool := NewStringPoolSize(sz.N, 32)
				for _, v := range src {
					pool.Append(v)
				}
				pool.Close()
			}
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkStringPoolIterator(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		pool := NewStringPoolSize(sz.N, 32)
		for range sz.N {
			pool.Append(RandBytes(32))
		}
		var x int
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			for b.Loop() {
				for v := range pool.Iterator() {
					x += len(v)
				}
			}
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
		_ = x
		pool.Close()
	}
}

func BenchmarkStringPoolMinMax(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		pool := NewStringPoolSize(sz.N, 32)
		for range sz.N {
			pool.Append(RandBytes(32))
		}
		var minv, maxv []byte
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			for b.Loop() {
				minv, maxv, _, _ = pool.MinMax()
			}
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
		_ = minv
		_ = maxv
		pool.Close()
	}
}

func BenchmarkStringPoolAppendTo(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		pool := NewStringPoolSize(sz.N, 32)
		for range sz.N {
			pool.Append(RandBytes(32))
		}
		var x int
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			for b.Loop() {
				p2 := NewStringPoolSize(sz.N, 32)
				pool.AppendTo(p2, nil)
				p2.Close()
			}
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
		_ = x
		pool.Close()
	}
}

func BenchmarkByteSliceAppend(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		src := RandByteSlices(sz.N, 32)
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			for b.Loop() {
				dst := make([][]byte, 0, len(src))
				for _, v := range src {
					dst = append(dst, slices.Clone(v))
				}
			}
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkByteSliceIterator(b *testing.B) {
	for _, sz := range BenchmarkSizes {
		src := RandByteSlices(sz.N, 32)
		var x int
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(sz.N * 32))
			for b.Loop() {
				for _, v := range src {
					x += len(v)
				}
			}
			b.ReportMetric(float64(sz.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
		_ = x
	}
}
