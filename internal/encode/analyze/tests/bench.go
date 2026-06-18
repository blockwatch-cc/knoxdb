// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
)

func AnalyzeBenchmark[T types.Integer](b *testing.B, fn AnalyzeFunc[T]) {
	for _, c := range tests.MakeBenchmarks[T]() {
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * arena.SizeFor[T]()))
			for b.Loop() {
				fn(c.Data)
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func AnalyzeFloatBenchmark[T types.Float](b *testing.B, fn AnalyzeFloatFunc[T]) {
	for _, c := range tests.MakeBenchmarks[T]() {
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * arena.SizeFor[T]()))
			for b.Loop() {
				fn(c.Data)
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}
