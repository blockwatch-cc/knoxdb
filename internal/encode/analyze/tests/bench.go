// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"testing"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
)

func AnalyzeBenchmark[T types.Integer](b *testing.B, fn AnalyzeFunc[T]) {
	for _, c := range tests.MakeBenchmarks[T]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * int(unsafe.Sizeof(T(0)))))
			for i := 0; i < b.N; i++ {
				fn(c.Data)
			}
		})
	}
}
