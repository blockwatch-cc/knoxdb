// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"slices"
	"testing"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/require"
)

func EncodeBenchmark[T types.Unsigned](b *testing.B, fn EncodeFunc[T]) {
	for _, c := range tests.MakeBenchmarks[T]() {
		minv, maxv := slices.Min(c.Data), slices.Max(c.Data)
		buf := make([]byte, 8*len(c.Data))
		var sz, n int
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * int(unsafe.Sizeof(T(0)))))
			for range b.N {
				buf, _ := fn(buf, c.Data, minv, maxv)
				sz += len(buf)
				n++
			}
			// b.ReportMetric(float64(sz)/float64(n), "mean_bytes")
			// b.ReportMetric(float64(minv), "min_val")
			// b.ReportMetric(float64(maxv), "max_val")
		})
	}
}

func DecodeBenchmark[T types.Unsigned](b *testing.B, enc EncodeFunc[T], dec DecodeFunc[T]) {
	for _, c := range tests.MakeBenchmarks[T]() {
		minv, maxv := slices.Min(c.Data), slices.Max(c.Data)
		buf, err := enc(make([]byte, 8*len(c.Data)), c.Data, minv, maxv)
		require.NoError(b, err)
		dst := make([]T, len(c.Data))
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * int(unsafe.Sizeof(T(0)))))
			for range b.N {
				dec(dst, buf)
			}
		})
	}
}

func CompareBenchmark[T types.Unsigned](b *testing.B, enc EncodeFunc[T], cmp CompareFunc) {
	for _, c := range tests.MakeBenchmarks[T]() {
		minv, maxv := slices.Min(c.Data), slices.Max(c.Data)
		buf, err := enc(make([]byte, 8*len(c.Data)), c.Data, minv, maxv)
		require.NoError(b, err)
		bits := bitset.NewBitset(len(c.Data))
		val := c.Data[len(c.Data)/2]

		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * int(unsafe.Sizeof(T(0)))))
			for range b.N {
				cmp(buf, uint64(val), bits)
			}
		})
	}
}

func CompareBenchmark2[T types.Unsigned](b *testing.B, enc EncodeFunc[T], cmp CompareFunc2) {
	for _, c := range tests.MakeBenchmarks[T]() {
		minv, maxv := slices.Min(c.Data), slices.Max(c.Data)
		buf, err := enc(make([]byte, 8*len(c.Data)), c.Data, minv, maxv)
		require.NoError(b, err)
		bits := bitset.NewBitset(len(c.Data))
		val := c.Data[len(c.Data)/2]
		from, to := max(val/2, minv+1), min(val*2, maxv-1)

		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * int(unsafe.Sizeof(T(0)))))
			for range b.N {
				cmp(buf, uint64(from), uint64(to), bits)
			}
		})
	}
}
