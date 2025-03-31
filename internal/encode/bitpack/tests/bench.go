// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"fmt"
	"slices"
	"testing"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/require"
)

func EncodeBenchmark[T types.Unsigned](b *testing.B, fn EncodeFunc[T]) {
	w := int(unsafe.Sizeof(T(0)))
	for _, c := range tests.MakeBenchmarks[T]() {
		minv, maxv := slices.Min(c.Data), slices.Max(c.Data)
		buf := make([]byte, w*c.N)
		var sz, n int
		b.Run(fmt.Sprintf("u%d/%s", w*8, c.Name), func(b *testing.B) {
			b.SetBytes(int64(w * c.N))
			for range b.N {
				buf, _, _ := fn(buf, c.Data, minv, maxv)
				sz += len(buf)
				n++
			}
		})
	}
}

func DecodeBenchmark[T types.Unsigned](b *testing.B, enc EncodeFunc[T], dec DecodeFunc[T]) {
	if enc == nil {
		enc = encode[T]
	}
	w := int(unsafe.Sizeof(T(0)))
	for _, c := range tests.MakeBenchmarks[T]() {
		minv, maxv := slices.Min(c.Data), slices.Max(c.Data)
		buf, log2, err := enc(make([]byte, w*c.N), c.Data, minv, maxv)
		require.NoError(b, err)
		dst := make([]T, c.N)
		b.Run(fmt.Sprintf("u%d/%s", w*8, c.Name), func(b *testing.B) {
			b.SetBytes(int64(w * c.N))
			for range b.N {
				dec(dst, buf, log2, minv)
			}
		})
	}
}

func CompareBenchmark[T types.Unsigned](b *testing.B, enc EncodeFunc[T], cmp CompareFunc) {
	if enc == nil {
		enc = encode[T]
	}
	w := int(unsafe.Sizeof(T(0)))
	for _, c := range tests.MakeBenchmarks[T]() {
		minv, maxv := slices.Min(c.Data), slices.Max(c.Data)
		buf, log2, err := enc(make([]byte, w*c.N), c.Data, minv, maxv)
		require.NoError(b, err)
		bits := bitset.NewBitset(c.N)
		val := c.Data[c.N/2]

		b.Run(fmt.Sprintf("u%d/%s", w*8, c.Name), func(b *testing.B) {
			b.SetBytes(int64(w * c.N))
			for range b.N {
				cmp(buf, log2, uint64(val), c.N, bits)
			}
		})
	}
}

func CompareBenchmark2[T types.Unsigned](b *testing.B, enc EncodeFunc[T], cmp CompareFunc2) {
	if enc == nil {
		enc = encode[T]
	}
	w := int(unsafe.Sizeof(T(0)))
	for _, c := range tests.MakeBenchmarks[T]() {
		minv, maxv := slices.Min(c.Data), slices.Max(c.Data)
		buf, log2, err := enc(make([]byte, w*c.N), c.Data, minv, maxv)
		require.NoError(b, err)
		bits := bitset.NewBitset(c.N)
		val := c.Data[c.N/2]
		from, to := max(val/2, minv+1), min(val*2, maxv-1)

		b.Run(fmt.Sprintf("u%d/%s", w*8, c.Name), func(b *testing.B) {
			b.SetBytes(int64(len(c.Data) * int(unsafe.Sizeof(T(0)))))
			for range b.N {
				cmp(buf, log2, uint64(from), uint64(to), c.N, bits)
			}
		})
	}
}
