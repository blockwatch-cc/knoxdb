package tests

import (
	"slices"
	"testing"
	"unsafe"

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
			for i := 0; i < b.N; i++ {
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
			for i := 0; i < b.N; i++ {
				dec(dst, buf)
			}
		})
	}
}
