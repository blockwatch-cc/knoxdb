package zip

import (
	"bytes"
	"io"
	"testing"

	"blockwatch.cc/knoxdb/internal/zip/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestFloat64(t *testing.T) {
	sz := 100
	buf := bytes.NewBuffer(nil)
	decodeFloats := make([]float64, sz)

	fl := util.RandFloats[float64](sz)
	_, err := EncodeFloat64(fl, buf)
	require.NoError(t, err)

	n, err := DecodeFloat64(decodeFloats, buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, sz, n)
	require.Equal(t, fl, decodeFloats)
}

func TestFloat32(t *testing.T) {
	sz := 100
	buf := bytes.NewBuffer(nil)
	decodeFloats := make([]float32, sz)

	fl := util.RandFloats[float32](sz)
	_, err := EncodeFloat32(fl, buf)
	require.NoError(t, err)

	n, err := DecodeFloat32(decodeFloats, buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, sz, n)
	require.Equal(t, fl, decodeFloats)
}

func BenchmarkEncodeFloat64(b *testing.B) {
	for _, n := range tests.BenchmarkSizes {
		fl := util.RandFloats[float64](n.L)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				EncodeFloat64(fl, io.Discard)
			}
		})
	}
}

func BenchmarkEncodeFloat32(b *testing.B) {
	for _, n := range tests.BenchmarkSizes {
		fl := util.RandFloats[float32](n.L)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				EncodeFloat32(fl, io.Discard)
			}
		})
	}
}

func BenchmarkDecodeFloat64(b *testing.B) {
	for _, n := range tests.BenchmarkSizes {
		buf := bytes.NewBuffer(nil)
		decodeFloats := make([]float64, n.L)
		EncodeFloat64(util.RandFloats[float64](n.L), buf)

		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				DecodeFloat64(decodeFloats, buf.Bytes())
			}
		})
	}
}

func BenchmarkDecodeFloat32(b *testing.B) {
	for _, n := range tests.BenchmarkSizes {
		buf := bytes.NewBuffer(nil)
		decodeFloats := make([]float32, n.L)
		EncodeFloat32(util.RandFloats[float32](n.L), buf)
		b.Run(n.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				DecodeFloat32(decodeFloats, buf.Bytes())
			}
		})
	}
}
