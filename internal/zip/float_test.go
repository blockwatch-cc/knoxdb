package zip

import (
	"bytes"
	"io"
	"testing"

	"blockwatch.cc/knoxdb/internal/tests"
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
	for _, c := range tests.MakeBenchmarks[float64]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				EncodeFloat64(c.Data, io.Discard)
			}
		})
	}
}

func BenchmarkEncodeFloat32(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[float32]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				EncodeFloat32(c.Data, io.Discard)
			}
		})
	}
}

func BenchmarkDecodeFloat64(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[float64]() {
		buf := bytes.NewBuffer(nil)
		decodeFloats := make([]float64, c.N)
		EncodeFloat64(util.RandFloats[float64](c.N), buf)

		b.Run(c.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				DecodeFloat64(decodeFloats, buf.Bytes())
			}
		})
	}
}

func BenchmarkDecodeFloat32(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[float32]() {
		buf := bytes.NewBuffer(nil)
		decodeFloats := make([]float32, c.N)
		EncodeFloat32(util.RandFloats[float32](c.N), buf)
		b.Run(c.Name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				DecodeFloat32(decodeFloats, buf.Bytes())
			}
		})
	}
}
