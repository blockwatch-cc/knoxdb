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
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 8))
			var sz int
			for range b.N {
				n, err := EncodeFloat64(c.Data, io.Discard)
				require.NoError(b, err)
				sz += n
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			b.ReportMetric(float64(sz*8/b.N/c.N), "bits/val")
			b.ReportMetric(100*float64(sz)/float64(b.N*c.N*8), "c(%)")
		})
	}
}

func BenchmarkEncodeFloat32(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[float32]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 4))
			var sz int
			for range b.N {
				n, err := EncodeFloat32(c.Data, io.Discard)
				require.NoError(b, err)
				sz += n
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			b.ReportMetric(float64(sz*8/b.N/c.N), "bits/val")
			b.ReportMetric(100*float64(sz)/float64(b.N*c.N*4), "c(%)")
		})
	}
}

func BenchmarkDecodeFloat64(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[float64]() {
		buf := bytes.NewBuffer(nil)
		decodeFloats := make([]float64, c.N)
		EncodeFloat64(c.Data, buf)

		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				_, err := DecodeFloat64(decodeFloats, buf.Bytes())
				require.NoError(b, err)
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkDecodeFloat32(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[float32]() {
		buf := bytes.NewBuffer(nil)
		decodeFloats := make([]float32, c.N)
		EncodeFloat32(c.Data, buf)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				DecodeFloat32(decodeFloats, buf.Bytes())
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}
