package generic

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/encode/s8b/tests"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	s8bBenchmarkSize    = tests.S8bBenchmarkSize
	s8bBenchmarksUint64 = tests.S8bBenchmarksUint64
	s8bBenchmarksUint32 = tests.S8bBenchmarksUint32
	s8bBenchmarksUint16 = tests.S8bBenchmarksUint16
	s8bBenchmarksUint8  = tests.S8bBenchmarksUint8
)

func BenchmarkDecodeUint64Generic(b *testing.B) {
	for _, bm := range s8bBenchmarksUint64 {
		in := bm.Fn(s8bBenchmarkSize)()
		out := make([]uint64, len(in))
		comp, _ := EncodeUint64(in)
		buf := util.ToByteSlice(comp)
		b.Run(bm.Name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.Size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				DecodeUint64(out, buf)
			}
		})
	}
}

func BenchmarkDecodeUint32Generic(b *testing.B) {
	for _, bm := range s8bBenchmarksUint32 {
		in := bm.Fn(s8bBenchmarkSize)()
		out := make([]uint32, len(in))
		comp, _ := EncodeUint64(in)
		buf := util.ToByteSlice(comp)
		b.Run(bm.Name, func(b *testing.B) {
			b.SetBytes(int64(4 * bm.Size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				DecodeUint32(out, buf)
			}
		})
	}
}

func BenchmarkDecodeUint16Generic(b *testing.B) {
	for _, bm := range s8bBenchmarksUint16 {
		in := bm.Fn(s8bBenchmarkSize)()
		out := make([]uint16, len(in))
		comp, _ := EncodeUint64(in)
		buf := util.ToByteSlice(comp)
		b.Run(bm.Name, func(b *testing.B) {
			b.SetBytes(int64(2 * bm.Size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				DecodeUint16(out, buf)
			}
		})
	}
}

func BenchmarkDecodeUint8Generic(b *testing.B) {
	for _, bm := range s8bBenchmarksUint8 {
		in := bm.Fn(s8bBenchmarkSize)()
		out := make([]uint8, len(in))
		comp, _ := EncodeUint64(in)
		buf := util.ToByteSlice(comp)
		b.Run(bm.Name, func(b *testing.B) {
			b.SetBytes(int64(1 * bm.Size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				DecodeUint8(out, buf)
			}
		})
	}
}

func BenchmarkCountBytesGeneric(b *testing.B) {
	for _, bm := range s8bBenchmarksUint64 {
		in := bm.Fn(s8bBenchmarkSize)()
		encoded, _ := EncodeUint64(in)
		buf := util.ToByteSlice(encoded)

		b.Run(bm.Name, func(b *testing.B) {
			b.SetBytes(int64(8 * bm.Size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				CountValues(buf)
			}
		})
	}
}
