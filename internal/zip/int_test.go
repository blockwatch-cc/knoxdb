// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package zip

import (
	"bytes"
	"fmt"
	"io"
	"slices"

	"testing"
	"testing/quick"

	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

// generic test helper wrappers
func encode[T types.Signed](in []T, buf *bytes.Buffer) (int, error) {
	switch util.SizeOf[T]() {
	case 8:
		return EncodeInt64(util.ReinterpretSlice[T, int64](slices.Clone(in)), buf)
	case 4:
		return EncodeInt32(util.ReinterpretSlice[T, int32](slices.Clone(in)), buf)
	case 2:
		return EncodeInt16(util.ReinterpretSlice[T, int16](slices.Clone(in)), buf)
	case 1:
		return EncodeInt8(util.ReinterpretSlice[T, int8](slices.Clone(in)), buf)
	default:
		return 0, fmt.Errorf("Invalid width")
	}
}

func decode[T types.Signed](out []T, buf []byte) error {
	var err error
	switch util.SizeOf[T]() {
	case 8:
		_, err = DecodeInt64(util.ReinterpretSlice[T, int64](out), buf)
	case 4:
		_, err = DecodeInt32(util.ReinterpretSlice[T, int32](out), buf)
	case 2:
		_, err = DecodeInt16(util.ReinterpretSlice[T, int16](out), buf)
	case 1:
		_, err = DecodeInt8(util.ReinterpretSlice[T, int8](out), buf)
	}
	return err
}

func read[T types.Signed](out []T, r io.Reader) error {
	var err error
	switch util.SizeOf[T]() {
	case 8:
		_, _, err = ReadInt64(util.ReinterpretSlice[T, uint64](out), r)
	case 4:
		_, _, err = ReadInt32(util.ReinterpretSlice[T, uint32](out), r)
	case 2:
		_, _, err = ReadInt16(util.ReinterpretSlice[T, uint16](out), r)
	case 1:
		_, _, err = ReadInt8(util.ReinterpretSlice[T, uint8](out), r)
	}
	return err
}

func TestEncodeInt(t *testing.T) {
	testEncodeInt[int64](t)
	testEncodeInt[int32](t)
	testEncodeInt[int16](t)
	testEncodeInt[int8](t)
}

var maxShift = map[int]int{
	1: 7,
	2: 14,
	4: 29,
	8: 59,
}

var uncompressedFormat = map[int]byte{
	1: intCompressedPacked, // no uncompressed 8 bit
	2: intUncompressed16,
	4: intUncompressed32,
	8: intUncompressed64,
}

func testEncodeInt[T types.Signed](t *testing.T) {
	t.Run(fmt.Sprintf("%T", T(0)), func(t *testing.T) {
		sz := util.SizeOf[T]()
		w := maxShift[sz]

		// nil slice
		buf := &bytes.Buffer{}
		_, err := EncodeInt16(nil, buf)
		require.NoError(t, err)
		require.Len(t, buf.Bytes(), 0)

		// generate random values (should use simple8b)
		input := tests.GenRnd[T](1000)
		slices.Sort(input)
		encodeInt(t, input, intCompressedPacked)

		// Generate same values (should use RLE)
		input = tests.GenConst[T](1000, 1<<w-13)
		encodeInt(t, input, intCompressedRLE)

		// Generate large values that are sorted. The deltas will be large
		// and the values should be stored uncompressed.
		large := []T{0, 1<<w + 2, 1 << w}
		encodeInt(t, large, uncompressedFormat[sz])

		// generate random values that are unsorted (should use simple8b with zigzag)
		input = tests.GenRndBits[T](1000, w)
		encodeInt(t, input, intCompressedPacked)

		// Generate large random values that are not sorted. The deltas will be large
		// and the values should be stored uncompressed.
		input = tests.GenRndBits[T](1000, sz*8)
		encodeInt(t, input, uncompressedFormat[sz])
	})
}

func encodeInt[T types.Signed](t *testing.T, input []T, encoding byte) {
	// Retrieve encoded bytes from encoder.
	buf := new(bytes.Buffer)
	_, err := encode(input, buf)
	require.NoError(t, err, "buf: %x", buf.Bytes())
	require.Equal(t, encoding, buf.Bytes()[0]>>4, "unexpected encoding")

	// use the matching decoder (with support for all enc types)
	result := make([]T, len(input))
	require.NoError(t, decode(result, buf.Bytes()), "buf: %x", buf.Bytes())

	// verify that input and output values match.
	require.Equal(t, input, result)
}

func TestEncodeIntQuick(t *testing.T) {
	testEncodeIntQuick[int64](t)
	testEncodeIntQuick[int32](t)
	testEncodeIntQuick[int16](t)
	testEncodeIntQuick[int8](t)
}

func testEncodeIntQuick[T types.Signed](t *testing.T) {
	quick.Check(func(values []T) bool {
		// Retrieve encoded bytes from encoder.
		buf := new(bytes.Buffer)
		_, err := encode(values, buf)
		require.NoError(t, err)

		// use the matching decoder (with support for all enc types)
		res := make([]T, len(values))
		require.NoError(t, decode(res, buf.Bytes()))

		// Verify that input and output values match.
		require.Equal(t, values, res)
		return true
	}, nil)
}

func TestIntDecodeCorrupt(t *testing.T) {
	testIntDecodeCorrupt[int64](t)
	testIntDecodeCorrupt[int32](t)
	testIntDecodeCorrupt[int16](t)
	testIntDecodeCorrupt[int8](t)
}

func testIntDecodeCorrupt[T types.Signed](t *testing.T) {
	cases := []string{
		"\x10\x14",         // Packed: not enough data
		"\x20\x00",         // RLE: not enough data for starting timestamp
		"\x2012345678\x90", // RLE: initial timestamp but invalid uvarint encoding
		"\x2012345678\x7f", // RLE: timestamp, RLE but invalid repeat
		"\x00123",          // Raw: data length not multiple of 8
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%T/%q", T(0), c), func(t *testing.T) {
			res := make([]T, 0)
			require.Error(t, decode(res, []byte(c)))
			require.Equal(t, []T{}, res)
		})
	}
}

func BenchmarkEncodeInt(b *testing.B) {
	benchmarkEncodeInt[int64](b)
	benchmarkEncodeInt[int32](b)
	benchmarkEncodeInt[int16](b)
	benchmarkEncodeInt[int8](b)
}

func benchmarkEncodeInt[T types.Signed](b *testing.B) {
	for _, c := range tests.MakeBenchmarks[T]() {
		buf := bytes.NewBuffer(nil)
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			b.SetBytes(int64(c.N * util.SizeOf[T]()))
			var sz int
			for range b.N {
				n, err := encode(c.Data, buf)
				require.NoError(b, err)
				buf.Reset()
				sz += n
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			b.ReportMetric(float64(sz*8/b.N/c.N), "bits/val")
			b.ReportMetric(100*float64(sz)/float64(b.N*c.N*util.SizeOf[T]()), "c(%)")
		})
	}
}

func BenchmarkDecodeIntUncompressed(b *testing.B) {
	benchmarkDecodeUncompressed[int64](b)
	benchmarkDecodeUncompressed[int32](b)
	benchmarkDecodeUncompressed[int16](b)
	benchmarkDecodeUncompressed[int8](b)
}

func benchmarkDecodeUncompressed[T types.Signed](b *testing.B) {
	sz := util.SizeOf[T]()
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[T](c.N, sz*8)
		buf := bytes.NewBuffer(nil)
		encode(src, buf)
		dst := make([]T, c.N)
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.SetBytes(int64(c.N * sz))
			b.ResetTimer()
			for range b.N {
				_ = decode(dst, buf.Bytes())
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkReadIntUncompressed(b *testing.B) {
	benchmarkReadUncompressed[int64](b)
	benchmarkReadUncompressed[int32](b)
	benchmarkReadUncompressed[int16](b)
	benchmarkReadUncompressed[int8](b)
}

func benchmarkReadUncompressed[T types.Signed](b *testing.B) {
	sz := util.SizeOf[T]()
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[T](c.N, sz*8)
		buf := bytes.NewBuffer(nil)
		encode(src, buf)
		dst := make([]T, c.N)
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.SetBytes(int64(c.N * sz))
			b.ResetTimer()
			for range b.N {
				_ = read(dst, bytes.NewBuffer(buf.Bytes()))
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkDecodeIntPacked(b *testing.B) {
	benchmarkDecodePacked[int64](b)
	benchmarkDecodePacked[int32](b)
	benchmarkDecodePacked[int16](b)
	benchmarkDecodePacked[int8](b)
}

func benchmarkDecodePacked[T types.Signed](b *testing.B) {
	w := maxShift[util.SizeOf[T]()]
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[T](c.N, w/2)
		buf := bytes.NewBuffer(nil)
		encode(src, buf)
		dst := make([]T, c.N)
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.SetBytes(int64(c.N * util.SizeOf[T]()))
			b.ResetTimer()
			for range b.N {
				_ = decode(dst, buf.Bytes())
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkReadIntPacked(b *testing.B) {
	benchmarkReadPacked[int64](b)
	benchmarkReadPacked[int32](b)
	benchmarkReadPacked[int16](b)
	benchmarkReadPacked[int8](b)
}

func benchmarkReadPacked[T types.Signed](b *testing.B) {
	w := maxShift[util.SizeOf[T]()]
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenRndBits[T](c.N, w/2)
		buf := bytes.NewBuffer(nil)
		encode(src, buf)
		dst := make([]T, c.N)
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.SetBytes(int64(c.N * util.SizeOf[T]()))
			for range b.N {
				_ = read(dst, bytes.NewBuffer(buf.Bytes()))
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkDecodeIntRLE(b *testing.B) {
	benchmarkDecodeRLE[int64](b)
	benchmarkDecodeRLE[int32](b)
	benchmarkDecodeRLE[int16](b)
	benchmarkDecodeRLE[int8](b)
}

func benchmarkDecodeRLE[T types.Signed](b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenSeq[T](c.N, 10) // sic, we're doing delta
		buf := bytes.NewBuffer(nil)
		encode(src, buf)
		dst := make([]T, c.N)
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.SetBytes(int64(c.N * util.SizeOf[T]()))
			b.ResetTimer()
			for range b.N {
				_ = decode(dst, buf.Bytes())
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkReadIntRLE(b *testing.B) {
	benchmarkReadRLE[int64](b)
	benchmarkReadRLE[int32](b)
	benchmarkReadRLE[int16](b)
	benchmarkReadRLE[int8](b)
}

func benchmarkReadRLE[T types.Signed](b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := tests.GenSeq[T](c.N, 10) // sic, we're doing delta
		buf := bytes.NewBuffer(nil)
		encode(src, buf)
		dst := make([]T, c.N)
		b.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(b *testing.B) {
			b.SetBytes(int64(c.N * util.SizeOf[T]()))
			b.ResetTimer()
			for range b.N {
				_ = read(dst, bytes.NewBuffer(buf.Bytes()))
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}
