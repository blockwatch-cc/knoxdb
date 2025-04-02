// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package zip

import (
	"bytes"
	"fmt"
	"slices"

	"sort"
	"testing"
	"testing/quick"

	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestEncodeInt16_Compare(t *testing.T) {
	// generate random values (should use simple8b)
	input := make([]int16, 1000)
	for i := 0; i < len(input); i++ {
		input[i] = int16(util.RandInt64n(1<<14 - 1))
	}
	sort.Slice(input, func(i int, j int) bool { return input[i] < input[j] })
	testEncodeInt16_Compare(t, input, intCompressedPacked)

	// Generate same values (should use RLE)
	for i := 0; i < len(input); i++ {
		input[i] = 12163
	}
	testEncodeInt16_Compare(t, input, intCompressedRLE)

	// Generate large values that are sorted. The deltas will be large
	// and the values should be stored uncompressed.
	large := []int16{0, 1<<14 + 2, 1 << 14}
	testEncodeInt16_Compare(t, large, intUncompressed16)

	// generate random values that are unsorted (should use simple8b with zigzag)
	for i := 0; i < len(input); i++ {
		input[i] = int16(util.RandInt64n(1<<14 - 1))
	}
	testEncodeInt16_Compare(t, input, intCompressedPacked)

	// Generate large random values that are not sorted. The deltas will be large
	// and the values should be stored uncompressed.
	for i := 0; i < len(input); i++ {
		input[i] = int16(util.RandUint32())
	}
	testEncodeInt16_Compare(t, input, intUncompressed16)
}

func testEncodeInt16_Compare(t *testing.T, input []int16, encoding byte) {
	buf := &bytes.Buffer{}
	_, err := EncodeInt16(slices.Clone(input), buf)
	buf2 := buf.Bytes()
	require.NoError(t, err, "buf: %x", buf2)
	require.Equal(t, encoding, buf2[0]>>4, "encoding")

	result := make([]int16, len(input))
	_, err = DecodeInt16(result, buf2)
	require.NoError(t, err, "buf: %x", buf2)
	require.Equal(t, input, result)
}

func TestEncodeInt16_NoValues(t *testing.T) {
	buf := &bytes.Buffer{}
	_, err := EncodeInt16(nil, buf)
	require.NoError(t, err)
	require.Len(t, buf.Bytes(), 0)
}

func TestEncodeInt16_Quick(t *testing.T) {
	quick.Check(func(values []int16) bool {
		// Retrieve encoded bytes from encoder.
		buf := &bytes.Buffer{}
		_, err := EncodeInt16(slices.Clone(values), buf)
		b := buf.Bytes()
		require.NoError(t, err)

		// use the matching decoder (with support for all enc types)
		res := make([]int16, len(values))
		_, err = DecodeInt16(res, b)
		require.NoError(t, err)

		// Verify that input and output values match.
		require.Equal(t, values, res)
		return true
	}, nil)
}

func TestInt16Decode_Corrupt(t *testing.T) {
	cases := []string{
		"\x10\x14",         // Packed: not enough data
		"\x20\x00",         // RLE: not enough data for starting timestamp
		"\x2012345678\x90", // RLE: initial timestamp but invalid uvarint encoding
		"\x2012345678\x7f", // RLE: timestamp, RLE but invalid repeat
		"\x00123",          // Raw: data length not multiple of 8
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%q", c), func(t *testing.T) {
			res := make([]int16, 0)
			_, err := DecodeInt16(res, []byte(c))
			require.Error(t, err)
			require.Equal(t, []int16{}, res)
		})
	}
}

func BenchmarkEncodeInt16(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[int16]() {
		buf := bytes.NewBuffer(nil)
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			b.SetBytes(int64(c.N*2))
			for range b.N {
				_, err := EncodeInt16(c.Data, buf)
				require.NoError(b, err)
				buf.Reset()
			}
		})
	}
}

func BenchmarkDecodeUncompressedInt16(b *testing.B) {
	values := []int16{
		-20522, 14384, -20104,
		-12212, -19417, -20367,
		14166, 3661, -3068,
		-19499, 3713, 16261,
		-20916, 10060, -22721,
		25316, -4450, 21677,
		27614, -11643, -6807,
		9446, -23940, -26821,
	}

	for _, c := range tests.BenchmarkSizes {
		src := make([]int16, c.N)
		for i := range c.N {
			src[i] = values[util.RandIntn(len(values))]
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt16(src, buf)

		dst := make([]int16, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 2))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = DecodeInt16(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkReadUncompressedInt16(b *testing.B) {
	values := []int16{
		-20522, 14384, -20104,
		-12212, -19417, -20367,
		14166, 3661, -3068,
		-19499, 3713, 16261,
		-20916, 10060, -22721,
		25316, -4450, 21677,
		27614, -11643, -6807,
		9446, -23940, -26821,
	}
	for _, c := range tests.BenchmarkSizes {
		src := make([]int16, c.N)
		for i := range c.N {
			src[i] = values[util.RandIntn(len(values))]
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt16(src, buf)

		dst := make([]uint16, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 2))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _, _ = ReadInt16(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkDecodePackedInt16(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int16, c.N)
		for i := range c.N {
			src[i] = int16(i*1000) + int16(util.RandIntn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt16(src, buf)

		dst := make([]int16, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 2))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = DecodeInt16(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkReadPackedInt16(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int16, c.N)
		for i := range c.N {
			src[i] = int16(i*1000) + int16(util.RandIntn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt16(src, buf)

		dst := make([]uint16, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 2))
			b.ReportAllocs()
			for range b.N {
				_, _, _ = ReadInt16(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkDecodeRLEInt16(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int16, c.N)
		var acc int16 = 10
		for i := range c.N {
			src[i] = acc
			acc += 10
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt16(src, buf)

		dst := make([]int16, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 2))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = DecodeInt16(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkReadRLEInt16(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int16, c.N)
		var acc int16 = 10
		for i := range c.N {
			src[i] = acc
			acc += 10
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt16(src, buf)

		dst := make([]uint16, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 2))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _, _ = ReadInt16(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}
