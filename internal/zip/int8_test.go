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

func TestEncodeInt8_Compare(t *testing.T) {
	// generate random values (should use simple8b)
	input := make([]int8, 1000)
	for i := 0; i < len(input); i++ {
		input[i] = int8(util.RandInt64n(1<<7 - 1))
	}
	sort.Slice(input, func(i int, j int) bool { return input[i] < input[j] })
	testEncodeInt8_Compare(t, input, intCompressedPacked)

	// Generate same values (should use RLE)
	for i := 0; i < len(input); i++ {
		input[i] = 127
	}
	testEncodeInt8_Compare(t, input, intCompressedRLE)

	// generate random values that are unsorted (should use simple8b with zigzag)
	for i := 0; i < len(input); i++ {
		input[i] = int8(util.RandInt64n(1<<14 - 1))
	}
	testEncodeInt8_Compare(t, input, intCompressedPacked)
}

func testEncodeInt8_Compare(t *testing.T, input []int8, encoding byte) {
	buf := &bytes.Buffer{}
	_, err := EncodeInt8(slices.Clone(input), buf)
	buf2 := buf.Bytes()
	require.NoError(t, err, "buf: %x", buf2)
	require.Equal(t, encoding, buf2[0]>>4, "encoding")

	result := make([]int8, len(input))
	_, err = DecodeInt8(result, buf2)
	require.NoError(t, err, "buf: %x", buf2)
	require.Equal(t, input, result)
}

func TestEncodeInt8_NoValues(t *testing.T) {
	buf := &bytes.Buffer{}
	_, err := EncodeInt8(nil, buf)
	require.NoError(t, err)
	require.Len(t, buf.Bytes(), 0)
}

func TestEncodeInt8_Quick(t *testing.T) {
	quick.Check(func(values []int8) bool {
		// Retrieve encoded bytes from encoder.
		buf := &bytes.Buffer{}
		_, err := EncodeInt8(slices.Clone(values), buf)
		b := buf.Bytes()
		require.NoError(t, err)

		// use the matching decoder (with support for all enc types)
		res := make([]int8, len(values))
		_, err = DecodeInt8(res, b)
		require.NoError(t, err)

		// Verify that input and output values match.
		require.Equal(t, values, res)
		return true
	}, nil)
}

func TestInt8Decode_Corrupt(t *testing.T) {
	cases := []string{
		"\x10\x14",         // Packed: not enough data
		"\x20\x00",         // RLE: not enough data for starting timestamp
		"\x2012345678\x90", // RLE: initial timestamp but invalid uvarint encoding
		"\x2012345678\x7f", // RLE: timestamp, RLE but invalid repeat
		"\x00123",          // Raw: data length not multiple of 8
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%q", c), func(t *testing.T) {
			res := make([]int8, 0)
			_, err := DecodeInt8(res, []byte(c))
			require.Error(t, err)
			require.Equal(t, []int8{}, res)
		})
	}
}

func BenchmarkEncodeInt8(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[int8]() {
		buf := bytes.NewBuffer(nil)
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			b.SetBytes(int64(c.N * 1))
			for range b.N {
				_, err := EncodeInt8(c.Data, buf)
				require.NoError(b, err)
				buf.Reset()
			}
		})
	}
}

func BenchmarkDecodePackedInt8(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int8, c.N)
		for i := range c.N {
			src[i] = int8(i*1000) + int8(util.RandIntn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt8(src, buf)

		dst := make([]int8, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 1))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = DecodeInt8(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkReadPackedInt8(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int8, c.N)
		for i := range c.N {
			src[i] = int8(i*1000) + int8(util.RandIntn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt8(src, buf)

		dst := make([]uint8, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 1))
			b.ReportAllocs()
			for range b.N {
				_, _, _ = ReadInt8(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkDecodeRLEInt8(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int8, c.N)
		var acc int8 = 10
		for i := range c.N {
			src[i] = acc
			acc += 10
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt8(src, buf)

		dst := make([]int8, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 1))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = DecodeInt8(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkReadRLEInt8(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int8, c.N)
		var acc int8 = 10
		for i := range c.N {
			src[i] = acc
			acc += 10
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt8(src, buf)

		dst := make([]uint8, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 1))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _, _ = ReadInt8(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}
