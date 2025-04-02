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

func TestEncodeInt32_Compare(t *testing.T) {
	// generate random values (should use simple8b)
	input := make([]int32, 1000)
	for i := 0; i < len(input); i++ {
		input[i] = int32(util.RandInt64n(100000))
	}
	sort.Slice(input, func(i int, j int) bool { return input[i] < input[j] })
	testEncodeInt32_Compare(t, input, intCompressedPacked)

	// Generate same values (should use RLE)
	for i := 0; i < len(input); i++ {
		input[i] = 1232342341
	}
	testEncodeInt32_Compare(t, input, intCompressedRLE)

	// Generate large values that are sorted. The deltas will be large
	// and the values should be stored uncompressed.
	large := []int32{0, 1<<30 + 2, 1 << 30}
	testEncodeInt32_Compare(t, large, intUncompressed32)

	// generate random values that are unsorted (should use simple8b with zigzag)
	for i := 0; i < len(input); i++ {
		input[i] = int32(util.RandInt64n(100000))
	}
	testEncodeInt32_Compare(t, input, intCompressedPacked)

	// Generate large random values that are not sorted. The deltas will be large
	// and the values should be stored uncompressed.
	for i := 0; i < len(input); i++ {
		input[i] = int32(util.RandUint32())
	}
	testEncodeInt32_Compare(t, input, intUncompressed32)
}

func testEncodeInt32_Compare(t *testing.T, input []int32, encoding byte) {
	buf := &bytes.Buffer{}
	_, err := EncodeInt32(slices.Clone(input), buf)
	buf2 := buf.Bytes()
	require.NoError(t, err, "buf: %x", buf2)
	require.Equal(t, encoding, buf2[0]>>4, "encoding")

	result := make([]int32, len(input))
	_, err = DecodeInt32(result, buf2)
	require.NoError(t, err, "buf: %x", buf2)
	require.Equal(t, input, result)
}

func TestEncodeInt32_NoValues(t *testing.T) {
	buf := &bytes.Buffer{}
	_, err := EncodeInt32(nil, buf)
	require.NoError(t, err)
	require.Len(t, buf.Bytes(), 0)
}

func TestEncodeInt32_Quick(t *testing.T) {
	quick.Check(func(values []int32) bool {
		// Retrieve encoded bytes from encoder.
		buf := &bytes.Buffer{}
		_, err := EncodeInt32(slices.Clone(values), buf)
		b := buf.Bytes()
		require.NoError(t, err)

		// use the matching decoder (with support for all enc types)
		res := make([]int32, len(values))
		_, err = DecodeInt32(res, b)
		require.NoError(t, err)

		// Verify that input and output values match.
		require.Equal(t, values, res)
		return true
	}, nil)
}

func TestInt32Decode_Corrupt(t *testing.T) {
	cases := []string{
		"\x10\x14",         // Packed: not enough data
		"\x20\x00",         // RLE: not enough data for starting timestamp
		"\x2012345678\x90", // RLE: initial timestamp but invalid uvarint encoding
		"\x2012345678\x7f", // RLE: timestamp, RLE but invalid repeat
		"\x00123",          // Raw: data length not multiple of 8
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%q", c), func(t *testing.T) {
			res := make([]int32, 0)
			_, err := DecodeInt32(res, []byte(c))
			require.Error(t, err)
			require.Equal(t, []int32{}, res)
		})
	}
}

func BenchmarkEncodeInt32(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[int32]() {
		buf := bytes.NewBuffer(nil)
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			b.SetBytes(int64(c.N * 4))
			for range b.N {
				_, err := EncodeInt32(c.Data, buf)
				require.NoError(b, err)
				buf.Reset()
			}
		})
	}
}

func BenchmarkDecodeUncompressedInt32(b *testing.B) {
	values := []int32{
		-2052281900, 1438442655, -2010452567,
		-1221292455, -1941700286, -2036753127,
		1432686216, 366324402, -306811373,
		-1949953187, 37133742, 322615366,
		-2093273755, 1006087192, -227212230,
		253323822, -445045444, 232778990,
		276141946, -132439744, -68075813,
		944688466, -239409312, -268213931,
	}

	for _, c := range tests.BenchmarkSizes {
		src := make([]int32, c.N)
		for i := range c.N {
			src[i] = values[util.RandIntn(len(values))]
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt32(src, buf)

		dst := make([]int32, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 4))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = DecodeInt32(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkReadUncompressedInt32(b *testing.B) {
	values := []int32{
		-2052281900, 1438442655, -2010452567,
		-1221292455, -1941700286, -2036753127,
		1432686216, 366324402, -306811373,
		-1949953187, 37133742, 322615366,
		-2093273755, 1006087192, -227212230,
		253323822, -445045444, 232778990,
		276141946, -132439744, -68075813,
		944688466, -239409312, -268213931,
	}

	for _, c := range tests.BenchmarkSizes {
		src := make([]int32, c.N)
		for i := range c.N {
			src[i] = values[util.RandIntn(len(values))]
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt32(src, buf)

		dst := make([]uint32, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 4))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _, _ = ReadInt32(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkDecodePackedInt32(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int32, c.N)
		for i := range c.N {
			src[i] = int32(i*1000) + int32(util.RandIntn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt32(src, buf)

		dst := make([]int32, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 4))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = DecodeInt32(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkReadPackedInt32(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int32, c.N)
		for i := range c.N {
			src[i] = int32(i*1000) + int32(util.RandIntn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt32(src, buf)

		dst := make([]uint32, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 4))
			b.ReportAllocs()
			for range b.N {
				_, _, _ = ReadInt32(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkDecodeRLEInt32(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int32, c.N)
		var acc int32 = 10
		for i := range c.N {
			src[i] = acc
			acc += 10
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt32(src, buf)

		dst := make([]int32, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 4))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = DecodeInt32(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkReadRLEInt32(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int32, c.N)
		var acc int32 = 10
		for i := range c.N {
			src[i] = acc
			acc += 10
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt32(src, buf)

		dst := make([]uint32, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 2))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _, _ = ReadInt32(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}
