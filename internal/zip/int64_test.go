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

func TestEncodeInt64_Compare(t *testing.T) {
	// generate random values (should use simple8b)
	input := make([]int64, 1000)
	for i := 0; i < len(input); i++ {
		input[i] = util.RandInt64n(100000) //- 50000
	}
	sort.Slice(input, func(i int, j int) bool { return input[i] < input[j] })
	testEncodeInt64_Compare(t, input, intCompressedPacked)

	// Generate same values (should use RLE)
	for i := 0; i < len(input); i++ {
		input[i] = 1232342341234
	}
	testEncodeInt64_Compare(t, input, intCompressedRLE)

	// Generate large values that are sorted. The deltas will be large
	// and the values should be stored uncompressed.
	large := []int64{0, 1<<60 + 2, 2<<60 + 2}
	testEncodeInt64_Compare(t, large, intUncompressed64)

	// generate random values that are unsorted (should use simple8b with zigzag)
	for i := 0; i < len(input); i++ {
		input[i] = util.RandInt64n(100000) //- 50000
	}
	testEncodeInt64_Compare(t, input, intCompressedPacked)

	// Generate large random values that are not sorted. The deltas will be large
	// and the values should be stored uncompressed.
	for i := 0; i < len(input); i++ {
		input[i] = int64(util.RandUint64())
	}
	testEncodeInt64_Compare(t, input, intUncompressed64)
}

func testEncodeInt64_Compare(t *testing.T, input []int64, encoding byte) {
	buf := &bytes.Buffer{}
	_, err := EncodeInt64(slices.Clone(input), buf)
	buf2 := buf.Bytes()
	require.NoError(t, err, "buf: %x", buf2)
	require.Equal(t, encoding, buf2[0]>>4, "encoding")

	result := make([]int64, len(input))
	_, err = DecodeInt64(result, buf2)
	require.NoError(t, err, "buf: %x", buf2)
	require.Equal(t, input, result)
}

func TestEncodeInt64_NoValues(t *testing.T) {
	buf := &bytes.Buffer{}
	_, err := EncodeInt64(nil, buf)
	require.NoError(t, err)
	require.Len(t, buf.Bytes(), 0)
}

func TestEncodeInt64_Quick(t *testing.T) {
	quick.Check(func(values []int64) bool {
		// Retrieve encoded bytes from encoder.
		buf := &bytes.Buffer{}
		_, err := EncodeInt64(slices.Clone(values), buf)
		b := buf.Bytes()
		require.NoError(t, err)

		// use the matching decoder (with support for all enc types)
		res := make([]int64, len(values))
		_, err = DecodeInt64(res, b)
		require.NoError(t, err)

		// Verify that input and output values match.
		require.Equal(t, values, res)
		return true
	}, nil)
}

func TestInt64Decode_Corrupt(t *testing.T) {
	cases := []string{
		"\x10\x14",         // Packed: not enough data
		"\x20\x00",         // RLE: not enough data for starting timestamp
		"\x2012345678\x90", // RLE: initial timestamp but invalid uvarint encoding
		"\x2012345678\x7f", // RLE: timestamp, RLE but invalid repeat
		"\x00123",          // Raw: data length not multiple of 8
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%q", c), func(t *testing.T) {
			res := make([]int64, 0)
			_, err := DecodeInt64(res, []byte(c))
			require.Error(t, err)
			require.Equal(t, []int64{}, res)
		})
	}
}

func BenchmarkEncodeInt64(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[int64]() {
		buf := bytes.NewBuffer(nil)
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				_, err := EncodeInt64(c.Data, buf)
				require.NoError(b, err)
				buf.Reset()
			}
		})
	}
}

func BenchmarkDecodeUncompressedInt64(b *testing.B) {
	values := []int64{
		-2352281900722994752, 1438442655375607923, -4110452567888190110,
		-1221292455668011702, -1941700286034261841, -2836753127140407751,
		1432686216250034552, 3663244026151507025, -3068113732684750258,
		-1949953187327444488, 3713374280993588804, 3226153669854871355,
		-2093273755080502606, 1006087192578600616, -2272122301622271655,
		2533238229511593671, -4450454445568858273, 2647789901083530435,
		2761419461769776844, -1324397441074946198, -680758138988210958,
		94468846694902125, -2394093124890745254, -2682139311758778198,
	}

	for _, c := range tests.BenchmarkSizes {
		src := make([]int64, c.N)
		for i := range c.N {
			src[i] = values[util.RandIntn(len(values))]
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt64(src, buf)

		dst := make([]int64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = DecodeInt64(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkReadUncompressedInt64(b *testing.B) {
	values := []int64{
		-2352281900722994752, 1438442655375607923, -4110452567888190110,
		-1221292455668011702, -1941700286034261841, -2836753127140407751,
		1432686216250034552, 3663244026151507025, -3068113732684750258,
		-1949953187327444488, 3713374280993588804, 3226153669854871355,
		-2093273755080502606, 1006087192578600616, -2272122301622271655,
		2533238229511593671, -4450454445568858273, 2647789901083530435,
		2761419461769776844, -1324397441074946198, -680758138988210958,
		94468846694902125, -2394093124890745254, -2682139311758778198,
	}

	for _, c := range tests.BenchmarkSizes {
		src := make([]int64, c.N)
		for i := range c.N {
			src[i] = values[util.RandIntn(len(values))]
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt64(src, buf)

		dst := make([]uint64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _, _ = ReadInt64(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkDecodePackedInt64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int64, c.N)
		for i := range c.N {
			src[i] = int64(i*1000) + int64(util.RandIntn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt64(src, buf)

		dst := make([]int64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = DecodeInt64(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkReadPackedInt64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int64, c.N)
		for i := range c.N {
			src[i] = int64(i*1000) + int64(util.RandIntn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt64(src, buf)

		dst := make([]uint64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			b.ReportAllocs()
			for range b.N {
				_, _, _ = ReadInt64(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkDecodeRLEInt64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int64, c.N)
		var acc int64 = 10
		for i := range c.N {
			src[i] = acc
			acc += 10
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt64(src, buf)

		dst := make([]int64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = DecodeInt64(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkReadRLEInt64(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int64, c.N)
		var acc int64 = 10
		for i := range c.N {
			src[i] = acc
			acc += 10
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt64(src, buf)

		dst := make([]uint64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _, _ = ReadInt64(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}
