// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package zip

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

	"sort"
	"testing"
	"testing/quick"
	"time"

	"blockwatch.cc/knoxdb/internal/encode/s8b"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestEncodeTime_Compare(t *testing.T) {
	// generate random values (should use simple8b)
	input := make([]int64, 1000)
	for i := 0; i < len(input); i++ {
		input[i] = util.RandInt64n(100000) //- 50000
	}
	sort.Slice(input, func(i int, j int) bool { return input[i] < input[j] })
	testEncodeTime_Compare(t, input, timeCompressedPacked)

	// Generate same values (should use RLE)
	for i := 0; i < len(input); i++ {
		input[i] = 1232342341234
	}
	testEncodeTime_Compare(t, input, timeCompressedRLE)

	// Generate large values that are sorted. The deltas will be large
	// and the values should be stored uncompressed.
	large := []int64{0, 1<<60 + 2, 2<<60 + 2}
	testEncodeTime_Compare(t, large, timeUncompressed)

	// generate random values that are unsorted (should use simple8b with zigzag)
	for i := 0; i < len(input); i++ {
		input[i] = util.RandInt64n(100000) //- 50000
	}
	testEncodeTime_Compare(t, input, timeCompressedZigZagPacked)

	// Generate large random values that are not sorted. The deltas will be large
	// and the values should be stored uncompressed.
	for i := 0; i < len(input); i++ {
		input[i] = int64(util.RandUint64())
	}
	testEncodeTime_Compare(t, input, timeUncompressed)
}

func testEncodeTime_Compare(t *testing.T, input []int64, encoding byte) {
	buf := &bytes.Buffer{}
	_, err := EncodeTime(slices.Clone(input), buf)
	buf2 := buf.Bytes()
	require.NoError(t, err, "buf: %x", buf2)
	require.Equal(t, encoding, buf2[0]>>4, "encoding")

	result := make([]int64, len(input))
	_, err = DecodeTime(result, buf2)
	require.NoError(t, err, "buf: %x", buf2)
	require.Equal(t, input, result)
}

func TestEncodeTime_NoValues(t *testing.T) {
	buf := &bytes.Buffer{}
	_, err := EncodeTime(nil, buf)
	require.NoError(t, err)
	require.Len(t, buf.Bytes(), 0)
}

func TestEncodeTime_Large_Range(t *testing.T) {
	src := []int64{1442369134000000000, 1442369135000000000}

	buf := &bytes.Buffer{}
	_, err := EncodeTime(slices.Clone(src), buf)
	b := buf.Bytes()
	require.NoError(t, err, "buf: %x", b)
	require.Equal(t, timeCompressedRLE, int(b[0]>>4), "wrong encoding")

	// use the matching decoder (with support for all enc types)
	res := make([]int64, len(src))
	_, err = DecodeTime(res, b)
	require.NoError(t, err)

	// Verify that input and output values match.
	require.Equal(t, src, res)
}

func TestEncodeTime_Uncompressed(t *testing.T) {
	src := []int64{time.Unix(0, 0).UnixNano(), time.Unix(1, 0).UnixNano()}

	// about 36.5yrs in NS resolution is max range for compressed format
	// This should cause the encoding to fallback to raw points
	src = append(src, time.Unix(2, (2<<59)).UnixNano())
	buf := &bytes.Buffer{}
	_, err := EncodeTime(slices.Clone(src), buf)
	b := buf.Bytes()
	require.NoError(t, err, "buf: %x", b)
	require.Len(t, b, 25, "len")
	require.Equal(t, timeUncompressed, int(b[0]>>4), "wrong encoding")

	// use the matching decoder (with support for all enc types)
	res := make([]int64, len(src))
	_, err = DecodeTime(res, b)
	require.NoError(t, err, "buf: %x", b)

	// Verify that input and output values match.
	require.Equal(t, src, res)
}

func TestEncodeTime_RLE(t *testing.T) {
	var src []int64
	for i := 0; i < 500; i++ {
		src = append(src, int64(i))
	}
	buf := &bytes.Buffer{}
	_, err := EncodeTime(slices.Clone(src), buf)
	b := buf.Bytes()
	require.NoError(t, err, "buf: %x", b)
	require.Len(t, b, 12, "len")
	require.Equal(t, timeCompressedRLE, int(b[0]>>4), "wrong encoding")

	// use the matching decoder (with support for all enc types)
	res := make([]int64, len(src))
	_, err = DecodeTime(res, b)
	require.NoError(t, err, "buf: %x", b)

	// Verify that input and output values match.
	require.Equal(t, src, res)
}

func TestEncodeTime_Reverse(t *testing.T) {
	src := []int64{3, 2, 0}
	buf := &bytes.Buffer{}
	_, err := EncodeTime(slices.Clone(src), buf)
	b := buf.Bytes()
	require.NoError(t, err, "buf: %x", b)
	require.Equal(t, timeCompressedZigZagPacked, int(b[0]>>4), "wrong encoding")

	// use the matching decoder (with support for all enc types)
	res := make([]int64, len(src))
	_, err = DecodeTime(res, b)
	require.NoError(t, err, "buf: %x", b)

	// Verify that input and output values match.
	require.Equal(t, src, res)
}

func TestEncodeTime_220SecondDelta(t *testing.T) {
	src := make([]int64, 0)
	now := time.Now()
	for i := range 220 {
		src = append(src, now.Truncate(time.Second).Add(time.Duration(i*60)*time.Second).UnixNano())
	}

	buf := &bytes.Buffer{}
	_, err := EncodeTime(slices.Clone(src), buf)
	b := buf.Bytes()
	require.NoError(t, err, "buf: %x", b)
	require.Equal(t, timeCompressedRLE, int(b[0]>>4), "wrong encoding")
	require.Equal(t, b[0]&0xf, byte(9), "wrong scale")
	require.Len(t, b, 12, "len")

	// use the matching decoder (with support for all enc types)
	res := make([]int64, len(src))
	_, err = DecodeTime(res, b)
	require.NoError(t, err, "buf: %x", b)

	// Verify that input and output values match.
	require.Equal(t, src, res)
}

func TestEncodeTime_Quick(t *testing.T) {
	quick.Check(func(values []int64) bool {
		// Retrieve encoded bytes from encoder.
		buf := &bytes.Buffer{}
		_, err := EncodeTime(values, buf)
		b := buf.Bytes()
		require.NoError(t, err)

		// use the matching decoder (with support for all enc types)
		res := make([]int64, len(values))
		_, err = DecodeTime(res, b)
		require.NoError(t, err)

		// Verify that input and output values match.
		require.Equal(t, values, res)
		return true
	}, nil)
}

func TestEncodeTime_RLESeconds(t *testing.T) {
	src := []int64{
		1444448158000000000,
		1444448168000000000,
		1444448178000000000,
		1444448188000000000,
		1444448198000000000,
		1444448208000000000,
	}
	buf := &bytes.Buffer{}
	_, err := EncodeTime(slices.Clone(src), buf)
	b := buf.Bytes()
	require.NoError(t, err, "buf: %x", b)
	require.Equal(t, timeCompressedRLE, int(b[0]>>4), "encoding")

	// use the matching decoder (with support for all enc types)
	res := make([]int64, len(src))
	_, err = DecodeTime(res, b)
	require.NoError(t, err, "buf: %x", b)

	// Verify that input and output values match.
	require.Equal(t, src, res)
}

func TestEncodeTime_Count_Uncompressed(t *testing.T) {
	src := []int64{time.Unix(0, 0).UnixNano(),
		time.Unix(1, 0).UnixNano(),
	}

	// about 36.5yrs in NS resolution is max range for compressed format
	// This should cause the encoding to fallback to raw points
	src = append(src, time.Unix(2, (2<<59)).UnixNano())

	buf := &bytes.Buffer{}
	_, err := EncodeTime(slices.Clone(src), buf)
	b := buf.Bytes()
	require.NoError(t, err, "buf: %x", b)
	require.Equal(t, timeUncompressed, int(b[0]>>4), "encoding")
	require.Equal(t, 3, CountTimestamps(b), "count")
}

func TestEncodeTime_Count_RLE(t *testing.T) {
	src := []int64{
		1444448158000000000,
		1444448168000000000,
		1444448178000000000,
		1444448188000000000,
		1444448198000000000,
		1444448208000000000,
	}
	buf := &bytes.Buffer{}
	_, err := EncodeTime(slices.Clone(src), buf)
	b := buf.Bytes()
	require.NoError(t, err, "buf: %x", b)
	require.Equal(t, timeCompressedRLE, int(b[0]>>4), "encoding")
	require.Equal(t, len(src), CountTimestamps(b), "count")
}

func TestEncodeTime_Count_Simple8(t *testing.T) {
	src := []int64{0, 1, 3}
	buf := &bytes.Buffer{}
	_, err := EncodeTime(slices.Clone(src), buf)
	b := buf.Bytes()
	require.NoError(t, err, "buf: %x", b)
	require.Equal(t, timeCompressedPacked, int(b[0]>>4), "encoding")
	require.Equal(t, 3, CountTimestamps(b), "count")
}

func TestTimeDecode_Corrupt(t *testing.T) {
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
			_, err := DecodeTime(res, []byte(c))
			require.Error(t, err)
			require.Equal(t, []int64{}, res)
		})
	}
}

func BenchmarkEncodeTime(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[int64]() {
		buf := bytes.NewBuffer(nil)
		slices.Sort(c.Data)
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			b.SetBytes(int64(c.N * 8))
			for range b.N {
				_, err := EncodeTime(c.Data, buf)
				require.NoError(b, err)
				buf.Reset()
			}
		})
	}
}

func BenchmarkDecodeUncompressedTime(b *testing.B) {
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
		EncodeTime(src, buf)

		dst := make([]int64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = DecodeTime(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkReadUncompressedTime(b *testing.B) {
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
		EncodeTime(src, buf)

		dst := make([]int64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _, _ = ReadTime(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkDecodePackedTime(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int64, c.N)
		for i := range c.N {
			src[i] = int64(i*1000) + int64(util.RandIntn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeTime(src, buf)

		dst := make([]int64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = DecodeTime(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkReadPackedTime(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int64, c.N)
		for i := range c.N {
			src[i] = int64(i*1000) + int64(util.RandIntn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeTime(src, buf)

		dst := make([]int64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			b.ReportAllocs()
			for range b.N {
				_, _, _ = ReadTime(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkDecodeRLETime(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int64, c.N)
		var acc int64 = 10
		for i := range c.N {
			src[i] = acc
			acc += 10
		}
		buf := bytes.NewBuffer(nil)
		EncodeTime(src, buf)

		dst := make([]int64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _ = DecodeTime(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkReadRLETime(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		src := make([]int64, c.N)
		var acc int64 = 10
		for i := range c.N {
			src[i] = acc
			acc += 10
		}
		buf := bytes.NewBuffer(nil)
		EncodeTime(src, buf)

		dst := make([]int64, c.N)
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, _, _ = ReadTime(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func CountTimestamps(b []byte) int {
	if len(b) == 0 {
		return 0
	}

	// Encoding type is stored in the 4 high bits of the first byte
	encoding := b[0] >> 4
	switch encoding {
	case timeUncompressed:
		// Uncompressed timestamps are just 8 bytes each
		return len(b[1:]) / 8
	case timeCompressedRLE, timeCompressedZigZagRLE:
		// First 9 bytes are the starting timestamp and scaling factor, skip over them
		i := 9
		// Next 1-10 bytes is our (scaled down by factor of 10) run length values
		_, n := binary.Uvarint(b[9:])
		i += n
		// Last 1-10 bytes is how many times the value repeats
		count, _ := binary.Uvarint(b[i:])
		return int(count) + 1
	case timeCompressedPacked, timeCompressedZigZagPacked:
		// First 9 bytes are the starting timestamp and scaling factor, skip over them
		count := s8b.CountValues(b[9:])
		return count + 1 // +1 is for the first uncompressed timestamp, starting timestamep in b[1:9]
	default:
		return 0
	}
}
