// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package zip

import (
	"bytes"
	"fmt"

	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/google/go-cmp/cmp"
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
	exp := make([]int32, len(input))
	copy(exp, input)

	buf := &bytes.Buffer{}
	_, err := EncodeInt32(input, buf)
	buf2 := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got, exp := buf2[0]>>4, encoding; got != exp {
		t.Fatalf("got encoding %v, expected %v", got, encoding)
	}

	result := make([]int32, len(input))
	_, err = DecodeInt32(result, buf2)
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got := result; !reflect.DeepEqual(got, exp) {
		t.Fatalf("-got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestEncodeInt32_NoValues(t *testing.T) {
	buf := &bytes.Buffer{}
	_, err := EncodeInt32(nil, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(b) > 0 {
		t.Fatalf("unexpected encoded length %d", len(b))
	}
}

func TestEncodeInt32_Quick(t *testing.T) {
	quick.Check(func(values []int32) bool {
		// Retrieve encoded bytes from encoder.
		buf := &bytes.Buffer{}
		_, err := EncodeInt32(values, buf)
		b := buf.Bytes()
		if err != nil {
			t.Fatal(err)
		}

		// use the matching decoder (with support for all enc types)
		got := make([]int32, len(values))
		_, err = DecodeInt32(got, b)
		if err != nil {
			t.Fatal(err)
		}

		// Verify that input and output values match.
		exp := values
		if !reflect.DeepEqual(exp, got) {
			t.Fatalf("mismatch enc=%d scale=%d:\n\nexp=%+v\n\ngot=%+v\n\n",
				b[0]>>4, b[0]&0xf, exp, got)
		}

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
			got := make([]int32, 0)
			_, err := DecodeInt32(got, []byte(c))
			if err == nil {
				t.Fatal("exp an err, got nil")
			}

			exp := []int32{}
			if !cmp.Equal(got, exp) {
				t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	}
}

func BenchmarkEncodeInt32stamps(b *testing.B) {
	var err error
	cases := []int{1024, 1 << 14, 1 << 16}

	for _, n := range cases {
		b.Run(fmt.Sprintf("%d_seq", n), func(b *testing.B) {
			src := make([]int32, n)
			for i := 0; i < n; i++ {
				src[i] = int32(i)
			}
			sort.Slice(src, func(i int, j int) bool { return src[i] < src[j] })

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err = EncodeInt32(src, bufResultBuffer); err != nil {
					b.Fatal(err)
				}
				bufResultBuffer.Reset()
			}
		})

		b.Run(fmt.Sprintf("%d_ran", n), func(b *testing.B) {
			src := make([]int32, n)
			for i := 0; i < n; i++ {
				src[i] = int32(util.RandUint32())
			}
			sort.Slice(src, func(i int, j int) bool { return src[i] < src[j] })

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err = EncodeInt32(src, bufResultBuffer); err != nil {
					b.Fatal(err)
				}
				bufResultBuffer.Reset()
			}
		})

		b.Run(fmt.Sprintf("%d_dup", n), func(b *testing.B) {
			src := make([]int32, n)
			for i := 0; i < n; i++ {
				src[i] = 1233242
			}

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err = EncodeInt32(src, bufResultBuffer); err != nil {
					b.Fatal(err)
				}
				bufResultBuffer.Reset()
			}
		})
	}
}

func BenchmarkInt32DecodeUncompressed(b *testing.B) {
	benchmarks := []int{
		1024,
		1 << 14,
		1 << 16,
	}

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

	for _, size := range benchmarks {
		src := make([]int32, size)
		for i := 0; i < size; i++ {
			src[i] = values[util.RandInt()%len(values)]
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt32(src, buf)

		dst := make([]int32, size)
		b.Run(fmt.Sprintf("buffer_%d", size), func(b *testing.B) {
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = DecodeInt32(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkInt32ReadUncompressed(b *testing.B) {
	benchmarks := []int{
		1024,
		1 << 14,
		1 << 16,
	}

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

	for _, size := range benchmarks {
		src := make([]int32, size)
		for i := 0; i < size; i++ {
			src[i] = values[util.RandInt()%len(values)]
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt32(src, buf)

		b.Run(fmt.Sprintf("reader_%d", size), func(b *testing.B) {
			dst := make([]uint32, size)
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _ = ReadInt32(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkInt32DecodePacked(b *testing.B) {
	benchmarks := []int{
		1024,
		1 << 14,
		1 << 16,
	}
	for _, size := range benchmarks {
		src := make([]int32, size)
		for i := 0; i < size; i++ {
			src[i] = int32(i*1000) + int32(util.RandIntn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt32(src, buf)

		b.Run(fmt.Sprintf("buffer_%d", size), func(b *testing.B) {
			dst := make([]int32, size)
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = DecodeInt32(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkInt32ReadPacked(b *testing.B) {
	benchmarks := []int{
		1024,
		1 << 14,
		1 << 16,
	}
	for _, size := range benchmarks {
		src := make([]int32, size)
		for i := 0; i < size; i++ {
			src[i] = int32(i*1000) + int32(util.RandIntn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt32(src, buf)

		b.Run(fmt.Sprintf("reader_%d", size), func(b *testing.B) {
			dst := make([]uint32, size)
			b.SetBytes(int64(size * 4))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _, _ = ReadInt32(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkInt32DecodeRLE(b *testing.B) {
	benchmarks := []struct {
		n     int
		delta int32
	}{
		{1024, 10},
		{1 << 14, 10},
		{1 << 16, 10},
	}
	for _, bm := range benchmarks {
		src := make([]int32, bm.n)
		var acc int32 = bm.delta
		for i := 0; i < bm.n; i++ {
			src[i] = acc
			acc += bm.delta
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt32(src, buf)

		dst := make([]int32, bm.n)
		b.Run(fmt.Sprintf("buffer_%d_delta_%d", bm.n, bm.delta), func(b *testing.B) {
			b.SetBytes(int64(bm.n * 4))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = DecodeInt32(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkInt32ReadRLE(b *testing.B) {
	benchmarks := []struct {
		n     int
		delta int32
	}{
		{1024, 10},
		{1 << 14, 10},
		{1 << 16, 10},
	}
	for _, bm := range benchmarks {
		src := make([]int32, bm.n)
		var acc int32 = bm.delta
		for i := 0; i < bm.n; i++ {
			src[i] = acc
			acc += bm.delta
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt32(src, buf)

		b.Run(fmt.Sprintf("reader_%d_delta_%d", bm.n, bm.delta), func(b *testing.B) {
			dst := make([]uint32, bm.n)
			b.SetBytes(int64(bm.n * 4))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _ = ReadInt32(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}
