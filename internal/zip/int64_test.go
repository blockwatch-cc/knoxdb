// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package zip

import (
	"bytes"
	"fmt"
	"unsafe"

	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/google/go-cmp/cmp"
)

func TestEncodeInt64_Compare(t *testing.T) {
	// generate random values (should use simple8b)
	input := make([]int64, 1000)
	for i := 0; i < len(input); i++ {
		input[i] = rand.Int63n(100000) //- 50000
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
		input[i] = rand.Int63n(100000) //- 50000
	}
	testEncodeInt64_Compare(t, input, intCompressedPacked)

	// Generate large random values that are not sorted. The deltas will be large
	// and the values should be stored uncompressed.
	for i := 0; i < len(input); i++ {
		input[i] = int64(rand.Uint64())
	}
	testEncodeInt64_Compare(t, input, intUncompressed64)
}

func testEncodeInt64_Compare(t *testing.T, input []int64, encoding byte) {
	exp := make([]int64, len(input))
	copy(exp, input)

	buf := &bytes.Buffer{}
	_, err := EncodeInt64(input, buf)
	buf2 := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got, exp := buf2[0]>>4, encoding; got != exp {
		t.Fatalf("got encoding %v, expected %v", got, encoding)
	}

	result := make([]int64, len(input))
	err = DecodeInt64(unsafe.Pointer(&result), buf2)
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got := result; !reflect.DeepEqual(got, exp) {
		t.Fatalf("-got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestEncodeInt64_NoValues(t *testing.T) {
	buf := &bytes.Buffer{}
	_, err := EncodeInt64(nil, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(b) > 0 {
		t.Fatalf("unexpected encoded length %d", len(b))
	}
}

func TestEncodeInt64_Quick(t *testing.T) {
	quick.Check(func(values []int64) bool {
		// Retrieve encoded bytes from encoder.
		buf := &bytes.Buffer{}
		_, err := EncodeInt64(values, buf)
		b := buf.Bytes()
		if err != nil {
			t.Fatal(err)
		}

		// use the matching decoder (with support for all enc types)
		got := make([]int64, len(values))
		err = DecodeInt64(unsafe.Pointer(&got), b)
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
			got := make([]int64, 0)
			err := DecodeInt64(unsafe.Pointer(&got), []byte(c))
			if err == nil {
				t.Fatal("exp an err, got nil")
			}

			exp := []int64{}
			if !cmp.Equal(got, exp) {
				t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	}
}

func BenchmarkEncodeInt64stamps(b *testing.B) {
	var err error
	cases := []int{1024, 1 << 14, 1 << 16}

	for _, n := range cases {
		b.Run(fmt.Sprintf("%d_seq", n), func(b *testing.B) {
			src := make([]int64, n)
			for i := 0; i < n; i++ {
				src[i] = int64(i)
			}
			sort.Slice(src, func(i int, j int) bool { return src[i] < src[j] })

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err = EncodeInt64(src, bufResultBuffer); err != nil {
					b.Fatal(err)
				}
				bufResultBuffer.Reset()
			}
		})

		b.Run(fmt.Sprintf("%d_ran", n), func(b *testing.B) {
			src := make([]int64, n)
			for i := 0; i < n; i++ {
				src[i] = int64(rand.Uint64())
			}
			sort.Slice(src, func(i int, j int) bool { return src[i] < src[j] })

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err = EncodeInt64(src, bufResultBuffer); err != nil {
					b.Fatal(err)
				}
				bufResultBuffer.Reset()
			}
		})

		b.Run(fmt.Sprintf("%d_dup", n), func(b *testing.B) {
			src := make([]int64, n)
			for i := 0; i < n; i++ {
				src[i] = 1233242
			}

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err = EncodeInt64(src, bufResultBuffer); err != nil {
					b.Fatal(err)
				}
				bufResultBuffer.Reset()
			}
		})
	}
}

func BenchmarkInt64DecodeUncompressed(b *testing.B) {
	benchmarks := []int{
		1024,
		1 << 14,
		1 << 16,
	}

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

	for _, size := range benchmarks {
		rand.Seed(int64(size * 1e3))
		src := make([]int64, size)
		for i := 0; i < size; i++ {
			src[i] = values[rand.Int()%len(values)]
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt64(src, buf)

		dst := make([]int64, size)
		b.Run(fmt.Sprintf("buffer_%d", size), func(b *testing.B) {
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = DecodeInt64(unsafe.Pointer(&dst), buf.Bytes())
			}
		})
	}
}

func BenchmarkInt64ReadUncompressed(b *testing.B) {
	benchmarks := []int{
		1024,
		1 << 14,
		1 << 16,
	}

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

	for _, size := range benchmarks {
		rand.Seed(int64(size * 1e3))
		src := make([]int64, size)
		for i := 0; i < size; i++ {
			src[i] = values[rand.Int()%len(values)]
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt64(src, buf)

		b.Run(fmt.Sprintf("reader_%d", size), func(b *testing.B) {
			dst := make([]int64, size)
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = ReadInt64(unsafe.Pointer(&dst), bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkInt64DecodePacked(b *testing.B) {
	benchmarks := []int{
		1024,
		1 << 14,
		1 << 16,
	}
	for _, size := range benchmarks {
		rand.Seed(int64(size * 1e3))

		src := make([]int64, size)
		for i := 0; i < size; i++ {
			src[i] = int64(i*1000) + int64(rand.Intn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt64(src, buf)

		b.Run(fmt.Sprintf("buffer_%d", size), func(b *testing.B) {
			dst := make([]int64, size)
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = DecodeInt64(unsafe.Pointer(&dst), buf.Bytes())
			}
		})
	}
}

func BenchmarkInt64ReadPacked(b *testing.B) {
	benchmarks := []int{
		1024,
		1 << 14,
		1 << 16,
	}
	for _, size := range benchmarks {
		rand.Seed(int64(size * 1e3))

		src := make([]int64, size)
		for i := 0; i < size; i++ {
			src[i] = int64(i*1000) + int64(rand.Intn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt64(src, buf)

		b.Run(fmt.Sprintf("reader_%d", size), func(b *testing.B) {
			dst := make([]int64, size)
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = ReadInt64(unsafe.Pointer(&dst), bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkInt64DecodeRLE(b *testing.B) {
	benchmarks := []struct {
		n     int
		delta int64
	}{
		{1024, 10},
		{1 << 14, 10},
		{1 << 16, 10},
	}
	for _, bm := range benchmarks {
		src := make([]int64, bm.n)
		var acc int64 = bm.delta
		for i := 0; i < bm.n; i++ {
			src[i] = acc
			acc += bm.delta
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt64(src, buf)

		dst := make([]int64, bm.n)
		b.Run(fmt.Sprintf("buffer_%d_delta_%d", bm.n, bm.delta), func(b *testing.B) {
			b.SetBytes(int64(bm.n * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = DecodeInt64(unsafe.Pointer(&dst), buf.Bytes())
			}
		})
	}
}

func BenchmarkInt64ReadRLE(b *testing.B) {
	benchmarks := []struct {
		n     int
		delta int64
	}{
		{1024, 10},
		{1 << 14, 10},
		{1 << 16, 10},
	}
	for _, bm := range benchmarks {
		src := make([]int64, bm.n)
		var acc int64 = bm.delta
		for i := 0; i < bm.n; i++ {
			src[i] = acc
			acc += bm.delta
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt64(src, buf)

		b.Run(fmt.Sprintf("reader_%d_delta_%d", bm.n, bm.delta), func(b *testing.B) {
			dst := make([]int64, bm.n)
			b.SetBytes(int64(bm.n * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = ReadInt64(unsafe.Pointer(&dst), bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}
