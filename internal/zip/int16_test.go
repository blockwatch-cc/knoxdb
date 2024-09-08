// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package zip

import (
	"bytes"
	"fmt"

	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/google/go-cmp/cmp"
)

func TestEncodeInt16_Compare(t *testing.T) {
	// generate random values (should use simple8b)
	input := make([]int16, 1000)
	for i := 0; i < len(input); i++ {
		input[i] = int16(rand.Int63n(1<<14 - 1))
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
		input[i] = int16(rand.Int63n(1<<14 - 1))
	}
	testEncodeInt16_Compare(t, input, intCompressedPacked)

	// Generate large random values that are not sorted. The deltas will be large
	// and the values should be stored uncompressed.
	for i := 0; i < len(input); i++ {
		input[i] = int16(rand.Uint32())
	}
	testEncodeInt16_Compare(t, input, intUncompressed16)
}

func testEncodeInt16_Compare(t *testing.T, input []int16, encoding byte) {
	exp := make([]int16, len(input))
	copy(exp, input)

	buf := &bytes.Buffer{}
	_, err := EncodeInt16(input, buf)
	buf2 := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got, exp := buf2[0]>>4, encoding; got != exp {
		t.Fatalf("got encoding %v, expected %v", got, encoding)
	}

	result := make([]int16, len(input))
	_, err = DecodeInt16(result, buf2)
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got := result; !reflect.DeepEqual(got, exp) {
		t.Fatalf("-got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestEncodeInt16_NoValues(t *testing.T) {
	buf := &bytes.Buffer{}
	_, err := EncodeInt16(nil, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(b) > 0 {
		t.Fatalf("unexpected encoded length %d", len(b))
	}
}

func TestEncodeInt16_Quick(t *testing.T) {
	quick.Check(func(values []int16) bool {
		// Retrieve encoded bytes from encoder.
		buf := &bytes.Buffer{}
		_, err := EncodeInt16(values, buf)
		b := buf.Bytes()
		if err != nil {
			t.Fatal(err)
		}

		// use the matching decoder (with support for all enc types)
		got := make([]int16, len(values))
		_, err = DecodeInt16(got, b)
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
			got := make([]int16, 0)
			_, err := DecodeInt16(got, []byte(c))
			if err == nil {
				t.Fatal("exp an err, got nil")
			}

			exp := []int16{}
			if !cmp.Equal(got, exp) {
				t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	}
}

func BenchmarkEncodeInt16stamps(b *testing.B) {
	var err error
	cases := []int{1024, 1 << 14, 1 << 16}

	for _, n := range cases {
		b.Run(fmt.Sprintf("%d_seq", n), func(b *testing.B) {
			src := make([]int16, n)
			for i := 0; i < n; i++ {
				src[i] = int16(i)
			}
			sort.Slice(src, func(i int, j int) bool { return src[i] < src[j] })

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err = EncodeInt16(src, bufResultBuffer); err != nil {
					b.Fatal(err)
				}
				bufResultBuffer.Reset()
			}
		})

		b.Run(fmt.Sprintf("%d_ran", n), func(b *testing.B) {
			src := make([]int16, n)
			for i := 0; i < n; i++ {
				src[i] = int16(rand.Uint32())
			}
			sort.Slice(src, func(i int, j int) bool { return src[i] < src[j] })

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err = EncodeInt16(src, bufResultBuffer); err != nil {
					b.Fatal(err)
				}
				bufResultBuffer.Reset()
			}
		})

		b.Run(fmt.Sprintf("%d_dup", n), func(b *testing.B) {
			src := make([]int16, n)
			for i := 0; i < n; i++ {
				src[i] = 12316
			}

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err = EncodeInt16(src, bufResultBuffer); err != nil {
					b.Fatal(err)
				}
				bufResultBuffer.Reset()
			}
		})
	}
}

func BenchmarkInt16DecodeUncompressed(b *testing.B) {
	benchmarks := []int{
		1024,
		1 << 14,
		1 << 16,
	}

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

	for _, size := range benchmarks {
		rand.Seed(int64(size * 1e3))
		src := make([]int16, size)
		for i := 0; i < size; i++ {
			src[i] = values[rand.Int()%len(values)]
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt16(src, buf)

		dst := make([]int16, size)
		b.Run(fmt.Sprintf("buffer_%d", size), func(b *testing.B) {
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = DecodeInt16(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkInt16ReadUncompressed(b *testing.B) {
	benchmarks := []int{
		1024,
		1 << 14,
		1 << 16,
	}

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

	for _, size := range benchmarks {
		rand.Seed(int64(size * 1e3))
		src := make([]int16, size)
		for i := 0; i < size; i++ {
			src[i] = values[rand.Int()%len(values)]
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt16(src, buf)

		b.Run(fmt.Sprintf("reader_%d", size), func(b *testing.B) {
			dst := make([]uint16, size)
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _ = ReadInt16(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkInt16DecodePacked(b *testing.B) {
	benchmarks := []int{
		1024,
		1 << 14,
		1 << 16,
	}
	for _, size := range benchmarks {
		rand.Seed(int64(size * 1e3))

		src := make([]int16, size)
		for i := 0; i < size; i++ {
			src[i] = int16(i*1000) + int16(rand.Intn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt16(src, buf)

		b.Run(fmt.Sprintf("buffer_%d", size), func(b *testing.B) {
			dst := make([]int16, size)
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = DecodeInt16(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkInt16ReadPacked(b *testing.B) {
	benchmarks := []int{
		1024,
		1 << 14,
		1 << 16,
	}
	for _, size := range benchmarks {
		rand.Seed(int64(size * 1e3))

		src := make([]int16, size)
		for i := 0; i < size; i++ {
			src[i] = int16(i*1000) + int16(rand.Intn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt16(src, buf)

		b.Run(fmt.Sprintf("reader_%d", size), func(b *testing.B) {
			dst := make([]uint16, size)
			b.SetBytes(int64(size * 4))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _, _ = ReadInt16(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkInt16DecodeRLE(b *testing.B) {
	benchmarks := []struct {
		n     int
		delta int16
	}{
		{1024, 10},
		{1 << 14, 10},
		{1 << 16, 10},
	}
	for _, bm := range benchmarks {
		src := make([]int16, bm.n)
		var acc int16 = bm.delta
		for i := 0; i < bm.n; i++ {
			src[i] = acc
			acc += bm.delta
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt16(src, buf)

		dst := make([]int16, bm.n)
		b.Run(fmt.Sprintf("buffer_%d_delta_%d", bm.n, bm.delta), func(b *testing.B) {
			b.SetBytes(int64(bm.n * 4))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = DecodeInt16(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkInt16ReadRLE(b *testing.B) {
	benchmarks := []struct {
		n     int
		delta int16
	}{
		{1024, 10},
		{1 << 14, 10},
		{1 << 16, 10},
	}
	for _, bm := range benchmarks {
		src := make([]int16, bm.n)
		var acc int16 = bm.delta
		for i := 0; i < bm.n; i++ {
			src[i] = acc
			acc += bm.delta
		}
		buf := bytes.NewBuffer(nil)
		EncodeInt16(src, buf)

		b.Run(fmt.Sprintf("reader_%d_delta_%d", bm.n, bm.delta), func(b *testing.B) {
			dst := make([]uint16, bm.n)
			b.SetBytes(int64(bm.n * 4))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _ = ReadInt16(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}
