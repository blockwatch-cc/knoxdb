// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package zip

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"reflect"
	"sort"
	"testing"
	"testing/quick"
	"time"

	"blockwatch.cc/knoxdb/internal/encode/s8b"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/google/go-cmp/cmp"
)

var (
	// bufResult       []byte
	bufResultBuffer = &bytes.Buffer{}
)

// func dumpBufs(a, b []byte) {
// 	longest := len(a)
// 	if len(b) > longest {
// 		longest = len(b)
// 	}

// 	for i := 0; i < longest; i++ {
// 		var as, bs string
// 		if i < len(a) {
// 			as = fmt.Sprintf("%08[1]b (%[1]d)", a[i])
// 		}
// 		if i < len(b) {
// 			bs = fmt.Sprintf("%08[1]b (%[1]d)", b[i])
// 		}

// 		same := as == bs
// 		fmt.Printf("%d (%d) %s - %s :: %v\n", i, i*8, as, bs, same)
// 	}
// 	fmt.Println()
// }

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
	exp := make([]int64, len(input))
	copy(exp, input)

	buf := &bytes.Buffer{}
	_, err := EncodeTime(input, buf)
	buf2 := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got, exp := buf2[0]>>4, encoding; got != exp {
		t.Fatalf("got encoding %v, expected %v", got, encoding)
	}

	result := make([]int64, len(input))
	_, err = DecodeTime(result, buf2)
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got := result; !reflect.DeepEqual(got, exp) {
		t.Fatalf("-got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestEncodeTime_NoValues(t *testing.T) {
	buf := &bytes.Buffer{}
	_, err := EncodeTime(nil, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(b) > 0 {
		t.Fatalf("unexpected encoded length %d", len(b))
	}
}

func TestEncodeTime_Large_Range(t *testing.T) {
	src := []int64{1442369134000000000, 1442369135000000000}
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := EncodeTime(src, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	// use the matching decoder (with support for all enc types)
	got := make([]int64, len(src))
	_, err = DecodeTime(got, b)
	if err != nil {
		t.Fatal(err)
	}

	// Verify that input and output values match.
	if !reflect.DeepEqual(exp, got) {
		t.Fatalf("mismatch enc=%d scale=%d:\n\nexp=%+v\n\ngot=%+v\n\n",
			b[0]>>4, b[0]&0xf, exp, got)
	}
}

func TestEncodeTime_Uncompressed(t *testing.T) {
	src := []int64{time.Unix(0, 0).UnixNano(), time.Unix(1, 0).UnixNano()}

	// about 36.5yrs in NS resolution is max range for compressed format
	// This should cause the encoding to fallback to raw points
	src = append(src, time.Unix(2, (2<<59)).UnixNano())
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := EncodeTime(src, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("expected error: %v", err)
	}

	if exp := 25; len(b) != exp {
		t.Fatalf("length mismatch: got %v, exp %v", len(b), exp)
	}

	if got := b[0] >> 4; got != timeUncompressed {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	// use the matching decoder (with support for all enc types)
	got := make([]int64, len(src))
	_, err = DecodeTime(got, b)
	if err != nil {
		t.Fatal(err)
	}

	// Verify that input and output values match.
	if !reflect.DeepEqual(exp, got) {
		t.Fatalf("mismatch enc=%d scale=%d:\n\nexp=%+v\n\ngot=%+v\n\n",
			b[0]>>4, b[0]&0xf, exp, got)
	}
}

func TestEncodeTime_RLE(t *testing.T) {
	var src []int64
	for i := 0; i < 500; i++ {
		src = append(src, int64(i))
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := EncodeTime(src, buf)
	b := buf.Bytes()
	if exp := 12; len(b) != exp {
		t.Fatalf("length mismatch: got %v, exp %v", len(b), exp)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// use the matching decoder (with support for all enc types)
	got := make([]int64, len(src))
	_, err = DecodeTime(got, b)
	if err != nil {
		t.Fatal(err)
	}

	// Verify that input and output values match.
	if !reflect.DeepEqual(exp, got) {
		t.Fatalf("mismatch enc=%d scale=%d:\n\nexp=%+v\n\ngot=%+v\n\n",
			b[0]>>4, b[0]&0xf, exp, got)
	}
}

func TestEncodeTime_Reverse(t *testing.T) {
	src := []int64{3, 2, 0}
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := EncodeTime(src, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedZigZagPacked {
		t.Fatalf("Wrong encoding used: expected uncompressed zigzag, got %v", got)
	}

	// use the matching decoder (with support for all enc types)
	got := make([]int64, len(src))
	_, err = DecodeTime(got, b)
	if err != nil {
		t.Fatal(err)
	}

	// Verify that input and output values match.
	if !reflect.DeepEqual(exp, got) {
		t.Fatalf("mismatch enc=%d scale=%d:\n\nexp=%+v\n\ngot=%+v\n\n",
			b[0]>>4, b[0]&0xf, exp, got)
	}
}

func TestEncodeTime_220SecondDelta(t *testing.T) {
	var src []int64
	now := time.Now()

	for i := 0; i < 220; i++ {
		src = append(src, now.Truncate(time.Second).Add(time.Duration(i*60)*time.Second).UnixNano())
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := EncodeTime(src, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Using RLE, should get 12 bytes
	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	if got := b[0] & 0xf; got != 9 {
		t.Fatalf("Wrong scale used: expected 10, got %v", got)
	}

	if exp := 12; len(b) != exp {
		t.Fatalf("unexpected length: got %v, exp %v\n%s", len(b), exp, hex.Dump(b))
	}

	// use the matching decoder (with support for all enc types)
	got := make([]int64, len(src))
	_, err = DecodeTime(got, b)
	if err != nil {
		t.Fatal(err)
	}

	// Verify that input and output values match.
	if !reflect.DeepEqual(exp, got) {
		t.Fatalf("mismatch enc=%d scale=%d:\n\nexp=%+v\n\ngot=%+v\n\n",
			b[0]>>4, b[0]&0xf, exp, got)
	}
}

func TestEncodeTime_Quick(t *testing.T) {
	quick.Check(func(values []int64) bool {
		// Retrieve encoded bytes from encoder.
		buf := &bytes.Buffer{}
		_, err := EncodeTime(values, buf)
		b := buf.Bytes()
		if err != nil {
			t.Fatal(err)
		}

		// use the matching decoder (with support for all enc types)
		got := make([]int64, len(values))
		_, err = DecodeTime(got, b)
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

func TestEncodeTime_RLESeconds(t *testing.T) {
	src := []int64{
		1444448158000000000,
		1444448168000000000,
		1444448178000000000,
		1444448188000000000,
		1444448198000000000,
		1444448208000000000,
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := EncodeTime(src, buf)
	b := buf.Bytes()
	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// use the matching decoder (with support for all enc types)
	got := make([]int64, len(src))
	_, err = DecodeTime(got, b)
	if err != nil {
		t.Fatal(err)
	}

	// Verify that input and output values match.
	if !reflect.DeepEqual(exp, got) {
		t.Fatalf("mismatch enc=%d scale=%d:\n\nexp=%+v\n\ngot=%+v\n\n",
			b[0]>>4, b[0]&0xf, exp, got)
	}
}

func TestEncodeTime_Count_Uncompressed(t *testing.T) {
	src := []int64{time.Unix(0, 0).UnixNano(),
		time.Unix(1, 0).UnixNano(),
	}

	// about 36.5yrs in NS resolution is max range for compressed format
	// This should cause the encoding to fallback to raw points
	src = append(src, time.Unix(2, (2<<59)).UnixNano())
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := EncodeTime(src, buf)
	b := buf.Bytes()
	if got := b[0] >> 4; got != timeUncompressed {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, exp := CountTimestamps(b), 3; got != exp {
		t.Fatalf("count mismatch: got %v, exp %v", got, exp)
	}
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
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := EncodeTime(src, buf)
	b := buf.Bytes()
	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, exp := CountTimestamps(b), len(exp); got != exp {
		t.Fatalf("count mismatch: got %v, exp %v", got, exp)
	}
}

func TestEncodeTime_Count_Simple8(t *testing.T) {
	src := []int64{0, 1, 3}

	buf := &bytes.Buffer{}
	_, err := EncodeTime(src, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedPacked {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, exp := CountTimestamps(b), 3; got != exp {
		t.Fatalf("count mismatch: got %v, exp %v", got, exp)
	}
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
			got := make([]int64, 0)
			_, err := DecodeTime(got, []byte(c))
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

func BenchmarkEncodeTimestamps(b *testing.B) {
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
				if _, err = EncodeTime(src, bufResultBuffer); err != nil {
					b.Fatal(err)
				}
				bufResultBuffer.Reset()
			}
		})

		b.Run(fmt.Sprintf("%d_ran", n), func(b *testing.B) {
			src := make([]int64, n)
			for i := 0; i < n; i++ {
				src[i] = int64(util.RandUint64())
			}
			sort.Slice(src, func(i int, j int) bool { return src[i] < src[j] })

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err = EncodeTime(src, bufResultBuffer); err != nil {
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
				if _, err = EncodeTime(src, bufResultBuffer); err != nil {
					b.Fatal(err)
				}
				bufResultBuffer.Reset()
			}
		})
	}
}

func BenchmarkTimeDecodeUncompressed(b *testing.B) {
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
		src := make([]int64, size)
		for i := 0; i < size; i++ {
			src[i] = values[util.RandInt()%len(values)]
		}
		buf := bytes.NewBuffer(nil)
		EncodeTime(src, buf)

		dst := make([]int64, size)
		b.Run(fmt.Sprintf("buffer_%d", size), func(b *testing.B) {
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = DecodeTime(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkTimeReadUncompressed(b *testing.B) {
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
		src := make([]int64, size)
		for i := 0; i < size; i++ {
			src[i] = values[util.RandInt()%len(values)]
		}
		buf := bytes.NewBuffer(nil)
		EncodeTime(src, buf)

		b.Run(fmt.Sprintf("reader_%d", size), func(b *testing.B) {
			dst := make([]int64, size)
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _ = ReadTime(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkTimeDecodePacked(b *testing.B) {
	benchmarks := []int{
		1024,
		1 << 14,
		1 << 16,
	}
	for _, size := range benchmarks {
		src := make([]int64, size)
		for i := 0; i < size; i++ {
			src[i] = int64(i*1000) + int64(util.RandIntn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeTime(src, buf)

		b.Run(fmt.Sprintf("buffer_%d", size), func(b *testing.B) {
			dst := make([]int64, size)
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = DecodeTime(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkTimeReadPacked(b *testing.B) {
	benchmarks := []int{
		1024,
		1 << 14,
		1 << 16,
	}
	for _, size := range benchmarks {
		src := make([]int64, size)
		for i := 0; i < size; i++ {
			src[i] = int64(i*1000) + int64(util.RandIntn(10))
		}
		buf := bytes.NewBuffer(nil)
		EncodeTime(src, buf)

		b.Run(fmt.Sprintf("reader_%d", size), func(b *testing.B) {
			dst := make([]int64, size)
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _, _ = ReadTime(dst, bytes.NewBuffer(buf.Bytes()))
			}
		})
	}
}

func BenchmarkTimeDecodeRLE(b *testing.B) {
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
		EncodeTime(src, buf)

		dst := make([]int64, bm.n)
		b.Run(fmt.Sprintf("buffer_%d_delta_%d", bm.n, bm.delta), func(b *testing.B) {
			b.SetBytes(int64(bm.n * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = DecodeTime(dst, buf.Bytes())
			}
		})
	}
}

func BenchmarkTimeReadRLE(b *testing.B) {
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
		EncodeTime(src, buf)

		b.Run(fmt.Sprintf("reader_%d_delta_%d", bm.n, bm.delta), func(b *testing.B) {
			dst := make([]int64, bm.n)
			b.SetBytes(int64(bm.n * 8))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
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
