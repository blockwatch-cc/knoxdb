// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package compress

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"
	"time"

	"blockwatch.cc/knoxdb/encoding/s8b"
	"github.com/google/go-cmp/cmp"
)

var (
	bufResult       []byte
	bufResultBuffer = &bytes.Buffer{}
)

func dumpBufs(a, b []byte) {
	longest := len(a)
	if len(b) > longest {
		longest = len(b)
	}

	for i := 0; i < longest; i++ {
		var as, bs string
		if i < len(a) {
			as = fmt.Sprintf("%08[1]b (%[1]d)", a[i])
		}
		if i < len(b) {
			bs = fmt.Sprintf("%08[1]b (%[1]d)", b[i])
		}

		same := as == bs
		fmt.Printf("%d (%d) %s - %s :: %v\n", i, i*8, as, bs, same)
	}
	fmt.Println()
}

func TestTimeArrayEncodeAll(t *testing.T) {
	now := time.Unix(0, 0)
	src := []int64{now.UnixNano()}

	for i := 1; i < 4; i++ {
		src = append(src, now.Add(time.Duration(i)*time.Second).UnixNano())
	}

	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := TimeArrayEncodeAll(src, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	var dec TimeDecoder
	dec.Init(b)
	for i, v := range exp {
		if !dec.Next() {
			t.Fatalf("Next == false, expected true")
		}

		if v != dec.Read() {
			t.Fatalf("Item %d mismatch, got %v, exp %v", i, dec.Read(), v)
		}
	}
}

// This test compares the ArrayEncoder to the original iterator encoder, byte for
// byte.
func TestTimeArrayEncodeAll_Compare(t *testing.T) {
	// generate random values (should use simple8b)
	input := make([]int64, 1000)
	for i := 0; i < len(input); i++ {
		input[i] = rand.Int63n(100000) //- 50000
	}
	sort.Slice(input, func(i int, j int) bool { return input[i] < input[j] })
	testTimeArrayEncodeAll_Compare(t, input, timeCompressedPackedSimple)

	// Generate same values (should use RLE)
	for i := 0; i < len(input); i++ {
		input[i] = 1232342341234
	}
	testTimeArrayEncodeAll_Compare(t, input, timeCompressedRLE)

	// Generate large values that are sorted. The deltas will be large
	// and the values should be stored uncompressed.
	large := []int64{0, 1<<60 + 2, 2<<60 + 2}
	testTimeArrayEncodeAll_Compare(t, large, timeUncompressed)

	// generate random values that are unsorted (should use simple8b with zigzag)
	for i := 0; i < len(input); i++ {
		input[i] = rand.Int63n(100000) //- 50000
	}
	testTimeArrayEncodeAll_Compare(t, input, timeCompressedZigZagPacked)

	// Generate large random values that are not sorted. The deltas will be large
	// and the values should be stored zig-zag uncompressed.
	for i := 0; i < len(input); i++ {
		input[i] = int64(rand.Uint64())
	}
	testTimeArrayEncodeAll_Compare(t, input, timeUncompressedZigZag)
}

func testTimeArrayEncodeAll_Compare(t *testing.T, input []int64, encoding byte) {
	exp := make([]int64, len(input))
	copy(exp, input)

	s := NewTimeEncoder(1000)
	for _, v := range input {
		s.Write(v)
	}

	buf1, err := s.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	legacyenc := encoding
	if legacyenc > 2 {
		legacyenc = 0
	}
	if got, exp := buf1[0]>>4, legacyenc; got != exp {
		t.Fatalf("got encoding %v, expected %v", got, legacyenc)
	}

	buf := &bytes.Buffer{}
	_, err = TimeArrayEncodeAll(input, buf)
	buf2 := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got, exp := buf2[0]>>4, encoding; got != exp {
		t.Fatalf("got encoding %v, expected %v", got, encoding)
	}

	result, err := TimeArrayDecodeAll(buf2, nil)
	if err != nil {
		dumpBufs(buf1, buf2)
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got := result; !reflect.DeepEqual(got, exp) {
		t.Fatalf("-got/+exp\n%s", cmp.Diff(got, exp))
	}

	// DEPRECATED: new encoder scales _before_ delta compression, but
	//             unpacks old values correctly
	// Check that the encoders are byte for byte the same...
	// if !bytes.Equal(buf1, buf2) {
	// 	dumpBufs(buf1, buf2)
	// 	t.Fatalf("Raw bytes differ for encoders")
	// }
}

func TestTimeArrayEncodeAll_NoValues(t *testing.T) {
	buf := &bytes.Buffer{}
	_, err := TimeArrayEncodeAll(nil, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var dec TimeDecoder
	dec.Init(b)
	if dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}
}

func TestTimeArrayEncodeAll_One(t *testing.T) {
	src := []int64{0}
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := TimeArrayEncodeAll(src, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedPackedSimple {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	var dec TimeDecoder
	dec.Init(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[0] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[0])
	}
}

func TestTimeArrayEncodeAll_Two(t *testing.T) {
	src := []int64{0, 1}
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := TimeArrayEncodeAll(src, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	var dec TimeDecoder
	dec.Init(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[0] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[0])
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[1] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[1])
	}
}

/*
func TestTimeArrayEncodeAll_Three(t *testing.T) {
	src := []int64{0, 1, 3}
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := TimeArrayEncodeAll(src, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedPackedSimple {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	var dec TimeDecoder
	dec.Init(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[0] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[0])
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[1] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[1])
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[2] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[2])
	}
}
*/

func TestTimeArrayEncodeAll_Large_Range(t *testing.T) {
	src := []int64{1442369134000000000, 1442369135000000000}
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := TimeArrayEncodeAll(src, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	var dec TimeDecoder
	dec.Init(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[0] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[2])
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[1] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[1])
	}
}

func TestTimeArrayEncodeAll_Uncompressed(t *testing.T) {
	src := []int64{time.Unix(0, 0).UnixNano(), time.Unix(1, 0).UnixNano()}

	// about 36.5yrs in NS resolution is max range for compressed format
	// This should cause the encoding to fallback to raw points
	src = append(src, time.Unix(2, (2<<59)).UnixNano())
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := TimeArrayEncodeAll(src, buf)
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

	var dec TimeDecoder
	dec.Init(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[0] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[0])
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[1] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[1])
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[2] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[2])
	}
}

func TestTimeArrayEncodeAll_RLE(t *testing.T) {
	var src []int64
	for i := 0; i < 500; i++ {
		src = append(src, int64(i))
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := TimeArrayEncodeAll(src, buf)
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

	var dec TimeDecoder
	dec.Init(b)
	for i, v := range exp {
		if !dec.Next() {
			t.Fatalf("Next == false, expected true")
		}

		if v != dec.Read() {
			t.Fatalf("Item %d mismatch, got %v, exp %v", i, dec.Read(), v)
		}
	}

	if dec.Next() {
		t.Fatalf("unexpected extra values")
	}
}

func TestTimeArrayEncodeAll_Reverse(t *testing.T) {
	src := []int64{3, 2, 0}
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := TimeArrayEncodeAll(src, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedZigZagPacked {
		t.Fatalf("Wrong encoding used: expected uncompressed zigzag, got %v", got)
	}

	var dec TimeDecoder
	dec.Init(b)
	i := 0
	for dec.Next() {
		if exp[i] != dec.Read() {
			t.Fatalf("read value %d mismatch: got %v, exp %v", i, dec.Read(), exp[i])
		}
		i++
	}
}

func TestTimeArrayEncodeAll_220SecondDelta(t *testing.T) {
	var src []int64
	now := time.Now()

	for i := 0; i < 220; i++ {
		src = append(src, now.Truncate(60*time.Second).Add(time.Duration(i*60)*time.Second).UnixNano())
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := TimeArrayEncodeAll(src, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Using RLE, should get 12 bytes
	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	if got := b[0] & 0xf; got != 10 {
		t.Fatalf("Wrong scale used: expected 10, got %v", got)
	}

	if exp := 12; len(b) != exp {
		t.Fatalf("unexpected length: got %v, exp %v\n%s", len(b), exp, hex.Dump(b))
	}

	var dec TimeDecoder
	dec.Init(b)
	i := 0
	for dec.Next() {
		if exp[i] != dec.Read() {
			t.Fatalf("read value %d mismatch: got %v, exp %v", i, dec.Read(), exp[i])
		}
		i++
	}

	if i != len(exp) {
		t.Fatalf("Read too few values: exp %d, got %d", len(exp), i)
	}

	if dec.Next() {
		t.Fatalf("expecte Next() = false, got true")
	}
}

func TestTimeArrayEncodeAll_Quick(t *testing.T) {
	quick.Check(func(values []int64) bool {
		// Write values to encoder.

		exp := make([]int64, len(values))
		for i, v := range values {
			exp[i] = int64(v)
		}

		// Retrieve encoded bytes from encoder.
		buf := &bytes.Buffer{}
		_, err := TimeArrayEncodeAll(values, buf)
		b := buf.Bytes()
		if err != nil {
			t.Fatal(err)
		}

		// use the matching decoder (with support for all enc types)
		got, err := TimeArrayDecodeAll(b, nil)
		if err != nil {
			t.Fatal(err)
		}

		// DEPRECATED: new types are unsupported by the classic decoder
		// Read values out of decoder.
		// got := make([]int64, 0, len(values))
		// var dec TimeDecoder
		// dec.Init(b)
		// for dec.Next() {
		// 	if err := dec.Error(); err != nil {
		// 		t.Fatal(err)
		// 	}
		// 	got = append(got, dec.Read())
		// }

		// Verify that input and output values match.
		if !reflect.DeepEqual(exp, got) {
			t.Fatalf("mismatch enc=%d scale=%d:\n\nexp=%+v\n\ngot=%+v\n\n",
				b[0]>>4, b[0]&0xf, exp, got)
		}

		return true
	}, nil)
}

func TestTimeArrayEncodeAll_RLESeconds(t *testing.T) {
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
	_, err := TimeArrayEncodeAll(src, buf)
	b := buf.Bytes()
	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var dec TimeDecoder
	dec.Init(b)
	for i, v := range exp {
		if !dec.Next() {
			t.Fatalf("Next == false, expected true")
		}

		if v != dec.Read() {
			t.Fatalf("Item %d mismatch, got %v, exp %v", i, dec.Read(), v)
		}
	}

	if dec.Next() {
		t.Fatalf("unexpected extra values")
	}
}

func TestTimeArrayEncodeAll_Count_Uncompressed(t *testing.T) {
	src := []int64{time.Unix(0, 0).UnixNano(),
		time.Unix(1, 0).UnixNano(),
	}

	// about 36.5yrs in NS resolution is max range for compressed format
	// This should cause the encoding to fallback to raw points
	src = append(src, time.Unix(2, (2<<59)).UnixNano())
	exp := make([]int64, len(src))
	copy(exp, src)

	buf := &bytes.Buffer{}
	_, err := TimeArrayEncodeAll(src, buf)
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

func TestTimeArrayEncodeAll_Count_RLE(t *testing.T) {
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
	_, err := TimeArrayEncodeAll(src, buf)
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

func TestTimeArrayEncodeAll_Count_Simple8(t *testing.T) {
	src := []int64{0, 1, 3}

	buf := &bytes.Buffer{}
	_, err := TimeArrayEncodeAll(src, buf)
	b := buf.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedPackedSimple {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, exp := CountTimestamps(b), 3; got != exp {
		t.Fatalf("count mismatch: got %v, exp %v", got, exp)
	}
}

func TestTimeArrayDecodeAll_NoValues(t *testing.T) {
	enc := NewTimeEncoder(0)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	exp := []int64{}
	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_One(t *testing.T) {
	enc := NewTimeEncoder(1)
	exp := []int64{0}
	for _, v := range exp {
		enc.Write(v)
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedPackedSimple {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Two(t *testing.T) {
	enc := NewTimeEncoder(2)
	exp := []int64{0, 1}
	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Three(t *testing.T) {
	exp := []int64{0, 1, 3}
	tmp := make([]int64, len(exp))
	copy(tmp, exp)
	enc := &bytes.Buffer{}
	TimeArrayEncodeAll(tmp, enc)

	b := enc.Bytes()

	if got := b[0] >> 4; got != timeCompressedPackedSimple {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Large_Range(t *testing.T) {
	enc := NewTimeEncoder(2)
	exp := []int64{1442369134000000000, 1442369135000000000}
	for _, v := range exp {
		enc.Write(v)
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Uncompressed(t *testing.T) {
	enc := NewTimeEncoder(3)
	exp := []int64{
		time.Unix(0, 0).UnixNano(),
		time.Unix(1, 0).UnixNano(),
		// about 36.5yrs in NS resolution is max range for compressed format
		// This should cause the encoding to fallback to raw points
		time.Unix(2, 2<<59).UnixNano(),
	}
	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("expected error: %v", err)
	}

	if exp := 25; len(b) != exp {
		t.Fatalf("length mismatch: got %v, exp %v", len(b), exp)
	}

	if got := b[0] >> 4; got != timeUncompressed {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_RLE(t *testing.T) {
	enc := NewTimeEncoder(512)
	var exp []int64
	for i := 0; i < 500; i++ {
		exp = append(exp, int64(i))
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if exp := 12; len(b) != exp {
		t.Fatalf("length mismatch: got %v, exp %v", len(b), exp)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Reverse(t *testing.T) {
	enc := NewTimeEncoder(3)
	exp := []int64{
		int64(3),
		int64(2),
		int64(0),
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeUncompressed {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Negative(t *testing.T) {
	enc := NewTimeEncoder(3)
	exp := []int64{
		-2352281900722994752, 1438442655375607923, -4110452567888190110,
		-1221292455668011702, -1941700286034261841, -2836753127140407751,
		1432686216250034552, 3663244026151507025, -3068113732684750258,
		-1949953187327444488, 3713374280993588804, 3226153669854871355,
		-2093273755080502606, 1006087192578600616, -2272122301622271655,
		2533238229511593671, -4450454445568858273, 2647789901083530435,
		2761419461769776844, -1324397441074946198, -680758138988210958,
		94468846694902125, -2394093124890745254, -2682139311758778198,
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeUncompressed {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_220SecondDelta(t *testing.T) {
	enc := NewTimeEncoder(256)
	var exp []int64
	now := time.Now()
	for i := 0; i < 220; i++ {
		exp = append(exp, now.Add(time.Duration(i*60)*time.Second).UnixNano())
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Using RLE, should get 12 bytes
	if exp := 12; len(b) != exp {
		t.Fatalf("unexpected length: got %v, exp %v", len(b), exp)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Quick(t *testing.T) {
	quick.Check(func(values []int64) bool {
		exp := make([]int64, len(values))
		for i, v := range values {
			exp[i] = int64(v)
		}

		enc := &bytes.Buffer{}
		TimeArrayEncodeAll(values, enc)

		buf := enc.Bytes()

		got, err := TimeArrayDecodeAll(buf, nil)
		if err != nil {
			t.Fatalf("unexpected decode error %q", err)
		}

		if !cmp.Equal(got, exp) {
			t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
		}

		return true
	}, nil)
}

func TestTimeArrayDecodeAll_RLESeconds(t *testing.T) {
	enc := NewTimeEncoder(6)
	exp := make([]int64, 6)

	exp[0] = int64(1444448158000000000)
	exp[1] = int64(1444448168000000000)
	exp[2] = int64(1444448178000000000)
	exp[3] = int64(1444448188000000000)
	exp[4] = int64(1444448198000000000)
	exp[5] = int64(1444448208000000000)

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Corrupt(t *testing.T) {
	cases := []string{
		"\x10\x14",         // Packed: not enough data
		"\x20\x00",         // RLE: not enough data for starting timestamp
		"\x2012345678\x90", // RLE: initial timestamp but invalid uvarint encoding
		"\x2012345678\x7f", // RLE: timestamp, RLE but invalid repeat
		"\x00123",          // Raw: data length not multiple of 8
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%q", c), func(t *testing.T) {
			got, err := TimeArrayDecodeAll([]byte(c), nil)
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
	cases := []int{10, 100, 1000}

	for _, n := range cases {
		enc := NewTimeEncoder(n)

		b.Run(fmt.Sprintf("%d_seq", n), func(b *testing.B) {
			src := make([]int64, n)
			for i := 0; i < n; i++ {
				src[i] = int64(i)
			}
			sort.Slice(src, func(i int, j int) bool { return src[i] < src[j] })

			input := make([]int64, len(src))
			copy(input, src)

			b.Run("itr", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					enc.Reset()
					for _, x := range src {
						enc.Write(x)
					}
					if bufResult, err = enc.Bytes(); err != nil {
						b.Fatal(err)
					}

					// Since the batch encoder needs to do a copy to reset the
					// input, we will add a copy here too.
					copy(input, src)
				}
			})

			b.Run("batch", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					if _, err = TimeArrayEncodeAll(input, bufResultBuffer); err != nil {
						b.Fatal(err)
					}
					copy(input, src) // Reset input that gets modified in IntegerArrayEncodeAll
					bufResultBuffer.Reset()
				}
			})

		})

		b.Run(fmt.Sprintf("%d_ran", n), func(b *testing.B) {
			src := make([]int64, n)
			for i := 0; i < n; i++ {
				src[i] = int64(rand.Uint64())
			}
			sort.Slice(src, func(i int, j int) bool { return src[i] < src[j] })

			input := make([]int64, len(src))
			copy(input, src)

			b.Run("itr", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					enc.Reset()
					for _, x := range src {
						enc.Write(x)
					}
					if bufResult, err = enc.Bytes(); err != nil {
						b.Fatal(err)
					}

					// Since the batch encoder needs to do a copy to reset the
					// input, we will add a copy here too.
					copy(input, src)
				}
			})

			b.Run("batch", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					if _, err = TimeArrayEncodeAll(input, bufResultBuffer); err != nil {
						b.Fatal(err)
					}
					copy(input, src) // Reset input that gets modified in IntegerArrayEncodeAll
					bufResultBuffer.Reset()
				}
			})
		})

		b.Run(fmt.Sprintf("%d_dup", n), func(b *testing.B) {
			src := make([]int64, n)
			for i := 0; i < n; i++ {
				src[i] = 1233242
			}

			input := make([]int64, len(src))
			copy(input, src)

			b.Run("itr", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					enc.Reset()
					for _, x := range src {
						enc.Write(x)
					}
					if bufResult, err = enc.Bytes(); err != nil {
						b.Fatal(err)
					}

					// Since the batch encoder needs to do a copy to reset the
					// input, we will add a copy here too.
					copy(input, src)
				}
			})

			b.Run("batch", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					if _, err = TimeArrayEncodeAll(input, bufResultBuffer); err != nil {
						b.Fatal(err)
					}
					copy(input, src) // Reset input that gets modified in IntegerArrayEncodeAll
					bufResultBuffer.Reset()
				}
			})
		})
	}
}

func BenchmarkTimeArrayDecodeAllUncompressed(b *testing.B) {
	benchmarks := []int{
		5,
		55,
		555,
		1000,
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

		enc := NewTimeEncoder(size)
		for i := 0; i < size; i++ {
			enc.Write(values[rand.Int()%len(values)])
		}
		bytes, _ := enc.Bytes()

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()

			dst := make([]int64, size)
			for i := 0; i < b.N; i++ {
				dst, _ = TimeArrayDecodeAll(bytes, dst)
			}
		})
	}
}

func BenchmarkTimeArrayDecodeAllPackedSimple(b *testing.B) {
	benchmarks := []int{
		5,
		55,
		555,
		1000,
	}
	for _, size := range benchmarks {
		rand.Seed(int64(size * 1e3))

		enc := NewTimeEncoder(size)
		for i := 0; i < size; i++ {
			// Small amount of randomness prevents RLE from being used
			enc.Write(int64(i*1000) + int64(rand.Intn(10)))
		}
		bytes, _ := enc.Bytes()

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.SetBytes(int64(size * 8))
			b.ReportAllocs()

			dst := make([]int64, size)
			for i := 0; i < b.N; i++ {
				dst, _ = TimeArrayDecodeAll(bytes, dst)
			}
		})
	}
}

func BenchmarkTimeArrayDecodeAllRLE(b *testing.B) {
	benchmarks := []struct {
		n     int
		delta int64
	}{
		{5, 10},
		{55, 10},
		{555, 10},
		{1000, 10},
	}
	for _, bm := range benchmarks {
		enc := NewTimeEncoder(bm.n)
		acc := int64(0)
		for i := 0; i < bm.n; i++ {
			enc.Write(acc)
			acc += bm.delta
		}
		bytes, _ := enc.Bytes()

		b.Run(fmt.Sprintf("%d_delta_%d", bm.n, bm.delta), func(b *testing.B) {
			b.SetBytes(int64(bm.n * 8))
			b.ReportAllocs()

			dst := make([]int64, bm.n)
			for i := 0; i < b.N; i++ {
				dst, _ = TimeArrayDecodeAll(bytes, dst)
			}
		})
	}
}

// --------------------------------------------------------------
// Legacy Timestamp encoder used in tests
//

// TimeEncoder encodes time.Time to byte slices.
type TimeEncoder interface {
	Write(t int64)
	Bytes() ([]byte, error)
	Reset()
}

type encoder struct {
	ts    []uint64
	bytes []byte
	enc   *s8b.Encoder
}

// NewTimeEncoder returns a TimeEncoder with an initial buffer ready to hold sz bytes.
func NewTimeEncoder(sz int) TimeEncoder {
	return &encoder{
		ts:  make([]uint64, 0, sz),
		enc: s8b.NewEncoder(),
	}
}

// Reset sets the encoder back to its initial state.
func (e *encoder) Reset() {
	e.ts = e.ts[:0]
	e.bytes = e.bytes[:0]
	e.enc.Reset()
}

// Write adds a timestamp to the compressed stream.
func (e *encoder) Write(t int64) {
	e.ts = append(e.ts, uint64(t))
}

func (e *encoder) reduce() (max, divisor uint64, rle bool, deltas []uint64) {
	// Compute the deltas in place to avoid allocating another slice
	deltas = e.ts
	// Starting values for a max and divisor
	max, divisor = 0, 1e12

	// Indicates whether the the deltas can be run-length encoded
	rle = true

	// Iterate in reverse so we can apply deltas in place
	for i := len(deltas) - 1; i > 0; i-- {

		// First differential encode the values
		deltas[i] = deltas[i] - deltas[i-1]

		// We also need to keep track of the max value and largest common divisor
		v := deltas[i]

		if v > max {
			max = v
		}

		// If our value is divisible by 10, break.  Otherwise, try the next smallest divisor.
		for divisor > 1 && v%divisor != 0 {
			divisor /= 10
		}

		// Skip the first value || see if prev = curr.  The deltas can be RLE if the are all equal.
		rle = i == len(deltas)-1 || rle && (deltas[i+1] == deltas[i])
	}
	return
}

// Bytes returns the encoded bytes of all written times.
func (e *encoder) Bytes() ([]byte, error) {
	if len(e.ts) == 0 {
		return e.bytes[:0], nil
	}

	// Maximum and largest common divisor.  rle is true if dts (the delta timestamps),
	// are all the same.
	max, div, rle, dts := e.reduce()

	// The deltas are all the same, so we can run-length encode them
	if rle && len(e.ts) > 1 {
		return e.encodeRLE(e.ts[0], e.ts[1], div, len(e.ts))
	}

	// We can't compress this time-range, the deltas exceed 1 << 60
	if max > s8b.MaxValue {
		return e.encodeRaw()
	}

	return e.encodePacked(div, dts)
}

func (e *encoder) encodePacked(div uint64, dts []uint64) ([]byte, error) {
	// Only apply the divisor if it's greater than 1 since division is expensive.
	if div > 1 {
		for _, v := range dts[1:] {
			if err := e.enc.Write(v / div); err != nil {
				return nil, err
			}
		}
	} else {
		for _, v := range dts[1:] {
			if err := e.enc.Write(v); err != nil {
				return nil, err
			}
		}
	}

	// The compressed deltas
	deltas, err := e.enc.Bytes()
	if err != nil {
		return nil, err
	}

	sz := 8 + 1 + len(deltas)
	if cap(e.bytes) < sz {
		e.bytes = make([]byte, sz)
	}
	b := e.bytes[:sz]

	// 4 high bits used for the encoding type
	b[0] = byte(timeCompressedPackedSimple) << 4
	// 4 low bits are the log10 divisor
	b[0] |= byte(math.Log10(float64(div)))

	// The first delta value
	binary.BigEndian.PutUint64(b[1:9], uint64(dts[0]))

	copy(b[9:], deltas)
	return b[:9+len(deltas)], nil
}

func (e *encoder) encodeRaw() ([]byte, error) {
	sz := 1 + len(e.ts)*8
	if cap(e.bytes) < sz {
		e.bytes = make([]byte, sz)
	}
	b := e.bytes[:sz]
	b[0] = byte(timeUncompressed) << 4
	for i, v := range e.ts {
		binary.BigEndian.PutUint64(b[1+i*8:1+i*8+8], uint64(v))
	}
	return b, nil
}

func (e *encoder) encodeRLE(first, delta, div uint64, n int) ([]byte, error) {
	// Large varints can take up to 10 bytes, we're encoding 3 + 1 byte type
	sz := 31
	if cap(e.bytes) < sz {
		e.bytes = make([]byte, sz)
	}
	b := e.bytes[:sz]
	// 4 high bits used for the encoding type
	b[0] = byte(timeCompressedRLE) << 4
	// 4 low bits are the log10 divisor
	b[0] |= byte(math.Log10(float64(div)))

	i := 1
	// The first timestamp
	binary.BigEndian.PutUint64(b[i:], uint64(first))
	i += 8
	// The first delta
	i += binary.PutUvarint(b[i:], uint64(delta/div))
	// The number of times the delta is repeated
	i += binary.PutUvarint(b[i:], uint64(n))

	return b[:i], nil
}

// TimeDecoder decodes a byte slice into timestamps.
type TimeDecoder struct {
	v    int64
	i, n int
	ts   []uint64
	dec  s8b.Decoder
	err  error

	// The delta value for a run-length encoded byte slice
	rleDelta int64

	encoding byte
}

// Init initializes the decoder with bytes to read from.
func (d *TimeDecoder) Init(b []byte) {
	d.v = 0
	d.i = 0
	d.ts = d.ts[:0]
	d.err = nil
	if len(b) > 0 {
		// Encoding type is stored in the 4 high bits of the first byte
		d.encoding = b[0] >> 4
	}
	d.decode(b)
}

// Next returns true if there are any timestamps remaining to be decoded.
func (d *TimeDecoder) Next() bool {
	if d.err != nil {
		return false
	}

	if d.encoding == timeCompressedRLE {
		if d.i >= d.n {
			return false
		}
		d.i++
		d.v += d.rleDelta
		return d.i < d.n
	}

	if d.i >= len(d.ts) {
		return false
	}
	d.v = int64(d.ts[d.i])
	d.i++
	return true
}

// Read returns the next timestamp from the decoder.
func (d *TimeDecoder) Read() int64 {
	return d.v
}

// Error returns the last error encountered by the decoder.
func (d *TimeDecoder) Error() error {
	return d.err
}

func (d *TimeDecoder) decode(b []byte) {
	if len(b) == 0 {
		return
	}

	switch d.encoding {
	case timeUncompressed:
		d.decodeRaw(b[1:])
	case timeCompressedRLE:
		d.decodeRLE(b)
	case timeCompressedPackedSimple:
		d.decodePacked(b)
	default:
		d.err = fmt.Errorf("unknown encoding: %v", d.encoding)
	}
}

func (d *TimeDecoder) decodePacked(b []byte) {
	if len(b) < 9 {
		d.err = fmt.Errorf("timeDecoder: not enough data to decode packed timestamps")
		return
	}
	div := uint64(math.Pow10(int(b[0] & 0xF)))
	first := uint64(binary.BigEndian.Uint64(b[1:9]))

	d.dec.SetBytes(b[9:])

	d.i = 0
	deltas := d.ts[:0]
	deltas = append(deltas, first)

	for d.dec.Next() {
		deltas = append(deltas, d.dec.Read())
	}

	// Compute the prefix sum and scale the deltas back up
	last := deltas[0]
	if div > 1 {
		for i := 1; i < len(deltas); i++ {
			dgap := deltas[i] * div
			deltas[i] = last + dgap
			last = deltas[i]
		}
	} else {
		for i := 1; i < len(deltas); i++ {
			deltas[i] += last
			last = deltas[i]
		}
	}

	d.i = 0
	d.ts = deltas
}

func (d *TimeDecoder) decodeRLE(b []byte) {
	if len(b) < 9 {
		d.err = fmt.Errorf("timeDecoder: not enough data for initial RLE timestamp")
		return
	}

	var i, n int

	// Lower 4 bits hold the 10 based exponent so we can scale the values back up
	mod := int64(math.Pow10(int(b[i] & 0xF)))
	i++

	// Next 8 bytes is the starting timestamp
	first := binary.BigEndian.Uint64(b[i : i+8])
	i += 8

	// Next 1-10 bytes is our (scaled down by factor of 10) run length values
	value, n := binary.Uvarint(b[i:])
	if n <= 0 {
		d.err = fmt.Errorf("timeDecoder: invalid run length in decodeRLE")
		return
	}

	// Scale the value back up
	value *= uint64(mod)
	i += n

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[i:])
	if n <= 0 {
		d.err = fmt.Errorf("timeDecoder: invalid repeat value in decodeRLE")
		return
	}

	d.v = int64(first - value)
	d.rleDelta = int64(value)

	d.i = -1
	d.n = int(count)
}

func (d *TimeDecoder) decodeRaw(b []byte) {
	d.i = 0
	d.ts = make([]uint64, len(b)/8)
	for i := range d.ts {
		d.ts[i] = binary.BigEndian.Uint64(b[i*8 : i*8+8])

		delta := d.ts[i]
		// Compute the prefix sum and scale the deltas back up
		if i > 0 {
			d.ts[i] = d.ts[i-1] + delta
		}
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
	case timeCompressedRLE:
		// First 9 bytes are the starting timestamp and scaling factor, skip over them
		i := 9
		// Next 1-10 bytes is our (scaled down by factor of 10) run length values
		_, n := binary.Uvarint(b[9:])
		i += n
		// Last 1-10 bytes is how many times the value repeats
		count, _ := binary.Uvarint(b[i:])
		return int(count)
	case timeCompressedPackedSimple:
		// First 9 bytes are the starting timestamp and scaling factor, skip over them
		count, _ := s8b.CountValues(b[9:])
		return count + 1 // +1 is for the first uncompressed timestamp, starting timestamep in b[1:9]
	default:
		return 0
	}
}
