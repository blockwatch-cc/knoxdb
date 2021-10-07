// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package vec

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"testing"

	"blockwatch.cc/knoxdb/util"
)

const Int16Size = 2

func randInt16Slice(n, u int) []int16 {
	s := make([]int16, n*u)
	for i := 0; i < n; i++ {
		s[i] = int16(rand.Intn(math.MaxInt16 + 1))
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

type Int16MatchTest struct {
	name   string
	slice  []int16
	match  int16 // used for every test
	match2 int16 // used for between tests
	result []byte
	count  int64
}

var (
	int16TestSlice_0 = []int16{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	int16EqualTestMatch_0  int16 = 5
	int16EqualTestResult_0       = []byte{0x6a, 0x1e, 0x48, 0x2c}

	int16NotEqualTestMatch_0  int16 = 5
	int16NotEqualTestResult_0       = []byte{0x95, 0xe1, 0xb7, 0xd3}

	int16LessTestMatch_0  int16 = 5
	int16LessTestResult_0       = []byte{0x05, 0x21, 0x27, 0x01}

	int16LessEqualTestMatch_0  int16 = 5
	int16LessEqualTestResult_0       = []byte{0x6f, 0x3f, 0x6f, 0x2d}

	int16GreaterTestMatch_0  int16 = 5
	int16GreaterTestResult_0       = []byte{0x90, 0xc0, 0x90, 0xd2}

	int16GreaterEqualTestMatch_0  int16 = 5
	int16GreaterEqualTestResult_0       = []byte{0xfa, 0xde, 0xd8, 0xfe}

	int16BetweenTestMatch_0  int16 = 5
	int16BetweenTestMatch_0b int16 = 10
	int16BetweenTestResult_0       = []byte{0xfa, 0x1e, 0xd8, 0x2c}

	// positive values only
	int16TestSlice_1 = []int16{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 5000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}
	int16EqualTestResult_1       = []byte{0x41, 0x42, 0xc4, 0x0e}
	int16EqualTestMatch_1  int16 = 5

	int16NotEqualTestResult_1       = []byte{0xbe, 0xbd, 0x3b, 0xf1}
	int16NotEqualTestMatch_1  int16 = 5

	int16LessTestResult_1       = []byte{0x0e, 0x00, 0x00, 0x00}
	int16LessTestMatch_1  int16 = 5

	int16LessEqualTestResult_1       = []byte{0x4f, 0x42, 0xc4, 0x0e}
	int16LessEqualTestMatch_1  int16 = 5

	int16GreaterTestResult_1       = []byte{0xb0, 0xbd, 0x3b, 0xf1}
	int16GreaterTestMatch_1  int16 = 5

	int16GreaterEqualTestResult_1       = []byte{0xf1, 0xff, 0xff, 0xff}
	int16GreaterEqualTestMatch_1  int16 = 5

	int16BetweenTestResult_1       = []byte{0xf1, 0x42, 0xc4, 0x0e}
	int16BetweenTestMatch_1  int16 = 5
	int16BetweenTestMatch_1b int16 = 10

	// negative and positive values mixed
	int16TestSlice_2 = []int16{
		-5, 2, -3, 5,
		7, 8, 9, -10,
		15, 50, 55, 500,
		1000, -5000, 113, 12,
		31, 32, 33, 34,
		35, -36, 37, 38,
		39, 40, -41, 42,
		43, 44, 45, -46,
	}
	int16EqualTestResult_2       = []byte{0x01, 0x0, 0x0, 0x0}
	int16EqualTestMatch_2  int16 = -5

	int16NotEqualTestResult_2       = []byte{0xfe, 0xff, 0xff, 0xff}
	int16NotEqualTestMatch_2  int16 = -5

	int16LessTestResult_2       = []byte{0x87, 0x20, 0x20, 0x84}
	int16LessTestMatch_2  int16 = 5

	int16LessEqualTestResult_2       = []byte{0x8f, 0x20, 0x20, 0x84}
	int16LessEqualTestMatch_2  int16 = 5

	int16GreaterTestResult_2       = []byte{0x70, 0xdf, 0xdf, 0x7b}
	int16GreaterTestMatch_2  int16 = 5

	int16GreaterEqualTestResult_2       = []byte{0x78, 0xdf, 0xdf, 0x7b}
	int16GreaterEqualTestMatch_2  int16 = 5

	int16BetweenTestResult_2       = []byte{0x78, 0x00, 0x00, 0x00}
	int16BetweenTestMatch_2  int16 = 5
	int16BetweenTestMatch_2b int16 = 10

	// extreme values
	int16TestSlice_3 = []int16{
		0, 0,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		0, 0,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		0, 0,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		0, 0,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
	}
	int16EqualTestResult_3       = []byte{0x80, 0x80, 0x80, 0x80}
	int16EqualTestMatch_3  int16 = math.MinInt16

	int16NotEqualTestResult_3       = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	int16NotEqualTestMatch_3  int16 = math.MinInt16

	int16LessTestResult_3       = []byte{0x0, 0x0, 0x0, 0x0}
	int16LessTestMatch_3  int16 = math.MinInt16

	int16LessEqualTestResult_3       = []byte{0x80, 0x80, 0x80, 0x80}
	int16LessEqualTestMatch_3  int16 = math.MinInt16

	int16GreaterTestResult_3       = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	int16GreaterTestMatch_3  int16 = math.MinInt16

	int16GreaterEqualTestResult_3       = []byte{0xff, 0xff, 0xff, 0xff}
	int16GreaterEqualTestMatch_3  int16 = math.MinInt16

	int16BetweenTestResult_3       = []byte{0x50, 0x50, 0x50, 0x50}
	int16BetweenTestMatch_3  int16 = math.MaxInt8
	int16BetweenTestMatch_3b int16 = math.MaxInt16
)

// creates an uint16 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - match, match2: are only copied to the resulting test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateInt16TestCase(name string, slice []int16, match, match2 int16, result []byte, length int) Int16MatchTest {
	if len(slice)%8 != 0 {
		panic("CreateInt16TestCase: length of slice has to be a multiple of 8")
	}
	if len(result) != bitFieldLen(len(slice)) {
		panic("CreateInt16TestCase: length of slice and length of result does not match")
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []int16
	var l int = length
	for l > 0 {
		new_slice = append(new_slice, slice...)
		l -= len(slice)
	}

	// create new result by concat of given result
	new_result := make([]byte, bitFieldLen(length))
	for i, _ := range new_result {
		new_result[i] = result[i%len(result)]
	}
	// clear the last unused bits
	if length%8 != 0 {
		new_result[len(new_result)-1] &= 0xff >> (8 - length%8)
	}
	// count number of ones
	var cnt int
	for _, v := range new_result {
		cnt += bits.OnesCount8(v)
	}
	return Int16MatchTest{
		name:   name,
		slice:  new_slice[:length],
		match:  match,
		match2: match2,
		result: new_result,
		count:  int64(cnt),
	}
}

// -----------------------------------------------------------------------------
// Equal Testcases
//
var int16EqualCases = []Int16MatchTest{
	{
		name:   "l0",
		slice:  make([]int16, 0),
		match:  int16EqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int16EqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt16TestCase("vec1", int16TestSlice_0, int16EqualTestMatch_0, 0, int16EqualTestResult_0, 32),
	CreateInt16TestCase("vec2", int16TestSlice_0, int16EqualTestMatch_0, 0, int16EqualTestResult_0, 256),
	CreateInt16TestCase("l32", int16TestSlice_1, int16EqualTestMatch_1, 0, int16EqualTestResult_1, 32),
	CreateInt16TestCase("l64", append(int16TestSlice_1, int16TestSlice_0...), int16EqualTestMatch_1, 0,
		append(int16EqualTestResult_1, int16EqualTestResult_0...), 64),
	CreateInt16TestCase("l128", append(int16TestSlice_1, int16TestSlice_0...), int16EqualTestMatch_1, 0,
		append(int16EqualTestResult_1, int16EqualTestResult_0...), 128),
	CreateInt16TestCase("l256", append(int16TestSlice_1, int16TestSlice_0...), int16EqualTestMatch_1, 0,
		append(int16EqualTestResult_1, int16EqualTestResult_0...), 256),
	CreateInt16TestCase("l512", append(int16TestSlice_1, int16TestSlice_0...), int16EqualTestMatch_1, 0,
		append(int16EqualTestResult_1, int16EqualTestResult_0...), 512),
	CreateInt16TestCase("l511", append(int16TestSlice_1, int16TestSlice_0...), int16EqualTestMatch_1, 0,
		append(int16EqualTestResult_1, int16EqualTestResult_0...), 511),
	CreateInt16TestCase("l255", append(int16TestSlice_1, int16TestSlice_0...), int16EqualTestMatch_1, 0,
		append(int16EqualTestResult_1, int16EqualTestResult_0...), 255),
	CreateInt16TestCase("l127", int16TestSlice_1, int16EqualTestMatch_1, 0, int16EqualTestResult_1, 127),
	CreateInt16TestCase("l63", int16TestSlice_1, int16EqualTestMatch_1, 0, int16EqualTestResult_1, 63),
	CreateInt16TestCase("l31", int16TestSlice_1, int16EqualTestMatch_1, 0, int16EqualTestResult_1, 31),
	CreateInt16TestCase("l23", int16TestSlice_1, int16EqualTestMatch_1, 0, int16EqualTestResult_1, 23),
	CreateInt16TestCase("l15", int16TestSlice_1, int16EqualTestMatch_1, 0, int16EqualTestResult_1, 15),
	CreateInt16TestCase("l7", int16TestSlice_1, int16EqualTestMatch_1, 0, int16EqualTestResult_1, 7),
	CreateInt16TestCase("neg256", int16TestSlice_2, int16EqualTestMatch_2, 0, int16EqualTestResult_2, 256),
	CreateInt16TestCase("neg32", int16TestSlice_2, int16EqualTestMatch_2, 0, int16EqualTestResult_2, 32),
	CreateInt16TestCase("neg31", int16TestSlice_2, int16EqualTestMatch_2, 0, int16EqualTestResult_2, 31),
	CreateInt16TestCase("ext256", int16TestSlice_3, int16EqualTestMatch_3, 0, int16EqualTestResult_3, 256),
	CreateInt16TestCase("ext32", int16TestSlice_3, int16EqualTestMatch_3, 0, int16EqualTestResult_3, 32),
	CreateInt16TestCase("ext31", int16TestSlice_3, int16EqualTestMatch_3, 0, int16EqualTestResult_3, 31),
}

func TestMatchInt16EqualGeneric(T *testing.T) {
	for _, c := range int16EqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt16EqualGeneric(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
	}
}

func TestMatchInt16EqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range int16EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt16EqualAVX2(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
		}
	}
}

func TestMatchInt16EqualAVX512(T *testing.T) {
	if !util.UseAVX512_BW {
		T.SkipNow()
	}
	for _, c := range int16EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt16EqualAVX512(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
		}
	}
}

// -----------------------------------------------------------------------------
// Equal benchmarks
//
func BenchmarkMatchInt16EqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16EqualGeneric(a, math.MaxInt16/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt16EqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16EqualAVX2(a, math.MaxInt16/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt16EqualAVX512(B *testing.B) {
	if !util.UseAVX512_BW {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16EqualAVX512(a, math.MaxInt16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
//
var int16NotEqualCases = []Int16MatchTest{
	{
		name:   "l0",
		slice:  make([]int16, 0),
		match:  int16NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int16NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt16TestCase("vec1", int16TestSlice_0, int16NotEqualTestMatch_0, 0, int16NotEqualTestResult_0, 32),
	CreateInt16TestCase("vec2", int16TestSlice_0, int16NotEqualTestMatch_0, 0, int16NotEqualTestResult_0, 256),
	CreateInt16TestCase("l32", int16TestSlice_1, int16NotEqualTestMatch_1, 0, int16NotEqualTestResult_1, 32),
	CreateInt16TestCase("l64", append(int16TestSlice_1, int16TestSlice_0...), int16NotEqualTestMatch_1, 0,
		append(int16NotEqualTestResult_1, int16NotEqualTestResult_0...), 64),
	CreateInt16TestCase("l128", append(int16TestSlice_1, int16TestSlice_0...), int16NotEqualTestMatch_1, 0,
		append(int16NotEqualTestResult_1, int16NotEqualTestResult_0...), 128),
	CreateInt16TestCase("l256", append(int16TestSlice_1, int16TestSlice_0...), int16NotEqualTestMatch_1, 0,
		append(int16NotEqualTestResult_1, int16NotEqualTestResult_0...), 256),
	CreateInt16TestCase("l512", append(int16TestSlice_1, int16TestSlice_0...), int16NotEqualTestMatch_1, 0,
		append(int16NotEqualTestResult_1, int16NotEqualTestResult_0...), 512),
	CreateInt16TestCase("l511", append(int16TestSlice_1, int16TestSlice_0...), int16NotEqualTestMatch_1, 0,
		append(int16NotEqualTestResult_1, int16NotEqualTestResult_0...), 511),
	CreateInt16TestCase("l255", append(int16TestSlice_1, int16TestSlice_0...), int16NotEqualTestMatch_1, 0,
		append(int16NotEqualTestResult_1, int16NotEqualTestResult_0...), 255),
	CreateInt16TestCase("l127", int16TestSlice_1, int16NotEqualTestMatch_1, 0, int16NotEqualTestResult_1, 127),
	CreateInt16TestCase("l63", int16TestSlice_1, int16NotEqualTestMatch_1, 0, int16NotEqualTestResult_1, 63),
	CreateInt16TestCase("l31", int16TestSlice_1, int16NotEqualTestMatch_1, 0, int16NotEqualTestResult_1, 31),
	CreateInt16TestCase("l23", int16TestSlice_1, int16NotEqualTestMatch_1, 0, int16NotEqualTestResult_1, 23),
	CreateInt16TestCase("l15", int16TestSlice_1, int16NotEqualTestMatch_1, 0, int16NotEqualTestResult_1, 15),
	CreateInt16TestCase("l7", int16TestSlice_1, int16NotEqualTestMatch_1, 0, int16NotEqualTestResult_1, 7),
	CreateInt16TestCase("neg256", int16TestSlice_2, int16NotEqualTestMatch_2, 0, int16NotEqualTestResult_2, 256),
	CreateInt16TestCase("neg32", int16TestSlice_2, int16NotEqualTestMatch_2, 0, int16NotEqualTestResult_2, 32),
	CreateInt16TestCase("neg31", int16TestSlice_2, int16NotEqualTestMatch_2, 0, int16NotEqualTestResult_2, 31),
	CreateInt16TestCase("ext256", int16TestSlice_3, int16NotEqualTestMatch_3, 0, int16NotEqualTestResult_3, 256),
	CreateInt16TestCase("ext32", int16TestSlice_3, int16NotEqualTestMatch_3, 0, int16NotEqualTestResult_3, 32),
	CreateInt16TestCase("ext31", int16TestSlice_3, int16NotEqualTestMatch_3, 0, int16NotEqualTestResult_3, 31),
}

func TestMatchInt16NotEqualGeneric(T *testing.T) {
	for _, c := range int16NotEqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt16NotEqualGeneric(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
	}
}

func TestMatchInt16NotEqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range int16NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt16NotEqualAVX2(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
		}
	}
}

func TestMatchInt16NotEqualAVX512(T *testing.T) {
	if !util.UseAVX512_BW {
		T.SkipNow()
	}
	for _, c := range int16NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt16NotEqualAVX512(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
		}
	}
}

// -----------------------------------------------------------------------------
// Not Equal benchmarks
//
func BenchmarkMatchInt16NotEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16NotEqualGeneric(a, math.MaxInt16/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt16NotEqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16NotEqualAVX2(a, math.MaxInt16/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt16NotEqualAVX512(B *testing.B) {
	if !util.UseAVX512_BW {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16NotEqualAVX512(a, math.MaxInt16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
//
var int16LessCases = []Int16MatchTest{
	{
		name:   "l0",
		slice:  make([]int16, 0),
		match:  int16LessTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int16LessTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt16TestCase("vec1", int16TestSlice_0, int16LessTestMatch_0, 0, int16LessTestResult_0, 32),
	CreateInt16TestCase("vec2", int16TestSlice_0, int16LessTestMatch_0, 0, int16LessTestResult_0, 256),
	CreateInt16TestCase("l32", int16TestSlice_1, int16LessTestMatch_1, 0, int16LessTestResult_1, 32),
	CreateInt16TestCase("l64", append(int16TestSlice_1, int16TestSlice_0...), int16LessTestMatch_1, 0,
		append(int16LessTestResult_1, int16LessTestResult_0...), 64),
	CreateInt16TestCase("l128", append(int16TestSlice_1, int16TestSlice_0...), int16LessTestMatch_1, 0,
		append(int16LessTestResult_1, int16LessTestResult_0...), 128),
	CreateInt16TestCase("l256", append(int16TestSlice_1, int16TestSlice_0...), int16LessTestMatch_1, 0,
		append(int16LessTestResult_1, int16LessTestResult_0...), 256),
	CreateInt16TestCase("l512", append(int16TestSlice_1, int16TestSlice_0...), int16LessTestMatch_1, 0,
		append(int16LessTestResult_1, int16LessTestResult_0...), 512),
	CreateInt16TestCase("l511", append(int16TestSlice_1, int16TestSlice_0...), int16LessTestMatch_1, 0,
		append(int16LessTestResult_1, int16LessTestResult_0...), 511),
	CreateInt16TestCase("l255", append(int16TestSlice_1, int16TestSlice_0...), int16LessTestMatch_1, 0,
		append(int16LessTestResult_1, int16LessTestResult_0...), 255),
	CreateInt16TestCase("l127", int16TestSlice_1, int16LessTestMatch_1, 0, int16LessTestResult_1, 127),
	CreateInt16TestCase("l63", int16TestSlice_1, int16LessTestMatch_1, 0, int16LessTestResult_1, 63),
	CreateInt16TestCase("l31", int16TestSlice_1, int16LessTestMatch_1, 0, int16LessTestResult_1, 31),
	CreateInt16TestCase("l23", int16TestSlice_1, int16LessTestMatch_1, 0, int16LessTestResult_1, 23),
	CreateInt16TestCase("l15", int16TestSlice_1, int16LessTestMatch_1, 0, int16LessTestResult_1, 15),
	CreateInt16TestCase("l7", int16TestSlice_1, int16LessTestMatch_1, 0, int16LessTestResult_1, 7),
	CreateInt16TestCase("neg256", int16TestSlice_2, int16LessTestMatch_2, 0, int16LessTestResult_2, 256),
	CreateInt16TestCase("neg32", int16TestSlice_2, int16LessTestMatch_2, 0, int16LessTestResult_2, 32),
	CreateInt16TestCase("neg31", int16TestSlice_2, int16LessTestMatch_2, 0, int16LessTestResult_2, 31),
	CreateInt16TestCase("ext256", int16TestSlice_3, int16LessTestMatch_3, 0, int16LessTestResult_3, 256),
	CreateInt16TestCase("ext32", int16TestSlice_3, int16LessTestMatch_3, 0, int16LessTestResult_3, 32),
	CreateInt16TestCase("ext31", int16TestSlice_3, int16LessTestMatch_3, 0, int16LessTestResult_3, 31),
}

func TestMatchInt16LessGeneric(T *testing.T) {
	for _, c := range int16LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt16LessThanGeneric(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
	}
}

func TestMatchInt16LessAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range int16LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt16LessThanAVX2(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
		}
	}
}

func TestMatchInt16LessAVX512(T *testing.T) {
	if !util.UseAVX512_BW {
		T.SkipNow()
	}
	for _, c := range int16LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt16LessThanAVX512(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
		}
	}
}

// -----------------------------------------------------------------------------
// Less benchmarks
//
func BenchmarkMatchInt16LessGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16LessThanGeneric(a, math.MaxInt16/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt16LessAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16LessThanAVX2(a, math.MaxInt16/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt16LessAVX512(B *testing.B) {
	if !util.UseAVX512_BW {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16LessThanAVX512(a, math.MaxInt16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//
var int16LessEqualCases = []Int16MatchTest{
	{
		name:   "l0",
		slice:  make([]int16, 0),
		match:  int16LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int16LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt16TestCase("vec1", int16TestSlice_0, int16LessEqualTestMatch_0, 0, int16LessEqualTestResult_0, 32),
	CreateInt16TestCase("vec2", int16TestSlice_0, int16LessEqualTestMatch_0, 0, int16LessEqualTestResult_0, 256),
	CreateInt16TestCase("l32", int16TestSlice_1, int16LessEqualTestMatch_1, 0, int16LessEqualTestResult_1, 32),
	CreateInt16TestCase("l64", append(int16TestSlice_1, int16TestSlice_0...), int16LessEqualTestMatch_1, 0,
		append(int16LessEqualTestResult_1, int16LessEqualTestResult_0...), 64),
	CreateInt16TestCase("l128", append(int16TestSlice_1, int16TestSlice_0...), int16LessEqualTestMatch_1, 0,
		append(int16LessEqualTestResult_1, int16LessEqualTestResult_0...), 128),
	CreateInt16TestCase("l256", append(int16TestSlice_1, int16TestSlice_0...), int16LessEqualTestMatch_1, 0,
		append(int16LessEqualTestResult_1, int16LessEqualTestResult_0...), 256),
	CreateInt16TestCase("l512", append(int16TestSlice_1, int16TestSlice_0...), int16LessEqualTestMatch_1, 0,
		append(int16LessEqualTestResult_1, int16LessEqualTestResult_0...), 512),
	CreateInt16TestCase("l511", append(int16TestSlice_1, int16TestSlice_0...), int16LessEqualTestMatch_1, 0,
		append(int16LessEqualTestResult_1, int16LessEqualTestResult_0...), 511),
	CreateInt16TestCase("l255", append(int16TestSlice_1, int16TestSlice_0...), int16LessEqualTestMatch_1, 0,
		append(int16LessEqualTestResult_1, int16LessEqualTestResult_0...), 255),
	CreateInt16TestCase("l127", int16TestSlice_1, int16LessEqualTestMatch_1, 0, int16LessEqualTestResult_1, 127),
	CreateInt16TestCase("l63", int16TestSlice_1, int16LessEqualTestMatch_1, 0, int16LessEqualTestResult_1, 63),
	CreateInt16TestCase("l31", int16TestSlice_1, int16LessEqualTestMatch_1, 0, int16LessEqualTestResult_1, 31),
	CreateInt16TestCase("l23", int16TestSlice_1, int16LessEqualTestMatch_1, 0, int16LessEqualTestResult_1, 23),
	CreateInt16TestCase("l15", int16TestSlice_1, int16LessEqualTestMatch_1, 0, int16LessEqualTestResult_1, 15),
	CreateInt16TestCase("l7", int16TestSlice_1, int16LessEqualTestMatch_1, 0, int16LessEqualTestResult_1, 7),
	CreateInt16TestCase("neg256", int16TestSlice_2, int16LessEqualTestMatch_2, 0, int16LessEqualTestResult_2, 256),
	CreateInt16TestCase("neg32", int16TestSlice_2, int16LessEqualTestMatch_2, 0, int16LessEqualTestResult_2, 32),
	CreateInt16TestCase("neg31", int16TestSlice_2, int16LessEqualTestMatch_2, 0, int16LessEqualTestResult_2, 31),
	CreateInt16TestCase("ext256", int16TestSlice_3, int16LessEqualTestMatch_3, 0, int16LessEqualTestResult_3, 256),
	CreateInt16TestCase("ext32", int16TestSlice_3, int16LessEqualTestMatch_3, 0, int16LessEqualTestResult_3, 32),
	CreateInt16TestCase("ext31", int16TestSlice_3, int16LessEqualTestMatch_3, 0, int16LessEqualTestResult_3, 31),
}

func TestMatchInt16LessEqualGeneric(T *testing.T) {
	for _, c := range int16LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt16LessThanEqualGeneric(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
	}
}

func TestMatchInt16LessEqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range int16LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt16LessThanEqualAVX2(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
		}
	}
}

func TestMatchInt16LessEqualAVX512(T *testing.T) {
	if !util.UseAVX512_BW {
		T.SkipNow()
	}
	for _, c := range int16LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt16LessThanEqualAVX512(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
		}
	}
}

// -----------------------------------------------------------------------------
// Less equal benchmarks
//
func BenchmarkMatchInt16LessEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16LessThanEqualGeneric(a, math.MaxInt16/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt16LessEqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16LessThanEqualAVX2(a, math.MaxInt16/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt16LessEqualAVX512(B *testing.B) {
	if !util.UseAVX512_BW {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16LessThanEqualAVX512(a, math.MaxInt16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
//
var int16GreaterCases = []Int16MatchTest{
	{
		name:   "l0",
		slice:  make([]int16, 0),
		match:  int16GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int16GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt16TestCase("vec1", int16TestSlice_0, int16GreaterTestMatch_0, 0, int16GreaterTestResult_0, 32),
	CreateInt16TestCase("vec2", int16TestSlice_0, int16GreaterTestMatch_0, 0, int16GreaterTestResult_0, 256),
	CreateInt16TestCase("l32", int16TestSlice_1, int16GreaterTestMatch_1, 0, int16GreaterTestResult_1, 32),
	CreateInt16TestCase("l64", append(int16TestSlice_1, int16TestSlice_0...), int16GreaterTestMatch_1, 0,
		append(int16GreaterTestResult_1, int16GreaterTestResult_0...), 64),
	CreateInt16TestCase("l128", append(int16TestSlice_1, int16TestSlice_0...), int16GreaterTestMatch_1, 0,
		append(int16GreaterTestResult_1, int16GreaterTestResult_0...), 128),
	CreateInt16TestCase("l256", append(int16TestSlice_1, int16TestSlice_0...), int16GreaterTestMatch_1, 0,
		append(int16GreaterTestResult_1, int16GreaterTestResult_0...), 256),
	CreateInt16TestCase("l512", append(int16TestSlice_1, int16TestSlice_0...), int16GreaterTestMatch_1, 0,
		append(int16GreaterTestResult_1, int16GreaterTestResult_0...), 512),
	CreateInt16TestCase("l511", append(int16TestSlice_1, int16TestSlice_0...), int16GreaterTestMatch_1, 0,
		append(int16GreaterTestResult_1, int16GreaterTestResult_0...), 511),
	CreateInt16TestCase("l255", append(int16TestSlice_1, int16TestSlice_0...), int16GreaterTestMatch_1, 0,
		append(int16GreaterTestResult_1, int16GreaterTestResult_0...), 255),
	CreateInt16TestCase("l127", int16TestSlice_1, int16GreaterTestMatch_1, 0, int16GreaterTestResult_1, 127),
	CreateInt16TestCase("l63", int16TestSlice_1, int16GreaterTestMatch_1, 0, int16GreaterTestResult_1, 63),
	CreateInt16TestCase("l31", int16TestSlice_1, int16GreaterTestMatch_1, 0, int16GreaterTestResult_1, 31),
	CreateInt16TestCase("l23", int16TestSlice_1, int16GreaterTestMatch_1, 0, int16GreaterTestResult_1, 23),
	CreateInt16TestCase("l15", int16TestSlice_1, int16GreaterTestMatch_1, 0, int16GreaterTestResult_1, 15),
	CreateInt16TestCase("l7", int16TestSlice_1, int16GreaterTestMatch_1, 0, int16GreaterTestResult_1, 7),
	CreateInt16TestCase("neg256", int16TestSlice_2, int16GreaterTestMatch_2, 0, int16GreaterTestResult_2, 256),
	CreateInt16TestCase("neg32", int16TestSlice_2, int16GreaterTestMatch_2, 0, int16GreaterTestResult_2, 32),
	CreateInt16TestCase("neg31", int16TestSlice_2, int16GreaterTestMatch_2, 0, int16GreaterTestResult_2, 31),
	CreateInt16TestCase("ext256", int16TestSlice_3, int16GreaterTestMatch_3, 0, int16GreaterTestResult_3, 256),
	CreateInt16TestCase("ext32", int16TestSlice_3, int16GreaterTestMatch_3, 0, int16GreaterTestResult_3, 32),
	CreateInt16TestCase("ext31", int16TestSlice_3, int16GreaterTestMatch_3, 0, int16GreaterTestResult_3, 31),
}

func TestMatchInt16GreaterGeneric(T *testing.T) {
	for _, c := range int16GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt16GreaterThanGeneric(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
	}
}

func TestMatchInt16GreaterAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range int16GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt16GreaterThanAVX2(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
		}
	}
}

func TestMatchInt16GreaterAVX512(T *testing.T) {
	if !util.UseAVX512_BW {
		T.SkipNow()
	}
	for _, c := range int16GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt16GreaterThanAVX512(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
		}
	}
}

// -----------------------------------------------------------------------------
// Greater benchmarks
//
func BenchmarkMatchInt16GreaterGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16GreaterThanGeneric(a, math.MaxInt16/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt16GreaterAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16GreaterThanAVX2(a, math.MaxInt16/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt16GreaterAVX512(B *testing.B) {
	if !util.UseAVX512_BW {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16GreaterThanAVX512(a, math.MaxInt16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//
var int16GreaterEqualCases = []Int16MatchTest{
	{
		name:   "l0",
		slice:  make([]int16, 0),
		match:  int16GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int16GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt16TestCase("vec1", int16TestSlice_0, int16GreaterEqualTestMatch_0, 0, int16GreaterEqualTestResult_0, 32),
	CreateInt16TestCase("vec2", int16TestSlice_0, int16GreaterEqualTestMatch_0, 0, int16GreaterEqualTestResult_0, 256),
	CreateInt16TestCase("l32", int16TestSlice_1, int16GreaterEqualTestMatch_1, 0, int16GreaterEqualTestResult_1, 32),
	CreateInt16TestCase("l64", append(int16TestSlice_1, int16TestSlice_0...), int16GreaterEqualTestMatch_1, 0,
		append(int16GreaterEqualTestResult_1, int16GreaterEqualTestResult_0...), 64),
	CreateInt16TestCase("l128", append(int16TestSlice_1, int16TestSlice_0...), int16GreaterEqualTestMatch_1, 0,
		append(int16GreaterEqualTestResult_1, int16GreaterEqualTestResult_0...), 128),
	CreateInt16TestCase("l256", append(int16TestSlice_1, int16TestSlice_0...), int16GreaterEqualTestMatch_1, 0,
		append(int16GreaterEqualTestResult_1, int16GreaterEqualTestResult_0...), 256),
	CreateInt16TestCase("l512", append(int16TestSlice_1, int16TestSlice_0...), int16GreaterEqualTestMatch_1, 0,
		append(int16GreaterEqualTestResult_1, int16GreaterEqualTestResult_0...), 512),
	CreateInt16TestCase("l511", append(int16TestSlice_1, int16TestSlice_0...), int16GreaterEqualTestMatch_1, 0,
		append(int16GreaterEqualTestResult_1, int16GreaterEqualTestResult_0...), 511),
	CreateInt16TestCase("l255", append(int16TestSlice_1, int16TestSlice_0...), int16GreaterEqualTestMatch_1, 0,
		append(int16GreaterEqualTestResult_1, int16GreaterEqualTestResult_0...), 255),
	CreateInt16TestCase("l127", int16TestSlice_1, int16GreaterEqualTestMatch_1, 0, int16GreaterEqualTestResult_1, 127),
	CreateInt16TestCase("l63", int16TestSlice_1, int16GreaterEqualTestMatch_1, 0, int16GreaterEqualTestResult_1, 63),
	CreateInt16TestCase("l31", int16TestSlice_1, int16GreaterEqualTestMatch_1, 0, int16GreaterEqualTestResult_1, 31),
	CreateInt16TestCase("l23", int16TestSlice_1, int16GreaterEqualTestMatch_1, 0, int16GreaterEqualTestResult_1, 23),
	CreateInt16TestCase("l15", int16TestSlice_1, int16GreaterEqualTestMatch_1, 0, int16GreaterEqualTestResult_1, 15),
	CreateInt16TestCase("l7", int16TestSlice_1, int16GreaterEqualTestMatch_1, 0, int16GreaterEqualTestResult_1, 7),
	CreateInt16TestCase("neg256", int16TestSlice_2, int16GreaterEqualTestMatch_2, 0, int16GreaterEqualTestResult_2, 256),
	CreateInt16TestCase("neg32", int16TestSlice_2, int16GreaterEqualTestMatch_2, 0, int16GreaterEqualTestResult_2, 32),
	CreateInt16TestCase("neg31", int16TestSlice_2, int16GreaterEqualTestMatch_2, 0, int16GreaterEqualTestResult_2, 31),
	CreateInt16TestCase("ext256", int16TestSlice_3, int16GreaterEqualTestMatch_3, 0, int16GreaterEqualTestResult_3, 256),
	CreateInt16TestCase("ext32", int16TestSlice_3, int16GreaterEqualTestMatch_3, 0, int16GreaterEqualTestResult_3, 32),
	CreateInt16TestCase("ext31", int16TestSlice_3, int16GreaterEqualTestMatch_3, 0, int16GreaterEqualTestResult_3, 31),
}

func TestMatchInt16GreaterEqualGeneric(T *testing.T) {
	for _, c := range int16GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt16GreaterThanEqualGeneric(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
	}
}

func TestMatchInt16GreaterEqualAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range int16GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt16GreaterThanEqualAVX2(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
		}
	}
}

func TestMatchInt16GreaterEqualAVX512(T *testing.T) {
	if !util.UseAVX512_BW {
		T.SkipNow()
	}
	for _, c := range int16GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt16GreaterThanEqualAVX512(c.slice, c.match, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
		}
	}
}

// -----------------------------------------------------------------------------
// Greater equal benchmarks
//
func BenchmarkMatchInt16GreaterEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16GreaterThanEqualGeneric(a, math.MaxInt16/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt16GreaterEqualAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16GreaterThanEqualAVX2(a, math.MaxInt16/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt16GreaterEqualAVX512(B *testing.B) {
	if !util.UseAVX512_BW {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16GreaterThanEqualAVX512(a, math.MaxInt16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
//
var int16BetweenCases = []Int16MatchTest{
	{
		name:   "l0",
		slice:  make([]int16, 0),
		match:  int16BetweenTestMatch_1,
		match2: int16BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int16BetweenTestMatch_1,
		match2: int16BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	},
	CreateInt16TestCase("vec1", int16TestSlice_0, int16BetweenTestMatch_0, int16BetweenTestMatch_0b, int16BetweenTestResult_0, 32),
	CreateInt16TestCase("vec2", int16TestSlice_0, int16BetweenTestMatch_0, int16BetweenTestMatch_0b, int16BetweenTestResult_0, 256),
	CreateInt16TestCase("l32", int16TestSlice_1, int16BetweenTestMatch_1, int16BetweenTestMatch_1b, int16BetweenTestResult_1, 32),
	CreateInt16TestCase("l64", append(int16TestSlice_1, int16TestSlice_0...), int16BetweenTestMatch_1, int16BetweenTestMatch_1b,
		append(int16BetweenTestResult_1, int16BetweenTestResult_0...), 64),
	CreateInt16TestCase("l128", append(int16TestSlice_1, int16TestSlice_0...), int16BetweenTestMatch_1, int16BetweenTestMatch_1b,
		append(int16BetweenTestResult_1, int16BetweenTestResult_0...), 128),
	CreateInt16TestCase("l256", append(int16TestSlice_1, int16TestSlice_0...), int16BetweenTestMatch_1, int16BetweenTestMatch_1b,
		append(int16BetweenTestResult_1, int16BetweenTestResult_0...), 256),
	CreateInt16TestCase("l512", append(int16TestSlice_1, int16TestSlice_0...), int16BetweenTestMatch_1, int16BetweenTestMatch_1b,
		append(int16BetweenTestResult_1, int16BetweenTestResult_0...), 512),
	CreateInt16TestCase("l511", append(int16TestSlice_1, int16TestSlice_0...), int16BetweenTestMatch_1, int16BetweenTestMatch_1b,
		append(int16BetweenTestResult_1, int16BetweenTestResult_0...), 511),
	CreateInt16TestCase("l255", append(int16TestSlice_1, int16TestSlice_0...), int16BetweenTestMatch_1, int16BetweenTestMatch_1b,
		append(int16BetweenTestResult_1, int16BetweenTestResult_0...), 255),
	CreateInt16TestCase("l127", int16TestSlice_1, int16BetweenTestMatch_1, int16BetweenTestMatch_1b, int16BetweenTestResult_1, 127),
	CreateInt16TestCase("l63", int16TestSlice_1, int16BetweenTestMatch_1, int16BetweenTestMatch_1b, int16BetweenTestResult_1, 63),
	CreateInt16TestCase("l31", int16TestSlice_1, int16BetweenTestMatch_1, int16BetweenTestMatch_1b, int16BetweenTestResult_1, 31),
	CreateInt16TestCase("l23", int16TestSlice_1, int16BetweenTestMatch_1, int16BetweenTestMatch_1b, int16BetweenTestResult_1, 23),
	CreateInt16TestCase("l15", int16TestSlice_1, int16BetweenTestMatch_1, int16BetweenTestMatch_1b, int16BetweenTestResult_1, 15),
	CreateInt16TestCase("l7", int16TestSlice_1, int16BetweenTestMatch_1, int16BetweenTestMatch_1b, int16BetweenTestResult_1, 7),
	CreateInt16TestCase("neg256", int16TestSlice_2, int16BetweenTestMatch_2, int16BetweenTestMatch_2b, int16BetweenTestResult_2, 256),
	CreateInt16TestCase("neg32", int16TestSlice_2, int16BetweenTestMatch_2, int16BetweenTestMatch_2b, int16BetweenTestResult_2, 32),
	CreateInt16TestCase("neg31", int16TestSlice_2, int16BetweenTestMatch_2, int16BetweenTestMatch_2b, int16BetweenTestResult_2, 31),
	CreateInt16TestCase("ext256", int16TestSlice_3, int16BetweenTestMatch_3, int16BetweenTestMatch_3b, int16BetweenTestResult_3, 256),
	CreateInt16TestCase("ext32", int16TestSlice_3, int16BetweenTestMatch_3, int16BetweenTestMatch_3b, int16BetweenTestResult_3, 32),
	CreateInt16TestCase("ext31", int16TestSlice_3, int16BetweenTestMatch_3, int16BetweenTestMatch_3b, int16BetweenTestResult_3, 31),
}

func TestMatchInt16BetweenGeneric(T *testing.T) {
	for _, c := range int16BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt16BetweenGeneric(c.slice, c.match, c.match2, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
	}
}

func TestMatchInt16BetweenAVX2(T *testing.T) {
	if !util.UseAVX2 {
		T.SkipNow()
	}
	for _, c := range int16BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt16BetweenAVX2(c.slice, c.match, c.match2, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
		}
	}
}

func TestMatchInt16BetweenAVX512(T *testing.T) {
	if !util.UseAVX512_BW {
		T.SkipNow()
	}
	for _, c := range int16BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt16BetweenAVX512(c.slice, c.match, c.match2, bits)
		if got, want := len(bits), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if got, want := cnt, c.count; got != want {
			T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
		}
		if bytes.Compare(bits, c.result) != 0 {
			T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
		}
		if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
			T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
		}
	}
}

// -----------------------------------------------------------------------------
// Between benchmarks
//
func BenchmarkMatchInt16BetweenGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16BetweenGeneric(a, math.MaxInt16/4, math.MaxInt16/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt16BetweenAVX2(B *testing.B) {
	if !util.UseAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16BetweenAVX2(a, math.MaxInt16/4, math.MaxInt16/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt16BetweenAVX512(B *testing.B) {
	if !util.UseAVX512_BW {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16BetweenAVX512(a, math.MaxInt16/4, math.MaxInt16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Int16 Slice
//
func TestUniqueInt16(T *testing.T) {
	a := randInt16Slice(1000, 5)
	b := UniqueInt16Slice(a)
	for i, _ := range b {
		// slice must be sorted and unique
		if i > 0 && b[i-1] > b[i] {
			T.Errorf("result is unsorted at pos %d", i)
		}
		if i > 0 && b[i-1] == b[i] {
			T.Errorf("result is not unique at pos %d", i)
		}
	}
}

func BenchmarkUniqueInt16(B *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 5)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				UniqueInt16Slice(a)
			}
		})
	}
}

func TestInt16SliceContains(T *testing.T) {
	// nil slice
	if Int16.Contains(nil, 1) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Int16.Contains([]int16{}, 1) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Int16.Contains([]int16{1}, 1) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Int16.Contains([]int16{1}, 2) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !Int16.Contains([]int16{-1, 3, 5, 7, 11, 13}, -1) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Int16.Contains([]int16{-1, 3, 5, 7, 11, 13}, 5) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Int16.Contains([]int16{-1, 3, 5, 7, 11, 13}, 13) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Int16.Contains([]int16{-1, 3, 5, 7, 11, 13}, 0) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Int16.Contains([]int16{-1, 3, 5, 7, 11, 13}, 2) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Int16.Contains([]int16{-1, 3, 5, 7, 11, 13}, 14) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkInt16SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Int16.Sort(randInt16Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				Int16.Contains(a, int16(rand.Intn(math.MaxInt16+1)))
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Int16.Sort(randInt16Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				Int16.Contains(a, a[rand.Intn(len(a))])
			}
		})
	}
}

func TestInt16SliceContainsRange(T *testing.T) {
	type VecTestRange struct {
		Name  string
		From  int16
		To    int16
		Match bool
	}

	type VecTestcase struct {
		Slice  []int16
		Ranges []VecTestRange
	}

	var tests = []VecTestcase{
		// nil slice
		VecTestcase{
			Slice: nil,
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: 0, To: 2, Match: false},
			},
		},
		// empty slice
		VecTestcase{
			Slice: []int16{},
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: 0, To: 2, Match: false},
			},
		},
		// 1-element slice
		VecTestcase{
			Slice: []int16{3},
			Ranges: []VecTestRange{
				VecTestRange{Name: "A", From: 0, To: 2, Match: false},   // Case A
				VecTestRange{Name: "B1", From: 1, To: 3, Match: true},   // Case B.1, D1
				VecTestRange{Name: "B3", From: 3, To: 4, Match: true},   // Case B.3, D3
				VecTestRange{Name: "E", From: 15, To: 16, Match: false}, // Case E
				VecTestRange{Name: "F", From: 1, To: 4, Match: true},    // Case F
			},
		},
		// 1-element slice, from == to
		VecTestcase{
			Slice: []int16{3},
			Ranges: []VecTestRange{
				VecTestRange{Name: "BCD", From: 3, To: 3, Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		VecTestcase{
			Slice: []int16{3, 5, 7, 11, 13},
			Ranges: []VecTestRange{
				VecTestRange{Name: "A", From: 0, To: 2, Match: false},    // Case A
				VecTestRange{Name: "B1a", From: 1, To: 3, Match: true},   // Case B.1
				VecTestRange{Name: "B1b", From: 3, To: 3, Match: true},   // Case B.1
				VecTestRange{Name: "B2a", From: 1, To: 4, Match: true},   // Case B.2
				VecTestRange{Name: "B2b", From: 1, To: 5, Match: true},   // Case B.2
				VecTestRange{Name: "B3a", From: 3, To: 4, Match: true},   // Case B.3
				VecTestRange{Name: "B3b", From: 3, To: 5, Match: true},   // Case B.3
				VecTestRange{Name: "C1a", From: 4, To: 5, Match: true},   // Case C.1
				VecTestRange{Name: "C1b", From: 4, To: 6, Match: true},   // Case C.1
				VecTestRange{Name: "C1c", From: 4, To: 7, Match: true},   // Case C.1
				VecTestRange{Name: "C1d", From: 5, To: 5, Match: true},   // Case C.1
				VecTestRange{Name: "C2a", From: 8, To: 8, Match: false},  // Case C.2
				VecTestRange{Name: "C2b", From: 8, To: 10, Match: false}, // Case C.2
				VecTestRange{Name: "D1a", From: 11, To: 13, Match: true}, // Case D.1
				VecTestRange{Name: "D1b", From: 12, To: 13, Match: true}, // Case D.1
				VecTestRange{Name: "D2", From: 12, To: 14, Match: true},  // Case D.2
				VecTestRange{Name: "D3a", From: 13, To: 13, Match: true}, // Case D.3
				VecTestRange{Name: "D3b", From: 13, To: 14, Match: true}, // Case D.3
				VecTestRange{Name: "E", From: 15, To: 16, Match: false},  // Case E
				VecTestRange{Name: "Fa", From: 0, To: 16, Match: true},   // Case F
				VecTestRange{Name: "Fb", From: 0, To: 13, Match: true},   // Case F
				VecTestRange{Name: "Fc", From: 3, To: 13, Match: true},   // Case F
			},
		},
		// real-word testcase
		VecTestcase{
			Slice: []int16{
				6994, 13740, 16923, 17979, 18093,
				25522, 26495, 27409, 27696, 30433,
			},
			Ranges: []VecTestRange{
				VecTestRange{Name: "1", From: 27852, To: 28180, Match: false},
				VecTestRange{Name: "2", From: 28180, To: 28508, Match: false},
				VecTestRange{Name: "3", From: 28508, To: 28835, Match: false},
				VecTestRange{Name: "4", From: 28835, To: 29163, Match: false},
				VecTestRange{Name: "5", From: 29163, To: 29491, Match: false},
				VecTestRange{Name: "6", From: 29491, To: 29818, Match: false},
				VecTestRange{Name: "7", From: 29818, To: 30146, Match: false},
				VecTestRange{Name: "8", From: 30146, To: 30474, Match: true},
			},
		},
	}

	for i, v := range tests {
		for _, r := range v.Ranges {
			if want, got := r.Match, Int16.ContainsRange(v.Slice, r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkInt16SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Int16.Sort(randInt16Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := int16(rand.Intn(math.MaxInt16+1)), int16(rand.Intn(math.MaxInt16+1))
				if min > max {
					min, max = max, min
				}
				Int16.ContainsRange(a, min, max)
			}
		})
	}
}
