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
)

const Int8Size = 1

func randInt8Slice(n, u int) []int8 {
	s := make([]int8, n*u)
	for i := 0; i < n; i++ {
		s[i] = int8(rand.Intn(math.MaxInt8 + 1))
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

type Int8MatchTest struct {
	name   string
	slice  []int8
	match  int8 // used for every test
	match2 int8 // used for between tests
	result []byte
	count  int64
}

var (
	int8TestSlice_0 = []int8{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	int8EqualTestMatch_0  int8 = 5
	int8EqualTestResult_0      = []byte{0x56, 0x78, 0x12, 0x34}

	int8NotEqualTestMatch_0  int8 = 5
	int8NotEqualTestResult_0      = []byte{0xa9, 0x87, 0xed, 0xcb}

	int8LessTestMatch_0  int8 = 5
	int8LessTestResult_0      = []byte{0xa0, 0x84, 0xe4, 0x80}

	int8LessEqualTestMatch_0  int8 = 5
	int8LessEqualTestResult_0      = []byte{0xf6, 0xfc, 0xf6, 0xb4}

	int8GreaterTestMatch_0  int8 = 5
	int8GreaterTestResult_0      = []byte{0x09, 0x03, 0x09, 0x4b}

	int8GreaterEqualTestMatch_0  int8 = 5
	int8GreaterEqualTestResult_0      = []byte{0x5f, 0x7b, 0x1b, 0x7f}

	int8BetweenTestMatch_0  int8 = 5
	int8BetweenTestMatch_0b int8 = 10
	int8BetweenTestResult_0      = []byte{0x5f, 0x78, 0x1b, 0x34}

	// positive values only
	int8TestSlice_1 = []int8{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 50,
		100, 50, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}
	int8EqualTestResult_1      = []byte{0x82, 0x42, 0x23, 0x70}
	int8EqualTestMatch_1  int8 = 5

	int8NotEqualTestResult_1      = []byte{0x7d, 0xbd, 0xdc, 0x8f}
	int8NotEqualTestMatch_1  int8 = 5

	int8LessTestResult_1      = []byte{0x70, 0x00, 0x00, 0x00}
	int8LessTestMatch_1  int8 = 5

	int8LessEqualTestResult_1      = []byte{0xf2, 0x42, 0x23, 0x70}
	int8LessEqualTestMatch_1  int8 = 5

	int8GreaterTestResult_1      = []byte{0x0d, 0xbd, 0xdc, 0x8f}
	int8GreaterTestMatch_1  int8 = 5

	int8GreaterEqualTestResult_1      = []byte{0x8f, 0xff, 0xff, 0xff}
	int8GreaterEqualTestMatch_1  int8 = 5

	int8BetweenTestResult_1      = []byte{0x8f, 0x42, 0x23, 0x70}
	int8BetweenTestMatch_1  int8 = 5
	int8BetweenTestMatch_1b int8 = 10

	// negative and positive values mixed
	int8TestSlice_2 = []int8{
		-5, 2, -3, 5,
		7, 8, 9, -10,
		15, 50, 55, 50,
		100, -50, 113, 12,
		31, 32, 33, 34,
		35, -36, 37, 38,
		39, 40, -41, 42,
		43, 44, 45, -46,
	}
	int8EqualTestResult_2      = []byte{0x80, 0x0, 0x0, 0x0}
	int8EqualTestMatch_2  int8 = -5

	int8NotEqualTestResult_2      = []byte{0x7f, 0xff, 0xff, 0xff}
	int8NotEqualTestMatch_2  int8 = -5

	int8LessTestResult_2      = []byte{0xe1, 0x04, 0x04, 0x21}
	int8LessTestMatch_2  int8 = 5

	int8LessEqualTestResult_2      = []byte{0xf1, 0x04, 0x04, 0x21}
	int8LessEqualTestMatch_2  int8 = 5

	int8GreaterTestResult_2      = []byte{0x0e, 0xfb, 0xfb, 0xde}
	int8GreaterTestMatch_2  int8 = 5

	int8GreaterEqualTestResult_2      = []byte{0x1e, 0xfb, 0xfb, 0xde}
	int8GreaterEqualTestMatch_2  int8 = 5

	int8BetweenTestResult_2      = []byte{0x1e, 0x00, 0x00, 0x00}
	int8BetweenTestMatch_2  int8 = 5
	int8BetweenTestMatch_2b int8 = 10

	// extreme values
	int8TestSlice_3 = []int8{
		0, 0,
		math.MaxInt8 / 4, math.MinInt8 / 4,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
		0, 0,
		math.MaxInt8 / 4, math.MinInt8 / 4,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
		0, 0,
		math.MaxInt8 / 4, math.MinInt8 / 4,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
		0, 0,
		math.MaxInt8 / 4, math.MinInt8 / 4,
		math.MaxInt8 / 2, math.MinInt8 / 2,
		math.MaxInt8, math.MinInt8,
	}
	int8EqualTestResult_3      = []byte{0x01, 0x01, 0x01, 0x01}
	int8EqualTestMatch_3  int8 = math.MinInt8

	int8NotEqualTestResult_3      = []byte{0xfe, 0xfe, 0xfe, 0xfe}
	int8NotEqualTestMatch_3  int8 = math.MinInt8

	int8LessTestResult_3      = []byte{0x0, 0x0, 0x0, 0x00}
	int8LessTestMatch_3  int8 = math.MinInt8

	int8LessEqualTestResult_3      = []byte{0x01, 0x01, 0x01, 0x01}
	int8LessEqualTestMatch_3  int8 = math.MinInt8

	int8GreaterTestResult_3      = []byte{0xfe, 0xfe, 0xfe, 0xfe}
	int8GreaterTestMatch_3  int8 = math.MinInt8

	int8GreaterEqualTestResult_3      = []byte{0xff, 0xff, 0xff, 0xff}
	int8GreaterEqualTestMatch_3  int8 = math.MinInt8

	int8BetweenTestResult_3      = []byte{0x0a, 0x0a, 0x0a, 0x0a}
	int8BetweenTestMatch_3  int8 = math.MaxInt8 / 2
	int8BetweenTestMatch_3b int8 = math.MaxInt8
)

// creates an uint8 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - match, match2: are only copied to the resulting test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateInt8TestCase(name string, slice []int8, match, match2 int8, result []byte, length int) Int8MatchTest {
	if len(slice)%8 != 0 {
		panic("CreateInt8TestCase: length of slice has to be a multiple of 8")
	}
	if len(result) != bitFieldLen(len(slice)) {
		panic("CreateInt8TestCase: length of slice and length of result does not match")
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []int8
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
		new_result[len(new_result)-1] &= 0xff << (8 - length%8)
	}
	// count number of ones
	var cnt int
	for _, v := range new_result {
		cnt += bits.OnesCount8(v)
	}
	return Int8MatchTest{
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
var int8EqualCases = []Int8MatchTest{
	{
		name:   "l0",
		slice:  make([]int8, 0),
		match:  int8EqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int8EqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt8TestCase("vec1", int8TestSlice_0, int8EqualTestMatch_0, 0, int8EqualTestResult_0, 32),
	CreateInt8TestCase("vec2", int8TestSlice_0, int8EqualTestMatch_0, 0, int8EqualTestResult_0, 512),
	CreateInt8TestCase("l32", int8TestSlice_1, int8EqualTestMatch_1, 0, int8EqualTestResult_1, 32),
	CreateInt8TestCase("l64", append(int8TestSlice_1, int8TestSlice_0...), int8EqualTestMatch_1, 0,
		append(int8EqualTestResult_1, int8EqualTestResult_0...), 64),
	CreateInt8TestCase("l128", append(int8TestSlice_1, int8TestSlice_0...), int8EqualTestMatch_1, 0,
		append(int8EqualTestResult_1, int8EqualTestResult_0...), 128),
	CreateInt8TestCase("l256", append(int8TestSlice_1, int8TestSlice_0...), int8EqualTestMatch_1, 0,
		append(int8EqualTestResult_1, int8EqualTestResult_0...), 256),
	CreateInt8TestCase("l512", append(int8TestSlice_1, int8TestSlice_0...), int8EqualTestMatch_1, 0,
		append(int8EqualTestResult_1, int8EqualTestResult_0...), 512),
	CreateInt8TestCase("l1024", append(int8TestSlice_1, int8TestSlice_0...), int8EqualTestMatch_1, 0,
		append(int8EqualTestResult_1, int8EqualTestResult_0...), 1024),
	CreateInt8TestCase("l1023", append(int8TestSlice_1, int8TestSlice_0...), int8EqualTestMatch_1, 0,
		append(int8EqualTestResult_1, int8EqualTestResult_0...), 1023),
	CreateInt8TestCase("l511", append(int8TestSlice_1, int8TestSlice_0...), int8EqualTestMatch_1, 0,
		append(int8EqualTestResult_1, int8EqualTestResult_0...), 511),
	CreateInt8TestCase("l255", append(int8TestSlice_1, int8TestSlice_0...), int8EqualTestMatch_1, 0,
		append(int8EqualTestResult_1, int8EqualTestResult_0...), 255),
	CreateInt8TestCase("l127", int8TestSlice_1, int8EqualTestMatch_1, 0, int8EqualTestResult_1, 127),
	CreateInt8TestCase("l63", int8TestSlice_1, int8EqualTestMatch_1, 0, int8EqualTestResult_1, 63),
	CreateInt8TestCase("l31", int8TestSlice_1, int8EqualTestMatch_1, 0, int8EqualTestResult_1, 31),
	CreateInt8TestCase("l23", int8TestSlice_1, int8EqualTestMatch_1, 0, int8EqualTestResult_1, 23),
	CreateInt8TestCase("l15", int8TestSlice_1, int8EqualTestMatch_1, 0, int8EqualTestResult_1, 15),
	CreateInt8TestCase("l7", int8TestSlice_1, int8EqualTestMatch_1, 0, int8EqualTestResult_1, 7),
	CreateInt8TestCase("neg512", int8TestSlice_2, int8EqualTestMatch_2, 0, int8EqualTestResult_2, 512),
	CreateInt8TestCase("neg32", int8TestSlice_2, int8EqualTestMatch_2, 0, int8EqualTestResult_2, 32),
	CreateInt8TestCase("neg31", int8TestSlice_2, int8EqualTestMatch_2, 0, int8EqualTestResult_2, 31),
	CreateInt8TestCase("ext512", int8TestSlice_3, int8EqualTestMatch_3, 0, int8EqualTestResult_3, 512),
	CreateInt8TestCase("ext32", int8TestSlice_3, int8EqualTestMatch_3, 0, int8EqualTestResult_3, 32),
	CreateInt8TestCase("ext31", int8TestSlice_3, int8EqualTestMatch_3, 0, int8EqualTestResult_3, 31),
}

func TestMatchInt8EqualGeneric(T *testing.T) {
	for _, c := range int8EqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt8EqualGeneric(c.slice, c.match, bits)
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

func TestMatchInt8EqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range int8EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt8EqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt8EqualAVX512(T *testing.T) {
	if !useAVX512_BW {
		T.SkipNow()
	}
	for _, c := range int8EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt8EqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt8EqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8EqualGeneric(a, math.MaxInt8/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt8EqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8EqualAVX2(a, math.MaxInt8/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt8EqualAVX512(B *testing.B) {
	if !useAVX512_BW {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8EqualAVX512(a, math.MaxInt8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
//
var int8NotEqualCases = []Int8MatchTest{
	{
		name:   "l0",
		slice:  make([]int8, 0),
		match:  int8NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int8NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt8TestCase("vec1", int8TestSlice_0, int8NotEqualTestMatch_0, 0, int8NotEqualTestResult_0, 32),
	CreateInt8TestCase("vec2", int8TestSlice_0, int8NotEqualTestMatch_0, 0, int8NotEqualTestResult_0, 512),
	CreateInt8TestCase("l32", int8TestSlice_1, int8NotEqualTestMatch_1, 0, int8NotEqualTestResult_1, 32),
	CreateInt8TestCase("l64", append(int8TestSlice_1, int8TestSlice_0...), int8NotEqualTestMatch_1, 0,
		append(int8NotEqualTestResult_1, int8NotEqualTestResult_0...), 64),
	CreateInt8TestCase("l128", append(int8TestSlice_1, int8TestSlice_0...), int8NotEqualTestMatch_1, 0,
		append(int8NotEqualTestResult_1, int8NotEqualTestResult_0...), 128),
	CreateInt8TestCase("l256", append(int8TestSlice_1, int8TestSlice_0...), int8NotEqualTestMatch_1, 0,
		append(int8NotEqualTestResult_1, int8NotEqualTestResult_0...), 256),
	CreateInt8TestCase("l512", append(int8TestSlice_1, int8TestSlice_0...), int8NotEqualTestMatch_1, 0,
		append(int8NotEqualTestResult_1, int8NotEqualTestResult_0...), 512),
	CreateInt8TestCase("l1024", append(int8TestSlice_1, int8TestSlice_0...), int8NotEqualTestMatch_1, 0,
		append(int8NotEqualTestResult_1, int8NotEqualTestResult_0...), 1024),
	CreateInt8TestCase("l1023", append(int8TestSlice_1, int8TestSlice_0...), int8NotEqualTestMatch_1, 0,
		append(int8NotEqualTestResult_1, int8NotEqualTestResult_0...), 1023),
	CreateInt8TestCase("l511", append(int8TestSlice_1, int8TestSlice_0...), int8NotEqualTestMatch_1, 0,
		append(int8NotEqualTestResult_1, int8NotEqualTestResult_0...), 511),
	CreateInt8TestCase("l255", append(int8TestSlice_1, int8TestSlice_0...), int8NotEqualTestMatch_1, 0,
		append(int8NotEqualTestResult_1, int8NotEqualTestResult_0...), 255),
	CreateInt8TestCase("l127", int8TestSlice_1, int8NotEqualTestMatch_1, 0, int8NotEqualTestResult_1, 127),
	CreateInt8TestCase("l63", int8TestSlice_1, int8NotEqualTestMatch_1, 0, int8NotEqualTestResult_1, 63),
	CreateInt8TestCase("l31", int8TestSlice_1, int8NotEqualTestMatch_1, 0, int8NotEqualTestResult_1, 31),
	CreateInt8TestCase("l23", int8TestSlice_1, int8NotEqualTestMatch_1, 0, int8NotEqualTestResult_1, 23),
	CreateInt8TestCase("l15", int8TestSlice_1, int8NotEqualTestMatch_1, 0, int8NotEqualTestResult_1, 15),
	CreateInt8TestCase("l7", int8TestSlice_1, int8NotEqualTestMatch_1, 0, int8NotEqualTestResult_1, 7),
	CreateInt8TestCase("neg512", int8TestSlice_2, int8NotEqualTestMatch_2, 0, int8NotEqualTestResult_2, 512),
	CreateInt8TestCase("neg32", int8TestSlice_2, int8NotEqualTestMatch_2, 0, int8NotEqualTestResult_2, 32),
	CreateInt8TestCase("neg31", int8TestSlice_2, int8NotEqualTestMatch_2, 0, int8NotEqualTestResult_2, 31),
	CreateInt8TestCase("ext512", int8TestSlice_3, int8NotEqualTestMatch_3, 0, int8NotEqualTestResult_3, 512),
	CreateInt8TestCase("ext32", int8TestSlice_3, int8NotEqualTestMatch_3, 0, int8NotEqualTestResult_3, 32),
	CreateInt8TestCase("ext31", int8TestSlice_3, int8NotEqualTestMatch_3, 0, int8NotEqualTestResult_3, 31),
}

func TestMatchInt8NotEqualGeneric(T *testing.T) {
	for _, c := range int8NotEqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt8NotEqualGeneric(c.slice, c.match, bits)
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

func TestMatchInt8NotEqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range int8NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt8NotEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt8NotEqualAVX512(T *testing.T) {
	if !useAVX512_BW {
		T.SkipNow()
	}
	for _, c := range int8NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt8NotEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt8NotEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8NotEqualGeneric(a, math.MaxInt8/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt8NotEqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8NotEqualAVX2(a, math.MaxInt8/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt8NotEqualAVX512(B *testing.B) {
	if !useAVX512_BW {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8NotEqualAVX512(a, math.MaxInt8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
//
var int8LessCases = []Int8MatchTest{
	{
		name:   "l0",
		slice:  make([]int8, 0),
		match:  int8LessTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int8LessTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt8TestCase("vec1", int8TestSlice_0, int8LessTestMatch_0, 0, int8LessTestResult_0, 32),
	CreateInt8TestCase("vec2", int8TestSlice_0, int8LessTestMatch_0, 0, int8LessTestResult_0, 512),
	CreateInt8TestCase("l32", int8TestSlice_1, int8LessTestMatch_1, 0, int8LessTestResult_1, 32),
	CreateInt8TestCase("l64", append(int8TestSlice_1, int8TestSlice_0...), int8LessTestMatch_1, 0,
		append(int8LessTestResult_1, int8LessTestResult_0...), 64),
	CreateInt8TestCase("l128", append(int8TestSlice_1, int8TestSlice_0...), int8LessTestMatch_1, 0,
		append(int8LessTestResult_1, int8LessTestResult_0...), 128),
	CreateInt8TestCase("l256", append(int8TestSlice_1, int8TestSlice_0...), int8LessTestMatch_1, 0,
		append(int8LessTestResult_1, int8LessTestResult_0...), 256),
	CreateInt8TestCase("l512", append(int8TestSlice_1, int8TestSlice_0...), int8LessTestMatch_1, 0,
		append(int8LessTestResult_1, int8LessTestResult_0...), 512),
	CreateInt8TestCase("l1024", append(int8TestSlice_1, int8TestSlice_0...), int8LessTestMatch_1, 0,
		append(int8LessTestResult_1, int8LessTestResult_0...), 1024),
	CreateInt8TestCase("l1023", append(int8TestSlice_1, int8TestSlice_0...), int8LessTestMatch_1, 0,
		append(int8LessTestResult_1, int8LessTestResult_0...), 1023),
	CreateInt8TestCase("l511", append(int8TestSlice_1, int8TestSlice_0...), int8LessTestMatch_1, 0,
		append(int8LessTestResult_1, int8LessTestResult_0...), 511),
	CreateInt8TestCase("l255", append(int8TestSlice_1, int8TestSlice_0...), int8LessTestMatch_1, 0,
		append(int8LessTestResult_1, int8LessTestResult_0...), 255),
	CreateInt8TestCase("l127", int8TestSlice_1, int8LessTestMatch_1, 0, int8LessTestResult_1, 127),
	CreateInt8TestCase("l63", int8TestSlice_1, int8LessTestMatch_1, 0, int8LessTestResult_1, 63),
	CreateInt8TestCase("l31", int8TestSlice_1, int8LessTestMatch_1, 0, int8LessTestResult_1, 31),
	CreateInt8TestCase("l23", int8TestSlice_1, int8LessTestMatch_1, 0, int8LessTestResult_1, 23),
	CreateInt8TestCase("l15", int8TestSlice_1, int8LessTestMatch_1, 0, int8LessTestResult_1, 15),
	CreateInt8TestCase("l7", int8TestSlice_1, int8LessTestMatch_1, 0, int8LessTestResult_1, 7),
	CreateInt8TestCase("neg512", int8TestSlice_2, int8LessTestMatch_2, 0, int8LessTestResult_2, 512),
	CreateInt8TestCase("neg32", int8TestSlice_2, int8LessTestMatch_2, 0, int8LessTestResult_2, 32),
	CreateInt8TestCase("neg31", int8TestSlice_2, int8LessTestMatch_2, 0, int8LessTestResult_2, 31),
	CreateInt8TestCase("ext512", int8TestSlice_3, int8LessTestMatch_3, 0, int8LessTestResult_3, 512),
	CreateInt8TestCase("ext32", int8TestSlice_3, int8LessTestMatch_3, 0, int8LessTestResult_3, 32),
	CreateInt8TestCase("ext31", int8TestSlice_3, int8LessTestMatch_3, 0, int8LessTestResult_3, 31),
}

func TestMatchInt8LessGeneric(T *testing.T) {
	for _, c := range int8LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt8LessThanGeneric(c.slice, c.match, bits)
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

func TestMatchInt8LessAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range int8LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt8LessThanAVX2(c.slice, c.match, bits)
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

func TestMatchInt8LessAVX512(T *testing.T) {
	if !useAVX512_BW {
		T.SkipNow()
	}
	for _, c := range int8LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt8LessThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt8LessGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8LessThanGeneric(a, math.MaxInt8/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt8LessAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8LessThanAVX2(a, math.MaxInt8/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt8LessAVX512(B *testing.B) {
	if !useAVX512_BW {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8LessThanAVX512(a, math.MaxInt8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//
var int8LessEqualCases = []Int8MatchTest{
	{
		name:   "l0",
		slice:  make([]int8, 0),
		match:  int8LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int8LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt8TestCase("vec1", int8TestSlice_0, int8LessEqualTestMatch_0, 0, int8LessEqualTestResult_0, 32),
	CreateInt8TestCase("vec2", int8TestSlice_0, int8LessEqualTestMatch_0, 0, int8LessEqualTestResult_0, 512),
	CreateInt8TestCase("l32", int8TestSlice_1, int8LessEqualTestMatch_1, 0, int8LessEqualTestResult_1, 32),
	CreateInt8TestCase("l64", append(int8TestSlice_1, int8TestSlice_0...), int8LessEqualTestMatch_1, 0,
		append(int8LessEqualTestResult_1, int8LessEqualTestResult_0...), 64),
	CreateInt8TestCase("l128", append(int8TestSlice_1, int8TestSlice_0...), int8LessEqualTestMatch_1, 0,
		append(int8LessEqualTestResult_1, int8LessEqualTestResult_0...), 128),
	CreateInt8TestCase("l256", append(int8TestSlice_1, int8TestSlice_0...), int8LessEqualTestMatch_1, 0,
		append(int8LessEqualTestResult_1, int8LessEqualTestResult_0...), 256),
	CreateInt8TestCase("l512", append(int8TestSlice_1, int8TestSlice_0...), int8LessEqualTestMatch_1, 0,
		append(int8LessEqualTestResult_1, int8LessEqualTestResult_0...), 512),
	CreateInt8TestCase("l1024", append(int8TestSlice_1, int8TestSlice_0...), int8LessEqualTestMatch_1, 0,
		append(int8LessEqualTestResult_1, int8LessEqualTestResult_0...), 1024),
	CreateInt8TestCase("l1023", append(int8TestSlice_1, int8TestSlice_0...), int8LessEqualTestMatch_1, 0,
		append(int8LessEqualTestResult_1, int8LessEqualTestResult_0...), 1023),
	CreateInt8TestCase("l511", append(int8TestSlice_1, int8TestSlice_0...), int8LessEqualTestMatch_1, 0,
		append(int8LessEqualTestResult_1, int8LessEqualTestResult_0...), 511),
	CreateInt8TestCase("l255", append(int8TestSlice_1, int8TestSlice_0...), int8LessEqualTestMatch_1, 0,
		append(int8LessEqualTestResult_1, int8LessEqualTestResult_0...), 255),
	CreateInt8TestCase("l127", int8TestSlice_1, int8LessEqualTestMatch_1, 0, int8LessEqualTestResult_1, 127),
	CreateInt8TestCase("l63", int8TestSlice_1, int8LessEqualTestMatch_1, 0, int8LessEqualTestResult_1, 63),
	CreateInt8TestCase("l31", int8TestSlice_1, int8LessEqualTestMatch_1, 0, int8LessEqualTestResult_1, 31),
	CreateInt8TestCase("l23", int8TestSlice_1, int8LessEqualTestMatch_1, 0, int8LessEqualTestResult_1, 23),
	CreateInt8TestCase("l15", int8TestSlice_1, int8LessEqualTestMatch_1, 0, int8LessEqualTestResult_1, 15),
	CreateInt8TestCase("l7", int8TestSlice_1, int8LessEqualTestMatch_1, 0, int8LessEqualTestResult_1, 7),
	CreateInt8TestCase("neg512", int8TestSlice_2, int8LessEqualTestMatch_2, 0, int8LessEqualTestResult_2, 512),
	CreateInt8TestCase("neg32", int8TestSlice_2, int8LessEqualTestMatch_2, 0, int8LessEqualTestResult_2, 32),
	CreateInt8TestCase("neg31", int8TestSlice_2, int8LessEqualTestMatch_2, 0, int8LessEqualTestResult_2, 31),
	CreateInt8TestCase("ext512", int8TestSlice_3, int8LessEqualTestMatch_3, 0, int8LessEqualTestResult_3, 512),
	CreateInt8TestCase("ext32", int8TestSlice_3, int8LessEqualTestMatch_3, 0, int8LessEqualTestResult_3, 32),
	CreateInt8TestCase("ext31", int8TestSlice_3, int8LessEqualTestMatch_3, 0, int8LessEqualTestResult_3, 31),
}

func TestMatchInt8LessEqualGeneric(T *testing.T) {
	for _, c := range int8LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt8LessThanEqualGeneric(c.slice, c.match, bits)
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

func TestMatchInt8LessEqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range int8LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt8LessThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt8LessEqualAVX512(T *testing.T) {
	if !useAVX512_BW {
		T.SkipNow()
	}
	for _, c := range int8LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt8LessThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt8LessEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8LessThanEqualGeneric(a, math.MaxInt8/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt8LessEqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8LessThanEqualAVX2(a, math.MaxInt8/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt8LessEqualAVX512(B *testing.B) {
	if !useAVX512_BW {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8LessThanEqualAVX512(a, math.MaxInt8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
//
var int8GreaterCases = []Int8MatchTest{
	{
		name:   "l0",
		slice:  make([]int8, 0),
		match:  int8GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int8GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt8TestCase("vec1", int8TestSlice_0, int8GreaterTestMatch_0, 0, int8GreaterTestResult_0, 32),
	CreateInt8TestCase("vec2", int8TestSlice_0, int8GreaterTestMatch_0, 0, int8GreaterTestResult_0, 512),
	CreateInt8TestCase("l32", int8TestSlice_1, int8GreaterTestMatch_1, 0, int8GreaterTestResult_1, 32),
	CreateInt8TestCase("l64", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterTestMatch_1, 0,
		append(int8GreaterTestResult_1, int8GreaterTestResult_0...), 64),
	CreateInt8TestCase("l128", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterTestMatch_1, 0,
		append(int8GreaterTestResult_1, int8GreaterTestResult_0...), 128),
	CreateInt8TestCase("l256", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterTestMatch_1, 0,
		append(int8GreaterTestResult_1, int8GreaterTestResult_0...), 256),
	CreateInt8TestCase("l512", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterTestMatch_1, 0,
		append(int8GreaterTestResult_1, int8GreaterTestResult_0...), 512),
	CreateInt8TestCase("l1024", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterTestMatch_1, 0,
		append(int8GreaterTestResult_1, int8GreaterTestResult_0...), 1024),
	CreateInt8TestCase("l1023", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterTestMatch_1, 0,
		append(int8GreaterTestResult_1, int8GreaterTestResult_0...), 1023),
	CreateInt8TestCase("l511", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterTestMatch_1, 0,
		append(int8GreaterTestResult_1, int8GreaterTestResult_0...), 511),
	CreateInt8TestCase("l255", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterTestMatch_1, 0,
		append(int8GreaterTestResult_1, int8GreaterTestResult_0...), 255),
	CreateInt8TestCase("l127", int8TestSlice_1, int8GreaterTestMatch_1, 0, int8GreaterTestResult_1, 127),
	CreateInt8TestCase("l63", int8TestSlice_1, int8GreaterTestMatch_1, 0, int8GreaterTestResult_1, 63),
	CreateInt8TestCase("l31", int8TestSlice_1, int8GreaterTestMatch_1, 0, int8GreaterTestResult_1, 31),
	CreateInt8TestCase("l23", int8TestSlice_1, int8GreaterTestMatch_1, 0, int8GreaterTestResult_1, 23),
	CreateInt8TestCase("l15", int8TestSlice_1, int8GreaterTestMatch_1, 0, int8GreaterTestResult_1, 15),
	CreateInt8TestCase("l7", int8TestSlice_1, int8GreaterTestMatch_1, 0, int8GreaterTestResult_1, 7),
	CreateInt8TestCase("neg512", int8TestSlice_2, int8GreaterTestMatch_2, 0, int8GreaterTestResult_2, 512),
	CreateInt8TestCase("neg32", int8TestSlice_2, int8GreaterTestMatch_2, 0, int8GreaterTestResult_2, 32),
	CreateInt8TestCase("neg31", int8TestSlice_2, int8GreaterTestMatch_2, 0, int8GreaterTestResult_2, 31),
	CreateInt8TestCase("ext512", int8TestSlice_3, int8GreaterTestMatch_3, 0, int8GreaterTestResult_3, 512),
	CreateInt8TestCase("ext32", int8TestSlice_3, int8GreaterTestMatch_3, 0, int8GreaterTestResult_3, 32),
	CreateInt8TestCase("ext31", int8TestSlice_3, int8GreaterTestMatch_3, 0, int8GreaterTestResult_3, 31),
}

func TestMatchInt8GreaterGeneric(T *testing.T) {
	for _, c := range int8GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt8GreaterThanGeneric(c.slice, c.match, bits)
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

func TestMatchInt8GreaterAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range int8GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt8GreaterThanAVX2(c.slice, c.match, bits)
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

func TestMatchInt8GreaterAVX512(T *testing.T) {
	if !useAVX512_BW {
		T.SkipNow()
	}
	for _, c := range int8GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt8GreaterThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt8GreaterGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8GreaterThanGeneric(a, math.MaxInt8/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt8GreaterAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8GreaterThanAVX2(a, math.MaxInt8/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt8GreaterAVX512(B *testing.B) {
	if !useAVX512_BW {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8GreaterThanAVX512(a, math.MaxInt8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//
var int8GreaterEqualCases = []Int8MatchTest{
	{
		name:   "l0",
		slice:  make([]int8, 0),
		match:  int8GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int8GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt8TestCase("vec1", int8TestSlice_0, int8GreaterEqualTestMatch_0, 0, int8GreaterEqualTestResult_0, 32),
	CreateInt8TestCase("vec2", int8TestSlice_0, int8GreaterEqualTestMatch_0, 0, int8GreaterEqualTestResult_0, 512),
	CreateInt8TestCase("l32", int8TestSlice_1, int8GreaterEqualTestMatch_1, 0, int8GreaterEqualTestResult_1, 32),
	CreateInt8TestCase("l64", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterEqualTestMatch_1, 0,
		append(int8GreaterEqualTestResult_1, int8GreaterEqualTestResult_0...), 64),
	CreateInt8TestCase("l128", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterEqualTestMatch_1, 0,
		append(int8GreaterEqualTestResult_1, int8GreaterEqualTestResult_0...), 128),
	CreateInt8TestCase("l256", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterEqualTestMatch_1, 0,
		append(int8GreaterEqualTestResult_1, int8GreaterEqualTestResult_0...), 256),
	CreateInt8TestCase("l512", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterEqualTestMatch_1, 0,
		append(int8GreaterEqualTestResult_1, int8GreaterEqualTestResult_0...), 512),
	CreateInt8TestCase("l1024", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterEqualTestMatch_1, 0,
		append(int8GreaterEqualTestResult_1, int8GreaterEqualTestResult_0...), 1024),
	CreateInt8TestCase("l1023", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterEqualTestMatch_1, 0,
		append(int8GreaterEqualTestResult_1, int8GreaterEqualTestResult_0...), 1023),
	CreateInt8TestCase("l511", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterEqualTestMatch_1, 0,
		append(int8GreaterEqualTestResult_1, int8GreaterEqualTestResult_0...), 511),
	CreateInt8TestCase("l255", append(int8TestSlice_1, int8TestSlice_0...), int8GreaterEqualTestMatch_1, 0,
		append(int8GreaterEqualTestResult_1, int8GreaterEqualTestResult_0...), 255),
	CreateInt8TestCase("l127", int8TestSlice_1, int8GreaterEqualTestMatch_1, 0, int8GreaterEqualTestResult_1, 127),
	CreateInt8TestCase("l63", int8TestSlice_1, int8GreaterEqualTestMatch_1, 0, int8GreaterEqualTestResult_1, 63),
	CreateInt8TestCase("l31", int8TestSlice_1, int8GreaterEqualTestMatch_1, 0, int8GreaterEqualTestResult_1, 31),
	CreateInt8TestCase("l23", int8TestSlice_1, int8GreaterEqualTestMatch_1, 0, int8GreaterEqualTestResult_1, 23),
	CreateInt8TestCase("l15", int8TestSlice_1, int8GreaterEqualTestMatch_1, 0, int8GreaterEqualTestResult_1, 15),
	CreateInt8TestCase("l7", int8TestSlice_1, int8GreaterEqualTestMatch_1, 0, int8GreaterEqualTestResult_1, 7),
	CreateInt8TestCase("neg512", int8TestSlice_2, int8GreaterEqualTestMatch_2, 0, int8GreaterEqualTestResult_2, 512),
	CreateInt8TestCase("neg32", int8TestSlice_2, int8GreaterEqualTestMatch_2, 0, int8GreaterEqualTestResult_2, 32),
	CreateInt8TestCase("neg31", int8TestSlice_2, int8GreaterEqualTestMatch_2, 0, int8GreaterEqualTestResult_2, 31),
	CreateInt8TestCase("ext512", int8TestSlice_3, int8GreaterEqualTestMatch_3, 0, int8GreaterEqualTestResult_3, 512),
	CreateInt8TestCase("ext32", int8TestSlice_3, int8GreaterEqualTestMatch_3, 0, int8GreaterEqualTestResult_3, 32),
	CreateInt8TestCase("ext31", int8TestSlice_3, int8GreaterEqualTestMatch_3, 0, int8GreaterEqualTestResult_3, 31),
}

func TestMatchInt8GreaterEqualGeneric(T *testing.T) {
	for _, c := range int8GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt8GreaterThanEqualGeneric(c.slice, c.match, bits)
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

func TestMatchInt8GreaterEqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range int8GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt8GreaterThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt8GreaterEqualAVX512(T *testing.T) {
	if !useAVX512_BW {
		T.SkipNow()
	}
	for _, c := range int8GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt8GreaterThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt8GreaterEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8GreaterThanEqualGeneric(a, math.MaxInt8/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt8GreaterEqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8GreaterThanEqualAVX2(a, math.MaxInt8/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt8GreaterEqualAVX512(B *testing.B) {
	if !useAVX512_BW {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8GreaterThanEqualAVX512(a, math.MaxInt8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
//
var int8BetweenCases = []Int8MatchTest{
	{
		name:   "l0",
		slice:  make([]int8, 0),
		match:  int8BetweenTestMatch_1,
		match2: int8BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int8BetweenTestMatch_1,
		match2: int8BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	},
	CreateInt8TestCase("vec1", int8TestSlice_0, int8BetweenTestMatch_0, int8BetweenTestMatch_0b, int8BetweenTestResult_0, 32),
	CreateInt8TestCase("vec2", int8TestSlice_0, int8BetweenTestMatch_0, int8BetweenTestMatch_0b, int8BetweenTestResult_0, 512),
	CreateInt8TestCase("l32", int8TestSlice_1, int8BetweenTestMatch_1, int8BetweenTestMatch_1b, int8BetweenTestResult_1, 32),
	CreateInt8TestCase("l64", append(int8TestSlice_1, int8TestSlice_0...), int8BetweenTestMatch_1, int8BetweenTestMatch_1b,
		append(int8BetweenTestResult_1, int8BetweenTestResult_0...), 64),
	CreateInt8TestCase("l128", append(int8TestSlice_1, int8TestSlice_0...), int8BetweenTestMatch_1, int8BetweenTestMatch_1b,
		append(int8BetweenTestResult_1, int8BetweenTestResult_0...), 128),
	CreateInt8TestCase("l256", append(int8TestSlice_1, int8TestSlice_0...), int8BetweenTestMatch_1, int8BetweenTestMatch_1b,
		append(int8BetweenTestResult_1, int8BetweenTestResult_0...), 256),
	CreateInt8TestCase("l512", append(int8TestSlice_1, int8TestSlice_0...), int8BetweenTestMatch_1, int8BetweenTestMatch_1b,
		append(int8BetweenTestResult_1, int8BetweenTestResult_0...), 512),
	CreateInt8TestCase("l1024", append(int8TestSlice_1, int8TestSlice_0...), int8BetweenTestMatch_1, int8BetweenTestMatch_1b,
		append(int8BetweenTestResult_1, int8BetweenTestResult_0...), 1024),
	CreateInt8TestCase("l1023", append(int8TestSlice_1, int8TestSlice_0...), int8BetweenTestMatch_1, int8BetweenTestMatch_1b,
		append(int8BetweenTestResult_1, int8BetweenTestResult_0...), 1023),
	CreateInt8TestCase("l511", append(int8TestSlice_1, int8TestSlice_0...), int8BetweenTestMatch_1, int8BetweenTestMatch_1b,
		append(int8BetweenTestResult_1, int8BetweenTestResult_0...), 511),
	CreateInt8TestCase("l255", append(int8TestSlice_1, int8TestSlice_0...), int8BetweenTestMatch_1, int8BetweenTestMatch_1b,
		append(int8BetweenTestResult_1, int8BetweenTestResult_0...), 255),
	CreateInt8TestCase("l127", int8TestSlice_1, int8BetweenTestMatch_1, int8BetweenTestMatch_1b, int8BetweenTestResult_1, 127),
	CreateInt8TestCase("l63", int8TestSlice_1, int8BetweenTestMatch_1, int8BetweenTestMatch_1b, int8BetweenTestResult_1, 63),
	CreateInt8TestCase("l31", int8TestSlice_1, int8BetweenTestMatch_1, int8BetweenTestMatch_1b, int8BetweenTestResult_1, 31),
	CreateInt8TestCase("l23", int8TestSlice_1, int8BetweenTestMatch_1, int8BetweenTestMatch_1b, int8BetweenTestResult_1, 23),
	CreateInt8TestCase("l15", int8TestSlice_1, int8BetweenTestMatch_1, int8BetweenTestMatch_1b, int8BetweenTestResult_1, 15),
	CreateInt8TestCase("l7", int8TestSlice_1, int8BetweenTestMatch_1, int8BetweenTestMatch_1b, int8BetweenTestResult_1, 7),
	CreateInt8TestCase("neg512", int8TestSlice_2, int8BetweenTestMatch_2, int8BetweenTestMatch_2b, int8BetweenTestResult_2, 512),
	CreateInt8TestCase("neg32", int8TestSlice_2, int8BetweenTestMatch_2, int8BetweenTestMatch_2b, int8BetweenTestResult_2, 32),
	CreateInt8TestCase("neg31", int8TestSlice_2, int8BetweenTestMatch_2, int8BetweenTestMatch_2b, int8BetweenTestResult_2, 31),
	CreateInt8TestCase("ext512", int8TestSlice_3, int8BetweenTestMatch_3, int8BetweenTestMatch_3b, int8BetweenTestResult_3, 512),
	CreateInt8TestCase("ext32", int8TestSlice_3, int8BetweenTestMatch_3, int8BetweenTestMatch_3b, int8BetweenTestResult_3, 32),
	CreateInt8TestCase("ext31", int8TestSlice_3, int8BetweenTestMatch_3, int8BetweenTestMatch_3b, int8BetweenTestResult_3, 31),
}

func TestMatchInt8BetweenGeneric(T *testing.T) {
	for _, c := range int8BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt8BetweenGeneric(c.slice, c.match, c.match2, bits)
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

func TestMatchInt8BetweenAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range int8BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt8BetweenAVX2(c.slice, c.match, c.match2, bits)
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

func TestMatchInt8BetweenAVX512(T *testing.T) {
	if !useAVX512_BW {
		T.SkipNow()
	}
	for _, c := range int8BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt8BetweenAVX512(c.slice, c.match, c.match2, bits)
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
func BenchmarkMatchInt8BetweenGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8BetweenGeneric(a, math.MaxInt8/4, math.MaxInt8/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt8BetweenAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8BetweenAVX2(a, math.MaxInt8/4, math.MaxInt8/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt8BetweenAVX512(B *testing.B) {
	if !useAVX512_BW {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8BetweenAVX512(a, math.MaxInt8/4, math.MaxInt8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Int8 Slice
//
func TestUniqueInt8(T *testing.T) {
	a := randInt8Slice(1000, 5)
	b := UniqueInt8Slice(a)
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

func BenchmarkUniqueInt8(B *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt8Slice(n, 5)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				UniqueInt8Slice(a)
			}
		})
	}
}

func TestInt8SliceContains(T *testing.T) {
	// nil slice
	if Int8.Contains(nil, 1) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Int8.Contains([]int8{}, 1) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Int8.Contains([]int8{1}, 1) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Int8.Contains([]int8{1}, 2) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !Int8.Contains([]int8{-1, 3, 5, 7, 11, 13}, -1) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Int8.Contains([]int8{-1, 3, 5, 7, 11, 13}, 5) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Int8.Contains([]int8{-1, 3, 5, 7, 11, 13}, 13) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Int8.Contains([]int8{-1, 3, 5, 7, 11, 13}, 0) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Int8.Contains([]int8{-1, 3, 5, 7, 11, 13}, 2) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Int8.Contains([]int8{-1, 3, 5, 7, 11, 13}, 14) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkInt8SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Int8.Sort(randInt8Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				Int8.Contains(a, int8(rand.Intn(math.MaxInt8+1)))
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Int8.Sort(randInt8Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				Int8.Contains(a, a[rand.Intn(len(a))])
			}
		})
	}
}

func TestInt8SliceContainsRange(T *testing.T) {
	type VecTestRange struct {
		Name  string
		From  int8
		To    int8
		Match bool
	}

	type VecTestcase struct {
		Slice  []int8
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
			Slice: []int8{},
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: 0, To: 2, Match: false},
			},
		},
		// 1-element slice
		VecTestcase{
			Slice: []int8{3},
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
			Slice: []int8{3},
			Ranges: []VecTestRange{
				VecTestRange{Name: "BCD", From: 3, To: 3, Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		VecTestcase{
			Slice: []int8{3, 5, 7, 11, 13},
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
			Slice: []int8{
				6, 13, 16, 17, 18,
				25, 26, 27, 27, 30,
			},
			Ranges: []VecTestRange{
				VecTestRange{Name: "1", From: 27, To: 28, Match: true},
				VecTestRange{Name: "2", From: 28, To: 28, Match: false},
				//VecTestRange{Name: "3", From: 28, To: 28, Match: false},
				VecTestRange{Name: "4", From: 28, To: 29, Match: false},
				VecTestRange{Name: "5", From: 29, To: 29, Match: false},
				//VecTestRange{Name: "6", From: 29, To: 29, Match: false},
				VecTestRange{Name: "7", From: 29, To: 30, Match: true},
				VecTestRange{Name: "8", From: 30, To: 30, Match: true},
			},
		},
	}

	for i, v := range tests {
		for _, r := range v.Ranges {
			if want, got := r.Match, Int8.ContainsRange(v.Slice, r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkInt8SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Int8.Sort(randInt8Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := int8(rand.Intn(math.MaxInt8+1)), int8(rand.Intn(math.MaxInt8+1))
				if min > max {
					min, max = max, min
				}
				Int8.ContainsRange(a, min, max)
			}
		})
	}
}
