// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"testing"
)

const Int64Size = 8

func randInt64Slice(n, u int) []int64 {
	s := make([]int64, n*u)
	for i := 0; i < n; i++ {
		s[i] = rand.Int63()
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

type Int64MatchTest struct {
	name   string
	slice  []int64
	match  int64 // used for every test
	match2 int64 // used for between tests
	result []byte
	count  int64
}

var (
	int64TestSlice_0 = []int64{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	int64EqualTestMatch_0  int64 = 5
	int64EqualTestResult_0       = []byte{0x6a, 0x1e, 0x48, 0x2c}

	int64NotEqualTestMatch_0  int64 = 5
	int64NotEqualTestResult_0       = []byte{0x95, 0xe1, 0xb7, 0xd3}

	int64LessTestMatch_0  int64 = 5
	int64LessTestResult_0       = []byte{0x05, 0x21, 0x27, 0x01}

	int64LessEqualTestMatch_0  int64 = 5
	int64LessEqualTestResult_0       = []byte{0x6f, 0x3f, 0x6f, 0x2d}

	int64GreaterTestMatch_0  int64 = 5
	int64GreaterTestResult_0       = []byte{0x90, 0xc0, 0x90, 0xd2}

	int64GreaterEqualTestMatch_0  int64 = 5
	int64GreaterEqualTestResult_0       = []byte{0xfa, 0xde, 0xd8, 0xfe}

	int64BetweenTestMatch_0  int64 = 5
	int64BetweenTestMatch_0b int64 = 10
	int64BetweenTestResult_0       = []byte{0xfa, 0x1e, 0xd8, 0x2c}

	// positive values only
	int64TestSlice_1 = []int64{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 500000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}
	int64EqualTestResult_1       = []byte{0x41, 0x42, 0xc4, 0x0e}
	int64EqualTestMatch_1  int64 = 5

	int64NotEqualTestResult_1       = []byte{0xbe, 0xbd, 0x3b, 0xf1}
	int64NotEqualTestMatch_1  int64 = 5

	int64LessTestResult_1       = []byte{0x0e, 0x00, 0x00, 0x00}
	int64LessTestMatch_1  int64 = 5

	int64LessEqualTestResult_1       = []byte{0x4f, 0x42, 0xc4, 0x0e}
	int64LessEqualTestMatch_1  int64 = 5

	int64GreaterTestResult_1       = []byte{0xb0, 0xbd, 0x3b, 0xf1}
	int64GreaterTestMatch_1  int64 = 5

	int64GreaterEqualTestResult_1       = []byte{0xf1, 0xff, 0xff, 0xff}
	int64GreaterEqualTestMatch_1  int64 = 5

	int64BetweenTestResult_1       = []byte{0xf1, 0x42, 0xc4, 0x0e}
	int64BetweenTestMatch_1  int64 = 5
	int64BetweenTestMatch_1b int64 = 10

	// negative and positive values mixed
	int64TestSlice_2 = []int64{
		-5, 2, -3, 5,
		7, 8, 9, -10,
		15, 50, 55, 500,
		1000, -500000, 113, 12,
		31, 32, 33, 34,
		35, -36, 37, 38,
		39, 40, -41, 42,
		43, 44, 45, -46,
	}
	int64EqualTestResult_2       = []byte{0x01, 0x0, 0x0, 0x0}
	int64EqualTestMatch_2  int64 = -5

	int64NotEqualTestResult_2       = []byte{0xfe, 0xff, 0xff, 0xff}
	int64NotEqualTestMatch_2  int64 = -5

	int64LessTestResult_2       = []byte{0x87, 0x20, 0x20, 0x84}
	int64LessTestMatch_2  int64 = 5

	int64LessEqualTestResult_2       = []byte{0x8f, 0x20, 0x20, 0x84}
	int64LessEqualTestMatch_2  int64 = 5

	int64GreaterTestResult_2       = []byte{0x70, 0xdf, 0xdf, 0x7b}
	int64GreaterTestMatch_2  int64 = 5

	int64GreaterEqualTestResult_2       = []byte{0x78, 0xdf, 0xdf, 0x7b}
	int64GreaterEqualTestMatch_2  int64 = 5

	int64BetweenTestResult_2       = []byte{0x78, 0x00, 0x00, 0x00}
	int64BetweenTestMatch_2  int64 = 5
	int64BetweenTestMatch_2b int64 = 10

	// extreme values
	int64TestSlice_3 = []int64{
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
		math.MaxInt64, math.MinInt64,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
		math.MaxInt64, math.MinInt64,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
		math.MaxInt64, math.MinInt64,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
		math.MaxInt64, math.MinInt64,
	}
	int64EqualTestResult_3       = []byte{0x80, 0x80, 0x80, 0x80}
	int64EqualTestMatch_3  int64 = math.MinInt64

	int64NotEqualTestResult_3       = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	int64NotEqualTestMatch_3  int64 = math.MinInt64

	int64LessTestResult_3       = []byte{0x0, 0x0, 0x0, 0x0}
	int64LessTestMatch_3  int64 = math.MinInt64

	int64LessEqualTestResult_3       = []byte{0x80, 0x80, 0x80, 0x80}
	int64LessEqualTestMatch_3  int64 = math.MinInt64

	int64GreaterTestResult_3       = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	int64GreaterTestMatch_3  int64 = math.MinInt64

	int64GreaterEqualTestResult_3       = []byte{0xff, 0xff, 0xff, 0xff}
	int64GreaterEqualTestMatch_3  int64 = math.MinInt64

	int64BetweenTestResult_3       = []byte{0x50, 0x50, 0x50, 0x50}
	int64BetweenTestMatch_3  int64 = math.MaxInt32
	int64BetweenTestMatch_3b int64 = math.MaxInt64
)

// creates an uint64 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - match, match2: are only copied to the resulting test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateInt64TestCase(name string, slice []int64, match, match2 int64, result []byte, length int) Int64MatchTest {
	if len(slice)%8 != 0 {
		panic("CreateInt64TestCase: length of slice has to be a multiple of 8")
	}
	if len(result) != bitFieldLen(len(slice)) {
		panic("CreateInt64TestCase: length of slice and length of result does not match")
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []int64
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
	return Int64MatchTest{
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
var int64EqualCases = []Int64MatchTest{
	{
		name:   "l0",
		slice:  make([]int64, 0),
		match:  int64EqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int64EqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt64TestCase("vec1", int64TestSlice_0, int64EqualTestMatch_0, 0, int64EqualTestResult_0, 32),
	CreateInt64TestCase("vec2", int64TestSlice_0, int64EqualTestMatch_0, 0, int64EqualTestResult_0, 64),
	CreateInt64TestCase("l32", int64TestSlice_1, int64EqualTestMatch_1, 0, int64EqualTestResult_1, 32),
	CreateInt64TestCase("l64", append(int64TestSlice_1, int64TestSlice_0...), int64EqualTestMatch_1, 0,
		append(int64EqualTestResult_1, int64EqualTestResult_0...), 64),
	CreateInt64TestCase("l128", append(int64TestSlice_1, int64TestSlice_0...), int64EqualTestMatch_1, 0,
		append(int64EqualTestResult_1, int64EqualTestResult_0...), 128),
	CreateInt64TestCase("l127", int64TestSlice_1, int64EqualTestMatch_1, 0, int64EqualTestResult_1, 127),
	CreateInt64TestCase("l63", int64TestSlice_1, int64EqualTestMatch_1, 0, int64EqualTestResult_1, 63),
	CreateInt64TestCase("l31", int64TestSlice_1, int64EqualTestMatch_1, 0, int64EqualTestResult_1, 31),
	CreateInt64TestCase("l23", int64TestSlice_1, int64EqualTestMatch_1, 0, int64EqualTestResult_1, 23),
	CreateInt64TestCase("l15", int64TestSlice_1, int64EqualTestMatch_1, 0, int64EqualTestResult_1, 15),
	CreateInt64TestCase("l7", int64TestSlice_1, int64EqualTestMatch_1, 0, int64EqualTestResult_1, 7),
	CreateInt64TestCase("neg64", int64TestSlice_2, int64EqualTestMatch_2, 0, int64EqualTestResult_2, 64),
	CreateInt64TestCase("neg32", int64TestSlice_2, int64EqualTestMatch_2, 0, int64EqualTestResult_2, 32),
	CreateInt64TestCase("neg31", int64TestSlice_2, int64EqualTestMatch_2, 0, int64EqualTestResult_2, 31),
	CreateInt64TestCase("ext64", int64TestSlice_3, int64EqualTestMatch_3, 0, int64EqualTestResult_3, 64),
	CreateInt64TestCase("ext32", int64TestSlice_3, int64EqualTestMatch_3, 0, int64EqualTestResult_3, 32),
	CreateInt64TestCase("ext31", int64TestSlice_3, int64EqualTestMatch_3, 0, int64EqualTestResult_3, 31),
}

func TestMatchInt64EqualGeneric(T *testing.T) {
	for _, c := range int64EqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt64EqualGeneric(c.slice, c.match, bits)
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

// -----------------------------------------------------------------------------
// Equal benchmarks
//
func BenchmarkMatchInt64EqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt64Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int64Size))
			for i := 0; i < B.N; i++ {
				matchInt64EqualGeneric(a, math.MaxInt64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
//
var int64NotEqualCases = []Int64MatchTest{
	{
		name:   "l0",
		slice:  make([]int64, 0),
		match:  int64NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int64NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt64TestCase("vec1", int64TestSlice_0, int64NotEqualTestMatch_0, 0, int64NotEqualTestResult_0, 32),
	CreateInt64TestCase("vec2", int64TestSlice_0, int64NotEqualTestMatch_0, 0, int64NotEqualTestResult_0, 64),
	CreateInt64TestCase("l32", int64TestSlice_1, int64NotEqualTestMatch_1, 0, int64NotEqualTestResult_1, 32),
	CreateInt64TestCase("l64", append(int64TestSlice_1, int64TestSlice_0...), int64NotEqualTestMatch_1, 0,
		append(int64NotEqualTestResult_1, int64NotEqualTestResult_0...), 64),
	CreateInt64TestCase("l128", append(int64TestSlice_1, int64TestSlice_0...), int64NotEqualTestMatch_1, 0,
		append(int64NotEqualTestResult_1, int64NotEqualTestResult_0...), 128),
	CreateInt64TestCase("l127", int64TestSlice_1, int64NotEqualTestMatch_1, 0, int64NotEqualTestResult_1, 127),
	CreateInt64TestCase("l63", int64TestSlice_1, int64NotEqualTestMatch_1, 0, int64NotEqualTestResult_1, 63),
	CreateInt64TestCase("l31", int64TestSlice_1, int64NotEqualTestMatch_1, 0, int64NotEqualTestResult_1, 31),
	CreateInt64TestCase("l23", int64TestSlice_1, int64NotEqualTestMatch_1, 0, int64NotEqualTestResult_1, 23),
	CreateInt64TestCase("l15", int64TestSlice_1, int64NotEqualTestMatch_1, 0, int64NotEqualTestResult_1, 15),
	CreateInt64TestCase("l7", int64TestSlice_1, int64NotEqualTestMatch_1, 0, int64NotEqualTestResult_1, 7),
	CreateInt64TestCase("neg64", int64TestSlice_2, int64NotEqualTestMatch_2, 0, int64NotEqualTestResult_2, 64),
	CreateInt64TestCase("neg32", int64TestSlice_2, int64NotEqualTestMatch_2, 0, int64NotEqualTestResult_2, 32),
	CreateInt64TestCase("neg31", int64TestSlice_2, int64NotEqualTestMatch_2, 0, int64NotEqualTestResult_2, 31),
	CreateInt64TestCase("ext64", int64TestSlice_3, int64NotEqualTestMatch_3, 0, int64NotEqualTestResult_3, 64),
	CreateInt64TestCase("ext32", int64TestSlice_3, int64NotEqualTestMatch_3, 0, int64NotEqualTestResult_3, 32),
	CreateInt64TestCase("ext31", int64TestSlice_3, int64NotEqualTestMatch_3, 0, int64NotEqualTestResult_3, 31),
}

func TestMatchInt64NotEqualGeneric(T *testing.T) {
	for _, c := range int64NotEqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt64NotEqualGeneric(c.slice, c.match, bits)
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

// -----------------------------------------------------------------------------
// Not Equal benchmarks
//
func BenchmarkMatchInt64NotEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt64Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int64Size))
			for i := 0; i < B.N; i++ {
				matchInt64NotEqualGeneric(a, math.MaxInt64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
//
var int64LessCases = []Int64MatchTest{
	{
		name:   "l0",
		slice:  make([]int64, 0),
		match:  int64LessTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int64LessTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt64TestCase("vec1", int64TestSlice_0, int64LessTestMatch_0, 0, int64LessTestResult_0, 32),
	CreateInt64TestCase("vec2", int64TestSlice_0, int64LessTestMatch_0, 0, int64LessTestResult_0, 64),
	CreateInt64TestCase("l32", int64TestSlice_1, int64LessTestMatch_1, 0, int64LessTestResult_1, 32),
	CreateInt64TestCase("l64", append(int64TestSlice_1, int64TestSlice_0...), int64LessTestMatch_1, 0,
		append(int64LessTestResult_1, int64LessTestResult_0...), 64),
	CreateInt64TestCase("l128", append(int64TestSlice_1, int64TestSlice_0...), int64LessTestMatch_1, 0,
		append(int64LessTestResult_1, int64LessTestResult_0...), 128),
	CreateInt64TestCase("l127", int64TestSlice_1, int64LessTestMatch_1, 0, int64LessTestResult_1, 127),
	CreateInt64TestCase("l63", int64TestSlice_1, int64LessTestMatch_1, 0, int64LessTestResult_1, 63),
	CreateInt64TestCase("l31", int64TestSlice_1, int64LessTestMatch_1, 0, int64LessTestResult_1, 31),
	CreateInt64TestCase("l23", int64TestSlice_1, int64LessTestMatch_1, 0, int64LessTestResult_1, 23),
	CreateInt64TestCase("l15", int64TestSlice_1, int64LessTestMatch_1, 0, int64LessTestResult_1, 15),
	CreateInt64TestCase("l7", int64TestSlice_1, int64LessTestMatch_1, 0, int64LessTestResult_1, 7),
	CreateInt64TestCase("neg64", int64TestSlice_2, int64LessTestMatch_2, 0, int64LessTestResult_2, 64),
	CreateInt64TestCase("neg32", int64TestSlice_2, int64LessTestMatch_2, 0, int64LessTestResult_2, 32),
	CreateInt64TestCase("neg31", int64TestSlice_2, int64LessTestMatch_2, 0, int64LessTestResult_2, 31),
	CreateInt64TestCase("ext64", int64TestSlice_3, int64LessTestMatch_3, 0, int64LessTestResult_3, 64),
	CreateInt64TestCase("ext32", int64TestSlice_3, int64LessTestMatch_3, 0, int64LessTestResult_3, 32),
	CreateInt64TestCase("ext31", int64TestSlice_3, int64LessTestMatch_3, 0, int64LessTestResult_3, 31),
}

func TestMatchInt64LessGeneric(T *testing.T) {
	for _, c := range int64LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt64LessThanGeneric(c.slice, c.match, bits)
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

// -----------------------------------------------------------------------------
// Less benchmarks
//
func BenchmarkMatchInt64LessGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt64Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int64Size))
			for i := 0; i < B.N; i++ {
				matchInt64LessThanGeneric(a, math.MaxInt64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//
var int64LessEqualCases = []Int64MatchTest{
	{
		name:   "l0",
		slice:  make([]int64, 0),
		match:  int64LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int64LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt64TestCase("vec1", int64TestSlice_0, int64LessEqualTestMatch_0, 0, int64LessEqualTestResult_0, 32),
	CreateInt64TestCase("vec2", int64TestSlice_0, int64LessEqualTestMatch_0, 0, int64LessEqualTestResult_0, 64),
	CreateInt64TestCase("l32", int64TestSlice_1, int64LessEqualTestMatch_1, 0, int64LessEqualTestResult_1, 32),
	CreateInt64TestCase("l64", append(int64TestSlice_1, int64TestSlice_0...), int64LessEqualTestMatch_1, 0,
		append(int64LessEqualTestResult_1, int64LessEqualTestResult_0...), 64),
	CreateInt64TestCase("l128", append(int64TestSlice_1, int64TestSlice_0...), int64LessEqualTestMatch_1, 0,
		append(int64LessEqualTestResult_1, int64LessEqualTestResult_0...), 128),
	CreateInt64TestCase("l127", int64TestSlice_1, int64LessEqualTestMatch_1, 0, int64LessEqualTestResult_1, 127),
	CreateInt64TestCase("l63", int64TestSlice_1, int64LessEqualTestMatch_1, 0, int64LessEqualTestResult_1, 63),
	CreateInt64TestCase("l31", int64TestSlice_1, int64LessEqualTestMatch_1, 0, int64LessEqualTestResult_1, 31),
	CreateInt64TestCase("l23", int64TestSlice_1, int64LessEqualTestMatch_1, 0, int64LessEqualTestResult_1, 23),
	CreateInt64TestCase("l15", int64TestSlice_1, int64LessEqualTestMatch_1, 0, int64LessEqualTestResult_1, 15),
	CreateInt64TestCase("l7", int64TestSlice_1, int64LessEqualTestMatch_1, 0, int64LessEqualTestResult_1, 7),
	CreateInt64TestCase("neg64", int64TestSlice_2, int64LessEqualTestMatch_2, 0, int64LessEqualTestResult_2, 64),
	CreateInt64TestCase("neg32", int64TestSlice_2, int64LessEqualTestMatch_2, 0, int64LessEqualTestResult_2, 32),
	CreateInt64TestCase("neg31", int64TestSlice_2, int64LessEqualTestMatch_2, 0, int64LessEqualTestResult_2, 31),
	CreateInt64TestCase("ext64", int64TestSlice_3, int64LessEqualTestMatch_3, 0, int64LessEqualTestResult_3, 64),
	CreateInt64TestCase("ext32", int64TestSlice_3, int64LessEqualTestMatch_3, 0, int64LessEqualTestResult_3, 32),
	CreateInt64TestCase("ext31", int64TestSlice_3, int64LessEqualTestMatch_3, 0, int64LessEqualTestResult_3, 31),
}

func TestMatchInt64LessEqualGeneric(T *testing.T) {
	for _, c := range int64LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt64LessThanEqualGeneric(c.slice, c.match, bits)
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

// -----------------------------------------------------------------------------
// Less equal benchmarks
//
func BenchmarkMatchInt64LessEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt64Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int64Size))
			for i := 0; i < B.N; i++ {
				matchInt64LessThanEqualGeneric(a, math.MaxInt64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
//
var int64GreaterCases = []Int64MatchTest{
	{
		name:   "l0",
		slice:  make([]int64, 0),
		match:  int64GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int64GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt64TestCase("vec1", int64TestSlice_0, int64GreaterTestMatch_0, 0, int64GreaterTestResult_0, 32),
	CreateInt64TestCase("vec2", int64TestSlice_0, int64GreaterTestMatch_0, 0, int64GreaterTestResult_0, 64),
	CreateInt64TestCase("l32", int64TestSlice_1, int64GreaterTestMatch_1, 0, int64GreaterTestResult_1, 32),
	CreateInt64TestCase("l64", append(int64TestSlice_1, int64TestSlice_0...), int64GreaterTestMatch_1, 0,
		append(int64GreaterTestResult_1, int64GreaterTestResult_0...), 64),
	CreateInt64TestCase("l128", append(int64TestSlice_1, int64TestSlice_0...), int64GreaterTestMatch_1, 0,
		append(int64GreaterTestResult_1, int64GreaterTestResult_0...), 128),
	CreateInt64TestCase("l127", int64TestSlice_1, int64GreaterTestMatch_1, 0, int64GreaterTestResult_1, 127),
	CreateInt64TestCase("l63", int64TestSlice_1, int64GreaterTestMatch_1, 0, int64GreaterTestResult_1, 63),
	CreateInt64TestCase("l31", int64TestSlice_1, int64GreaterTestMatch_1, 0, int64GreaterTestResult_1, 31),
	CreateInt64TestCase("l23", int64TestSlice_1, int64GreaterTestMatch_1, 0, int64GreaterTestResult_1, 23),
	CreateInt64TestCase("l15", int64TestSlice_1, int64GreaterTestMatch_1, 0, int64GreaterTestResult_1, 15),
	CreateInt64TestCase("l7", int64TestSlice_1, int64GreaterTestMatch_1, 0, int64GreaterTestResult_1, 7),
	CreateInt64TestCase("neg64", int64TestSlice_2, int64GreaterTestMatch_2, 0, int64GreaterTestResult_2, 64),
	CreateInt64TestCase("neg32", int64TestSlice_2, int64GreaterTestMatch_2, 0, int64GreaterTestResult_2, 32),
	CreateInt64TestCase("neg31", int64TestSlice_2, int64GreaterTestMatch_2, 0, int64GreaterTestResult_2, 31),
	CreateInt64TestCase("ext64", int64TestSlice_3, int64GreaterTestMatch_3, 0, int64GreaterTestResult_3, 64),
	CreateInt64TestCase("ext32", int64TestSlice_3, int64GreaterTestMatch_3, 0, int64GreaterTestResult_3, 32),
	CreateInt64TestCase("ext31", int64TestSlice_3, int64GreaterTestMatch_3, 0, int64GreaterTestResult_3, 31),
}

func TestMatchInt64GreaterGeneric(T *testing.T) {
	for _, c := range int64GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt64GreaterThanGeneric(c.slice, c.match, bits)
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

// -----------------------------------------------------------------------------
// Greater benchmarks
//
func BenchmarkMatchInt64GreaterGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt64Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int64Size))
			for i := 0; i < B.N; i++ {
				matchInt64GreaterThanGeneric(a, math.MaxInt64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//
var int64GreaterEqualCases = []Int64MatchTest{
	{
		name:   "l0",
		slice:  make([]int64, 0),
		match:  int64GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int64GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt64TestCase("vec1", int64TestSlice_0, int64GreaterEqualTestMatch_0, 0, int64GreaterEqualTestResult_0, 32),
	CreateInt64TestCase("vec2", int64TestSlice_0, int64GreaterEqualTestMatch_0, 0, int64GreaterEqualTestResult_0, 64),
	CreateInt64TestCase("l32", int64TestSlice_1, int64GreaterEqualTestMatch_1, 0, int64GreaterEqualTestResult_1, 32),
	CreateInt64TestCase("l64", append(int64TestSlice_1, int64TestSlice_0...), int64GreaterEqualTestMatch_1, 0,
		append(int64GreaterEqualTestResult_1, int64GreaterEqualTestResult_0...), 64),
	CreateInt64TestCase("l128", append(int64TestSlice_1, int64TestSlice_0...), int64GreaterEqualTestMatch_1, 0,
		append(int64GreaterEqualTestResult_1, int64GreaterEqualTestResult_0...), 128),
	CreateInt64TestCase("l127", int64TestSlice_1, int64GreaterEqualTestMatch_1, 0, int64GreaterEqualTestResult_1, 127),
	CreateInt64TestCase("l63", int64TestSlice_1, int64GreaterEqualTestMatch_1, 0, int64GreaterEqualTestResult_1, 63),
	CreateInt64TestCase("l31", int64TestSlice_1, int64GreaterEqualTestMatch_1, 0, int64GreaterEqualTestResult_1, 31),
	CreateInt64TestCase("l23", int64TestSlice_1, int64GreaterEqualTestMatch_1, 0, int64GreaterEqualTestResult_1, 23),
	CreateInt64TestCase("l15", int64TestSlice_1, int64GreaterEqualTestMatch_1, 0, int64GreaterEqualTestResult_1, 15),
	CreateInt64TestCase("l7", int64TestSlice_1, int64GreaterEqualTestMatch_1, 0, int64GreaterEqualTestResult_1, 7),
	CreateInt64TestCase("neg64", int64TestSlice_2, int64GreaterEqualTestMatch_2, 0, int64GreaterEqualTestResult_2, 64),
	CreateInt64TestCase("neg32", int64TestSlice_2, int64GreaterEqualTestMatch_2, 0, int64GreaterEqualTestResult_2, 32),
	CreateInt64TestCase("neg31", int64TestSlice_2, int64GreaterEqualTestMatch_2, 0, int64GreaterEqualTestResult_2, 31),
	CreateInt64TestCase("ext64", int64TestSlice_3, int64GreaterEqualTestMatch_3, 0, int64GreaterEqualTestResult_3, 64),
	CreateInt64TestCase("ext32", int64TestSlice_3, int64GreaterEqualTestMatch_3, 0, int64GreaterEqualTestResult_3, 32),
	CreateInt64TestCase("ext31", int64TestSlice_3, int64GreaterEqualTestMatch_3, 0, int64GreaterEqualTestResult_3, 31),
}

func TestMatchInt64GreaterEqualGeneric(T *testing.T) {
	for _, c := range int64GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt64GreaterThanEqualGeneric(c.slice, c.match, bits)
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

// -----------------------------------------------------------------------------
// Greater equal benchmarks
//
func BenchmarkMatchInt64GreaterEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt64Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int64Size))
			for i := 0; i < B.N; i++ {
				matchInt64GreaterThanEqualGeneric(a, math.MaxInt64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
//
var int64BetweenCases = []Int64MatchTest{
	{
		name:   "l0",
		slice:  make([]int64, 0),
		match:  int64BetweenTestMatch_1,
		match2: int64BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int64BetweenTestMatch_1,
		match2: int64BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	},
	CreateInt64TestCase("vec1", int64TestSlice_0, int64BetweenTestMatch_0, int64BetweenTestMatch_0b, int64BetweenTestResult_0, 32),
	CreateInt64TestCase("vec2", int64TestSlice_0, int64BetweenTestMatch_0, int64BetweenTestMatch_0b, int64BetweenTestResult_0, 64),
	CreateInt64TestCase("l32", int64TestSlice_1, int64BetweenTestMatch_1, int64BetweenTestMatch_1b, int64BetweenTestResult_1, 32),
	CreateInt64TestCase("l64", append(int64TestSlice_1, int64TestSlice_0...), int64BetweenTestMatch_1, int64BetweenTestMatch_1b,
		append(int64BetweenTestResult_1, int64BetweenTestResult_0...), 64),
	CreateInt64TestCase("l128", append(int64TestSlice_1, int64TestSlice_0...), int64BetweenTestMatch_1, int64BetweenTestMatch_1b,
		append(int64BetweenTestResult_1, int64BetweenTestResult_0...), 128),
	CreateInt64TestCase("l127", int64TestSlice_1, int64BetweenTestMatch_1, int64BetweenTestMatch_1b, int64BetweenTestResult_1, 127),
	CreateInt64TestCase("l63", int64TestSlice_1, int64BetweenTestMatch_1, int64BetweenTestMatch_1b, int64BetweenTestResult_1, 63),
	CreateInt64TestCase("l31", int64TestSlice_1, int64BetweenTestMatch_1, int64BetweenTestMatch_1b, int64BetweenTestResult_1, 31),
	CreateInt64TestCase("l23", int64TestSlice_1, int64BetweenTestMatch_1, int64BetweenTestMatch_1b, int64BetweenTestResult_1, 23),
	CreateInt64TestCase("l15", int64TestSlice_1, int64BetweenTestMatch_1, int64BetweenTestMatch_1b, int64BetweenTestResult_1, 15),
	CreateInt64TestCase("l7", int64TestSlice_1, int64BetweenTestMatch_1, int64BetweenTestMatch_1b, int64BetweenTestResult_1, 7),
	CreateInt64TestCase("neg64", int64TestSlice_2, int64BetweenTestMatch_2, int64BetweenTestMatch_2b, int64BetweenTestResult_2, 64),
	CreateInt64TestCase("neg32", int64TestSlice_2, int64BetweenTestMatch_2, int64BetweenTestMatch_2b, int64BetweenTestResult_2, 32),
	CreateInt64TestCase("neg31", int64TestSlice_2, int64BetweenTestMatch_2, int64BetweenTestMatch_2b, int64BetweenTestResult_2, 31),
	CreateInt64TestCase("ext64", int64TestSlice_3, int64BetweenTestMatch_3, int64BetweenTestMatch_3b, int64BetweenTestResult_3, 64),
	CreateInt64TestCase("ext32", int64TestSlice_3, int64BetweenTestMatch_3, int64BetweenTestMatch_3b, int64BetweenTestResult_3, 32),
	CreateInt64TestCase("ext31", int64TestSlice_3, int64BetweenTestMatch_3, int64BetweenTestMatch_3b, int64BetweenTestResult_3, 31),
}

func TestMatchInt64BetweenGeneric(T *testing.T) {
	for _, c := range int64BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt64BetweenGeneric(c.slice, c.match, c.match2, bits)
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

// -----------------------------------------------------------------------------
// Between benchmarks
//
func BenchmarkMatchInt64BetweenGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt64Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int64Size))
			for i := 0; i < B.N; i++ {
				matchInt64BetweenGeneric(a, math.MaxInt64/4, math.MaxInt64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Int64 Slice
//
func TestUniqueInt64(T *testing.T) {
	a := randInt64Slice(1000, 5)
	b := UniqueInt64Slice(a)
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

func BenchmarkUniqueInt64(B *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt64Slice(n, 5)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				UniqueInt64Slice(a)
			}
		})
	}
}

func TestInt64SliceContains(T *testing.T) {
	// nil slice
	if Int64.Contains(nil, 1) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Int64.Contains([]int64{}, 1) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Int64.Contains([]int64{1}, 1) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Int64.Contains([]int64{1}, 2) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !Int64.Contains([]int64{-1, 3, 5, 7, 11, 13}, -1) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Int64.Contains([]int64{-1, 3, 5, 7, 11, 13}, 5) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Int64.Contains([]int64{-1, 3, 5, 7, 11, 13}, 13) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Int64.Contains([]int64{-1, 3, 5, 7, 11, 13}, 0) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Int64.Contains([]int64{-1, 3, 5, 7, 11, 13}, 2) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Int64.Contains([]int64{-1, 3, 5, 7, 11, 13}, 14) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkInt64SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Int64.Sort(randInt64Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				Int64.Contains(a, rand.Int63())
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Int64.Sort(randInt64Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				Int64.Contains(a, a[rand.Intn(len(a))])
			}
		})
	}
}

func TestInt64SliceContainsRange(T *testing.T) {
	type VecTestRange struct {
		Name  string
		From  int64
		To    int64
		Match bool
	}

	type VecTestcase struct {
		Slice  []int64
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
			Slice: []int64{},
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: 0, To: 2, Match: false},
			},
		},
		// 1-element slice
		VecTestcase{
			Slice: []int64{3},
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
			Slice: []int64{3},
			Ranges: []VecTestRange{
				VecTestRange{Name: "BCD", From: 3, To: 3, Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		VecTestcase{
			Slice: []int64{3, 5, 7, 11, 13},
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
			Slice: []int64{
				699421, 1374016, 1692360, 1797909, 1809339,
				2552208, 2649552, 2740915, 2769610, 3043393,
			},
			Ranges: []VecTestRange{
				VecTestRange{Name: "1", From: 2785281, To: 2818048, Match: false},
				VecTestRange{Name: "2", From: 2818049, To: 2850816, Match: false},
				VecTestRange{Name: "3", From: 2850817, To: 2883584, Match: false},
				VecTestRange{Name: "4", From: 2883585, To: 2916352, Match: false},
				VecTestRange{Name: "5", From: 2916353, To: 2949120, Match: false},
				VecTestRange{Name: "6", From: 2949121, To: 2981888, Match: false},
				VecTestRange{Name: "7", From: 2981889, To: 3014656, Match: false},
				VecTestRange{Name: "8", From: 3014657, To: 3047424, Match: true},
			},
		},
	}

	for i, v := range tests {
		for _, r := range v.Ranges {
			if want, got := r.Match, Int64.ContainsRange(v.Slice, r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkInt64SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Int64.Sort(randInt64Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := rand.Int63(), rand.Int63()
				if min > max {
					min, max = max, min
				}
				Int64.ContainsRange(a, min, max)
			}
		})
	}
}
