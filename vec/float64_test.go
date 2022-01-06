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

const Float64Size = 8

func randFloat64Slice(n, u int) []float64 {
	s := make([]float64, n*u)
	for i := 0; i < n; i++ {
		s[i] = rand.Float64()
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

type Float64MatchTest struct {
	name   string
	slice  []float64
	match  float64
	match2 float64
	result []byte
	count  int64
}

var (
	float64TestSlice_0 = []float64{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	float64EqualTestMatch_0  float64 = 5
	float64EqualTestResult_0         = []byte{0x6a, 0x1e, 0x48, 0x2c}

	float64NotEqualTestMatch_0  float64 = 5
	float64NotEqualTestResult_0         = []byte{0x95, 0xe1, 0xb7, 0xd3}

	float64LessTestMatch_0  float64 = 5
	float64LessTestResult_0         = []byte{0x05, 0x21, 0x27, 0x01}

	float64LessEqualTestMatch_0  float64 = 5
	float64LessEqualTestResult_0         = []byte{0x6f, 0x3f, 0x6f, 0x2d}

	float64GreaterTestMatch_0  float64 = 5
	float64GreaterTestResult_0         = []byte{0x90, 0xc0, 0x90, 0xd2}

	float64GreaterEqualTestMatch_0  float64 = 5
	float64GreaterEqualTestResult_0         = []byte{0xfa, 0xde, 0xd8, 0xfe}

	float64BetweenTestMatch_0  float64 = 5
	float64BetweenTestMatch_0b float64 = 10
	float64BetweenTestResult_0         = []byte{0xfa, 0x1e, 0xd8, 0x2c}

	// positive int values only
	float64TestSlice_1 = []float64{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 500000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}
	float64EqualTestResult_1         = []byte{0x41, 0x42, 0xc4, 0x0e}
	float64EqualTestMatch_1  float64 = 5

	float64NotEqualTestResult_1         = []byte{0xbe, 0xbd, 0x3b, 0xf1}
	float64NotEqualTestMatch_1  float64 = 5

	float64LessTestResult_1         = []byte{0x0e, 0x00, 0x00, 0x00}
	float64LessTestMatch_1  float64 = 5

	float64LessEqualTestResult_1         = []byte{0x4f, 0x42, 0xc4, 0x0e}
	float64LessEqualTestMatch_1  float64 = 5

	float64GreaterTestResult_1         = []byte{0xb0, 0xbd, 0x3b, 0xf1}
	float64GreaterTestMatch_1  float64 = 5

	float64GreaterEqualTestResult_1         = []byte{0xf1, 0xff, 0xff, 0xff}
	float64GreaterEqualTestMatch_1  float64 = 5

	float64BetweenTestResult_1         = []byte{0xf1, 0x42, 0xc4, 0x0e}
	float64BetweenTestMatch_1  float64 = 5
	float64BetweenTestMatch_1b float64 = 10

	// negative and positive values mixed
	float64TestSlice_2 = []float64{
		-5.12, 2.5, -3.1, 5.45,
		7.125, 8.2, 9.4, -10.25,
		15.25, 50.25, 55.25, 500.25,
		1000.25, -500000.25, 113.25, 12.25,
		31.25, 32.25, 33.25, 34.25,
		35, -36, 37.25, 38.25,
		39.25, 40.25, -41.25, 42.25,
		43.25, 44.25, 45.25, -46.25,
	}
	float64EqualTestResult_2         = []byte{0x01, 0x0, 0x0, 0x0}
	float64EqualTestMatch_2  float64 = -5.12

	float64NotEqualTestResult_2         = []byte{0xfe, 0xff, 0xff, 0xff}
	float64NotEqualTestMatch_2  float64 = -5.12

	float64LessTestResult_2         = []byte{0x80, 0x20, 0x20, 0x84}
	float64LessTestMatch_2  float64 = -5.12

	float64LessEqualTestResult_2         = []byte{0x81, 0x20, 0x20, 0x84}
	float64LessEqualTestMatch_2  float64 = -5.12

	float64GreaterTestResult_2         = []byte{0x7e, 0xdf, 0xdf, 0x7b}
	float64GreaterTestMatch_2  float64 = -5.12

	float64GreaterEqualTestResult_2         = []byte{0x7f, 0xdf, 0xdf, 0x7b}
	float64GreaterEqualTestMatch_2  float64 = -5.12

	float64BetweenTestResult_2         = []byte{0x7f, 0x00, 0x00, 0x00}
	float64BetweenTestMatch_2  float64 = -5.12
	float64BetweenTestMatch_2b float64 = 10

	// extreme values
	float64TestSlice_3 = []float64{
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
	}
	float64EqualTestResult_3         = []byte{0x44, 0x44, 0x44, 0x44}
	float64EqualTestMatch_3  float64 = math.MaxFloat64

	float64NotEqualTestResult_3         = []byte{0xbb, 0xbb, 0xbb, 0xbb}
	float64NotEqualTestMatch_3  float64 = math.MaxFloat64

	float64LessTestResult_3         = []byte{0xbb, 0xbb, 0xbb, 0xbb}
	float64LessTestMatch_3  float64 = math.MaxFloat64

	float64LessEqualTestResult_3         = []byte{0xff, 0xff, 0xff, 0xff}
	float64LessEqualTestMatch_3  float64 = math.MaxFloat64

	float64GreaterTestResult_3         = []byte{0x0, 0x0, 0x0, 0x0}
	float64GreaterTestMatch_3  float64 = math.MaxFloat64

	float64GreaterEqualTestResult_3         = []byte{0x44, 0x44, 0x44, 0x44}
	float64GreaterEqualTestMatch_3  float64 = math.MaxFloat64

	float64BetweenTestResult_3         = []byte{0x55, 0x55, 0x55, 0x55}
	float64BetweenTestMatch_3  float64 = math.MaxFloat32
	float64BetweenTestMatch_3b float64 = math.MaxFloat64

	// NaN/Inf values
	float64TestSlice_4 = []float64{
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
		math.Inf(-1), math.Inf(0),
		math.NaN(), math.NaN(),
	}
	float64EqualTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float64EqualTestMatch_4  float64 = math.NaN()

	float64NotEqualTestResult_4         = []byte{0xff, 0xff, 0xff, 0xff}
	float64NotEqualTestMatch_4  float64 = math.NaN()

	float64LessTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float64LessTestMatch_4  float64 = math.NaN()

	float64LessEqualTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float64LessEqualTestMatch_4  float64 = math.NaN()

	float64GreaterTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float64GreaterTestMatch_4  float64 = math.NaN()

	float64GreaterEqualTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float64GreaterEqualTestMatch_4  float64 = math.NaN()

	float64BetweenTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float64BetweenTestMatch_4  float64 = math.NaN()
	float64BetweenTestMatch_4b float64 = math.NaN()
)

// creates an uint64 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - match, match2: are only copied to the resulting test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateFloat64TestCase(name string, slice []float64, match, match2 float64, result []byte, length int) Float64MatchTest {
	if len(slice)%8 != 0 {
		panic("CreateUint64TestCase: length of slice has to be a multiple of 8")
	}
	if len(result) != bitFieldLen(len(slice)) {
		panic("CreateUint64TestCase: length of slice and length of result does not match")
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []float64
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
	return Float64MatchTest{
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
var float64EqualCases = []Float64MatchTest{
	{
		name:   "l0",
		slice:  make([]float64, 0),
		match:  float64EqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  float64EqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateFloat64TestCase("vec1", float64TestSlice_0, float64EqualTestMatch_0, 0, float64EqualTestResult_0, 32),
	CreateFloat64TestCase("vec2", float64TestSlice_0, float64EqualTestMatch_0, 0, float64EqualTestResult_0, 64),
	CreateFloat64TestCase("l32", float64TestSlice_1, float64EqualTestMatch_1, 0, float64EqualTestResult_1, 32),
	CreateFloat64TestCase("l64", append(float64TestSlice_1, float64TestSlice_0...), float64EqualTestMatch_1, 0,
		append(float64EqualTestResult_1, float64EqualTestResult_0...), 64),
	CreateFloat64TestCase("l128", append(float64TestSlice_1, float64TestSlice_0...), float64EqualTestMatch_1, 0,
		append(float64EqualTestResult_1, float64EqualTestResult_0...), 128),
	CreateFloat64TestCase("l127", append(float64TestSlice_1, float64TestSlice_0...), float64EqualTestMatch_1, 0,
		append(float64EqualTestResult_1, float64EqualTestResult_0...), 127),
	CreateFloat64TestCase("l63", float64TestSlice_1, float64EqualTestMatch_1, 0, float64EqualTestResult_1, 63),
	CreateFloat64TestCase("l31", float64TestSlice_1, float64EqualTestMatch_1, 0, float64EqualTestResult_1, 31),
	CreateFloat64TestCase("l23", float64TestSlice_1, float64EqualTestMatch_1, 0, float64EqualTestResult_1, 23),
	CreateFloat64TestCase("l15", float64TestSlice_1, float64EqualTestMatch_1, 0, float64EqualTestResult_1, 15),
	CreateFloat64TestCase("l7", float64TestSlice_1, float64EqualTestMatch_1, 0, float64EqualTestResult_1, 7),
	CreateFloat64TestCase("neg64", float64TestSlice_2, float64EqualTestMatch_2, 0, float64EqualTestResult_2, 64),
	CreateFloat64TestCase("neg32", float64TestSlice_2, float64EqualTestMatch_2, 0, float64EqualTestResult_2, 32),
	CreateFloat64TestCase("neg31", float64TestSlice_2, float64EqualTestMatch_2, 0, float64EqualTestResult_2, 31),
	CreateFloat64TestCase("ext64", float64TestSlice_3, float64EqualTestMatch_3, 0, float64EqualTestResult_3, 64),
	CreateFloat64TestCase("ext32", float64TestSlice_3, float64EqualTestMatch_3, 0, float64EqualTestResult_3, 32),
	CreateFloat64TestCase("ext31", float64TestSlice_3, float64EqualTestMatch_3, 0, float64EqualTestResult_3, 31),
	CreateFloat64TestCase("nan64", float64TestSlice_4, float64EqualTestMatch_4, 0, float64EqualTestResult_4, 64),
	CreateFloat64TestCase("nan32", float64TestSlice_4, float64EqualTestMatch_4, 0, float64EqualTestResult_4, 32),
	CreateFloat64TestCase("nan31", float64TestSlice_4, float64EqualTestMatch_4, 0, float64EqualTestResult_4, 31),
}

func TestMatchFloat64EqualGeneric(T *testing.T) {
	for _, c := range float64EqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchFloat64EqualGeneric(c.slice, c.match, bits)
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
func BenchmarkMatchFloat64EqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randFloat64Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Float64Size))
			for i := 0; i < B.N; i++ {
				matchFloat64EqualGeneric(a, 0.5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
//
var float64NotEqualCases = []Float64MatchTest{
	{
		name:   "l0",
		slice:  make([]float64, 0),
		match:  float64NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  float64NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateFloat64TestCase("vec1", float64TestSlice_0, float64NotEqualTestMatch_0, 0, float64NotEqualTestResult_0, 32),
	CreateFloat64TestCase("vec2", float64TestSlice_0, float64NotEqualTestMatch_0, 0, float64NotEqualTestResult_0, 64),
	CreateFloat64TestCase("l32", float64TestSlice_1, float64NotEqualTestMatch_1, 0, float64NotEqualTestResult_1, 32),
	CreateFloat64TestCase("l64", append(float64TestSlice_1, float64TestSlice_0...), float64NotEqualTestMatch_1, 0,
		append(float64NotEqualTestResult_1, float64NotEqualTestResult_0...), 64),
	CreateFloat64TestCase("l128", append(float64TestSlice_1, float64TestSlice_0...), float64NotEqualTestMatch_1, 0,
		append(float64NotEqualTestResult_1, float64NotEqualTestResult_0...), 128),
	CreateFloat64TestCase("l127", append(float64TestSlice_1, float64TestSlice_0...), float64NotEqualTestMatch_1, 0,
		append(float64NotEqualTestResult_1, float64NotEqualTestResult_0...), 127),
	CreateFloat64TestCase("l63", float64TestSlice_1, float64NotEqualTestMatch_1, 0, float64NotEqualTestResult_1, 63),
	CreateFloat64TestCase("l31", float64TestSlice_1, float64NotEqualTestMatch_1, 0, float64NotEqualTestResult_1, 31),
	CreateFloat64TestCase("l23", float64TestSlice_1, float64NotEqualTestMatch_1, 0, float64NotEqualTestResult_1, 23),
	CreateFloat64TestCase("l15", float64TestSlice_1, float64NotEqualTestMatch_1, 0, float64NotEqualTestResult_1, 15),
	CreateFloat64TestCase("l7", float64TestSlice_1, float64NotEqualTestMatch_1, 0, float64NotEqualTestResult_1, 7),
	CreateFloat64TestCase("neg64", float64TestSlice_2, float64NotEqualTestMatch_2, 0, float64NotEqualTestResult_2, 64),
	CreateFloat64TestCase("neg32", float64TestSlice_2, float64NotEqualTestMatch_2, 0, float64NotEqualTestResult_2, 32),
	CreateFloat64TestCase("neg31", float64TestSlice_2, float64NotEqualTestMatch_2, 0, float64NotEqualTestResult_2, 31),
	CreateFloat64TestCase("ext64", float64TestSlice_3, float64NotEqualTestMatch_3, 0, float64NotEqualTestResult_3, 64),
	CreateFloat64TestCase("ext32", float64TestSlice_3, float64NotEqualTestMatch_3, 0, float64NotEqualTestResult_3, 32),
	CreateFloat64TestCase("ext31", float64TestSlice_3, float64NotEqualTestMatch_3, 0, float64NotEqualTestResult_3, 31),
	CreateFloat64TestCase("nan64", float64TestSlice_4, float64NotEqualTestMatch_4, 0, float64NotEqualTestResult_4, 64),
	CreateFloat64TestCase("nan32", float64TestSlice_4, float64NotEqualTestMatch_4, 0, float64NotEqualTestResult_4, 32),
	CreateFloat64TestCase("nan31", float64TestSlice_4, float64NotEqualTestMatch_4, 0, float64NotEqualTestResult_4, 31),
}

func TestMatchFloat64NotEqualGeneric(T *testing.T) {
	for _, c := range float64NotEqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchFloat64NotEqualGeneric(c.slice, c.match, bits)
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
func BenchmarkMatchFloat64NotEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randFloat64Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Float64Size))
			for i := 0; i < B.N; i++ {
				matchFloat64NotEqualGeneric(a, 0.5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
//
var float64LessCases = []Float64MatchTest{
	{
		name:   "l0",
		slice:  make([]float64, 0),
		match:  float64LessTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  float64LessTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateFloat64TestCase("vec1", float64TestSlice_0, float64LessTestMatch_0, 0, float64LessTestResult_0, 32),
	CreateFloat64TestCase("vec2", float64TestSlice_0, float64LessTestMatch_0, 0, float64LessTestResult_0, 64),
	CreateFloat64TestCase("l32", float64TestSlice_1, float64LessTestMatch_1, 0, float64LessTestResult_1, 32),
	CreateFloat64TestCase("l64", append(float64TestSlice_1, float64TestSlice_0...), float64LessTestMatch_1, 0,
		append(float64LessTestResult_1, float64LessTestResult_0...), 64),
	CreateFloat64TestCase("l128", append(float64TestSlice_1, float64TestSlice_0...), float64LessTestMatch_1, 0,
		append(float64LessTestResult_1, float64LessTestResult_0...), 128),
	CreateFloat64TestCase("l127", append(float64TestSlice_1, float64TestSlice_0...), float64LessTestMatch_1, 0,
		append(float64LessTestResult_1, float64LessTestResult_0...), 127),
	CreateFloat64TestCase("l63", float64TestSlice_1, float64LessTestMatch_1, 0, float64LessTestResult_1, 63),
	CreateFloat64TestCase("l31", float64TestSlice_1, float64LessTestMatch_1, 0, float64LessTestResult_1, 31),
	CreateFloat64TestCase("l23", float64TestSlice_1, float64LessTestMatch_1, 0, float64LessTestResult_1, 23),
	CreateFloat64TestCase("l15", float64TestSlice_1, float64LessTestMatch_1, 0, float64LessTestResult_1, 15),
	CreateFloat64TestCase("l7", float64TestSlice_1, float64LessTestMatch_1, 0, float64LessTestResult_1, 7),
	CreateFloat64TestCase("neg64", float64TestSlice_2, float64LessTestMatch_2, 0, float64LessTestResult_2, 64),
	CreateFloat64TestCase("neg32", float64TestSlice_2, float64LessTestMatch_2, 0, float64LessTestResult_2, 32),
	CreateFloat64TestCase("neg31", float64TestSlice_2, float64LessTestMatch_2, 0, float64LessTestResult_2, 31),
	CreateFloat64TestCase("ext64", float64TestSlice_3, float64LessTestMatch_3, 0, float64LessTestResult_3, 64),
	CreateFloat64TestCase("ext32", float64TestSlice_3, float64LessTestMatch_3, 0, float64LessTestResult_3, 32),
	CreateFloat64TestCase("ext31", float64TestSlice_3, float64LessTestMatch_3, 0, float64LessTestResult_3, 31),
	CreateFloat64TestCase("nan64", float64TestSlice_4, float64LessTestMatch_4, 0, float64LessTestResult_4, 64),
	CreateFloat64TestCase("nan32", float64TestSlice_4, float64LessTestMatch_4, 0, float64LessTestResult_4, 32),
	CreateFloat64TestCase("nan31", float64TestSlice_4, float64LessTestMatch_4, 0, float64LessTestResult_4, 31),
}

func TestMatchFloat64LessGeneric(T *testing.T) {
	for _, c := range float64LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchFloat64LessThanGeneric(c.slice, c.match, bits)
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
func BenchmarkMatchFloat64LessGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randFloat64Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Float64Size))
			for i := 0; i < B.N; i++ {
				matchFloat64LessThanGeneric(a, 0.5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//
var float64LessEqualCases = []Float64MatchTest{
	{
		name:   "l0",
		slice:  make([]float64, 0),
		match:  float64LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  float64LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateFloat64TestCase("vec1", float64TestSlice_0, float64LessEqualTestMatch_0, 0, float64LessEqualTestResult_0, 32),
	CreateFloat64TestCase("vec2", float64TestSlice_0, float64LessEqualTestMatch_0, 0, float64LessEqualTestResult_0, 64),
	CreateFloat64TestCase("l32", float64TestSlice_1, float64LessEqualTestMatch_1, 0, float64LessEqualTestResult_1, 32),
	CreateFloat64TestCase("l64", append(float64TestSlice_1, float64TestSlice_0...), float64LessEqualTestMatch_1, 0,
		append(float64LessEqualTestResult_1, float64LessEqualTestResult_0...), 64),
	CreateFloat64TestCase("l128", append(float64TestSlice_1, float64TestSlice_0...), float64LessEqualTestMatch_1, 0,
		append(float64LessEqualTestResult_1, float64LessEqualTestResult_0...), 128),
	CreateFloat64TestCase("l127", append(float64TestSlice_1, float64TestSlice_0...), float64LessEqualTestMatch_1, 0,
		append(float64LessEqualTestResult_1, float64LessEqualTestResult_0...), 127),
	CreateFloat64TestCase("l63", float64TestSlice_1, float64LessEqualTestMatch_1, 0, float64LessEqualTestResult_1, 63),
	CreateFloat64TestCase("l31", float64TestSlice_1, float64LessEqualTestMatch_1, 0, float64LessEqualTestResult_1, 31),
	CreateFloat64TestCase("l23", float64TestSlice_1, float64LessEqualTestMatch_1, 0, float64LessEqualTestResult_1, 23),
	CreateFloat64TestCase("l15", float64TestSlice_1, float64LessEqualTestMatch_1, 0, float64LessEqualTestResult_1, 15),
	CreateFloat64TestCase("l7", float64TestSlice_1, float64LessEqualTestMatch_1, 0, float64LessEqualTestResult_1, 7),
	CreateFloat64TestCase("neg64", float64TestSlice_2, float64LessEqualTestMatch_2, 0, float64LessEqualTestResult_2, 64),
	CreateFloat64TestCase("neg32", float64TestSlice_2, float64LessEqualTestMatch_2, 0, float64LessEqualTestResult_2, 32),
	CreateFloat64TestCase("neg31", float64TestSlice_2, float64LessEqualTestMatch_2, 0, float64LessEqualTestResult_2, 31),
	CreateFloat64TestCase("ext64", float64TestSlice_3, float64LessEqualTestMatch_3, 0, float64LessEqualTestResult_3, 64),
	CreateFloat64TestCase("ext32", float64TestSlice_3, float64LessEqualTestMatch_3, 0, float64LessEqualTestResult_3, 32),
	CreateFloat64TestCase("ext31", float64TestSlice_3, float64LessEqualTestMatch_3, 0, float64LessEqualTestResult_3, 31),
	CreateFloat64TestCase("nan64", float64TestSlice_4, float64LessEqualTestMatch_4, 0, float64LessEqualTestResult_4, 64),
	CreateFloat64TestCase("nan32", float64TestSlice_4, float64LessEqualTestMatch_4, 0, float64LessEqualTestResult_4, 32),
	CreateFloat64TestCase("nan31", float64TestSlice_4, float64LessEqualTestMatch_4, 0, float64LessEqualTestResult_4, 31),
}

func TestMatchFloat64LessEqualGeneric(T *testing.T) {
	for _, c := range float64LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchFloat64LessThanEqualGeneric(c.slice, c.match, bits)
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
func BenchmarkMatchFloat64LessEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randFloat64Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Float64Size))
			for i := 0; i < B.N; i++ {
				matchFloat64LessThanEqualGeneric(a, 0.5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
//
var float64GreaterCases = []Float64MatchTest{
	{
		name:   "l0",
		slice:  make([]float64, 0),
		match:  float64GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  float64GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateFloat64TestCase("vec1", float64TestSlice_0, float64GreaterTestMatch_0, 0, float64GreaterTestResult_0, 32),
	CreateFloat64TestCase("vec2", float64TestSlice_0, float64GreaterTestMatch_0, 0, float64GreaterTestResult_0, 64),
	CreateFloat64TestCase("l32", float64TestSlice_1, float64GreaterTestMatch_1, 0, float64GreaterTestResult_1, 32),
	CreateFloat64TestCase("l64", append(float64TestSlice_1, float64TestSlice_0...), float64GreaterTestMatch_1, 0,
		append(float64GreaterTestResult_1, float64GreaterTestResult_0...), 64),
	CreateFloat64TestCase("l128", append(float64TestSlice_1, float64TestSlice_0...), float64GreaterTestMatch_1, 0,
		append(float64GreaterTestResult_1, float64GreaterTestResult_0...), 128),
	CreateFloat64TestCase("l127", append(float64TestSlice_1, float64TestSlice_0...), float64GreaterTestMatch_1, 0,
		append(float64GreaterTestResult_1, float64GreaterTestResult_0...), 127),
	CreateFloat64TestCase("l63", float64TestSlice_1, float64GreaterTestMatch_1, 0, float64GreaterTestResult_1, 63),
	CreateFloat64TestCase("l31", float64TestSlice_1, float64GreaterTestMatch_1, 0, float64GreaterTestResult_1, 31),
	CreateFloat64TestCase("l23", float64TestSlice_1, float64GreaterTestMatch_1, 0, float64GreaterTestResult_1, 23),
	CreateFloat64TestCase("l15", float64TestSlice_1, float64GreaterTestMatch_1, 0, float64GreaterTestResult_1, 15),
	CreateFloat64TestCase("l7", float64TestSlice_1, float64GreaterTestMatch_1, 0, float64GreaterTestResult_1, 7),
	CreateFloat64TestCase("neg64", float64TestSlice_2, float64GreaterTestMatch_2, 0, float64GreaterTestResult_2, 64),
	CreateFloat64TestCase("neg32", float64TestSlice_2, float64GreaterTestMatch_2, 0, float64GreaterTestResult_2, 32),
	CreateFloat64TestCase("neg31", float64TestSlice_2, float64GreaterTestMatch_2, 0, float64GreaterTestResult_2, 31),
	CreateFloat64TestCase("ext64", float64TestSlice_3, float64GreaterTestMatch_3, 0, float64GreaterTestResult_3, 64),
	CreateFloat64TestCase("ext32", float64TestSlice_3, float64GreaterTestMatch_3, 0, float64GreaterTestResult_3, 32),
	CreateFloat64TestCase("ext31", float64TestSlice_3, float64GreaterTestMatch_3, 0, float64GreaterTestResult_3, 31),
	CreateFloat64TestCase("nan64", float64TestSlice_4, float64GreaterTestMatch_4, 0, float64GreaterTestResult_4, 64),
	CreateFloat64TestCase("nan32", float64TestSlice_4, float64GreaterTestMatch_4, 0, float64GreaterTestResult_4, 32),
	CreateFloat64TestCase("nan31", float64TestSlice_4, float64GreaterTestMatch_4, 0, float64GreaterTestResult_4, 31),
}

func TestMatchFloat64GreaterGeneric(T *testing.T) {
	for _, c := range float64GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchFloat64GreaterThanGeneric(c.slice, c.match, bits)
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
func BenchmarkMatchFloat64GreaterGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randFloat64Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Float64Size))
			for i := 0; i < B.N; i++ {
				matchFloat64GreaterThanGeneric(a, 0.5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//
var float64GreaterEqualCases = []Float64MatchTest{
	{
		name:   "l0",
		slice:  make([]float64, 0),
		match:  float64GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  float64GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateFloat64TestCase("vec1", float64TestSlice_0, float64GreaterEqualTestMatch_0, 0, float64GreaterEqualTestResult_0, 32),
	CreateFloat64TestCase("vec2", float64TestSlice_0, float64GreaterEqualTestMatch_0, 0, float64GreaterEqualTestResult_0, 64),
	CreateFloat64TestCase("l32", float64TestSlice_1, float64GreaterEqualTestMatch_1, 0, float64GreaterEqualTestResult_1, 32),
	CreateFloat64TestCase("l64", append(float64TestSlice_1, float64TestSlice_0...), float64GreaterEqualTestMatch_1, 0,
		append(float64GreaterEqualTestResult_1, float64GreaterEqualTestResult_0...), 64),
	CreateFloat64TestCase("l128", append(float64TestSlice_1, float64TestSlice_0...), float64GreaterEqualTestMatch_1, 0,
		append(float64GreaterEqualTestResult_1, float64GreaterEqualTestResult_0...), 128),
	CreateFloat64TestCase("l127", append(float64TestSlice_1, float64TestSlice_0...), float64GreaterEqualTestMatch_1, 0,
		append(float64GreaterEqualTestResult_1, float64GreaterEqualTestResult_0...), 127),
	CreateFloat64TestCase("l63", float64TestSlice_1, float64GreaterEqualTestMatch_1, 0, float64GreaterEqualTestResult_1, 63),
	CreateFloat64TestCase("l31", float64TestSlice_1, float64GreaterEqualTestMatch_1, 0, float64GreaterEqualTestResult_1, 31),
	CreateFloat64TestCase("l23", float64TestSlice_1, float64GreaterEqualTestMatch_1, 0, float64GreaterEqualTestResult_1, 23),
	CreateFloat64TestCase("l15", float64TestSlice_1, float64GreaterEqualTestMatch_1, 0, float64GreaterEqualTestResult_1, 15),
	CreateFloat64TestCase("l7", float64TestSlice_1, float64GreaterEqualTestMatch_1, 0, float64GreaterEqualTestResult_1, 7),
	CreateFloat64TestCase("neg64", float64TestSlice_2, float64GreaterEqualTestMatch_2, 0, float64GreaterEqualTestResult_2, 64),
	CreateFloat64TestCase("neg32", float64TestSlice_2, float64GreaterEqualTestMatch_2, 0, float64GreaterEqualTestResult_2, 32),
	CreateFloat64TestCase("neg31", float64TestSlice_2, float64GreaterEqualTestMatch_2, 0, float64GreaterEqualTestResult_2, 31),
	CreateFloat64TestCase("ext64", float64TestSlice_3, float64GreaterEqualTestMatch_3, 0, float64GreaterEqualTestResult_3, 64),
	CreateFloat64TestCase("ext32", float64TestSlice_3, float64GreaterEqualTestMatch_3, 0, float64GreaterEqualTestResult_3, 32),
	CreateFloat64TestCase("ext31", float64TestSlice_3, float64GreaterEqualTestMatch_3, 0, float64GreaterEqualTestResult_3, 31),
	CreateFloat64TestCase("nan64", float64TestSlice_4, float64GreaterEqualTestMatch_4, 0, float64GreaterEqualTestResult_4, 64),
	CreateFloat64TestCase("nan32", float64TestSlice_4, float64GreaterEqualTestMatch_4, 0, float64GreaterEqualTestResult_4, 32),
	CreateFloat64TestCase("nan31", float64TestSlice_4, float64GreaterEqualTestMatch_4, 0, float64GreaterEqualTestResult_4, 31),
}

func TestMatchFloat64GreaterEqualGeneric(T *testing.T) {
	for _, c := range float64GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchFloat64GreaterThanEqualGeneric(c.slice, c.match, bits)
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
func BenchmarkMatchFloat64GreaterEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randFloat64Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Float64Size))
			for i := 0; i < B.N; i++ {
				matchFloat64GreaterThanEqualGeneric(a, 0.5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
//
var float64BetweenCases = []Float64MatchTest{
	{
		name:   "l0",
		slice:  make([]float64, 0),
		match:  float64BetweenTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  float64BetweenTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateFloat64TestCase("vec1", float64TestSlice_0, float64BetweenTestMatch_0, float64BetweenTestMatch_0b, float64BetweenTestResult_0, 32),
	CreateFloat64TestCase("vec2", float64TestSlice_0, float64BetweenTestMatch_0, float64BetweenTestMatch_0b, float64BetweenTestResult_0, 64),
	CreateFloat64TestCase("l32", float64TestSlice_1, float64BetweenTestMatch_1, float64BetweenTestMatch_1b, float64BetweenTestResult_1, 32),
	CreateFloat64TestCase("l64", append(float64TestSlice_1, float64TestSlice_0...), float64BetweenTestMatch_1, float64BetweenTestMatch_1b,
		append(float64BetweenTestResult_1, float64BetweenTestResult_0...), 64),
	CreateFloat64TestCase("l128", append(float64TestSlice_1, float64TestSlice_0...), float64BetweenTestMatch_1, float64BetweenTestMatch_1b,
		append(float64BetweenTestResult_1, float64BetweenTestResult_0...), 128),
	CreateFloat64TestCase("l127", append(float64TestSlice_1, float64TestSlice_0...), float64BetweenTestMatch_1, float64BetweenTestMatch_1b,
		append(float64BetweenTestResult_1, float64BetweenTestResult_0...), 127),
	CreateFloat64TestCase("l63", float64TestSlice_1, float64BetweenTestMatch_1, float64BetweenTestMatch_1b, float64BetweenTestResult_1, 63),
	CreateFloat64TestCase("l31", float64TestSlice_1, float64BetweenTestMatch_1, float64BetweenTestMatch_1b, float64BetweenTestResult_1, 31),
	CreateFloat64TestCase("l23", float64TestSlice_1, float64BetweenTestMatch_1, float64BetweenTestMatch_1b, float64BetweenTestResult_1, 23),
	CreateFloat64TestCase("l15", float64TestSlice_1, float64BetweenTestMatch_1, float64BetweenTestMatch_1b, float64BetweenTestResult_1, 15),
	CreateFloat64TestCase("l7", float64TestSlice_1, float64BetweenTestMatch_1, float64BetweenTestMatch_1b, float64BetweenTestResult_1, 7),
	CreateFloat64TestCase("neg64", float64TestSlice_2, float64BetweenTestMatch_2, float64BetweenTestMatch_2b, float64BetweenTestResult_2, 64),
	CreateFloat64TestCase("neg32", float64TestSlice_2, float64BetweenTestMatch_2, float64BetweenTestMatch_2b, float64BetweenTestResult_2, 32),
	CreateFloat64TestCase("neg31", float64TestSlice_2, float64BetweenTestMatch_2, float64BetweenTestMatch_2b, float64BetweenTestResult_2, 31),
	CreateFloat64TestCase("ext64", float64TestSlice_3, float64BetweenTestMatch_3, float64BetweenTestMatch_3b, float64BetweenTestResult_3, 64),
	CreateFloat64TestCase("ext32", float64TestSlice_3, float64BetweenTestMatch_3, float64BetweenTestMatch_3b, float64BetweenTestResult_3, 32),
	CreateFloat64TestCase("ext31", float64TestSlice_3, float64BetweenTestMatch_3, float64BetweenTestMatch_3b, float64BetweenTestResult_3, 31),
	CreateFloat64TestCase("nan64", float64TestSlice_4, float64BetweenTestMatch_4, float64BetweenTestMatch_4b, float64BetweenTestResult_4, 64),
	CreateFloat64TestCase("nan32", float64TestSlice_4, float64BetweenTestMatch_4, float64BetweenTestMatch_4b, float64BetweenTestResult_4, 32),
	CreateFloat64TestCase("nan31", float64TestSlice_4, float64BetweenTestMatch_4, float64BetweenTestMatch_4b, float64BetweenTestResult_4, 31),
}

func TestMatchFloat64BetweenGeneric(T *testing.T) {
	for _, c := range float64BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchFloat64BetweenGeneric(c.slice, c.match, c.match2, bits)
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
func BenchmarkMatchFloat64BetweenGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randFloat64Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Float64Size))
			for i := 0; i < B.N; i++ {
				matchFloat64BetweenGeneric(a, 0.25, 0.5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Float64 Slice
//
func TestFloat64SliceContains(T *testing.T) {
	// nil slice
	if Float64.Contains(nil, 1) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Float64.Contains([]float64{}, 1) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Float64.Contains([]float64{1}, 1) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Float64.Contains([]float64{1}, 2) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !Float64.Contains([]float64{1, 3, 5, 7, 11, 13}, 1) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Float64.Contains([]float64{1, 3, 5, 7, 11, 13}, 5) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Float64.Contains([]float64{1, 3, 5, 7, 11, 13}, 13) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Float64.Contains([]float64{1, 3, 5, 7, 11, 13}, 0) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Float64.Contains([]float64{1, 3, 5, 7, 11, 13}, 2) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Float64.Contains([]float64{1, 3, 5, 7, 11, 13}, 14) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkFloat64SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Float64.Sort(randFloat64Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				Float64.Contains(a, rand.Float64())
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Float64.Sort(randFloat64Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				Float64.Contains(a, a[rand.Intn(len(a))])
			}
		})
	}
}

func TestFloat64SliceContainsRange(T *testing.T) {
	type VecTestRange struct {
		Name  string
		From  float64
		To    float64
		Match bool
	}

	type VecTestcase struct {
		Slice  []float64
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
			Slice: []float64{},
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: 0, To: 2, Match: false},
			},
		},
		// 1-element slice
		VecTestcase{
			Slice: []float64{3},
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
			Slice: []float64{3},
			Ranges: []VecTestRange{
				VecTestRange{Name: "BCD", From: 3, To: 3, Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		VecTestcase{
			Slice: []float64{3, 5, 7, 11, 13},
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
			Slice: []float64{
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
			if want, got := r.Match, Float64.ContainsRange(v.Slice, r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkFloat64SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Float64.Sort(randFloat64Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := rand.Float64(), rand.Float64()
				if min > max {
					min, max = max, min
				}
				Float64.ContainsRange(a, min, max)
			}
		})
	}
}
