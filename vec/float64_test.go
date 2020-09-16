// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package vec

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"testing"
)

func randFloat64Slice(n, u int) []float64 {
	s := make([]float64, n*u)
	for i := 0; i < n; i++ {
		s[i] = rand.Float64()
	}
	for i := 0; i < u; i++ {
		s = append(s, s[:n]...)
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
	float64NotEqualTestMatch_0  float64 = 5
	float64NotEqualTestResult_0         = []byte{0xa9, 0x87, 0xed, 0xcb}

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
	float64EqualTestResult_1         = []byte{0x82, 0x42, 0x23, 0x70}
	float64EqualTestMatch_1  float64 = 5
	float64EqualTestCount_1  int64   = 10

	float64NotEqualTestResult_1         = []byte{0x7d, 0xbd, 0xdc, 0x8f}
	float64NotEqualTestMatch_1  float64 = 5

	float64LessTestResult_1         = []byte{0x70, 0x00, 0x00, 0x00}
	float64LessTestMatch_1  float64 = 5
	float64LessTestCount_1  int64   = 3

	float64LessEqualTestResult_1         = []byte{0xf2, 0x42, 0x23, 0x70}
	float64LessEqualTestMatch_1  float64 = 5
	float64LessEqualTestCount_1  int64   = 13

	float64GreaterTestResult_1         = []byte{0x0d, 0xbd, 0xdc, 0x8f}
	float64GreaterTestMatch_1  float64 = 5
	float64GreaterTestCount_1  int64   = 19

	float64GreaterEqualTestResult_1         = []byte{0x8f, 0xff, 0xff, 0xff}
	float64GreaterEqualTestMatch_1  float64 = 5
	float64GreaterEqualTestCount_1  int64   = 29

	float64BetweenTestResult_1         = []byte{0x8f, 0x42, 0x23, 0x70}
	float64BetweenTestMatch_1  float64 = 5
	float64BetweenTestMatch_1b float64 = 10
	float64BetweenTestCount_1  int64   = 13

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
	float64EqualTestResult_2         = []byte{0x80, 0x0, 0x0, 0x0}
	float64EqualTestMatch_2  float64 = -5.12
	float64EqualTestCount_2  int64   = 1

	float64NotEqualTestResult_2         = []byte{0x7f, 0xff, 0xff, 0xff}
	float64NotEqualTestMatch_2  float64 = -5.12

	float64LessTestResult_2         = []byte{0x01, 0x04, 0x04, 0x21}
	float64LessTestMatch_2  float64 = -5.12
	float64LessTestCount_2  int64   = 5

	float64LessEqualTestResult_2         = []byte{0x81, 0x04, 0x04, 0x21}
	float64LessEqualTestMatch_2  float64 = -5.12
	float64LessEqualTestCount_2  int64   = 6

	float64GreaterTestResult_2         = []byte{0x7e, 0xfb, 0xfb, 0xde}
	float64GreaterTestMatch_2  float64 = -5.12
	float64GreaterTestCount_2  int64   = 26

	float64GreaterEqualTestResult_2         = []byte{0xfe, 0xfb, 0xfb, 0xde}
	float64GreaterEqualTestMatch_2  float64 = -5.12
	float64GreaterEqualTestCount_2  int64   = 27

	float64BetweenTestResult_2         = []byte{0xfe, 0x00, 0x00, 0x00}
	float64BetweenTestMatch_2  float64 = -5.12
	float64BetweenTestMatch_2b float64 = 10
	float64BetweenTestCount_2  int64   = 7

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
	float64EqualTestResult_3         = []byte{0x22, 0x22, 0x22, 0x22}
	float64EqualTestMatch_3  float64 = math.MaxFloat64
	float64EqualTestCount_3  int64   = 8

	float64NotEqualTestResult_3         = []byte{0xdd, 0xdd, 0xdd, 0xdd}
	float64NotEqualTestMatch_3  float64 = math.MaxFloat64

	float64LessTestResult_3         = []byte{0xdd, 0xdd, 0xdd, 0xdd}
	float64LessTestMatch_3  float64 = math.MaxFloat64
	float64LessTestCount_3  int64   = 24

	float64LessEqualTestResult_3         = []byte{0xff, 0xff, 0xff, 0xff}
	float64LessEqualTestMatch_3  float64 = math.MaxFloat64
	float64LessEqualTestCount_3  int64   = 32

	float64GreaterTestResult_3         = []byte{0x0, 0x0, 0x0, 0x0}
	float64GreaterTestMatch_3  float64 = math.MaxFloat64
	float64GreaterTestCount_3  int64   = 0

	float64GreaterEqualTestResult_3         = []byte{0x22, 0x22, 0x22, 0x22}
	float64GreaterEqualTestMatch_3  float64 = math.MaxFloat64
	float64GreaterEqualTestCount_3  int64   = 8

	float64BetweenTestResult_3         = []byte{0xaa, 0xaa, 0xaa, 0xaa}
	float64BetweenTestMatch_3  float64 = math.MaxFloat32
	float64BetweenTestMatch_3b float64 = math.MaxFloat64
	float64BetweenTestCount_3  int64   = 16

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
	float64EqualTestCount_4  int64   = 0

	float64NotEqualTestResult_4         = []byte{0xff, 0xff, 0xff, 0xff}
	float64NotEqualTestMatch_4  float64 = math.NaN()

	float64LessTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float64LessTestMatch_4  float64 = math.NaN()
	float64LessTestCount_4  int64   = 0

	float64LessEqualTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float64LessEqualTestMatch_4  float64 = math.NaN()
	float64LessEqualTestCount_4  int64   = 0

	float64GreaterTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float64GreaterTestMatch_4  float64 = math.NaN()
	float64GreaterTestCount_4  int64   = 0

	float64GreaterEqualTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float64GreaterEqualTestMatch_4  float64 = math.NaN()
	float64GreaterEqualTestCount_4  int64   = 0

	float64BetweenTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float64BetweenTestMatch_4  float64 = math.NaN()
	float64BetweenTestMatch_4b float64 = math.NaN()
	float64BetweenTestCount_4  int64   = 0
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
	/*new_slice := make([]uint64, length)
	for i, _ := range new_slice {
		new_slice[i] = slice[i%len(slice)]
	}*/

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
		new_result[len(new_result)-1] &= 0xff << (8 - length%8)
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
	Float64MatchTest{
		name: "vec1",
		slice: []float64{
			0, 5, 3, 5, // Y1
			7, 5, 5, 9, // Y2
			3, 5, 5, 5, // Y3
			5, 0, 113, 12, // Y4

			4, 2, 3, 5, // Y5
			7, 3, 5, 9, // Y6
			3, 13, 5, 5, // Y7
			42, 5, 113, 12, // Y8
		},
		match:  5,
		result: []byte{0x56, 0x78, 0x12, 0x34},
		count:  13,
	}, {
		name:   "l32",
		slice:  float64TestSlice_1,
		match:  float64EqualTestMatch_1,
		result: float64EqualTestResult_1,
		count:  float64EqualTestCount_1,
	}, {
		name:   "l64",
		slice:  append(float64TestSlice_1, float64TestSlice_1...),
		match:  float64EqualTestMatch_1,
		result: append(float64EqualTestResult_1, float64EqualTestResult_1...),
		count:  float64EqualTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  float64TestSlice_1[:31],
		match:  float64EqualTestMatch_1,
		result: float64EqualTestResult_1,
		count:  float64EqualTestCount_1,
	}, {
		name:   "l23",
		slice:  float64TestSlice_1[:23],
		match:  float64EqualTestMatch_1,
		result: []byte{0x82, 0x42, 0x22}, // last match is gone
		count:  float64EqualTestCount_1 - 4,
	}, {
		name:   "l15",
		slice:  float64TestSlice_1[:15],
		match:  float64EqualTestMatch_1,
		result: float64EqualTestResult_1[:2],
		count:  float64EqualTestCount_1 - 6,
	}, {
		name:   "l7",
		slice:  float64TestSlice_1[:7],
		match:  float64EqualTestMatch_1,
		result: float64EqualTestResult_1[:1],
		count:  float64EqualTestCount_1 - 8,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  float64TestSlice_2,
		match:  float64EqualTestMatch_2,
		result: float64EqualTestResult_2,
		count:  float64EqualTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  float64TestSlice_2[:31],
		match:  float64EqualTestMatch_2,
		result: float64EqualTestResult_2,
		count:  float64EqualTestCount_2,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  float64TestSlice_3,
		match:  float64EqualTestMatch_3,
		result: float64EqualTestResult_3,
		count:  float64EqualTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  float64TestSlice_3[:31],
		match:  float64EqualTestMatch_3,
		result: float64EqualTestResult_3,
		count:  8,
	}, {
		// NaN, Inf
		name:   "nan32",
		slice:  float64TestSlice_4,
		match:  float64EqualTestMatch_4,
		result: float64EqualTestResult_4,
		count:  float64EqualTestCount_4,
	}, {
		name:   "nan31",
		slice:  float64TestSlice_4[:31],
		match:  float64EqualTestMatch_4,
		result: float64EqualTestResult_4,
		count:  float64EqualTestCount_4,
	},
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

func TestMatchFloat64EqualAVX2(T *testing.T) {
	for _, c := range float64EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat64EqualAVX2(c.slice, c.match, bits)
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
// BenchmarkMatchFloat64EqualGeneric/32-8  	30000000      45.2 ns/op	5669.15 MB/s
// BenchmarkMatchFloat64EqualGeneric/128-8 	10000000     163 ns/op	6244.31 MB/s
// BenchmarkMatchFloat64EqualGeneric/1024-8	 1000000    1265 ns/op	6475.16 MB/s
// BenchmarkMatchFloat64EqualGeneric/4096-8	  300000    4858 ns/op	6744.84 MB/s
// BenchmarkMatchFloat64EqualGeneric/65536-8   20000   78689 ns/op	6662.73 MB/s
// BenchmarkMatchFloat64EqualGeneric/131072-8  10000  165887 ns/op	6321.02 MB/s
func BenchmarkMatchFloat64EqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64EqualGeneric(a, 5, bits)
			}
		})
	}
}

// BenchmarkMatchFloat64EqualAVX2/32-8     100000000     13.2 ns/op	19357.71 MB/s
// BenchmarkMatchFloat64EqualAVX2/128-8   	50000000     40.7 ns/op	25157.90 MB/s
// BenchmarkMatchFloat64EqualAVX2/1024-8  	 5000000    310 ns/op	26346.46 MB/s
// BenchmarkMatchFloat64EqualAVX2/4096-8  	 1000000   1391 ns/op	23552.31 MB/s
// BenchmarkMatchFloat64EqualAVX2/65536-8 	   50000  31474 ns/op	16657.80 MB/s
// BenchmarkMatchFloat64EqualAVX2/131072-8     20000  61115 ns/op	17157.18 MB/s
func BenchmarkMatchFloat64EqualAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64EqualAVX2(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
// BenchmarkMatchFloat64EqualAVX2Scalar/31-8    	50000000     38.9 ns/op	6371.31 MB/s
// BenchmarkMatchFloat64EqualAVX2Scalar/127-8   	20000000     66.0 ns/op	15393.19 MB/s
// BenchmarkMatchFloat64EqualAVX2Scalar/1023-8  	 5000000    319 ns/op	25624.28 MB/s
// BenchmarkMatchFloat64EqualAVX2Scalar/4095-8  	 1000000   1377 ns/op	23778.72 MB/s
// BenchmarkMatchFloat64EqualAVX2Scalar/65535-8 	   50000  29679 ns/op	17664.76 MB/s
// BenchmarkMatchFloat64EqualAVX2Scalar/131071-8       20000  59205 ns/op	17710.78 MB/s
func BenchmarkMatchFloat64EqualAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64EqualAVX2(a, 5, bits)
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
	CreateFloat64TestCase("l32", float64TestSlice_1, float64NotEqualTestMatch_1, 0, float64NotEqualTestResult_1, 32),
	CreateFloat64TestCase("l64", float64TestSlice_1, float64NotEqualTestMatch_1, 0, float64NotEqualTestResult_1, 64),
	CreateFloat64TestCase("l31", float64TestSlice_1, float64NotEqualTestMatch_1, 0, float64NotEqualTestResult_1, 31),
	CreateFloat64TestCase("l23", float64TestSlice_1, float64NotEqualTestMatch_1, 0, float64NotEqualTestResult_1, 23),
	CreateFloat64TestCase("l15", float64TestSlice_1, float64NotEqualTestMatch_1, 0, float64NotEqualTestResult_1, 15),
	CreateFloat64TestCase("l7", float64TestSlice_1, float64NotEqualTestMatch_1, 0, float64NotEqualTestResult_1, 7),
	CreateFloat64TestCase("neg32", float64TestSlice_2, float64NotEqualTestMatch_2, 0, float64NotEqualTestResult_2, 32),
	CreateFloat64TestCase("neg31", float64TestSlice_2, float64NotEqualTestMatch_2, 0, float64NotEqualTestResult_2, 31),
	CreateFloat64TestCase("ext32", float64TestSlice_3, float64NotEqualTestMatch_3, 0, float64NotEqualTestResult_3, 32),
	CreateFloat64TestCase("ext31", float64TestSlice_3, float64NotEqualTestMatch_3, 0, float64NotEqualTestResult_3, 31),
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

func TestMatchFloat64NotEqualAVX2(T *testing.T) {
	for _, c := range float64NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat64NotEqualAVX2(c.slice, c.match, bits)
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

func BenchmarkMatchFloat64NotEqualGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randFloat64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64NotEqualGeneric(a, 5, bits)
			}
		})
	}
}

func BenchmarkMatchFloat64NotEqualAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randFloat64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64NotEqualAVX2(a, 5, bits)
			}
		})
	}
}

func BenchmarkMatchFloat64NotEqualAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randFloat64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64NotEqualAVX2(a, 5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
//
var float64LessCases = []Float64MatchTest{
	Float64MatchTest{
		name: "vec1",
		slice: []float64{
			0, 5, 3, 5, // Y1
			7, 5, 5, 9, // Y2
			3, 5, 5, 5, // Y3
			5, 0, 113, 12, // Y4

			4, 2, 3, 5, // Y5
			7, 3, 5, 9, // Y6
			3, 13, 5, 5, // Y7
			42, 5, 113, 12, // Y8
		},
		match:  5,
		result: []byte{0xa0, 0x84, 0xe4, 0x80},
		count:  9,
	}, {
		name:   "l32",
		slice:  float64TestSlice_1,
		match:  float64LessTestMatch_1,
		result: float64LessTestResult_1,
		count:  float64LessTestCount_1,
	}, {
		name:   "l64",
		slice:  append(float64TestSlice_1, float64TestSlice_1...),
		match:  float64LessTestMatch_1,
		result: append(float64LessTestResult_1, float64LessTestResult_1...),
		count:  float64LessTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  float64TestSlice_1[:31],
		match:  float64LessTestMatch_1,
		result: float64LessTestResult_1,
		count:  float64LessTestCount_1,
	}, {
		name:   "l23",
		slice:  float64TestSlice_1[:23],
		match:  float64LessTestMatch_1,
		result: float64LessTestResult_1[:3],
		count:  float64LessTestCount_1,
	}, {
		name:   "l15",
		slice:  float64TestSlice_1[:15],
		match:  float64LessTestMatch_1,
		result: float64LessTestResult_1[:2],
		count:  float64LessTestCount_1,
	}, {
		name:   "l7",
		slice:  float64TestSlice_1[:7],
		match:  float64LessTestMatch_1,
		result: float64LessTestResult_1[:1],
		count:  float64LessTestCount_1,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  float64TestSlice_2,
		match:  float64LessTestMatch_2,
		result: float64LessTestResult_2,
		count:  float64LessTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  float64TestSlice_2[:31],
		match:  float64LessTestMatch_2,
		result: []byte{0x01, 0x04, 0x04, 0x20}, // last bit off
		count:  float64LessTestCount_2 - 1,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  float64TestSlice_3,
		match:  float64LessTestMatch_3,
		result: float64LessTestResult_3,
		count:  float64LessTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  float64TestSlice_3[:31],
		match:  float64LessTestMatch_3,
		result: []byte{0xdd, 0xdd, 0xdd, 0xdc},
		count:  float64LessTestCount_3 - 1,
	}, {
		// NaN, Inf
		name:   "nan32",
		slice:  float64TestSlice_4,
		match:  float64LessTestMatch_4,
		result: float64LessTestResult_4,
		count:  float64LessTestCount_4,
	}, {
		name:   "nan31",
		slice:  float64TestSlice_4[:31],
		match:  float64LessTestMatch_4,
		result: float64LessTestResult_4,
		count:  float64LessTestCount_4,
	},
}

func TestMatchFloat64LessGeneric(T *testing.T) {
	for _, c := range float64LessCases {
		// pre-allocate the result slice and fill with poison
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

func TestMatchFloat64LessAVX2(T *testing.T) {
	for _, c := range float64LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat64LessThanAVX2(c.slice, c.match, bits)
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
// BenchmarkMatchFloat64LessGeneric/32-8   	10000000     139 ns/op	1831.54 MB/s
// BenchmarkMatchFloat64LessGeneric/128-8  	 3000000     525 ns/op	1947.79 MB/s
// BenchmarkMatchFloat64LessGeneric/1024-8 	  300000    4289 ns/op	1909.69 MB/s
// BenchmarkMatchFloat64LessGeneric/4096-8 	  100000   16821 ns/op	1947.93 MB/s
// BenchmarkMatchFloat64LessGeneric/65536-8	    5000  267706 ns/op	1958.45 MB/s
// BenchmarkMatchFloat64LessGeneric/131072-8    3000  540058 ns/op	1941.60 MB/s
func BenchmarkMatchFloat64LessGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64LessThanGeneric(a, 5, bits)
			}
		})
	}
}

// BenchmarkMatchFloat64LessAVX2/32-8      100000000     12.3 ns/op	20870.40 MB/s
// BenchmarkMatchFloat64LessAVX2/128-8  	50000000     38.1 ns/op	26883.20 MB/s
// BenchmarkMatchFloat64LessAVX2/1024-8 	 5000000    291 ns/op	28120.14 MB/s
// BenchmarkMatchFloat64LessAVX2/4096-8 	 1000000   1371 ns/op	23885.25 MB/s
// BenchmarkMatchFloat64LessAVX2/65536-8	   50000  27816 ns/op	18847.90 MB/s
// BenchmarkMatchFloat64LessAVX2/131072-8      30000  55877 ns/op	18765.57 MB/s
func BenchmarkMatchFloat64LessAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64LessThanAVX2(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
// BenchmarkMatchFloat64LessAVX2Scalar/31-8  	30000000    41.1 ns/op	6030.08 MB/s
// BenchmarkMatchFloat64LessAVX2Scalar/127-8 	20000000    64.2 ns/op	15832.04 MB/s
// BenchmarkMatchFloat64LessAVX2Scalar/1023-8	 5000000   314 ns/op	26006.55 MB/s
// BenchmarkMatchFloat64LessAVX2Scalar/4095-8	 1000000  1387 ns/op	23617.87 MB/s
// BenchmarkMatchFloat64LessAVX2Scalar/65535-8     50000 27817 ns/op	18846.92 MB/s
// BenchmarkMatchFloat64LessAVX2Scalar/131071-8    30000 56416 ns/op	18586.28 MB/s
func BenchmarkMatchFloat64LessAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64LessThanAVX2(a, 5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//
var float64LessEqualCases = []Float64MatchTest{
	Float64MatchTest{
		name: "vec1",
		slice: []float64{
			0, 5, 3, 5, // Y1
			7, 5, 5, 9, // Y2
			3, 5, 5, 5, // Y3
			5, 0, 113, 12, // Y4

			4, 2, 3, 5, // Y5
			7, 3, 5, 9, // Y6
			3, 13, 5, 5, // Y7
			42, 5, 113, 12, // Y8
		},
		match:  5,
		result: []byte{0xf6, 0xfc, 0xf6, 0xb4},
		count:  22,
	}, {
		name:   "l32",
		slice:  float64TestSlice_1,
		match:  float64LessEqualTestMatch_1,
		result: float64LessEqualTestResult_1,
		count:  float64LessEqualTestCount_1,
	}, {
		name:   "l64",
		slice:  append(float64TestSlice_1, float64TestSlice_1...),
		match:  float64LessEqualTestMatch_1,
		result: append(float64LessEqualTestResult_1, float64LessEqualTestResult_1...),
		count:  float64LessEqualTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  float64TestSlice_1[:31],
		match:  float64LessEqualTestMatch_1,
		result: float64LessEqualTestResult_1,
		count:  float64LessEqualTestCount_1,
	}, {
		name:   "l23",
		slice:  float64TestSlice_1[:23],
		match:  float64LessEqualTestMatch_1,
		result: []byte{0xf2, 0x42, 0x22}, // last match is gone
		count:  float64LessEqualTestCount_1 - 4,
	}, {
		name:   "l15",
		slice:  float64TestSlice_1[:15],
		match:  float64LessEqualTestMatch_1,
		result: float64LessEqualTestResult_1[:2],
		count:  float64LessEqualTestCount_1 - 6,
	}, {
		name:   "l7",
		slice:  float64TestSlice_1[:7],
		match:  float64LessEqualTestMatch_1,
		result: float64LessEqualTestResult_1[:1],
		count:  float64LessEqualTestCount_1 - 8,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  float64TestSlice_2,
		match:  float64LessEqualTestMatch_2,
		result: float64LessEqualTestResult_2,
		count:  float64LessEqualTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  float64TestSlice_2[:31],
		match:  float64LessEqualTestMatch_2,
		result: []byte{0x81, 0x04, 0x04, 0x20}, // last bit off
		count:  float64LessEqualTestCount_2 - 1,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  float64TestSlice_3,
		match:  float64LessEqualTestMatch_3,
		result: float64LessEqualTestResult_3,
		count:  float64LessEqualTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  float64TestSlice_3[:31],
		match:  float64LessEqualTestMatch_3,
		result: []byte{0xff, 0xff, 0xff, 0xfe},
		count:  float64LessEqualTestCount_3 - 1,
	}, {
		// NaN, Inf
		name:   "nan32",
		slice:  float64TestSlice_4,
		match:  float64LessEqualTestMatch_4,
		result: float64LessEqualTestResult_4,
		count:  float64LessEqualTestCount_4,
	}, {
		name:   "nan31",
		slice:  float64TestSlice_4[:31],
		match:  float64LessEqualTestMatch_4,
		result: float64LessEqualTestResult_4,
		count:  float64LessEqualTestCount_4,
	},
}

func TestMatchFloat64LessEqualGeneric(T *testing.T) {
	for _, c := range float64LessEqualCases {
		// pre-allocate the result slice and fill with poison
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

func TestMatchFloat64LessEqualAVX2(T *testing.T) {
	for _, c := range float64LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat64LessThanEqualAVX2(c.slice, c.match, bits)
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
// Less Equal benchmarks
//
// BenchmarkMatchFloat64LessEqualGeneric/32-8   10000000     144 ns/op	1768.25 MB/s
// BenchmarkMatchFloat64LessEqualGeneric/128-8   3000000     527 ns/op	1939.54 MB/s
// BenchmarkMatchFloat64LessEqualGeneric/1024-8   300000    4294 ns/op	1907.59 MB/s
// BenchmarkMatchFloat64LessEqualGeneric/4096-8   100000   17093 ns/op	1916.98 MB/s
// BenchmarkMatchFloat64LessEqualGeneric/65536-8    5000  269674 ns/op	1944.15 MB/s
// BenchmarkMatchFloat64LessEqualGeneric/131072-8   3000  543906 ns/op	1927.86 MB/s
func BenchmarkMatchFloat64LessEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64LessThanEqualGeneric(a, 5, bits)
			}
		})
	}
}

// BenchmarkMatchFloat64LessEqualAVX2/32-8     100000000     13.4 ns/op	19133.28 MB/s
// BenchmarkMatchFloat64LessEqualAVX2/128-8    	50000000     38.3 ns/op	26722.81 MB/s
// BenchmarkMatchFloat64LessEqualAVX2/1024-8   	 5000000    294 ns/op	27807.72 MB/s
// BenchmarkMatchFloat64LessEqualAVX2/4096-8   	 1000000   1370 ns/op	23906.37 MB/s
// BenchmarkMatchFloat64LessEqualAVX2/65536-8  	   50000  28210 ns/op	18585.09 MB/s
// BenchmarkMatchFloat64LessEqualAVX2/131072-8 	   30000  57076 ns/op	18371.31 MB/s
func BenchmarkMatchFloat64LessEqualAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64LessThanEqualAVX2(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
// BenchmarkMatchFloat64LessEqualAVX2Scalar/31-8    50000000    37.1 ns/op	6681.81 MB/s
// BenchmarkMatchFloat64LessEqualAVX2Scalar/127-8   20000000    62.2 ns/op	16329.16 MB/s
// BenchmarkMatchFloat64LessEqualAVX2Scalar/1023-8   5000000   322 ns/op	25395.10 MB/s
// BenchmarkMatchFloat64LessEqualAVX2Scalar/4095-8   1000000  1381 ns/op	23720.41 MB/s
// BenchmarkMatchFloat64LessEqualAVX2Scalar/65535-8    50000 28001 ns/op	18723.50 MB/s
// BenchmarkMatchFloat64LessEqualAVX2Scalar/131071-8   30000 57441 ns/op	18254.40 MB/s
func BenchmarkMatchFloat64LessEqualAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64LessThanEqualAVX2(a, 5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
//
var float64GreaterCases = []Float64MatchTest{
	Float64MatchTest{
		name: "vec1",
		slice: []float64{
			0, 5, 3, 5, // Y1
			7, 5, 5, 9, // Y2
			3, 5, 5, 5, // Y3
			5, 0, 113, 12, // Y4

			4, 2, 3, 5, // Y5
			7, 3, 5, 9, // Y6
			3, 13, 5, 5, // Y7
			42, 5, 113, 12, // Y8
		},
		match:  5,
		result: []byte{0x09, 0x03, 0x09, 0x4b},
		count:  10,
	}, {
		name:   "l32",
		slice:  float64TestSlice_1,
		match:  float64GreaterTestMatch_1,
		result: float64GreaterTestResult_1,
		count:  float64GreaterTestCount_1,
	}, {
		name:   "l64",
		slice:  append(float64TestSlice_1, float64TestSlice_1...),
		match:  float64GreaterTestMatch_1,
		result: append(float64GreaterTestResult_1, float64GreaterTestResult_1...),
		count:  float64GreaterTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  float64TestSlice_1[:31],
		match:  float64GreaterTestMatch_1,
		result: []byte{0x0d, 0xbd, 0xdc, 0x8e},
		count:  float64GreaterTestCount_1 - 1,
	}, {
		name:   "l23",
		slice:  float64TestSlice_1[:23],
		match:  float64GreaterTestMatch_1,
		result: float64GreaterTestResult_1[:3],
		count:  float64GreaterTestCount_1 - 5,
	}, {
		name:   "l15",
		slice:  float64TestSlice_1[:15],
		match:  float64GreaterTestMatch_1,
		result: []byte{0x0d, 0xbc},
		count:  float64GreaterTestCount_1 - 11,
	}, {
		name:   "l7",
		slice:  float64TestSlice_1[:7],
		match:  float64GreaterTestMatch_1,
		result: []byte{0x0c},
		count:  2,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  float64TestSlice_2,
		match:  float64GreaterTestMatch_2,
		result: float64GreaterTestResult_2,
		count:  float64GreaterTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  float64TestSlice_2[:31],
		match:  float64GreaterTestMatch_2,
		result: float64GreaterTestResult_2,
		count:  float64GreaterTestCount_2,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  float64TestSlice_3,
		match:  float64GreaterTestMatch_3,
		result: float64GreaterTestResult_3,
		count:  float64GreaterTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  float64TestSlice_3[:31],
		match:  float64GreaterTestMatch_3,
		result: float64GreaterTestResult_3,
		count:  float64GreaterTestCount_3,
	}, {
		// NaN, Inf
		name:   "nan32",
		slice:  float64TestSlice_4,
		match:  float64GreaterTestMatch_4,
		result: float64GreaterTestResult_4,
		count:  float64GreaterTestCount_4,
	}, {
		name:   "nan31",
		slice:  float64TestSlice_4[:31],
		match:  float64GreaterTestMatch_4,
		result: float64GreaterTestResult_4,
		count:  float64GreaterTestCount_4,
	},
}

func TestMatchFloat64GreaterGeneric(T *testing.T) {
	for _, c := range float64GreaterCases {
		// pre-allocate the result slice and fill with poison
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

func TestMatchFloat64GreaterAVX2(T *testing.T) {
	for _, c := range float64GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat64GreaterThanAVX2(c.slice, c.match, bits)
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
// BenchmarkMatchFloat64GreaterGeneric/32-8   	30000000      47.9 ns/op	5339.77 MB/s
// BenchmarkMatchFloat64GreaterGeneric/128-8  	10000000     159 ns/op	6417.15 MB/s
// BenchmarkMatchFloat64GreaterGeneric/1024-8 	 1000000    1211 ns/op	6759.46 MB/s
// BenchmarkMatchFloat64GreaterGeneric/4096-8 	  300000    5109 ns/op	6413.52 MB/s
// BenchmarkMatchFloat64GreaterGeneric/65536-8	   20000   78623 ns/op	6668.33 MB/s
// BenchmarkMatchFloat64GreaterGeneric/131072-8    10000  176833 ns/op	5929.75 MB/s
func BenchmarkMatchFloat64GreaterGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64GreaterThanGeneric(a, 5, bits)
			}
		})
	}
}

// BenchmarkMatchFloat64GreaterAVX2/32-8     100000000     13.9 ns/op	18448.43 MB/s
// BenchmarkMatchFloat64GreaterAVX2/128-8     30000000     39.2 ns/op	26128.81 MB/s
// BenchmarkMatchFloat64GreaterAVX2/1024-8     5000000    297 ns/op	27572.54 MB/s
// BenchmarkMatchFloat64GreaterAVX2/4096-8     1000000   1464 ns/op	22381.11 MB/s
// BenchmarkMatchFloat64GreaterAVX2/65536-8      50000  29792 ns/op	17598.11 MB/s
// BenchmarkMatchFloat64GreaterAVX2/131072-8     30000  60542 ns/op	17319.64 MB/s
func BenchmarkMatchFloat64GreaterAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64GreaterThanAVX2(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
// BenchmarkMatchFloat64GreaterAVX2Scalar/31-8     30000000    45.9 ns/op	5406.36 MB/s
// BenchmarkMatchFloat64GreaterAVX2Scalar/127-8    20000000    64.6 ns/op	15716.01 MB/s
// BenchmarkMatchFloat64GreaterAVX2Scalar/1023-8   	5000000   331 ns/op	24663.64 MB/s
// BenchmarkMatchFloat64GreaterAVX2Scalar/4095-8   	1000000  1397 ns/op	23448.83 MB/s
// BenchmarkMatchFloat64GreaterAVX2Scalar/65535-8  	  50000 28420 ns/op	18447.55 MB/s
// BenchmarkMatchFloat64GreaterAVX2Scalar/131071-8 	  30000 66203 ns/op	15838.59 MB/s
func BenchmarkMatchFloat64GreaterAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64GreaterThanAVX2(a, 5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//
var float64GreaterEqualCases = []Float64MatchTest{
	Float64MatchTest{
		name: "vec1",
		slice: []float64{
			0, 5, 3, 5, // Y1
			7, 5, 5, 9, // Y2
			3, 5, 5, 5, // Y3
			5, 0, 113, 12, // Y4

			4, 2, 3, 5, // Y5
			7, 3, 5, 9, // Y6
			3, 13, 5, 5, // Y7
			42, 5, 113, 12, // Y8
		},
		match:  5,
		result: []byte{0x5f, 0x7b, 0x1b, 0x7f},
		count:  23,
	}, {
		name:   "l32",
		slice:  float64TestSlice_1,
		match:  float64GreaterEqualTestMatch_1,
		result: float64GreaterEqualTestResult_1,
		count:  float64GreaterEqualTestCount_1,
	}, {
		name:   "l64",
		slice:  append(float64TestSlice_1, float64TestSlice_1...),
		match:  float64GreaterEqualTestMatch_1,
		result: append(float64GreaterEqualTestResult_1, float64GreaterEqualTestResult_1...),
		count:  float64GreaterEqualTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  float64TestSlice_1[:31],
		match:  float64GreaterEqualTestMatch_1,
		result: []byte{0x8f, 0xff, 0xff, 0xfe},
		count:  float64GreaterEqualTestCount_1 - 1,
	}, {
		name:   "l23",
		slice:  float64TestSlice_1[:23],
		match:  float64GreaterEqualTestMatch_1,
		result: []byte{0x8f, 0xff, 0xfe},
		count:  float64GreaterEqualTestCount_1 - 9,
	}, {
		name:   "l15",
		slice:  float64TestSlice_1[:15],
		match:  float64GreaterEqualTestMatch_1,
		result: []byte{0x8f, 0xfe},
		count:  float64GreaterEqualTestCount_1 - 17,
	}, {
		name:   "l7",
		slice:  float64TestSlice_1[:7],
		match:  float64GreaterEqualTestMatch_1,
		result: []byte{0x8e},
		count:  float64GreaterEqualTestCount_1 - 25,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  float64TestSlice_2,
		match:  float64GreaterEqualTestMatch_2,
		result: float64GreaterEqualTestResult_2,
		count:  float64GreaterEqualTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  float64TestSlice_2[:31],
		match:  float64GreaterEqualTestMatch_2,
		result: float64GreaterEqualTestResult_2,
		count:  float64GreaterEqualTestCount_2,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  float64TestSlice_3,
		match:  float64GreaterEqualTestMatch_3,
		result: float64GreaterEqualTestResult_3,
		count:  float64GreaterEqualTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  float64TestSlice_3[:31],
		match:  float64GreaterEqualTestMatch_3,
		result: float64GreaterEqualTestResult_3,
		count:  float64GreaterEqualTestCount_3,
	}, {
		// NaN, Inf
		name:   "nan32",
		slice:  float64TestSlice_4,
		match:  float64GreaterEqualTestMatch_4,
		result: float64GreaterEqualTestResult_4,
		count:  float64GreaterEqualTestCount_4,
	}, {
		name:   "nan31",
		slice:  float64TestSlice_4[:31],
		match:  float64GreaterEqualTestMatch_4,
		result: float64GreaterEqualTestResult_4,
		count:  float64GreaterEqualTestCount_4,
	},
}

func TestMatchFloat64GreaterEqualGeneric(T *testing.T) {
	for _, c := range float64GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
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

func TestMatchFloat64GreaterEqualAVX2(T *testing.T) {
	for _, c := range float64GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat64GreaterThanEqualAVX2(c.slice, c.match, bits)
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
// Greater Equal benchmarks
//
// BenchmarkMatchFloat64GreaterEqualGeneric/32-8   	30000000     50.0 ns/op	5122.19 MB/s
// BenchmarkMatchFloat64GreaterEqualGeneric/128-8  	10000000    173 ns/op	5906.48 MB/s
// BenchmarkMatchFloat64GreaterEqualGeneric/1024-8 	 1000000   1518 ns/op	5396.04 MB/s
// BenchmarkMatchFloat64GreaterEqualGeneric/4096-8 	  300000   5048 ns/op	6490.51 MB/s
// BenchmarkMatchFloat64GreaterEqualGeneric/65536-8    20000  86678 ns/op	6048.67 MB/s
// BenchmarkMatchFloat64GreaterEqualGeneric/131072-8   10000 176572 ns/op	5938.52 MB/s
func BenchmarkMatchFloat64GreaterEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64GreaterThanEqualGeneric(a, 5, bits)
			}
		})
	}
}

// BenchmarkMatchFloat64GreaterEqualAVX2/32-8      100000000     14.3 ns/op	17876.22 MB/s
// BenchmarkMatchFloat64GreaterEqualAVX2/128-8     	50000000     41.8 ns/op	24525.14 MB/s
// BenchmarkMatchFloat64GreaterEqualAVX2/1024-8    	 5000000    306 ns/op	26714.34 MB/s
// BenchmarkMatchFloat64GreaterEqualAVX2/4096-8    	 1000000   1357 ns/op	24132.62 MB/s
// BenchmarkMatchFloat64GreaterEqualAVX2/65536-8   	   50000  30125 ns/op	17403.22 MB/s
// BenchmarkMatchFloat64GreaterEqualAVX2/131072-8  	   20000  60686 ns/op	17278.49 MB/s
func BenchmarkMatchFloat64GreaterEqualAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64GreaterThanEqualAVX2(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
// BenchmarkMatchFloat64GreaterEqualAVX2Scalar/31-8    	30000000    49.6 ns/op	5000.59 MB/s
// BenchmarkMatchFloat64GreaterEqualAVX2Scalar/127-8   	20000000    69.0 ns/op	14719.37 MB/s
// BenchmarkMatchFloat64GreaterEqualAVX2Scalar/1023-8  	 5000000   342 ns/op	23905.89 MB/s
// BenchmarkMatchFloat64GreaterEqualAVX2Scalar/4095-8  	 1000000  1467 ns/op	22319.75 MB/s
// BenchmarkMatchFloat64GreaterEqualAVX2Scalar/65535-8 	   50000 28860 ns/op	18166.11 MB/s
// BenchmarkMatchFloat64GreaterEqualAVX2Scalar/131071-8    30000 74479 ns/op	14078.70 MB/s
func BenchmarkMatchFloat64GreaterEqualAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64GreaterThanEqualAVX2(a, 5, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
//
var float64BetweenCases = []Float64MatchTest{
	Float64MatchTest{
		name: "vec1",
		slice: []float64{
			0, 5, 3, 5, // Y1
			7, 5, 5, 9, // Y2
			3, 5, 5, 5, // Y3
			5, 0, 113, 12, // Y4

			4, 2, 3, 5, // Y5
			7, 3, 5, 9, // Y6
			3, 13, 5, 5, // Y7
			42, 5, 113, 12, // Y8
		},
		match:  5,
		match2: 10,
		result: []byte{0x5f, 0x78, 0x1b, 0x34},
		count:  17,
	}, {
		name:   "l32",
		slice:  float64TestSlice_1,
		match:  float64BetweenTestMatch_1,
		match2: float64BetweenTestMatch_1b,
		result: float64BetweenTestResult_1,
		count:  float64BetweenTestCount_1,
	}, {
		name:   "l64",
		slice:  append(float64TestSlice_1, float64TestSlice_1...),
		match:  float64BetweenTestMatch_1,
		match2: float64BetweenTestMatch_1b,
		result: append(float64BetweenTestResult_1, float64BetweenTestResult_1...),
		count:  float64BetweenTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  float64TestSlice_1[:31],
		match:  float64BetweenTestMatch_1,
		match2: float64BetweenTestMatch_1b,
		result: float64BetweenTestResult_1,
		count:  float64BetweenTestCount_1,
	}, {
		name:   "l23",
		slice:  float64TestSlice_1[:23],
		match:  float64BetweenTestMatch_1,
		match2: float64BetweenTestMatch_1b,
		result: []byte{0x8f, 0x42, 0x22}, // last bit off!
		count:  float64BetweenTestCount_1 - 4,
	}, {
		name:   "l15",
		slice:  float64TestSlice_1[:15],
		match:  float64BetweenTestMatch_1,
		match2: float64BetweenTestMatch_1b,
		result: float64BetweenTestResult_1[:2],
		count:  float64BetweenTestCount_1 - 6,
	}, {
		name:   "l7",
		slice:  float64TestSlice_1[:7],
		match:  float64BetweenTestMatch_1,
		match2: float64BetweenTestMatch_1b,
		result: []byte{0x8e},
		count:  float64BetweenTestCount_1 - 9,
	}, {
		name:   "l0",
		slice:  make([]float64, 0),
		match:  float64BetweenTestMatch_1,
		match2: float64BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  float64BetweenTestMatch_1,
		match2: float64BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	}, {
		// with negative values
		name:   "neg32",
		slice:  float64TestSlice_2,
		match:  float64BetweenTestMatch_2,
		match2: float64BetweenTestMatch_2b,
		result: float64BetweenTestResult_2,
		count:  float64BetweenTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  float64TestSlice_2[:31],
		match:  float64BetweenTestMatch_2,
		match2: float64BetweenTestMatch_2b,
		result: float64BetweenTestResult_2,
		count:  float64BetweenTestCount_2,
	},
	{
		// with extreme values
		name:   "ext32",
		slice:  float64TestSlice_3,
		match:  float64BetweenTestMatch_3,
		match2: float64BetweenTestMatch_3b,
		result: float64BetweenTestResult_3,
		count:  float64BetweenTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  float64TestSlice_3[:31],
		match:  float64BetweenTestMatch_3,
		match2: float64BetweenTestMatch_3b,
		result: float64BetweenTestResult_3,
		count:  float64BetweenTestCount_3,
	}, {
		// NaN, Inf
		name:   "nan32",
		slice:  float64TestSlice_4,
		match:  float64BetweenTestMatch_4,
		match2: float64BetweenTestMatch_4b,
		result: float64BetweenTestResult_4,
		count:  float64BetweenTestCount_4,
	}, {
		name:   "nan31",
		slice:  float64TestSlice_4[:31],
		match:  float64BetweenTestMatch_4,
		match2: float64BetweenTestMatch_4b,
		result: float64BetweenTestResult_4,
		count:  float64BetweenTestCount_4,
	},
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

func TestMatchFloat64BetweenAVX2(T *testing.T) {
	for _, c := range float64BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat64BetweenAVX2(c.slice, c.match, c.match2, bits)
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
// BenchmarkMatchFloat64BetweenGeneric/32-8  	20000000      54.0 ns/op	4738.95 MB/s
// BenchmarkMatchFloat64BetweenGeneric/128-8 	10000000     164 ns/op	6218.46 MB/s
// BenchmarkMatchFloat64BetweenGeneric/1024-8	 1000000    1232 ns/op	6648.72 MB/s
// BenchmarkMatchFloat64BetweenGeneric/4096-8	  300000    4809 ns/op	6812.58 MB/s
// BenchmarkMatchFloat64BetweenGeneric/65536-8     20000   77084 ns/op	6801.51 MB/s
// BenchmarkMatchFloat64BetweenGeneric/131072-8    10000  155968 ns/op	6722.99 MB/s
func BenchmarkMatchFloat64BetweenGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64BetweenGeneric(a, 5, 10, bits)
			}
		})
	}
}

// BenchmarkMatchFloat64BetweenAVX2/32-8       100000000    15.8 ns/op	16242.67 MB/s
// BenchmarkMatchFloat64BetweenAVX2/128-8    	20000000    59.4 ns/op	17252.78 MB/s
// BenchmarkMatchFloat64BetweenAVX2/1024-8   	 3000000   477 ns/op	17166.78 MB/s
// BenchmarkMatchFloat64BetweenAVX2/4096-8   	 1000000  1992 ns/op	16443.68 MB/s
// BenchmarkMatchFloat64BetweenAVX2/65536-8  	   50000 33769 ns/op	15525.48 MB/s
// BenchmarkMatchFloat64BetweenAVX2/131072-8 	   20000 67585 ns/op	15514.80 MB/s
func BenchmarkMatchFloat64BetweenAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64BetweenAVX2(a, 5, 10, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
// BenchmarkMatchFloat64BetweenAVX2Scalar/31-8    	30000000    48.7 ns/op	5087.77 MB/s
// BenchmarkMatchFloat64BetweenAVX2Scalar/127-8   	20000000    91.5 ns/op	11106.70 MB/s
// BenchmarkMatchFloat64BetweenAVX2Scalar/1023-8  	 3000000   508 ns/op	16090.16 MB/s
// BenchmarkMatchFloat64BetweenAVX2Scalar/4095-8  	 1000000  2025 ns/op	16171.18 MB/s
// BenchmarkMatchFloat64BetweenAVX2Scalar/65535-8 	   50000 34510 ns/op	15191.90 MB/s
// BenchmarkMatchFloat64BetweenAVX2Scalar/131071-8	   20000 68741 ns/op	15253.86 MB/s
func BenchmarkMatchFloat64BetweenAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat64Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * 8))
			for i := 0; i < B.N; i++ {
				matchFloat64BetweenAVX2(a, 5, 10, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Float64 Slice
//
func TestFloat64SliceContains(T *testing.T) {
	// nil slice
	if Float64Slice(nil).Contains(1) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Float64Slice([]float64{}).Contains(1) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Float64Slice([]float64{1}).Contains(1) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Float64Slice([]float64{1}).Contains(2) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !Float64Slice([]float64{1, 3, 5, 7, 11, 13}).Contains(1) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Float64Slice([]float64{1, 3, 5, 7, 11, 13}).Contains(5) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Float64Slice([]float64{1, 3, 5, 7, 11, 13}).Contains(13) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Float64Slice([]float64{1, 3, 5, 7, 11, 13}).Contains(0) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Float64Slice([]float64{1, 3, 5, 7, 11, 13}).Contains(2) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Float64Slice([]float64{1, 3, 5, 7, 11, 13}).Contains(14) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkFloat64SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Float64Slice(randFloat64Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(rand.Float64())
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Float64Slice(randFloat64Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(a[rand.Intn(len(a))])
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
			if want, got := r.Match, Float64Slice(v.Slice).ContainsRange(r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkFloat64SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Float64Slice(randFloat64Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := rand.Float64(), rand.Float64()
				if min > max {
					min, max = max, min
				}
				a.ContainsRange(min, max)
			}
		})
	}
}
