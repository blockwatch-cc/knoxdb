// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc
//

package vec

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

const Int8Size = 1

func randInt8Slice(n, u int) []int8 {
	s := make([]int8, n*u)
	for i := 0; i < n; i++ {
		s[i] = int8(rand.Intn(math.MaxInt8 + 1))
	}
	for i := 0; i < u; i++ {
		s = append(s, s[:n]...)
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
	int8EqualTestResult_1       = []byte{0x82, 0x42, 0x23, 0x70}
	int8EqualTestMatch_1  int8  = 5
	int8EqualTestCount_1  int64 = 10

	int8LessTestResult_1       = []byte{0x70, 0x00, 0x00, 0x00}
	int8LessTestMatch_1  int8  = 5
	int8LessTestCount_1  int64 = 3

	int8LessEqualTestResult_1       = []byte{0xf2, 0x42, 0x23, 0x70}
	int8LessEqualTestMatch_1  int8  = 5
	int8LessEqualTestCount_1  int64 = 13

	int8GreaterTestResult_1       = []byte{0x0d, 0xbd, 0xdc, 0x8f}
	int8GreaterTestMatch_1  int8  = 5
	int8GreaterTestCount_1  int64 = 19

	int8GreaterEqualTestResult_1       = []byte{0x8f, 0xff, 0xff, 0xff}
	int8GreaterEqualTestMatch_1  int8  = 5
	int8GreaterEqualTestCount_1  int64 = 29

	int8BetweenTestResult_1       = []byte{0x8f, 0x42, 0x23, 0x70}
	int8BetweenTestMatch_1  int8  = 5
	int8BetweenTestMatch_1b int8  = 10
	int8BetweenTestCount_1  int64 = 13

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
	int8EqualTestResult_2       = []byte{0x80, 0x0, 0x0, 0x0}
	int8EqualTestMatch_2  int8  = -5
	int8EqualTestCount_2  int64 = 1

	int8LessTestResult_2       = []byte{0xe1, 0x04, 0x04, 0x21}
	int8LessTestMatch_2  int8  = 5
	int8LessTestCount_2  int64 = 8

	int8LessEqualTestResult_2       = []byte{0xf1, 0x04, 0x04, 0x21}
	int8LessEqualTestMatch_2  int8  = 5
	int8LessEqualTestCount_2  int64 = 9

	int8GreaterTestResult_2       = []byte{0x0e, 0xfb, 0xfb, 0xde}
	int8GreaterTestMatch_2  int8  = 5
	int8GreaterTestCount_2  int64 = 23

	int8GreaterEqualTestResult_2       = []byte{0x1e, 0xfb, 0xfb, 0xde}
	int8GreaterEqualTestMatch_2  int8  = 5
	int8GreaterEqualTestCount_2  int64 = 24

	int8BetweenTestResult_2       = []byte{0x1e, 0x00, 0x00, 0x00}
	int8BetweenTestMatch_2  int8  = 5
	int8BetweenTestMatch_2b int8  = 10
	int8BetweenTestCount_2  int64 = 4

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
	int8EqualTestResult_3       = []byte{0x01, 0x01, 0x01, 0x01}
	int8EqualTestMatch_3  int8  = math.MinInt8
	int8EqualTestCount_3  int64 = 4

	int8LessTestResult_3       = []byte{0x0, 0x0, 0x0, 0x00}
	int8LessTestMatch_3  int8  = math.MinInt8
	int8LessTestCount_3  int64 = 0

	int8LessEqualTestResult_3       = []byte{0x01, 0x01, 0x01, 0x01}
	int8LessEqualTestMatch_3  int8  = math.MinInt8
	int8LessEqualTestCount_3  int64 = 4

	int8GreaterTestResult_3       = []byte{0xfe, 0xfe, 0xfe, 0xfe}
	int8GreaterTestMatch_3  int8  = math.MinInt8
	int8GreaterTestCount_3  int64 = 28

	int8GreaterEqualTestResult_3       = []byte{0xff, 0xff, 0xff, 0xff}
	int8GreaterEqualTestMatch_3  int8  = math.MinInt8
	int8GreaterEqualTestCount_3  int64 = 32

	int8BetweenTestResult_3       = []byte{0x0a, 0x0a, 0x0a, 0x0a}
	int8BetweenTestMatch_3  int8  = math.MaxInt8 / 2
	int8BetweenTestMatch_3b int8  = math.MaxInt8
	int8BetweenTestCount_3  int64 = 8
)

// -----------------------------------------------------------------------------
// Equal Testcases
//
var int8EqualCases = []Int8MatchTest{
	Int8MatchTest{
		name: "vec1",
		// 	// test vector to find shuffle/perm positions
		// 	slice: []int8{
		// 		// 0x1, 0x2, 0x3, 0x4, // Y1
		// 		// 0x5, 0x6, 0x7, 0x8, // Y2
		// 		// 0x9, 0xa, 0xb, 0xc, // Y3
		// 		// 0xd, 0xe, 0xf, 0x0, // Y4

		// 		// 0x11, 0x12, 0x13, 0x14, // Y5
		// 		// 0x15, 0x16, 0x17, 0x18, // Y6
		// 		// 0x19, 0x1a, 0x1b, 0x1c, // Y7
		// 		// 0x1d, 0x1e, 0x1f, 0x10, // Y8
		// 	},
		// 	match:  5,
		// 	result: []byte{0x01, 0x0, 0x0, 0x0},
		// 	count:  1,
		// },{
		slice: []int8{
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
		slice:  int8TestSlice_1,
		match:  int8EqualTestMatch_1,
		result: int8EqualTestResult_1,
		count:  int8EqualTestCount_1,
	}, {
		name:   "l64",
		slice:  append(int8TestSlice_1, int8TestSlice_1...),
		match:  int8EqualTestMatch_1,
		result: append(int8EqualTestResult_1, int8EqualTestResult_1...),
		count:  int8EqualTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  int8TestSlice_1[:31],
		match:  int8EqualTestMatch_1,
		result: int8EqualTestResult_1,
		count:  int8EqualTestCount_1,
	}, {
		name:   "l23",
		slice:  int8TestSlice_1[:23],
		match:  int8EqualTestMatch_1,
		result: []byte{0x82, 0x42, 0x22}, // last bit off!
		count:  int8EqualTestCount_1 - 4,
	}, {
		name:   "l15",
		slice:  int8TestSlice_1[:15],
		match:  int8EqualTestMatch_1,
		result: int8EqualTestResult_1[:2],
		count:  int8EqualTestCount_1 - 6,
	}, {
		name:   "l7",
		slice:  int8TestSlice_1[:7],
		match:  int8EqualTestMatch_1,
		result: int8EqualTestResult_1[:1],
		count:  int8EqualTestCount_1 - 8,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  int8TestSlice_2,
		match:  int8EqualTestMatch_2,
		result: int8EqualTestResult_2,
		count:  int8EqualTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  int8TestSlice_2[:31],
		match:  int8EqualTestMatch_2,
		result: int8EqualTestResult_2,
		count:  int8EqualTestCount_2,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  int8TestSlice_3,
		match:  int8EqualTestMatch_3,
		result: int8EqualTestResult_3,
		count:  int8EqualTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  int8TestSlice_3[:31],
		match:  int8EqualTestMatch_3,
		result: []byte{0x01, 0x01, 0x01, 0x00},
		count:  3,
	},
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

/*
func TestMatchInt8EqualAVX2(T *testing.T) {
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
*/
// -----------------------------------------------------------------------------
// Equal benchmarks
//
func BenchmarkMatchInt8EqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8EqualGeneric(a, 5, bits)
			}
		})
	}
}

/*
func BenchmarkMatchInt8EqualAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8EqualAVX2(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchInt8EqualAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8EqualAVX2(a, 5, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Less Testcases
//
var int8LessCases = []Int8MatchTest{
	Int8MatchTest{
		name: "vec1",
		slice: []int8{
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
		slice:  int8TestSlice_1,
		match:  int8LessTestMatch_1,
		result: int8LessTestResult_1,
		count:  int8LessTestCount_1,
	}, {
		name:   "l64",
		slice:  append(int8TestSlice_1, int8TestSlice_1...),
		match:  int8LessTestMatch_1,
		result: append(int8LessTestResult_1, int8LessTestResult_1...),
		count:  int8LessTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  int8TestSlice_1[:31],
		match:  int8LessTestMatch_1,
		result: int8LessTestResult_1,
		count:  int8LessTestCount_1,
	}, {
		name:   "l23",
		slice:  int8TestSlice_1[:23],
		match:  int8LessTestMatch_1,
		result: int8LessTestResult_1[:3],
		count:  int8LessTestCount_1,
	}, {
		name:   "l15",
		slice:  int8TestSlice_1[:15],
		match:  int8LessTestMatch_1,
		result: int8LessTestResult_1[:2],
		count:  int8LessTestCount_1,
	}, {
		name:   "l7",
		slice:  int8TestSlice_1[:7],
		match:  int8LessTestMatch_1,
		result: int8LessTestResult_1[:1],
		count:  int8LessTestCount_1,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  int8TestSlice_2,
		match:  int8LessTestMatch_2,
		result: int8LessTestResult_2,
		count:  int8LessTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  int8TestSlice_2[:31],
		match:  int8LessTestMatch_2,
		result: []byte{0xe1, 0x04, 0x04, 0x20}, // last bit off
		count:  int8LessTestCount_2 - 1,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  int8TestSlice_3,
		match:  int8LessTestMatch_3,
		result: int8LessTestResult_3,
		count:  int8LessTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  int8TestSlice_3[:31],
		match:  int8LessTestMatch_3,
		result: int8LessTestResult_3, // still zeros
		count:  int8LessTestCount_3,
	},
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

/*
func TestMatchInt8LessAVX2(T *testing.T) {
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
*/
// -----------------------------------------------------------------------------
// Less benchmarks
//
func BenchmarkMatchInt8LessGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8LessThanGeneric(a, 5, bits)
			}
		})
	}
}

/*
func BenchmarkMatchInt8LessAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8LessThanAVX2(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchInt8LessAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8LessThanAVX2(a, 5, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Less Equal Testcases
//
var int8LessEqualCases = []Int8MatchTest{
	Int8MatchTest{
		name: "vec1",
		slice: []int8{
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
		slice:  int8TestSlice_1,
		match:  int8LessEqualTestMatch_1,
		result: int8LessEqualTestResult_1,
		count:  int8LessEqualTestCount_1,
	}, {
		name:   "l64",
		slice:  append(int8TestSlice_1, int8TestSlice_1...),
		match:  int8LessEqualTestMatch_1,
		result: append(int8LessEqualTestResult_1, int8LessEqualTestResult_1...),
		count:  int8LessEqualTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  int8TestSlice_1[:31],
		match:  int8LessEqualTestMatch_1,
		result: int8LessEqualTestResult_1,
		count:  int8LessEqualTestCount_1,
	}, {
		name:   "l23",
		slice:  int8TestSlice_1[:23],
		match:  int8LessEqualTestMatch_1,
		result: []byte{0xf2, 0x42, 0x22},
		count:  int8LessEqualTestCount_1 - 4,
	}, {
		name:   "l15",
		slice:  int8TestSlice_1[:15],
		match:  int8LessEqualTestMatch_1,
		result: int8LessEqualTestResult_1[:2],
		count:  int8LessEqualTestCount_1 - 6,
	}, {
		name:   "l7",
		slice:  int8TestSlice_1[:7],
		match:  int8LessEqualTestMatch_1,
		result: int8LessEqualTestResult_1[:1],
		count:  int8LessEqualTestCount_1 - 8,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  int8TestSlice_2,
		match:  int8LessEqualTestMatch_2,
		result: int8LessEqualTestResult_2,
		count:  int8LessEqualTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  int8TestSlice_2[:31],
		match:  int8LessEqualTestMatch_2,
		result: []byte{0xf1, 0x04, 0x04, 0x20}, // last bit off
		count:  int8LessEqualTestCount_2 - 1,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  int8TestSlice_3,
		match:  int8LessEqualTestMatch_3,
		result: int8LessEqualTestResult_3,
		count:  int8LessEqualTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  int8TestSlice_3[:31],
		match:  int8LessEqualTestMatch_3,
		result: []byte{0x1, 0x1, 0x1, 0x0}, // last off
		count:  int8LessEqualTestCount_3 - 1,
	},
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

/*
func TestMatchInt8LessEqualAVX2(T *testing.T) {
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
*/
// -----------------------------------------------------------------------------
// Less equal benchmarks
//
func BenchmarkMatchInt8LessEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8LessThanEqualGeneric(a, 5, bits)
			}
		})
	}
}

/*
func BenchmarkMatchInt8LessEqualAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8LessThanEqualAVX2(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchInt8LessEqualAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8LessThanEqualAVX2(a, 5, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Greater Testcases
//
var int8GreaterCases = []Int8MatchTest{
	Int8MatchTest{
		name: "vec1",
		slice: []int8{
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
		slice:  int8TestSlice_1,
		match:  int8GreaterTestMatch_1,
		result: int8GreaterTestResult_1,
		count:  int8GreaterTestCount_1,
	}, {
		name:   "l64",
		slice:  append(int8TestSlice_1, int8TestSlice_1...),
		match:  int8GreaterTestMatch_1,
		result: append(int8GreaterTestResult_1, int8GreaterTestResult_1...),
		count:  int8GreaterTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  int8TestSlice_1[:31],
		match:  int8GreaterTestMatch_1,
		result: []byte{0x0d, 0xbd, 0xdc, 0x8e},
		count:  int8GreaterTestCount_1 - 1,
	}, {
		name:   "l23",
		slice:  int8TestSlice_1[:23],
		match:  int8GreaterTestMatch_1,
		result: int8GreaterTestResult_1[:3],
		count:  int8GreaterTestCount_1 - 5,
	}, {
		name:   "l15",
		slice:  int8TestSlice_1[:15],
		match:  int8GreaterTestMatch_1,
		result: []byte{0x0d, 0xbc},
		count:  int8GreaterTestCount_1 - 11,
	}, {
		name:   "l7",
		slice:  int8TestSlice_1[:7],
		match:  int8GreaterTestMatch_1,
		result: []byte{0x0c},
		count:  2,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  int8TestSlice_2,
		match:  int8GreaterTestMatch_2,
		result: int8GreaterTestResult_2,
		count:  int8GreaterTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  int8TestSlice_2[:31],
		match:  int8GreaterTestMatch_2,
		result: []byte{0x0e, 0xfb, 0xfb, 0xde},
		count:  int8GreaterTestCount_2,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  int8TestSlice_3,
		match:  int8GreaterTestMatch_3,
		result: int8GreaterTestResult_3,
		count:  int8GreaterTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  int8TestSlice_3[:31],
		match:  int8GreaterTestMatch_3,
		result: int8GreaterTestResult_3, // still zeros
		count:  int8GreaterTestCount_3,
	},
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

/*
func TestMatchInt8GreaterAVX2(T *testing.T) {
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
*/
// -----------------------------------------------------------------------------
// Greater benchmarks
//
func BenchmarkMatchInt8GreaterGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8GreaterThanGeneric(a, 5, bits)
			}
		})
	}
}

/*
func BenchmarkMatchInt8GreaterAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8GreaterThanAVX2(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchInt8GreaterAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8GreaterThanAVX2(a, 5, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Greater Equal Testcases
//
var int8GreaterEqualCases = []Int8MatchTest{
	Int8MatchTest{
		name: "vec1",
		slice: []int8{
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
		slice:  int8TestSlice_1,
		match:  int8GreaterEqualTestMatch_1,
		result: int8GreaterEqualTestResult_1,
		count:  int8GreaterEqualTestCount_1,
	}, {
		name:   "l64",
		slice:  append(int8TestSlice_1, int8TestSlice_1...),
		match:  int8GreaterEqualTestMatch_1,
		result: append(int8GreaterEqualTestResult_1, int8GreaterEqualTestResult_1...),
		count:  int8GreaterEqualTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  int8TestSlice_1[:31],
		match:  int8GreaterEqualTestMatch_1,
		result: []byte{0x8f, 0xff, 0xff, 0xfe},
		count:  int8GreaterEqualTestCount_1 - 1,
	}, {
		name:   "l23",
		slice:  int8TestSlice_1[:23],
		match:  int8GreaterEqualTestMatch_1,
		result: []byte{0x8f, 0xff, 0xfe},
		count:  int8GreaterEqualTestCount_1 - 9,
	}, {
		name:   "l15",
		slice:  int8TestSlice_1[:15],
		match:  int8GreaterEqualTestMatch_1,
		result: []byte{0x8f, 0xfe},
		count:  int8GreaterEqualTestCount_1 - 17,
	}, {
		name:   "l7",
		slice:  int8TestSlice_1[:7],
		match:  int8GreaterEqualTestMatch_1,
		result: []byte{0x8e},
		count:  4,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  int8TestSlice_2,
		match:  int8GreaterEqualTestMatch_2,
		result: int8GreaterEqualTestResult_2,
		count:  int8GreaterEqualTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  int8TestSlice_2[:31],
		match:  int8GreaterEqualTestMatch_2,
		result: int8GreaterEqualTestResult_2,
		count:  int8GreaterEqualTestCount_2,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  int8TestSlice_3,
		match:  int8GreaterEqualTestMatch_3,
		result: int8GreaterEqualTestResult_3,
		count:  int8GreaterEqualTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  int8TestSlice_3[:31],
		match:  int8GreaterEqualTestMatch_3,
		result: []byte{0xff, 0xff, 0xff, 0xfe}, // last off
		count:  int8GreaterEqualTestCount_3 - 1,
	},
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

/*
func TestMatchInt8GreaterEqualAVX2(T *testing.T) {
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
*/
// -----------------------------------------------------------------------------
// Greater equal benchmarks
//
func BenchmarkMatchInt8GreaterEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8GreaterThanEqualGeneric(a, 5, bits)
			}
		})
	}
}

/*
func BenchmarkMatchInt8GreaterEqualAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8GreaterThanEqualAVX2(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchInt8GreaterEqualAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8GreaterThanEqualAVX2(a, 5, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Between Testcases
//
var int8BetweenCases = []Int8MatchTest{
	Int8MatchTest{
		name: "vec1",
		slice: []int8{
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
		slice:  int8TestSlice_1,
		match:  int8BetweenTestMatch_1,
		match2: int8BetweenTestMatch_1b,
		result: int8BetweenTestResult_1,
		count:  int8BetweenTestCount_1,
	}, {
		name:   "l64",
		slice:  append(int8TestSlice_1, int8TestSlice_1...),
		match:  int8BetweenTestMatch_1,
		match2: int8BetweenTestMatch_1b,
		result: append(int8BetweenTestResult_1, int8BetweenTestResult_1...),
		count:  int8BetweenTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  int8TestSlice_1[:31],
		match:  int8BetweenTestMatch_1,
		match2: int8BetweenTestMatch_1b,
		result: int8BetweenTestResult_1,
		count:  int8BetweenTestCount_1,
	}, {
		name:   "l23",
		slice:  int8TestSlice_1[:23],
		match:  int8BetweenTestMatch_1,
		match2: int8BetweenTestMatch_1b,
		result: []byte{0x8f, 0x42, 0x22}, // last bit off!
		count:  int8BetweenTestCount_1 - 4,
	}, {
		name:   "l15",
		slice:  int8TestSlice_1[:15],
		match:  int8BetweenTestMatch_1,
		match2: int8BetweenTestMatch_1b,
		result: int8BetweenTestResult_1[:2],
		count:  int8BetweenTestCount_1 - 6,
	}, {
		name:   "l7",
		slice:  int8TestSlice_1[:7],
		match:  int8BetweenTestMatch_1,
		match2: int8BetweenTestMatch_1b,
		result: []byte{0x8e},
		count:  int8BetweenTestCount_1 - 9,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  int8TestSlice_2,
		match:  int8BetweenTestMatch_2,
		match2: int8BetweenTestMatch_2b,
		result: int8BetweenTestResult_2,
		count:  int8BetweenTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  int8TestSlice_2[:31],
		match:  int8BetweenTestMatch_2,
		match2: int8BetweenTestMatch_2b,
		result: int8BetweenTestResult_2,
		count:  int8BetweenTestCount_2,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  int8TestSlice_3,
		match:  int8BetweenTestMatch_3,
		match2: int8BetweenTestMatch_3b,
		result: int8BetweenTestResult_3,
		count:  int8BetweenTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  int8TestSlice_3[:31],
		match:  int8BetweenTestMatch_3,
		match2: int8BetweenTestMatch_3b,
		result: []byte{0x0a, 0x0a, 0x0a, 0x0a},
		count:  8,
	},
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

/*
func TestMatchInt8BetweenAVX2(T *testing.T) {
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
*/
// -----------------------------------------------------------------------------
// Between benchmarks
//
// BenchmarkMatchInt8BetweenGeneric/32-8  30000000      54.7 ns/op	4683.30 MB/s
// BenchmarkMatchInt8BetweenGeneric/128-8 10000000     163 ns/op	6261.85 MB/s
// BenchmarkMatchInt8BetweenGeneric/1024-8 1000000    1211 ns/op	6762.88 MB/s
// BenchmarkMatchInt8BetweenGeneric/4096-8  300000    4985 ns/op	6572.26 MB/s
// BenchmarkMatchInt8BetweenGeneric/65536-8  20000   80903 ns/op	6480.44 MB/s
// BenchmarkMatchInt8BetweenGeneric/131072-8 10000  158714 ns/op	6606.70 MB/s
func BenchmarkMatchInt8BetweenGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8BetweenGeneric(a, 5, 10, bits)
			}
		})
	}
}

// BenchmarkMatchInt8BetweenAVX2/32-8     100000000     15.1 ns/op	16959.99 MB/s
// BenchmarkMatchInt8BetweenAVX2/128-8    	30000000     48.5 ns/op	21117.50 MB/s
// BenchmarkMatchInt8BetweenAVX2/1024-8   	 5000000    362 ns/op	22578.41 MB/s
// BenchmarkMatchInt8BetweenAVX2/4096-8   	 1000000   1610 ns/op	20345.03 MB/s
// BenchmarkMatchInt8BetweenAVX2/65536-8  	   50000  28742 ns/op	18240.79 MB/s
// BenchmarkMatchInt8BetweenAVX2/131072-8 	   20000  60508 ns/op	17329.33 MB/s
/*func BenchmarkMatchInt8BetweenAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8BetweenAVX2(a, 5, 10, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
// BenchmarkMatchInt8BetweenAVX2Scalar/31-8      30000000     38.2 ns/op	6494.10 MB/s
// BenchmarkMatchInt8BetweenAVX2Scalar/127-8     20000000     70.3 ns/op	14452.14 MB/s
// BenchmarkMatchInt8BetweenAVX2Scalar/1023-8     3000000    384 ns/op	21292.42 MB/s
// BenchmarkMatchInt8BetweenAVX2Scalar/4095-8     1000000   1652 ns/op	19828.53 MB/s
// BenchmarkMatchInt8BetweenAVX2Scalar/65535-8      50000  28695 ns/op	18270.74 MB/s
// BenchmarkMatchInt8BetweenAVX2Scalar/131071-8     30000  59239 ns/op	17700.62 MB/s
func BenchmarkMatchInt8BetweenAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randInt8Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				matchInt8BetweenAVX2(a, 5, 10, bits)
			}
		})
	}
}
*/
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
	if Int8Slice(nil).Contains(1) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Int8Slice([]int8{}).Contains(1) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Int8Slice([]int8{1}).Contains(1) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Int8Slice([]int8{1}).Contains(2) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !Int8Slice([]int8{-1, 3, 5, 7, 11, 13}).Contains(-1) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Int8Slice([]int8{-1, 3, 5, 7, 11, 13}).Contains(5) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Int8Slice([]int8{-1, 3, 5, 7, 11, 13}).Contains(13) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Int8Slice([]int8{-1, 3, 5, 7, 11, 13}).Contains(0) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Int8Slice([]int8{-1, 3, 5, 7, 11, 13}).Contains(2) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Int8Slice([]int8{-1, 3, 5, 7, 11, 13}).Contains(14) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkInt8SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Int8Slice(randInt8Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(int8(rand.Intn(math.MaxInt8 + 1)))
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Int8Slice(randInt8Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(a[rand.Intn(len(a))])
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
			if want, got := r.Match, Int8Slice(v.Slice).ContainsRange(r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkInt8SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Int8Slice(randInt8Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := int8(rand.Intn(math.MaxInt8+1)), int8(rand.Intn(math.MaxInt8+1))
				if min > max {
					min, max = max, min
				}
				a.ContainsRange(min, max)
			}
		})
	}
}
