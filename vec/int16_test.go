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

const Int16Size = 2

func randInt16Slice(n, u int) []int16 {
	s := make([]int16, n*u)
	for i := 0; i < n; i++ {
		s[i] = int16(rand.Intn(math.MaxInt16 + 1))
	}
	for i := 0; i < u; i++ {
		s = append(s, s[:n]...)
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
	int16EqualTestResult_1       = []byte{0x82, 0x42, 0x23, 0x70}
	int16EqualTestMatch_1  int16 = 5
	int16EqualTestCount_1  int64 = 10

	int16LessTestResult_1       = []byte{0x70, 0x00, 0x00, 0x00}
	int16LessTestMatch_1  int16 = 5
	int16LessTestCount_1  int64 = 3

	int16LessEqualTestResult_1       = []byte{0xf2, 0x42, 0x23, 0x70}
	int16LessEqualTestMatch_1  int16 = 5
	int16LessEqualTestCount_1  int64 = 13

	int16GreaterTestResult_1       = []byte{0x0d, 0xbd, 0xdc, 0x8f}
	int16GreaterTestMatch_1  int16 = 5
	int16GreaterTestCount_1  int64 = 19

	int16GreaterEqualTestResult_1       = []byte{0x8f, 0xff, 0xff, 0xff}
	int16GreaterEqualTestMatch_1  int16 = 5
	int16GreaterEqualTestCount_1  int64 = 29

	int16BetweenTestResult_1       = []byte{0x8f, 0x42, 0x23, 0x70}
	int16BetweenTestMatch_1  int16 = 5
	int16BetweenTestMatch_1b int16 = 10
	int16BetweenTestCount_1  int64 = 13

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
	int16EqualTestResult_2       = []byte{0x80, 0x0, 0x0, 0x0}
	int16EqualTestMatch_2  int16 = -5
	int16EqualTestCount_2  int64 = 1

	int16LessTestResult_2       = []byte{0xe1, 0x04, 0x04, 0x21}
	int16LessTestMatch_2  int16 = 5
	int16LessTestCount_2  int64 = 8

	int16LessEqualTestResult_2       = []byte{0xf1, 0x04, 0x04, 0x21}
	int16LessEqualTestMatch_2  int16 = 5
	int16LessEqualTestCount_2  int64 = 9

	int16GreaterTestResult_2       = []byte{0x0e, 0xfb, 0xfb, 0xde}
	int16GreaterTestMatch_2  int16 = 5
	int16GreaterTestCount_2  int64 = 23

	int16GreaterEqualTestResult_2       = []byte{0x1e, 0xfb, 0xfb, 0xde}
	int16GreaterEqualTestMatch_2  int16 = 5
	int16GreaterEqualTestCount_2  int64 = 24

	int16BetweenTestResult_2       = []byte{0x1e, 0x00, 0x00, 0x00}
	int16BetweenTestMatch_2  int16 = 5
	int16BetweenTestMatch_2b int16 = 10
	int16BetweenTestCount_2  int64 = 4

	// extreme values
	int16TestSlice_3 = []int16{
        0,0,
		math.MaxInt8/2, math.MinInt8/2,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
        0,0,
		math.MaxInt8/2, math.MinInt8/2,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
        0,0,
		math.MaxInt8/2, math.MinInt8/2,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
        0,0,
		math.MaxInt8/2, math.MinInt8/2,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
	}
	int16EqualTestResult_3       = []byte{0x01, 0x01, 0x01, 0x01}
	int16EqualTestMatch_3  int16 = math.MinInt16
	int16EqualTestCount_3  int64 = 4

	int16LessTestResult_3       = []byte{0x0, 0x0, 0x0, 0x00}
	int16LessTestMatch_3  int16 = math.MinInt16
	int16LessTestCount_3  int64 = 0

	int16LessEqualTestResult_3       = []byte{0x01, 0x01, 0x01, 0x01}
	int16LessEqualTestMatch_3  int16 = math.MinInt16
	int16LessEqualTestCount_3  int64 = 4

	int16GreaterTestResult_3       = []byte{0xfe, 0xfe, 0xfe, 0xfe}
	int16GreaterTestMatch_3  int16 = math.MinInt16
	int16GreaterTestCount_3  int64 = 28

	int16GreaterEqualTestResult_3       = []byte{0xff, 0xff, 0xff, 0xff}
	int16GreaterEqualTestMatch_3  int16 = math.MinInt16
	int16GreaterEqualTestCount_3  int64 = 32

	int16BetweenTestResult_3       = []byte{0x0a, 0x0a, 0x0a, 0x0a}
	int16BetweenTestMatch_3  int16 = math.MaxInt8
	int16BetweenTestMatch_3b int16 = math.MaxInt16
	int16BetweenTestCount_3  int64 = 8
)

// -----------------------------------------------------------------------------
// Equal Testcases
//
var int16EqualCases = []Int16MatchTest{
	Int16MatchTest{
		name: "vec1",
		// 	// test vector to find shuffle/perm positions
		// 	slice: []int16{
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
		slice: []int16{
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
		slice:  int16TestSlice_1,
		match:  int16EqualTestMatch_1,
		result: int16EqualTestResult_1,
		count:  int16EqualTestCount_1,
	}, {
		name:   "l64",
		slice:  append(int16TestSlice_1, int16TestSlice_1...),
		match:  int16EqualTestMatch_1,
		result: append(int16EqualTestResult_1, int16EqualTestResult_1...),
		count:  int16EqualTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  int16TestSlice_1[:31],
		match:  int16EqualTestMatch_1,
		result: int16EqualTestResult_1,
		count:  int16EqualTestCount_1,
	}, {
		name:   "l23",
		slice:  int16TestSlice_1[:23],
		match:  int16EqualTestMatch_1,
		result: []byte{0x82, 0x42, 0x22}, // last bit off!
		count:  int16EqualTestCount_1 - 4,
	}, {
		name:   "l15",
		slice:  int16TestSlice_1[:15],
		match:  int16EqualTestMatch_1,
		result: int16EqualTestResult_1[:2],
		count:  int16EqualTestCount_1 - 6,
	}, {
		name:   "l7",
		slice:  int16TestSlice_1[:7],
		match:  int16EqualTestMatch_1,
		result: int16EqualTestResult_1[:1],
		count:  int16EqualTestCount_1 - 8,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  int16TestSlice_2,
		match:  int16EqualTestMatch_2,
		result: int16EqualTestResult_2,
		count:  int16EqualTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  int16TestSlice_2[:31],
		match:  int16EqualTestMatch_2,
		result: int16EqualTestResult_2,
		count:  int16EqualTestCount_2,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  int16TestSlice_3,
		match:  int16EqualTestMatch_3,
		result: int16EqualTestResult_3,
		count:  int16EqualTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  int16TestSlice_3[:31],
		match:  int16EqualTestMatch_3,
		result: []byte{0x01, 0x01, 0x01, 0x00},
		count:  3,
	},
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
/*
func TestMatchInt16EqualAVX2(T *testing.T) {
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
*/
// -----------------------------------------------------------------------------
// Equal benchmarks
//
func BenchmarkMatchInt16EqualGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16EqualGeneric(a, 5, bits)
			}
		})
	}
}
/*
func BenchmarkMatchInt16EqualAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16EqualAVX2(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchInt16EqualAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16EqualAVX2(a, 5, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Less Testcases
//
var int16LessCases = []Int16MatchTest{
	Int16MatchTest{
		name: "vec1",
		slice: []int16{
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
		slice:  int16TestSlice_1,
		match:  int16LessTestMatch_1,
		result: int16LessTestResult_1,
		count:  int16LessTestCount_1,
	}, {
		name:   "l64",
		slice:  append(int16TestSlice_1, int16TestSlice_1...),
		match:  int16LessTestMatch_1,
		result: append(int16LessTestResult_1, int16LessTestResult_1...),
		count:  int16LessTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  int16TestSlice_1[:31],
		match:  int16LessTestMatch_1,
		result: int16LessTestResult_1,
		count:  int16LessTestCount_1,
	}, {
		name:   "l23",
		slice:  int16TestSlice_1[:23],
		match:  int16LessTestMatch_1,
		result: int16LessTestResult_1[:3],
		count:  int16LessTestCount_1,
	}, {
		name:   "l15",
		slice:  int16TestSlice_1[:15],
		match:  int16LessTestMatch_1,
		result: int16LessTestResult_1[:2],
		count:  int16LessTestCount_1,
	}, {
		name:   "l7",
		slice:  int16TestSlice_1[:7],
		match:  int16LessTestMatch_1,
		result: int16LessTestResult_1[:1],
		count:  int16LessTestCount_1,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  int16TestSlice_2,
		match:  int16LessTestMatch_2,
		result: int16LessTestResult_2,
		count:  int16LessTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  int16TestSlice_2[:31],
		match:  int16LessTestMatch_2,
		result: []byte{0xe1, 0x04, 0x04, 0x20}, // last bit off
		count:  int16LessTestCount_2 - 1,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  int16TestSlice_3,
		match:  int16LessTestMatch_3,
		result: int16LessTestResult_3,
		count:  int16LessTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  int16TestSlice_3[:31],
		match:  int16LessTestMatch_3,
		result: int16LessTestResult_3, // still zeros
		count:  int16LessTestCount_3,
	},
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
/*
func TestMatchInt16LessAVX2(T *testing.T) {
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
*/
// -----------------------------------------------------------------------------
// Less benchmarks
//
func BenchmarkMatchInt16LessGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16LessThanGeneric(a, 5, bits)
			}
		})
	}
}
/*
func BenchmarkMatchInt16LessAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16LessThanAVX2(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchInt16LessAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16LessThanAVX2(a, 5, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Less Equal Testcases
//
var int16LessEqualCases = []Int16MatchTest{
	Int16MatchTest{
		name: "vec1",
		slice: []int16{
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
		slice:  int16TestSlice_1,
		match:  int16LessEqualTestMatch_1,
		result: int16LessEqualTestResult_1,
		count:  int16LessEqualTestCount_1,
	}, {
		name:   "l64",
		slice:  append(int16TestSlice_1, int16TestSlice_1...),
		match:  int16LessEqualTestMatch_1,
		result: append(int16LessEqualTestResult_1, int16LessEqualTestResult_1...),
		count:  int16LessEqualTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  int16TestSlice_1[:31],
		match:  int16LessEqualTestMatch_1,
		result: int16LessEqualTestResult_1,
		count:  int16LessEqualTestCount_1,
	}, {
		name:   "l23",
		slice:  int16TestSlice_1[:23],
		match:  int16LessEqualTestMatch_1,
		result: []byte{0xf2, 0x42, 0x22},
		count:  int16LessEqualTestCount_1 - 4,
	}, {
		name:   "l15",
		slice:  int16TestSlice_1[:15],
		match:  int16LessEqualTestMatch_1,
		result: int16LessEqualTestResult_1[:2],
		count:  int16LessEqualTestCount_1 - 6,
	}, {
		name:   "l7",
		slice:  int16TestSlice_1[:7],
		match:  int16LessEqualTestMatch_1,
		result: int16LessEqualTestResult_1[:1],
		count:  int16LessEqualTestCount_1 - 8,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  int16TestSlice_2,
		match:  int16LessEqualTestMatch_2,
		result: int16LessEqualTestResult_2,
		count:  int16LessEqualTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  int16TestSlice_2[:31],
		match:  int16LessEqualTestMatch_2,
		result: []byte{0xf1, 0x04, 0x04, 0x20}, // last bit off
		count:  int16LessEqualTestCount_2 - 1,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  int16TestSlice_3,
		match:  int16LessEqualTestMatch_3,
		result: int16LessEqualTestResult_3,
		count:  int16LessEqualTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  int16TestSlice_3[:31],
		match:  int16LessEqualTestMatch_3,
		result: []byte{0x1, 0x1, 0x1, 0x0}, // last off
		count:  int16LessEqualTestCount_3 - 1,
	},
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
/*
func TestMatchInt16LessEqualAVX2(T *testing.T) {
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
*/
// -----------------------------------------------------------------------------
// Less equal benchmarks
//
func BenchmarkMatchInt16LessEqualGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16LessThanEqualGeneric(a, 5, bits)
			}
		})
	}
}
/*
func BenchmarkMatchInt16LessEqualAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16LessThanEqualAVX2(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchInt16LessEqualAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16LessThanEqualAVX2(a, 5, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Greater Testcases
//
var int16GreaterCases = []Int16MatchTest{
	Int16MatchTest{
		name: "vec1",
		slice: []int16{
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
		slice:  int16TestSlice_1,
		match:  int16GreaterTestMatch_1,
		result: int16GreaterTestResult_1,
		count:  int16GreaterTestCount_1,
	}, {
		name:   "l64",
		slice:  append(int16TestSlice_1, int16TestSlice_1...),
		match:  int16GreaterTestMatch_1,
		result: append(int16GreaterTestResult_1, int16GreaterTestResult_1...),
		count:  int16GreaterTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  int16TestSlice_1[:31],
		match:  int16GreaterTestMatch_1,
		result: []byte{0x0d, 0xbd, 0xdc, 0x8e},
		count:  int16GreaterTestCount_1 - 1,
	}, {
		name:   "l23",
		slice:  int16TestSlice_1[:23],
		match:  int16GreaterTestMatch_1,
		result: int16GreaterTestResult_1[:3],
		count:  int16GreaterTestCount_1 - 5,
	}, {
		name:   "l15",
		slice:  int16TestSlice_1[:15],
		match:  int16GreaterTestMatch_1,
		result: []byte{0x0d, 0xbc},
		count:  int16GreaterTestCount_1 - 11,
	}, {
		name:   "l7",
		slice:  int16TestSlice_1[:7],
		match:  int16GreaterTestMatch_1,
		result: []byte{0x0c},
		count:  2,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  int16TestSlice_2,
		match:  int16GreaterTestMatch_2,
		result: int16GreaterTestResult_2,
		count:  int16GreaterTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  int16TestSlice_2[:31],
		match:  int16GreaterTestMatch_2,
		result: []byte{0x0e, 0xfb, 0xfb, 0xde},
		count:  int16GreaterTestCount_2,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  int16TestSlice_3,
		match:  int16GreaterTestMatch_3,
		result: int16GreaterTestResult_3,
		count:  int16GreaterTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  int16TestSlice_3[:31],
		match:  int16GreaterTestMatch_3,
		result: int16GreaterTestResult_3, // still zeros
		count:  int16GreaterTestCount_3,
	},
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
/*
func TestMatchInt16GreaterAVX2(T *testing.T) {
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
*/
// -----------------------------------------------------------------------------
// Greater benchmarks
//
func BenchmarkMatchInt16GreaterGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16GreaterThanGeneric(a, 5, bits)
			}
		})
	}
}
/*
func BenchmarkMatchInt16GreaterAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16GreaterThanAVX2(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchInt16GreaterAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16GreaterThanAVX2(a, 5, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Greater Equal Testcases
//
var int16GreaterEqualCases = []Int16MatchTest{
	Int16MatchTest{
		name: "vec1",
		slice: []int16{
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
		slice:  int16TestSlice_1,
		match:  int16GreaterEqualTestMatch_1,
		result: int16GreaterEqualTestResult_1,
		count:  int16GreaterEqualTestCount_1,
	}, {
		name:   "l64",
		slice:  append(int16TestSlice_1, int16TestSlice_1...),
		match:  int16GreaterEqualTestMatch_1,
		result: append(int16GreaterEqualTestResult_1, int16GreaterEqualTestResult_1...),
		count:  int16GreaterEqualTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  int16TestSlice_1[:31],
		match:  int16GreaterEqualTestMatch_1,
		result: []byte{0x8f, 0xff, 0xff, 0xfe},
		count:  int16GreaterEqualTestCount_1 - 1,
	}, {
		name:   "l23",
		slice:  int16TestSlice_1[:23],
		match:  int16GreaterEqualTestMatch_1,
		result: []byte{0x8f, 0xff, 0xfe},
		count:  int16GreaterEqualTestCount_1 - 9,
	}, {
		name:   "l15",
		slice:  int16TestSlice_1[:15],
		match:  int16GreaterEqualTestMatch_1,
		result: []byte{0x8f, 0xfe},
		count:  int16GreaterEqualTestCount_1 - 17,
	}, {
		name:   "l7",
		slice:  int16TestSlice_1[:7],
		match:  int16GreaterEqualTestMatch_1,
		result: []byte{0x8e},
		count:  4,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  int16TestSlice_2,
		match:  int16GreaterEqualTestMatch_2,
		result: int16GreaterEqualTestResult_2,
		count:  int16GreaterEqualTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  int16TestSlice_2[:31],
		match:  int16GreaterEqualTestMatch_2,
		result: int16GreaterEqualTestResult_2,
		count:  int16GreaterEqualTestCount_2,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  int16TestSlice_3,
		match:  int16GreaterEqualTestMatch_3,
		result: int16GreaterEqualTestResult_3,
		count:  int16GreaterEqualTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  int16TestSlice_3[:31],
		match:  int16GreaterEqualTestMatch_3,
		result: []byte{0xff, 0xff, 0xff, 0xfe}, // last off
		count:  int16GreaterEqualTestCount_3 - 1,
	},
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
/*
func TestMatchInt16GreaterEqualAVX2(T *testing.T) {
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
*/
// -----------------------------------------------------------------------------
// Greater equal benchmarks
//
func BenchmarkMatchInt16GreaterEqualGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16GreaterThanEqualGeneric(a, 5, bits)
			}
		})
	}
}
/*
func BenchmarkMatchInt16GreaterEqualAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16GreaterThanEqualAVX2(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchInt16GreaterEqualAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16GreaterThanEqualAVX2(a, 5, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Between Testcases
//
var int16BetweenCases = []Int16MatchTest{
	Int16MatchTest{
		name: "vec1",
		slice: []int16{
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
		slice:  int16TestSlice_1,
		match:  int16BetweenTestMatch_1,
		match2: int16BetweenTestMatch_1b,
		result: int16BetweenTestResult_1,
		count:  int16BetweenTestCount_1,
	}, {
		name:   "l64",
		slice:  append(int16TestSlice_1, int16TestSlice_1...),
		match:  int16BetweenTestMatch_1,
		match2: int16BetweenTestMatch_1b,
		result: append(int16BetweenTestResult_1, int16BetweenTestResult_1...),
		count:  int16BetweenTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  int16TestSlice_1[:31],
		match:  int16BetweenTestMatch_1,
		match2: int16BetweenTestMatch_1b,
		result: int16BetweenTestResult_1,
		count:  int16BetweenTestCount_1,
	}, {
		name:   "l23",
		slice:  int16TestSlice_1[:23],
		match:  int16BetweenTestMatch_1,
		match2: int16BetweenTestMatch_1b,
		result: []byte{0x8f, 0x42, 0x22}, // last bit off!
		count:  int16BetweenTestCount_1 - 4,
	}, {
		name:   "l15",
		slice:  int16TestSlice_1[:15],
		match:  int16BetweenTestMatch_1,
		match2: int16BetweenTestMatch_1b,
		result: int16BetweenTestResult_1[:2],
		count:  int16BetweenTestCount_1 - 6,
	}, {
		name:   "l7",
		slice:  int16TestSlice_1[:7],
		match:  int16BetweenTestMatch_1,
		match2: int16BetweenTestMatch_1b,
		result: []byte{0x8e},
		count:  int16BetweenTestCount_1 - 9,
	}, {
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
	}, {
		// with negative values
		name:   "neg32",
		slice:  int16TestSlice_2,
		match:  int16BetweenTestMatch_2,
		match2: int16BetweenTestMatch_2b,
		result: int16BetweenTestResult_2,
		count:  int16BetweenTestCount_2,
	}, {
		// with negative values, test scalar algorithm
		name:   "neg31",
		slice:  int16TestSlice_2[:31],
		match:  int16BetweenTestMatch_2,
		match2: int16BetweenTestMatch_2b,
		result: int16BetweenTestResult_2,
		count:  int16BetweenTestCount_2,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  int16TestSlice_3,
		match:  int16BetweenTestMatch_3,
		match2: int16BetweenTestMatch_3b,
		result: int16BetweenTestResult_3,
		count:  int16BetweenTestCount_3,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  int16TestSlice_3[:31],
		match:  int16BetweenTestMatch_3,
		match2: int16BetweenTestMatch_3b,
		result: []byte{0x0a, 0x0a, 0x0a, 0x0a},
		count:  8,
	},
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
/*
func TestMatchInt16BetweenAVX2(T *testing.T) {
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
*/
// -----------------------------------------------------------------------------
// Between benchmarks
//
// BenchmarkMatchInt16BetweenGeneric/32-8  30000000      54.7 ns/op	4683.30 MB/s
// BenchmarkMatchInt16BetweenGeneric/128-8 10000000     163 ns/op	6261.85 MB/s
// BenchmarkMatchInt16BetweenGeneric/1024-8 1000000    1211 ns/op	6762.88 MB/s
// BenchmarkMatchInt16BetweenGeneric/4096-8  300000    4985 ns/op	6572.26 MB/s
// BenchmarkMatchInt16BetweenGeneric/65536-8  20000   80903 ns/op	6480.44 MB/s
// BenchmarkMatchInt16BetweenGeneric/131072-8 10000  158714 ns/op	6606.70 MB/s
func BenchmarkMatchInt16BetweenGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16BetweenGeneric(a, 5, 10, bits)
			}
		})
	}
}

// BenchmarkMatchInt16BetweenAVX2/32-8     100000000     15.1 ns/op	16959.99 MB/s
// BenchmarkMatchInt16BetweenAVX2/128-8    	30000000     48.5 ns/op	21117.50 MB/s
// BenchmarkMatchInt16BetweenAVX2/1024-8   	 5000000    362 ns/op	22578.41 MB/s
// BenchmarkMatchInt16BetweenAVX2/4096-8   	 1000000   1610 ns/op	20345.03 MB/s
// BenchmarkMatchInt16BetweenAVX2/65536-8  	   50000  28742 ns/op	18240.79 MB/s
// BenchmarkMatchInt16BetweenAVX2/131072-8 	   20000  60508 ns/op	17329.33 MB/s
/*func BenchmarkMatchInt16BetweenAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16BetweenAVX2(a, 5, 10, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
// BenchmarkMatchInt16BetweenAVX2Scalar/31-8      30000000     38.2 ns/op	6494.10 MB/s
// BenchmarkMatchInt16BetweenAVX2Scalar/127-8     20000000     70.3 ns/op	14452.14 MB/s
// BenchmarkMatchInt16BetweenAVX2Scalar/1023-8     3000000    384 ns/op	21292.42 MB/s
// BenchmarkMatchInt16BetweenAVX2Scalar/4095-8     1000000   1652 ns/op	19828.53 MB/s
// BenchmarkMatchInt16BetweenAVX2Scalar/65535-8      50000  28695 ns/op	18270.74 MB/s
// BenchmarkMatchInt16BetweenAVX2Scalar/131071-8     30000  59239 ns/op	17700.62 MB/s
func BenchmarkMatchInt16BetweenAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Int16Size))
			for i := 0; i < B.N; i++ {
				matchInt16BetweenAVX2(a, 5, 10, bits)
			}
		})
	}
}
*/
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
	if Int16Slice(nil).Contains(1) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Int16Slice([]int16{}).Contains(1) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Int16Slice([]int16{1}).Contains(1) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Int16Slice([]int16{1}).Contains(2) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !Int16Slice([]int16{-1, 3, 5, 7, 11, 13}).Contains(-1) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Int16Slice([]int16{-1, 3, 5, 7, 11, 13}).Contains(5) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Int16Slice([]int16{-1, 3, 5, 7, 11, 13}).Contains(13) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Int16Slice([]int16{-1, 3, 5, 7, 11, 13}).Contains(0) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Int16Slice([]int16{-1, 3, 5, 7, 11, 13}).Contains(2) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Int16Slice([]int16{-1, 3, 5, 7, 11, 13}).Contains(14) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkInt16SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Int16Slice(randInt16Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(int16(rand.Intn(math.MaxInt16 + 1)))
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Int16Slice(randInt16Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(a[rand.Intn(len(a))])
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
			if want, got := r.Match, Int16Slice(v.Slice).ContainsRange(r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkInt16SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Int16Slice(randInt16Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := int16(rand.Intn(math.MaxInt16 + 1)), int16(rand.Intn(math.MaxInt16 + 1))
				if min > max {
					min, max = max, min
				}
				a.ContainsRange(min, max)
			}
		})
	}
}
