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

const Uint8Size = 1

type Uint8MatchTest struct {
	name   string
	slice  []uint8
	match  uint8 // used for every test
	match2 uint8 // used for between tests
	result []byte
	count  int64
}

var (
	/*	uint8TestSlice_1 = []uint8{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 50000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}*/
	uint8TestSlice_1 = []uint8{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 50,
		100, 50, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}
	uint8EqualTestResult_1       = []byte{0x82, 0x42, 0x23, 0x70}
	uint8EqualTestMatch_1  uint8 = 5
	uint8EqualTestCount_1  int64 = 10

	uint8LessTestResult_1       = []byte{0x70, 0x00, 0x00, 0x00}
	uint8LessTestMatch_1  uint8 = 5
	uint8LessTestCount_1  int64 = 3

	uint8LessEqualTestResult_1       = []byte{0xf2, 0x42, 0x23, 0x70}
	uint8LessEqualTestMatch_1  uint8 = 5
	uint8LessEqualTestCount_1  int64 = 13

	uint8GreaterTestResult_1       = []byte{0x0d, 0xbd, 0xdc, 0x8f}
	uint8GreaterTestMatch_1  uint8 = 5
	uint8GreaterTestCount_1  int64 = 19

	uint8GreaterEqualTestResult_1       = []byte{0x8f, 0xff, 0xff, 0xff}
	uint8GreaterEqualTestMatch_1  uint8 = 5
	uint8GreaterEqualTestCount_1  int64 = 29

	uint8BetweenTestResult_1       = []byte{0x8f, 0x42, 0x23, 0x70}
	uint8BetweenTestMatch_1  uint8 = 5
	uint8BetweenTestMatch_1b uint8 = 10
	uint8BetweenTestCount_1  int64 = 13

	// extreme values
	uint8TestSlice_2 = []uint8{
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
		0, math.MaxInt8 / 2, math.MaxInt8, math.MaxUint8,
	}
	uint8EqualTestResult_2       = []byte{0x11, 0x11, 0x11, 0x11}
	uint8EqualTestMatch_2  uint8 = math.MaxUint8
	uint8EqualTestCount_2  int64 = 8

	uint8LessTestResult_2       = []byte{0xee, 0xee, 0xee, 0xee}
	uint8LessTestMatch_2  uint8 = math.MaxUint8
	uint8LessTestCount_2  int64 = 24

	uint8LessEqualTestResult_2       = []byte{0xff, 0xff, 0xff, 0xff}
	uint8LessEqualTestMatch_2  uint8 = math.MaxUint8
	uint8LessEqualTestCount_2  int64 = 32

	uint8GreaterTestResult_2       = []byte{0x00, 0x00, 0x00, 0x00}
	uint8GreaterTestMatch_2  uint8 = math.MaxUint8
	uint8GreaterTestCount_2  int64 = 0

	uint8GreaterEqualTestResult_2       = []byte{0x11, 0x11, 0x11, 0x11}
	uint8GreaterEqualTestMatch_2  uint8 = math.MaxUint8
	uint8GreaterEqualTestCount_2  int64 = 8

	uint8BetweenTestResult_2       = []byte{0x33, 0x33, 0x33, 0x33}
	uint8BetweenTestMatch_2  uint8 = math.MaxInt8
	uint8BetweenTestMatch_2b uint8 = math.MaxUint8
	uint8BetweenTestCount_2  int64 = 16
)

func randUint8Slice(n, u int) []uint8 {
	s := make([]uint8, n*u)
	for i := 0; i < n; i++ {
		s[i] = uint8(rand.Intn(math.MaxUint8 + 1))
	}
	for i := 0; i < u; i++ {
		s = append(s, s[:n]...)
	}
	return s
}

// -----------------------------------------------------------------------------
// Equal Testcases
//
var uint8EqualCases = []Uint8MatchTest{
	Uint8MatchTest{
		name: "vec1",
		// 	// test vector to find shuffle/perm positions
		// 	slice: []int64{
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
		slice: []uint8{
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
		slice:  uint8TestSlice_1,
		match:  uint8EqualTestMatch_1,
		result: uint8EqualTestResult_1,
		count:  uint8EqualTestCount_1,
	}, {
		name:   "l64",
		slice:  append(uint8TestSlice_1, uint8TestSlice_1...),
		match:  uint8EqualTestMatch_1,
		result: append(uint8EqualTestResult_1, uint8EqualTestResult_1...),
		count:  uint8EqualTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  uint8TestSlice_1[:31],
		match:  uint8EqualTestMatch_1,
		result: uint8EqualTestResult_1,
		count:  uint8EqualTestCount_1,
	}, {
		name:   "l23",
		slice:  uint8TestSlice_1[:23],
		match:  uint8EqualTestMatch_1,
		result: []byte{0x82, 0x42, 0x22}, // last bit off!
		count:  uint8EqualTestCount_1 - 4,
	}, {
		name:   "l15",
		slice:  uint8TestSlice_1[:15],
		match:  uint8EqualTestMatch_1,
		result: uint8EqualTestResult_1[:2],
		count:  uint8EqualTestCount_1 - 6,
	}, {
		name:   "l7",
		slice:  uint8TestSlice_1[:7],
		match:  uint8EqualTestMatch_1,
		result: uint8EqualTestResult_1[:1],
		count:  uint8EqualTestCount_1 - 8,
	}, {
		name:   "l0",
		slice:  make([]uint8, 0),
		match:  uint8EqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint8EqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  uint8TestSlice_2,
		match:  uint8EqualTestMatch_2,
		result: uint8EqualTestResult_2,
		count:  uint8EqualTestCount_2,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  uint8TestSlice_2[:31],
		match:  uint8EqualTestMatch_2,
		result: []byte{0x11, 0x11, 0x11, 0x10},
		count:  7,
	},
}

func TestMatchUint8EqualGeneric(T *testing.T) {
	for _, c := range uint8EqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint8EqualGeneric(c.slice, c.match, bits)
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
func TestMatchUint8EqualAVX2(T *testing.T) {
	for _, c := range uint8EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint8EqualAVX2(c.slice, c.match, bits)
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
func BenchmarkMatchUint8EqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8EqualGeneric(a, math.MaxUint8/2, bits)
			}
		})
	}
}

/*
func BenchmarkMatchUint8EqualAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8EqualAVX2(a, 5, bits)
			}
		})
	}
}

func BenchmarkMatchUint8EqualAVX512(B *testing.B) {
	if !useAVX512_BW {
		return
	}
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8EqualAVX512(a, 5, bits)
			}
		})
	}
}

func BenchmarkMatchUint8EqualAVX2Opt(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8EqualAVX2Opt(a, 5, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchUint8EqualAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8EqualAVX2(a, math.MaxUint8/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Less Testcases
//
var uint8LessCases = []Uint8MatchTest{
	Uint8MatchTest{
		name: "vec1",
		slice: []uint8{
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
		slice:  uint8TestSlice_1,
		match:  uint8LessTestMatch_1,
		result: uint8LessTestResult_1,
		count:  uint8LessTestCount_1,
	}, {
		name:   "l64",
		slice:  append(uint8TestSlice_1, uint8TestSlice_1...),
		match:  uint8LessTestMatch_1,
		result: append(uint8LessTestResult_1, uint8LessTestResult_1...),
		count:  uint8LessTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  uint8TestSlice_1[:31],
		match:  uint8LessTestMatch_1,
		result: uint8LessTestResult_1,
		count:  uint8LessTestCount_1,
	}, {
		name:   "l23",
		slice:  uint8TestSlice_1[:23],
		match:  uint8LessTestMatch_1,
		result: uint8LessTestResult_1[:3],
		count:  uint8LessTestCount_1,
	}, {
		name:   "l15",
		slice:  uint8TestSlice_1[:15],
		match:  uint8LessTestMatch_1,
		result: uint8LessTestResult_1[:2],
		count:  uint8LessTestCount_1,
	}, {
		name:   "l7",
		slice:  uint8TestSlice_1[:7],
		match:  uint8LessTestMatch_1,
		result: uint8LessTestResult_1[:1],
		count:  uint8LessTestCount_1,
	}, {
		name:   "l0",
		slice:  make([]uint8, 0),
		match:  uint8LessTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint8LessTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  uint8TestSlice_2,
		match:  uint8LessTestMatch_2,
		result: uint8LessTestResult_2,
		count:  uint8LessTestCount_2,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  uint8TestSlice_2[:31],
		match:  uint8LessTestMatch_2,
		result: uint8LessTestResult_2,
		count:  uint8LessTestCount_2,
	},
}

func TestMatchUint8LessGeneric(T *testing.T) {
	for _, c := range uint8LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint8LessThanGeneric(c.slice, c.match, bits)
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
func TestMatchUint8LessAVX2(T *testing.T) {
	for _, c := range uint8LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint8LessThanAVX2(c.slice, c.match, bits)
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
func BenchmarkMatchUint8LessGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8LessThanGeneric(a, math.MaxUint8/2, bits)
			}
		})
	}
}

/*
func BenchmarkMatchUint8LessAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8LessThanAVX2(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchUint8LessAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8LessThanAVX2(a, math.MaxUint8/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Less Equal Testcases
//
var uint8LessEqualCases = []Uint8MatchTest{
	Uint8MatchTest{
		name: "vec1",
		slice: []uint8{
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
		slice:  uint8TestSlice_1,
		match:  uint8LessEqualTestMatch_1,
		result: uint8LessEqualTestResult_1,
		count:  uint8LessEqualTestCount_1,
	}, {
		name:   "l64",
		slice:  append(uint8TestSlice_1, uint8TestSlice_1...),
		match:  uint8LessEqualTestMatch_1,
		result: append(uint8LessEqualTestResult_1, uint8LessEqualTestResult_1...),
		count:  uint8LessEqualTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  uint8TestSlice_1[:31],
		match:  uint8LessEqualTestMatch_1,
		result: uint8LessEqualTestResult_1,
		count:  uint8LessEqualTestCount_1,
	}, {
		name:   "l23",
		slice:  uint8TestSlice_1[:23],
		match:  uint8LessEqualTestMatch_1,
		result: []byte{0xf2, 0x42, 0x22},
		count:  uint8LessEqualTestCount_1 - 4,
	}, {
		name:   "l15",
		slice:  uint8TestSlice_1[:15],
		match:  uint8LessEqualTestMatch_1,
		result: uint8LessEqualTestResult_1[:2],
		count:  uint8LessEqualTestCount_1 - 6,
	}, {
		name:   "l7",
		slice:  uint8TestSlice_1[:7],
		match:  uint8LessEqualTestMatch_1,
		result: uint8LessEqualTestResult_1[:1],
		count:  uint8LessEqualTestCount_1 - 8,
	}, {
		name:   "l0",
		slice:  make([]uint8, 0),
		match:  uint8LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint8LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  uint8TestSlice_2,
		match:  uint8LessEqualTestMatch_2,
		result: uint8LessEqualTestResult_2,
		count:  uint8LessEqualTestCount_2,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  uint8TestSlice_2[:31],
		match:  uint8LessEqualTestMatch_2,
		result: []byte{0xff, 0xff, 0xff, 0xfe}, // last off
		count:  uint8LessEqualTestCount_2 - 1,
	},
}

func TestMatchUint8LessEqualGeneric(T *testing.T) {
	for _, c := range uint8LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint8LessThanEqualGeneric(c.slice, c.match, bits)
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
func TestMatchUint8LessEqualAVX2(T *testing.T) {
	for _, c := range uint8LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint8LessThanEqualAVX2(c.slice, c.match, bits)
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
func BenchmarkMatchUint8LessEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8LessThanEqualGeneric(a, math.MaxUint8/2, bits)
			}
		})
	}
}

/*
func BenchmarkMatchUint8LessEqualAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8LessThanEqualAVX2(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchUint8LessEqualAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8LessThanEqualAVX2(a, math.MaxUint8/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Greater Testcases
//
var uint8GreaterCases = []Uint8MatchTest{
	Uint8MatchTest{
		name: "vec1",
		slice: []uint8{
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
		slice:  uint8TestSlice_1,
		match:  uint8GreaterTestMatch_1,
		result: uint8GreaterTestResult_1,
		count:  uint8GreaterTestCount_1,
	}, {
		name:   "l64",
		slice:  append(uint8TestSlice_1, uint8TestSlice_1...),
		match:  uint8GreaterTestMatch_1,
		result: append(uint8GreaterTestResult_1, uint8GreaterTestResult_1...),
		count:  uint8GreaterTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  uint8TestSlice_1[:31],
		match:  uint8GreaterTestMatch_1,
		result: []byte{0x0d, 0xbd, 0xdc, 0x8e},
		count:  uint8GreaterTestCount_1 - 1,
	}, {
		name:   "l23",
		slice:  uint8TestSlice_1[:23],
		match:  uint8GreaterTestMatch_1,
		result: uint8GreaterTestResult_1[:3],
		count:  uint8GreaterTestCount_1 - 5,
	}, {
		name:   "l15",
		slice:  uint8TestSlice_1[:15],
		match:  uint8GreaterTestMatch_1,
		result: []byte{0x0d, 0xbc},
		count:  uint8GreaterTestCount_1 - 11,
	}, {
		name:   "l7",
		slice:  uint8TestSlice_1[:7],
		match:  uint8GreaterTestMatch_1,
		result: []byte{0x0c},
		count:  2,
	}, {
		name:   "l0",
		slice:  make([]uint8, 0),
		match:  uint8GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint8GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  uint8TestSlice_2,
		match:  uint8GreaterTestMatch_2,
		result: uint8GreaterTestResult_2,
		count:  uint8GreaterTestCount_2,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  uint8TestSlice_2[:31],
		match:  uint8GreaterTestMatch_2,
		result: uint8GreaterTestResult_2, // still zeros
		count:  uint8GreaterTestCount_2,
	},
}

func TestMatchUint8GreaterGeneric(T *testing.T) {
	for _, c := range uint8GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint8GreaterThanGeneric(c.slice, c.match, bits)
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
func TestMatchUint8GreaterAVX2(T *testing.T) {
	for _, c := range uint8GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint8GreaterThanAVX2(c.slice, c.match, bits)
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
func BenchmarkMatchUint8GreaterGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8GreaterThanGeneric(a, math.MaxUint8/2, bits)
			}
		})
	}
}

/*
func BenchmarkMatchUint8GreaterAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8GreaterThanAVX2(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchUint8GreaterAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8GreaterThanAVX2(a, math.MaxUint8/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Greater Equal Testcases
//
var uint8GreaterEqualCases = []Uint8MatchTest{
	Uint8MatchTest{
		name: "vec1",
		slice: []uint8{
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
		slice:  uint8TestSlice_1,
		match:  uint8GreaterEqualTestMatch_1,
		result: uint8GreaterEqualTestResult_1,
		count:  uint8GreaterEqualTestCount_1,
	}, {
		name:   "l64",
		slice:  append(uint8TestSlice_1, uint8TestSlice_1...),
		match:  uint8GreaterEqualTestMatch_1,
		result: append(uint8GreaterEqualTestResult_1, uint8GreaterEqualTestResult_1...),
		count:  uint8GreaterEqualTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  uint8TestSlice_1[:31],
		match:  uint8GreaterEqualTestMatch_1,
		result: []byte{0x8f, 0xff, 0xff, 0xfe},
		count:  uint8GreaterEqualTestCount_1 - 1,
	}, {
		name:   "l23",
		slice:  uint8TestSlice_1[:23],
		match:  uint8GreaterEqualTestMatch_1,
		result: []byte{0x8f, 0xff, 0xfe},
		count:  uint8GreaterEqualTestCount_1 - 9,
	}, {
		name:   "l15",
		slice:  uint8TestSlice_1[:15],
		match:  uint8GreaterEqualTestMatch_1,
		result: []byte{0x8f, 0xfe},
		count:  uint8GreaterEqualTestCount_1 - 17,
	}, {
		name:   "l7",
		slice:  uint8TestSlice_1[:7],
		match:  uint8GreaterEqualTestMatch_1,
		result: []byte{0x8e},
		count:  4,
	}, {
		name:   "l0",
		slice:  make([]uint8, 0),
		match:  uint8GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint8GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  uint8TestSlice_2,
		match:  uint8GreaterEqualTestMatch_2,
		result: uint8GreaterEqualTestResult_2,
		count:  uint8GreaterEqualTestCount_2,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  uint8TestSlice_2[:31],
		match:  uint8GreaterEqualTestMatch_2,
		result: []byte{0x11, 0x11, 0x11, 0x10}, // last off
		count:  uint8GreaterEqualTestCount_2 - 1,
	},
}

func TestMatchUint8GreaterEqualGeneric(T *testing.T) {
	for _, c := range uint8GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint8GreaterThanEqualGeneric(c.slice, c.match, bits)
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
func TestMatchUint8GreaterEqualAVX2(T *testing.T) {
	for _, c := range uint8GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint8GreaterThanEqualAVX2(c.slice, c.match, bits)
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
func BenchmarkMatchUint8GreaterEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8GreaterThanEqualGeneric(a, math.MaxUint8/2, bits)
			}
		})
	}
}

/*
func BenchmarkMatchUint8GreaterEqualAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8GreaterThanEqualAVX2(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchUint8GreaterEqualAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8GreaterThanEqualAVX2(a, math.MaxUint8/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Between Testcases
//
var uint8BetweenCases = []Uint8MatchTest{
	Uint8MatchTest{
		name: "vec1",
		slice: []uint8{
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
		slice:  uint8TestSlice_1,
		match:  uint8BetweenTestMatch_1,
		match2: uint8BetweenTestMatch_1b,
		result: uint8BetweenTestResult_1,
		count:  uint8BetweenTestCount_1,
	}, {
		name:   "l64",
		slice:  append(uint8TestSlice_1, uint8TestSlice_1...),
		match:  uint8BetweenTestMatch_1,
		match2: uint8BetweenTestMatch_1b,
		result: append(uint8BetweenTestResult_1, uint8BetweenTestResult_1...),
		count:  uint8BetweenTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  uint8TestSlice_1[:31],
		match:  uint8BetweenTestMatch_1,
		match2: uint8BetweenTestMatch_1b,
		result: uint8BetweenTestResult_1,
		count:  uint8BetweenTestCount_1,
	}, {
		name:   "l23",
		slice:  uint8TestSlice_1[:23],
		match:  uint8BetweenTestMatch_1,
		match2: uint8BetweenTestMatch_1b,
		result: []byte{0x8f, 0x42, 0x22}, // last bit off!
		count:  uint8BetweenTestCount_1 - 4,
	}, {
		name:   "l15",
		slice:  uint8TestSlice_1[:15],
		match:  uint8BetweenTestMatch_1,
		match2: uint8BetweenTestMatch_1b,
		result: uint8BetweenTestResult_1[:2],
		count:  uint8BetweenTestCount_1 - 6,
	}, {
		name:   "l7",
		slice:  uint8TestSlice_1[:7],
		match:  uint8BetweenTestMatch_1,
		match2: uint8BetweenTestMatch_1b,
		result: []byte{0x8e},
		count:  uint8BetweenTestCount_1 - 9,
	}, {
		name:   "l0",
		slice:  make([]uint8, 0),
		match:  uint8BetweenTestMatch_1,
		match2: uint8BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint8BetweenTestMatch_1,
		match2: uint8BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  uint8TestSlice_2,
		match:  uint8BetweenTestMatch_2,
		match2: uint8BetweenTestMatch_2b,
		result: uint8BetweenTestResult_2,
		count:  uint8BetweenTestCount_2,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  uint8TestSlice_2[:31],
		match:  uint8BetweenTestMatch_2,
		match2: uint8BetweenTestMatch_2b,
		result: []byte{0x33, 0x33, 0x33, 0x32},
		count:  15,
	},
}

func TestMatchUint8BetweenGeneric(T *testing.T) {
	for _, c := range uint8BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint8BetweenGeneric(c.slice, c.match, c.match2, bits)
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
func TestMatchUint8BetweenAVX2(T *testing.T) {
	for _, c := range uint8BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint8BetweenAVX2(c.slice, c.match, c.match2, bits)
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
// BenchmarkMatchUint8BetweenGeneric/32-8     	30000000      47.3 ns/op	5417.18 MB/s
// BenchmarkMatchUint8BetweenGeneric/128-8    	10000000     159 ns/op	6436.91 MB/s
// BenchmarkMatchUint8BetweenGeneric/1024-8   	 1000000    1201 ns/op	6820.23 MB/s
// BenchmarkMatchUint8BetweenGeneric/4096-8   	  300000    4937 ns/op	6636.40 MB/s
// BenchmarkMatchUint8BetweenGeneric/65536-8  	   20000   79233 ns/op	6616.96 MB/s
// BenchmarkMatchUint8BetweenGeneric/131072-8 	   10000  161598 ns/op	6488.79 MB/s
func BenchmarkMatchUint8BetweenGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8BetweenGeneric(a, 5, 10, bits)
			}
		})
	}
}

// BenchmarkMatchUint8BetweenAVX2/32-8      100000000     14.8 ns/op	17284.10 MB/s
// BenchmarkMatchUint8BetweenAVX2/128-8      30000000     48.9 ns/op	20953.59 MB/s
// BenchmarkMatchUint8BetweenAVX2/1024-8      5000000    370 ns/op	22089.64 MB/s
// BenchmarkMatchUint8BetweenAVX2/4096-8      1000000   1629 ns/op	20114.61 MB/s
// BenchmarkMatchUint8BetweenAVX2/65536-8       50000  29559 ns/op	17736.52 MB/s
// BenchmarkMatchUint8BetweenAVX2/131072-8      20000  58059 ns/op	18060.42 MB/s
/*func BenchmarkMatchUint8BetweenAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8BetweenAVX2(a, 5, 10, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
// BenchmarkMatchUint8BetweenAVX2Scalar/31-8     	50000000     38.2 ns/op	6492.97 MB/s
// BenchmarkMatchUint8BetweenAVX2Scalar/127-8    	20000000     70.6 ns/op	14397.10 MB/s
// BenchmarkMatchUint8BetweenAVX2Scalar/1023-8   	 5000000    389 ns/op	21000.09 MB/s
// BenchmarkMatchUint8BetweenAVX2Scalar/4095-8   	 1000000   1624 ns/op	20161.18 MB/s
// BenchmarkMatchUint8BetweenAVX2Scalar/65535-8  	   50000  28713 ns/op	18258.82 MB/s
// BenchmarkMatchUint8BetweenAVX2Scalar/131071-8 	   20000  58733 ns/op	17853.05 MB/s
func BenchmarkMatchUint8BetweenAVX2Scalar(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randUint8Slice(n.l-1, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8BetweenAVX2(a, 5, 10, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------
// Uint8 Slice
//
func TestUniqueUint8(T *testing.T) {
	a := randUint8Slice(1000, 5)
	b := UniqueUint8Slice(a)
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

func BenchmarkUniqueUint8(B *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint8Slice(n, 5)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				UniqueUint8Slice(a)
			}
		})
	}
}

func TestUint8SliceContains(T *testing.T) {
	// nil slice
	if Uint8Slice(nil).Contains(1) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Uint8Slice([]uint8{}).Contains(1) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Uint8Slice([]uint8{1}).Contains(1) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Uint8Slice([]uint8{1}).Contains(2) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !Uint8Slice([]uint8{1, 3, 5, 7, 11, 13}).Contains(1) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Uint8Slice([]uint8{1, 3, 5, 7, 11, 13}).Contains(5) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Uint8Slice([]uint8{1, 3, 5, 7, 11, 13}).Contains(13) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Uint8Slice([]uint8{1, 3, 5, 7, 11, 13}).Contains(0) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Uint8Slice([]uint8{1, 3, 5, 7, 11, 13}).Contains(2) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Uint8Slice([]uint8{1, 3, 5, 7, 11, 13}).Contains(14) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkUint8SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Uint8Slice(randUint8Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(uint8(rand.Intn(math.MaxUint8 + 1)))
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Uint8Slice(randUint8Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(a[rand.Intn(len(a))])
			}
		})
	}
}

func TestUint8SliceContainsRange(T *testing.T) {
	type VecTestRange struct {
		Name  string
		From  uint8
		To    uint8
		Match bool
	}

	type VecTestcase struct {
		Slice  []uint8
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
			Slice: []uint8{},
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: 0, To: 2, Match: false},
			},
		},
		// 1-element slice
		VecTestcase{
			Slice: []uint8{3},
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
			Slice: []uint8{3},
			Ranges: []VecTestRange{
				VecTestRange{Name: "BCD", From: 3, To: 3, Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		VecTestcase{
			Slice: []uint8{3, 5, 7, 11, 13},
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
			Slice: []uint8{
				6, 13, 16, 17, 18,
				25, 26, 27, 27, 30,
			},
			Ranges: []VecTestRange{
				VecTestRange{Name: "1", From: 27, To: 28, Match: true},
				VecTestRange{Name: "2", From: 28, To: 28, Match: false},
				VecTestRange{Name: "3", From: 28, To: 29, Match: false},
				//				VecTestRange{Name: "4", From: 28, To: 29, Match: false},
				VecTestRange{Name: "5", From: 29, To: 29, Match: false},
				//				VecTestRange{Name: "6", From: 29, To: 29, Match: false},
				VecTestRange{Name: "7", From: 29, To: 30, Match: true},
				VecTestRange{Name: "8", From: 30, To: 30, Match: true},
			},
		},
	}

	for i, v := range tests {
		for _, r := range v.Ranges {
			if want, got := r.Match, Uint8Slice(v.Slice).ContainsRange(r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkUint8SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Uint8Slice(randUint8Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := uint8(rand.Intn(math.MaxUint8+1)), uint8(rand.Intn(math.MaxUint8+1))
				if min > max {
					min, max = max, min
				}
				a.ContainsRange(min, max)
			}
		})
	}
}
