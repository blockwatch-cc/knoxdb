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

const Uint16Size = 2

type Uint16MatchTest struct {
	name   string
	slice  []uint16
	match  uint16 // used for every test
	match2 uint16 // used for between tests
	result []byte
	count  int64
}

var (
	uint16TestSlice_1 = []uint16{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 50000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}
	uint16EqualTestResult_1        = []byte{0x82, 0x42, 0x23, 0x70}
	uint16EqualTestMatch_1  uint16 = 5
	uint16EqualTestCount_1  int64  = 10

	uint16LessTestResult_1        = []byte{0x70, 0x00, 0x00, 0x00}
	uint16LessTestMatch_1  uint16 = 5
	uint16LessTestCount_1  int64  = 3

	uint16LessEqualTestResult_1        = []byte{0xf2, 0x42, 0x23, 0x70}
	uint16LessEqualTestMatch_1  uint16 = 5
	uint16LessEqualTestCount_1  int64  = 13

	uint16GreaterTestResult_1        = []byte{0x0d, 0xbd, 0xdc, 0x8f}
	uint16GreaterTestMatch_1  uint16 = 5
	uint16GreaterTestCount_1  int64  = 19

	uint16GreaterEqualTestResult_1        = []byte{0x8f, 0xff, 0xff, 0xff}
	uint16GreaterEqualTestMatch_1  uint16 = 5
	uint16GreaterEqualTestCount_1  int64  = 29

	uint16BetweenTestResult_1        = []byte{0x8f, 0x42, 0x23, 0x70}
	uint16BetweenTestMatch_1  uint16 = 5
	uint16BetweenTestMatch_1b uint16 = 10
	uint16BetweenTestCount_1  int64  = 13

	// extreme values
	uint16TestSlice_2 = []uint16{
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
		0, math.MaxInt8, math.MaxUint8, math.MaxUint16,
	}
	uint16EqualTestResult_2        = []byte{0x11, 0x11, 0x11, 0x11}
	uint16EqualTestMatch_2  uint16 = math.MaxUint16
	uint16EqualTestCount_2  int64  = 8

	uint16LessTestResult_2        = []byte{0xee, 0xee, 0xee, 0xee}
	uint16LessTestMatch_2  uint16 = math.MaxUint16
	uint16LessTestCount_2  int64  = 24

	uint16LessEqualTestResult_2        = []byte{0xff, 0xff, 0xff, 0xff}
	uint16LessEqualTestMatch_2  uint16 = math.MaxUint16
	uint16LessEqualTestCount_2  int64  = 32

	uint16GreaterTestResult_2        = []byte{0x00, 0x00, 0x00, 0x00}
	uint16GreaterTestMatch_2  uint16 = math.MaxUint16
	uint16GreaterTestCount_2  int64  = 0

	uint16GreaterEqualTestResult_2        = []byte{0x11, 0x11, 0x11, 0x11}
	uint16GreaterEqualTestMatch_2  uint16 = math.MaxUint16
	uint16GreaterEqualTestCount_2  int64  = 8

	uint16BetweenTestResult_2        = []byte{0x33, 0x33, 0x33, 0x33}
	uint16BetweenTestMatch_2  uint16 = math.MaxUint8
	uint16BetweenTestMatch_2b uint16 = math.MaxUint16
	uint16BetweenTestCount_2  int64  = 16
)

func randUint16Slice(n, u int) []uint16 {
	s := make([]uint16, n*u)
	for i := 0; i < n; i++ {
		s[i] = uint16(rand.Intn(math.MaxUint16 + 1))
	}
	for i := 0; i < u; i++ {
		s = append(s, s[:n]...)
	}
	return s
}

// -----------------------------------------------------------------------------
// Equal Testcases
//
var uint16EqualCases = []Uint16MatchTest{
	Uint16MatchTest{
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
		slice: []uint16{
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
		slice:  uint16TestSlice_1,
		match:  uint16EqualTestMatch_1,
		result: uint16EqualTestResult_1,
		count:  uint16EqualTestCount_1,
	}, {
		name:   "l64",
		slice:  append(uint16TestSlice_1, uint16TestSlice_1...),
		match:  uint16EqualTestMatch_1,
		result: append(uint16EqualTestResult_1, uint16EqualTestResult_1...),
		count:  uint16EqualTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  uint16TestSlice_1[:31],
		match:  uint16EqualTestMatch_1,
		result: uint16EqualTestResult_1,
		count:  uint16EqualTestCount_1,
	}, {
		name:   "l23",
		slice:  uint16TestSlice_1[:23],
		match:  uint16EqualTestMatch_1,
		result: []byte{0x82, 0x42, 0x22}, // last bit off!
		count:  uint16EqualTestCount_1 - 4,
	}, {
		name:   "l15",
		slice:  uint16TestSlice_1[:15],
		match:  uint16EqualTestMatch_1,
		result: uint16EqualTestResult_1[:2],
		count:  uint16EqualTestCount_1 - 6,
	}, {
		name:   "l7",
		slice:  uint16TestSlice_1[:7],
		match:  uint16EqualTestMatch_1,
		result: uint16EqualTestResult_1[:1],
		count:  uint16EqualTestCount_1 - 8,
	}, {
		name:   "l0",
		slice:  make([]uint16, 0),
		match:  uint16EqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint16EqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  uint16TestSlice_2,
		match:  uint16EqualTestMatch_2,
		result: uint16EqualTestResult_2,
		count:  uint16EqualTestCount_2,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  uint16TestSlice_2[:31],
		match:  uint16EqualTestMatch_2,
		result: []byte{0x11, 0x11, 0x11, 0x10},
		count:  7,
	},
}

func TestMatchUint16EqualGeneric(T *testing.T) {
	for _, c := range uint16EqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint16EqualGeneric(c.slice, c.match, bits)
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
func TestMatchUint16EqualAVX2(T *testing.T) {
	for _, c := range uint16EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint16EqualAVX2(c.slice, c.match, bits)
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
func BenchmarkMatchUint16EqualGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16EqualGeneric(a, math.MaxUint16/2, bits)
			}
		})
	}
}

/*
func BenchmarkMatchUint16EqualAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16EqualAVX2(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchUint16EqualAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16EqualAVX2(a, math.MaxUint16/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Less Testcases
//
var uint16LessCases = []Uint16MatchTest{
	Uint16MatchTest{
		name: "vec1",
		slice: []uint16{
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
		slice:  uint16TestSlice_1,
		match:  uint16LessTestMatch_1,
		result: uint16LessTestResult_1,
		count:  uint16LessTestCount_1,
	}, {
		name:   "l64",
		slice:  append(uint16TestSlice_1, uint16TestSlice_1...),
		match:  uint16LessTestMatch_1,
		result: append(uint16LessTestResult_1, uint16LessTestResult_1...),
		count:  uint16LessTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  uint16TestSlice_1[:31],
		match:  uint16LessTestMatch_1,
		result: uint16LessTestResult_1,
		count:  uint16LessTestCount_1,
	}, {
		name:   "l23",
		slice:  uint16TestSlice_1[:23],
		match:  uint16LessTestMatch_1,
		result: uint16LessTestResult_1[:3],
		count:  uint16LessTestCount_1,
	}, {
		name:   "l15",
		slice:  uint16TestSlice_1[:15],
		match:  uint16LessTestMatch_1,
		result: uint16LessTestResult_1[:2],
		count:  uint16LessTestCount_1,
	}, {
		name:   "l7",
		slice:  uint16TestSlice_1[:7],
		match:  uint16LessTestMatch_1,
		result: uint16LessTestResult_1[:1],
		count:  uint16LessTestCount_1,
	}, {
		name:   "l0",
		slice:  make([]uint16, 0),
		match:  uint16LessTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint16LessTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  uint16TestSlice_2,
		match:  uint16LessTestMatch_2,
		result: uint16LessTestResult_2,
		count:  uint16LessTestCount_2,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  uint16TestSlice_2[:31],
		match:  uint16LessTestMatch_2,
		result: uint16LessTestResult_2,
		count:  uint16LessTestCount_2,
	},
}

func TestMatchUint16LessGeneric(T *testing.T) {
	for _, c := range uint16LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint16LessThanGeneric(c.slice, c.match, bits)
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
func TestMatchUint16LessAVX2(T *testing.T) {
	for _, c := range uint16LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint16LessThanAVX2(c.slice, c.match, bits)
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
func BenchmarkMatchUint16LessGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16LessThanGeneric(a, math.MaxUint16/2, bits)
			}
		})
	}
}

/*
func BenchmarkMatchUint16LessAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16LessThanAVX2(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchUint16LessAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16LessThanAVX2(a, math.MaxUint16/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Less Equal Testcases
//
var uint16LessEqualCases = []Uint16MatchTest{
	Uint16MatchTest{
		name: "vec1",
		slice: []uint16{
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
		slice:  uint16TestSlice_1,
		match:  uint16LessEqualTestMatch_1,
		result: uint16LessEqualTestResult_1,
		count:  uint16LessEqualTestCount_1,
	}, {
		name:   "l64",
		slice:  append(uint16TestSlice_1, uint16TestSlice_1...),
		match:  uint16LessEqualTestMatch_1,
		result: append(uint16LessEqualTestResult_1, uint16LessEqualTestResult_1...),
		count:  uint16LessEqualTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  uint16TestSlice_1[:31],
		match:  uint16LessEqualTestMatch_1,
		result: uint16LessEqualTestResult_1,
		count:  uint16LessEqualTestCount_1,
	}, {
		name:   "l23",
		slice:  uint16TestSlice_1[:23],
		match:  uint16LessEqualTestMatch_1,
		result: []byte{0xf2, 0x42, 0x22},
		count:  uint16LessEqualTestCount_1 - 4,
	}, {
		name:   "l15",
		slice:  uint16TestSlice_1[:15],
		match:  uint16LessEqualTestMatch_1,
		result: uint16LessEqualTestResult_1[:2],
		count:  uint16LessEqualTestCount_1 - 6,
	}, {
		name:   "l7",
		slice:  uint16TestSlice_1[:7],
		match:  uint16LessEqualTestMatch_1,
		result: uint16LessEqualTestResult_1[:1],
		count:  uint16LessEqualTestCount_1 - 8,
	}, {
		name:   "l0",
		slice:  make([]uint16, 0),
		match:  uint16LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint16LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  uint16TestSlice_2,
		match:  uint16LessEqualTestMatch_2,
		result: uint16LessEqualTestResult_2,
		count:  uint16LessEqualTestCount_2,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  uint16TestSlice_2[:31],
		match:  uint16LessEqualTestMatch_2,
		result: []byte{0xff, 0xff, 0xff, 0xfe}, // last off
		count:  uint16LessEqualTestCount_2 - 1,
	},
}

func TestMatchUint16LessEqualGeneric(T *testing.T) {
	for _, c := range uint16LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint16LessThanEqualGeneric(c.slice, c.match, bits)
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
func TestMatchUint16LessEqualAVX2(T *testing.T) {
	for _, c := range uint16LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint16LessThanEqualAVX2(c.slice, c.match, bits)
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
func BenchmarkMatchUint16LessEqualGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16LessThanEqualGeneric(a, math.MaxUint16/2, bits)
			}
		})
	}
}

/*
func BenchmarkMatchUint16LessEqualAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16LessThanEqualAVX2(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchUint16LessEqualAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16LessThanEqualAVX2(a, math.MaxUint16/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Greater Testcases
//
var uint16GreaterCases = []Uint16MatchTest{
	Uint16MatchTest{
		name: "vec1",
		slice: []uint16{
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
		slice:  uint16TestSlice_1,
		match:  uint16GreaterTestMatch_1,
		result: uint16GreaterTestResult_1,
		count:  uint16GreaterTestCount_1,
	}, {
		name:   "l64",
		slice:  append(uint16TestSlice_1, uint16TestSlice_1...),
		match:  uint16GreaterTestMatch_1,
		result: append(uint16GreaterTestResult_1, uint16GreaterTestResult_1...),
		count:  uint16GreaterTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  uint16TestSlice_1[:31],
		match:  uint16GreaterTestMatch_1,
		result: []byte{0x0d, 0xbd, 0xdc, 0x8e},
		count:  uint16GreaterTestCount_1 - 1,
	}, {
		name:   "l23",
		slice:  uint16TestSlice_1[:23],
		match:  uint16GreaterTestMatch_1,
		result: uint16GreaterTestResult_1[:3],
		count:  uint16GreaterTestCount_1 - 5,
	}, {
		name:   "l15",
		slice:  uint16TestSlice_1[:15],
		match:  uint16GreaterTestMatch_1,
		result: []byte{0x0d, 0xbc},
		count:  uint16GreaterTestCount_1 - 11,
	}, {
		name:   "l7",
		slice:  uint16TestSlice_1[:7],
		match:  uint16GreaterTestMatch_1,
		result: []byte{0x0c},
		count:  2,
	}, {
		name:   "l0",
		slice:  make([]uint16, 0),
		match:  uint16GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint16GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  uint16TestSlice_2,
		match:  uint16GreaterTestMatch_2,
		result: uint16GreaterTestResult_2,
		count:  uint16GreaterTestCount_2,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  uint16TestSlice_2[:31],
		match:  uint16GreaterTestMatch_2,
		result: uint16GreaterTestResult_2, // still zeros
		count:  uint16GreaterTestCount_2,
	},
}

func TestMatchUint16GreaterGeneric(T *testing.T) {
	for _, c := range uint16GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint16GreaterThanGeneric(c.slice, c.match, bits)
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
func TestMatchUint16GreaterAVX2(T *testing.T) {
	for _, c := range uint16GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint16GreaterThanAVX2(c.slice, c.match, bits)
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
func BenchmarkMatchUint16GreaterGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16GreaterThanGeneric(a, math.MaxUint16/2, bits)
			}
		})
	}
}

/*
func BenchmarkMatchUint16GreaterAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16GreaterThanAVX2(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchUint16GreaterAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16GreaterThanAVX2(a, math.MaxUint16/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Greater Equal Testcases
//
var uint16GreaterEqualCases = []Uint16MatchTest{
	Uint16MatchTest{
		name: "vec1",
		slice: []uint16{
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
		slice:  uint16TestSlice_1,
		match:  uint16GreaterEqualTestMatch_1,
		result: uint16GreaterEqualTestResult_1,
		count:  uint16GreaterEqualTestCount_1,
	}, {
		name:   "l64",
		slice:  append(uint16TestSlice_1, uint16TestSlice_1...),
		match:  uint16GreaterEqualTestMatch_1,
		result: append(uint16GreaterEqualTestResult_1, uint16GreaterEqualTestResult_1...),
		count:  uint16GreaterEqualTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  uint16TestSlice_1[:31],
		match:  uint16GreaterEqualTestMatch_1,
		result: []byte{0x8f, 0xff, 0xff, 0xfe},
		count:  uint16GreaterEqualTestCount_1 - 1,
	}, {
		name:   "l23",
		slice:  uint16TestSlice_1[:23],
		match:  uint16GreaterEqualTestMatch_1,
		result: []byte{0x8f, 0xff, 0xfe},
		count:  uint16GreaterEqualTestCount_1 - 9,
	}, {
		name:   "l15",
		slice:  uint16TestSlice_1[:15],
		match:  uint16GreaterEqualTestMatch_1,
		result: []byte{0x8f, 0xfe},
		count:  uint16GreaterEqualTestCount_1 - 17,
	}, {
		name:   "l7",
		slice:  uint16TestSlice_1[:7],
		match:  uint16GreaterEqualTestMatch_1,
		result: []byte{0x8e},
		count:  4,
	}, {
		name:   "l0",
		slice:  make([]uint16, 0),
		match:  uint16GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint16GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  uint16TestSlice_2,
		match:  uint16GreaterEqualTestMatch_2,
		result: uint16GreaterEqualTestResult_2,
		count:  uint16GreaterEqualTestCount_2,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  uint16TestSlice_2[:31],
		match:  uint16GreaterEqualTestMatch_2,
		result: []byte{0x11, 0x11, 0x11, 0x10}, // last off
		count:  uint16GreaterEqualTestCount_2 - 1,
	},
}

func TestMatchUint16GreaterEqualGeneric(T *testing.T) {
	for _, c := range uint16GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint16GreaterThanEqualGeneric(c.slice, c.match, bits)
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
func TestMatchUint16GreaterEqualAVX2(T *testing.T) {
	for _, c := range uint16GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint16GreaterThanEqualAVX2(c.slice, c.match, bits)
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
func BenchmarkMatchUint16GreaterEqualGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16GreaterThanEqualGeneric(a, math.MaxUint16/2, bits)
			}
		})
	}
}

/*
func BenchmarkMatchUint16GreaterEqualAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16GreaterThanEqualAVX2(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchUint16GreaterEqualAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16GreaterThanEqualAVX2(a, math.MaxUint16/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Between Testcases
//
var uint16BetweenCases = []Uint16MatchTest{
	Uint16MatchTest{
		name: "vec1",
		slice: []uint16{
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
		slice:  uint16TestSlice_1,
		match:  uint16BetweenTestMatch_1,
		match2: uint16BetweenTestMatch_1b,
		result: uint16BetweenTestResult_1,
		count:  uint16BetweenTestCount_1,
	}, {
		name:   "l64",
		slice:  append(uint16TestSlice_1, uint16TestSlice_1...),
		match:  uint16BetweenTestMatch_1,
		match2: uint16BetweenTestMatch_1b,
		result: append(uint16BetweenTestResult_1, uint16BetweenTestResult_1...),
		count:  uint16BetweenTestCount_1 * 2,
	}, {
		name:   "l31",
		slice:  uint16TestSlice_1[:31],
		match:  uint16BetweenTestMatch_1,
		match2: uint16BetweenTestMatch_1b,
		result: uint16BetweenTestResult_1,
		count:  uint16BetweenTestCount_1,
	}, {
		name:   "l23",
		slice:  uint16TestSlice_1[:23],
		match:  uint16BetweenTestMatch_1,
		match2: uint16BetweenTestMatch_1b,
		result: []byte{0x8f, 0x42, 0x22}, // last bit off!
		count:  uint16BetweenTestCount_1 - 4,
	}, {
		name:   "l15",
		slice:  uint16TestSlice_1[:15],
		match:  uint16BetweenTestMatch_1,
		match2: uint16BetweenTestMatch_1b,
		result: uint16BetweenTestResult_1[:2],
		count:  uint16BetweenTestCount_1 - 6,
	}, {
		name:   "l7",
		slice:  uint16TestSlice_1[:7],
		match:  uint16BetweenTestMatch_1,
		match2: uint16BetweenTestMatch_1b,
		result: []byte{0x8e},
		count:  uint16BetweenTestCount_1 - 9,
	}, {
		name:   "l0",
		slice:  make([]uint16, 0),
		match:  uint16BetweenTestMatch_1,
		match2: uint16BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint16BetweenTestMatch_1,
		match2: uint16BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	}, {
		// with extreme values
		name:   "ext32",
		slice:  uint16TestSlice_2,
		match:  uint16BetweenTestMatch_2,
		match2: uint16BetweenTestMatch_2b,
		result: uint16BetweenTestResult_2,
		count:  uint16BetweenTestCount_2,
	}, {
		// with extreme values, test scalar algorithm
		name:   "ext31",
		slice:  uint16TestSlice_2[:31],
		match:  uint16BetweenTestMatch_2,
		match2: uint16BetweenTestMatch_2b,
		result: []byte{0x33, 0x33, 0x33, 0x32},
		count:  15,
	},
}

func TestMatchUint16BetweenGeneric(T *testing.T) {
	for _, c := range uint16BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint16BetweenGeneric(c.slice, c.match, c.match2, bits)
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
func TestMatchUint16BetweenAVX2(T *testing.T) {
	for _, c := range uint16BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint16BetweenAVX2(c.slice, c.match, c.match2, bits)
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
// BenchmarkMatchUint16BetweenGeneric/32-8     	30000000      47.3 ns/op	5417.18 MB/s
// BenchmarkMatchUint16BetweenGeneric/128-8    	10000000     159 ns/op	6436.91 MB/s
// BenchmarkMatchUint16BetweenGeneric/1024-8   	 1000000    1201 ns/op	6820.23 MB/s
// BenchmarkMatchUint16BetweenGeneric/4096-8   	  300000    4937 ns/op	6636.40 MB/s
// BenchmarkMatchUint16BetweenGeneric/65536-8  	   20000   79233 ns/op	6616.96 MB/s
// BenchmarkMatchUint16BetweenGeneric/131072-8 	   10000  161598 ns/op	6488.79 MB/s
func BenchmarkMatchUint16BetweenGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16BetweenGeneric(a, 5, 10, bits)
			}
		})
	}
}

// BenchmarkMatchUint16BetweenAVX2/32-8      100000000     14.8 ns/op	17284.10 MB/s
// BenchmarkMatchUint16BetweenAVX2/128-8      30000000     48.9 ns/op	20953.59 MB/s
// BenchmarkMatchUint16BetweenAVX2/1024-8      5000000    370 ns/op	22089.64 MB/s
// BenchmarkMatchUint16BetweenAVX2/4096-8      1000000   1629 ns/op	20114.61 MB/s
// BenchmarkMatchUint16BetweenAVX2/65536-8       50000  29559 ns/op	17736.52 MB/s
// BenchmarkMatchUint16BetweenAVX2/131072-8      20000  58059 ns/op	18060.42 MB/s
/*func BenchmarkMatchUint16BetweenAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16BetweenAVX2(a, 5, 10, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
// BenchmarkMatchUint16BetweenAVX2Scalar/31-8     	50000000     38.2 ns/op	6492.97 MB/s
// BenchmarkMatchUint16BetweenAVX2Scalar/127-8    	20000000     70.6 ns/op	14397.10 MB/s
// BenchmarkMatchUint16BetweenAVX2Scalar/1023-8   	 5000000    389 ns/op	21000.09 MB/s
// BenchmarkMatchUint16BetweenAVX2Scalar/4095-8   	 1000000   1624 ns/op	20161.18 MB/s
// BenchmarkMatchUint16BetweenAVX2Scalar/65535-8  	   50000  28713 ns/op	18258.82 MB/s
// BenchmarkMatchUint16BetweenAVX2Scalar/131071-8 	   20000  58733 ns/op	17853.05 MB/s
func BenchmarkMatchUint16BetweenAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16BetweenAVX2(a, 5, 10, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------
// Uint16 Slice
//
func TestUniqueUint16(T *testing.T) {
	a := randUint16Slice(1000, 5)
	b := UniqueUint16Slice(a)
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

func BenchmarkUniqueUint16(B *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint16Slice(n, 5)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				UniqueUint16Slice(a)
			}
		})
	}
}

func TestUint16SliceContains(T *testing.T) {
	// nil slice
	if Uint16Slice(nil).Contains(1) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Uint16Slice([]uint16{}).Contains(1) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Uint16Slice([]uint16{1}).Contains(1) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Uint16Slice([]uint16{1}).Contains(2) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !Uint16Slice([]uint16{1, 3, 5, 7, 11, 13}).Contains(1) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Uint16Slice([]uint16{1, 3, 5, 7, 11, 13}).Contains(5) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Uint16Slice([]uint16{1, 3, 5, 7, 11, 13}).Contains(13) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Uint16Slice([]uint16{1, 3, 5, 7, 11, 13}).Contains(0) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Uint16Slice([]uint16{1, 3, 5, 7, 11, 13}).Contains(2) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Uint16Slice([]uint16{1, 3, 5, 7, 11, 13}).Contains(14) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkUint16SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Uint16Slice(randUint16Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(uint16(rand.Intn(math.MaxUint16 + 1)))
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Uint16Slice(randUint16Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(a[rand.Intn(len(a))])
			}
		})
	}
}

func TestUint16SliceContainsRange(T *testing.T) {
	type VecTestRange struct {
		Name  string
		From  uint16
		To    uint16
		Match bool
	}

	type VecTestcase struct {
		Slice  []uint16
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
			Slice: []uint16{},
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: 0, To: 2, Match: false},
			},
		},
		// 1-element slice
		VecTestcase{
			Slice: []uint16{3},
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
			Slice: []uint16{3},
			Ranges: []VecTestRange{
				VecTestRange{Name: "BCD", From: 3, To: 3, Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		VecTestcase{
			Slice: []uint16{3, 5, 7, 11, 13},
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
			Slice: []uint16{
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
			if want, got := r.Match, Uint16Slice(v.Slice).ContainsRange(r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkUint16SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Uint16Slice(randUint16Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := uint16(rand.Intn(math.MaxUint16+1)), uint16(rand.Intn(math.MaxUint16+1))
				if min > max {
					min, max = max, min
				}
				a.ContainsRange(min, max)
			}
		})
	}
}
