// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//

package vec

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
    "math/bits"
	"testing"
)

const Uint64Size = 8

type Uint64MatchTest struct {
	name   string
	slice  []uint64
	match  uint64 // used for every test
	match2 uint64 // used for between tests
	result []byte
	count  int64
}

var (
    uint64TestSlice_0 = []uint64{
			0, 5, 3, 5, // Y1
			7, 5, 5, 9, // Y2
			3, 5, 5, 5, // Y3
			5, 0, 113, 12, // Y4

			4, 2, 3, 5, // Y5
			7, 3, 5, 9, // Y6
			3, 13, 5, 5, // Y7
			42, 5, 113, 12, // Y8
		}
    uint64EqualTestMatch_0 uint64 = 5
	uint64EqualTestResult_0 =  []byte{0x56, 0x78, 0x12, 0x34}

    uint64LessTestMatch_0 uint64 = 5
	uint64LessTestResult_0 = []byte{0xa0, 0x84, 0xe4, 0x80}

	uint64LessEqualTestMatch_0 uint64 = 5
	uint64LessEqualTestResult_0 = []byte{0xf6, 0xfc, 0xf6, 0xb4}

	uint64GreaterTestMatch_0 uint64 = 5
	uint64GreaterTestResult_0 = []byte{0x09, 0x03, 0x09, 0x4b}

	uint64GreaterEqualTestMatch_0 uint64 = 5
	uint64GreaterEqualTestResult_0 = []byte{0x5f, 0x7b, 0x1b, 0x7f}

	uint64BetweenTestMatch_0 uint64 = 5
	uint64BetweenTestMatch_0b uint64 = 10
	uint64BetweenTestResult_0 = []byte{0x5f, 0x78, 0x1b, 0x34}

	uint64TestSlice_1 = []uint64{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 500000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}
	uint64EqualTestResult_1        = []byte{0x82, 0x42, 0x23, 0x70}
	uint64EqualTestMatch_1  uint64 = 5

	uint64LessTestResult_1        = []byte{0x70, 0x00, 0x00, 0x00}
	uint64LessTestMatch_1  uint64 = 5

	uint64LessEqualTestResult_1        = []byte{0xf2, 0x42, 0x23, 0x70}
	uint64LessEqualTestMatch_1  uint64 = 5

	uint64GreaterTestResult_1        = []byte{0x0d, 0xbd, 0xdc, 0x8f}
	uint64GreaterTestMatch_1  uint64 = 5

	uint64GreaterEqualTestResult_1        = []byte{0x8f, 0xff, 0xff, 0xff}
	uint64GreaterEqualTestMatch_1  uint64 = 5

	uint64BetweenTestResult_1        = []byte{0x8f, 0x42, 0x23, 0x70}
	uint64BetweenTestMatch_1  uint64 = 5
	uint64BetweenTestMatch_1b uint64 = 10

	// extreme values
	uint64TestSlice_2 = []uint64{
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
		math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64,
	}
	uint64EqualTestResult_2        = []byte{0x11, 0x11, 0x11, 0x11}
	uint64EqualTestMatch_2  uint64 = math.MaxUint64

	uint64LessTestResult_2        = []byte{0xee, 0xee, 0xee, 0xee}
	uint64LessTestMatch_2  uint64 = math.MaxUint64

	uint64LessEqualTestResult_2        = []byte{0xff, 0xff, 0xff, 0xff}
	uint64LessEqualTestMatch_2  uint64 = math.MaxUint64

	uint64GreaterTestResult_2        = []byte{0x00, 0x00, 0x00, 0x00}
	uint64GreaterTestMatch_2  uint64 = math.MaxUint64

	uint64GreaterEqualTestResult_2        = []byte{0x11, 0x11, 0x11, 0x11}
	uint64GreaterEqualTestMatch_2  uint64 = math.MaxUint64

	uint64BetweenTestResult_2        = []byte{0x33, 0x33, 0x33, 0x33}
	uint64BetweenTestMatch_2  uint64 = math.MaxUint32
	uint64BetweenTestMatch_2b uint64 = math.MaxUint64
)

func randUint64Slice(n, u int) []uint64 {
	s := make([]uint64, n*u)
	for i := 0; i < n; i++ {
		s[i] = rand.Uint64()
	}
	for i := 0; i < u; i++ {
		s = append(s, s[:n]...)
	}
	return s
}

// creates an uint64 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - match, match2: are only copied to the resulting test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateUint64TestCase(name string, slice []uint64, match, match2 uint64, result []byte, length int) Uint64MatchTest {
	if len(slice)%8 != 0 {
		panic("CreateUint64TestCase: length of slice has to be a multiple of 8")
	}
	if len(result) != bitFieldLen(len(slice)) {
		panic("CreateUint64TestCase: length of slice and length of result does not match")
	}

	// create new slice by concat of given slice
	new_slice := make([]uint64, length)
	for i, _ := range new_slice {
		new_slice[i] = slice[i%len(slice)]
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
    return Uint64MatchTest{
		name:   name,
		slice:  new_slice,
		match:  match,
		match2: match2,
		result: new_result,
		count:  int64(cnt),
	}
}

// -----------------------------------------------------------------------------
// Equal Testcases
//

var uint64EqualCases = []Uint64MatchTest{
	{
		name:   "l0",
		slice:  make([]uint64, 0),
		match:  uint64EqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint64EqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateUint64TestCase("vec1", uint64TestSlice_0, uint64EqualTestMatch_0, 0, uint64EqualTestResult_0, 32),
	CreateUint64TestCase("l32", uint64TestSlice_1, uint64EqualTestMatch_1, 0, uint64EqualTestResult_1, 32),
	CreateUint64TestCase("l64", append(uint64TestSlice_1, uint64TestSlice_0...), uint64EqualTestMatch_1, 0,
		append(uint64EqualTestResult_1, uint64EqualTestResult_0...), 64),
	CreateUint64TestCase("l31", uint64TestSlice_1, uint64EqualTestMatch_1, 0, uint64EqualTestResult_1, 31),
	CreateUint64TestCase("l23", uint64TestSlice_1, uint64EqualTestMatch_1, 0, uint64EqualTestResult_1, 23),
	CreateUint64TestCase("l15", uint64TestSlice_1, uint64EqualTestMatch_1, 0, uint64EqualTestResult_1, 15),
	CreateUint64TestCase("l7", uint64TestSlice_1, uint64EqualTestMatch_1, 0, uint64EqualTestResult_1, 7),
	// with extreme values
	CreateUint64TestCase("ext32", uint64TestSlice_2, uint64EqualTestMatch_2, 0, uint64EqualTestResult_2, 32),
	CreateUint64TestCase("ext31", uint64TestSlice_2, uint64EqualTestMatch_2, 0, uint64EqualTestResult_2, 31),
}

func TestMatchUint64EqualGeneric(T *testing.T) {
	for _, c := range uint64EqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint64EqualGeneric(c.slice, c.match, bits)
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

func TestMatchUint64EqualAVX2(T *testing.T) {
	for _, c := range uint64EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint64EqualAVX2(c.slice, c.match, bits)
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
func BenchmarkMatchUint64EqualGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64EqualGeneric(a, math.MaxUint64/2, bits)
			}
		})
	}
}

func BenchmarkMatchUint64EqualAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64EqualAVX2(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchUint64EqualAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64EqualAVX2(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
//

var uint64LessCases = []Uint64MatchTest{
	{
		name:   "l0",
		slice:  make([]uint64, 0),
		match:  uint64LessTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint64LessTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateUint64TestCase("vec1", uint64TestSlice_0, uint64LessTestMatch_0, 0, uint64LessTestResult_0, 32),
	CreateUint64TestCase("l32", uint64TestSlice_1, uint64LessTestMatch_1, 0, uint64LessTestResult_1, 32),
	CreateUint64TestCase("l64", append(uint64TestSlice_1, uint64TestSlice_0...), uint64LessTestMatch_1, 0,
		append(uint64LessTestResult_1, uint64LessTestResult_0...), 64),
	CreateUint64TestCase("l31", uint64TestSlice_1, uint64LessTestMatch_1, 0, uint64LessTestResult_1, 31),
	CreateUint64TestCase("l23", uint64TestSlice_1, uint64LessTestMatch_1, 0, uint64LessTestResult_1, 23),
	CreateUint64TestCase("l15", uint64TestSlice_1, uint64LessTestMatch_1, 0, uint64LessTestResult_1, 15),
	CreateUint64TestCase("l7", uint64TestSlice_1, uint64LessTestMatch_1, 0, uint64LessTestResult_1, 7),
	// with extreme values
	CreateUint64TestCase("ext32", uint64TestSlice_2, uint64LessTestMatch_2, 0, uint64LessTestResult_2, 32),
	CreateUint64TestCase("ext31", uint64TestSlice_2, uint64LessTestMatch_2, 0, uint64LessTestResult_2, 31),
}

func TestMatchUint64LessGeneric(T *testing.T) {
	for _, c := range uint64LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint64LessThanGeneric(c.slice, c.match, bits)
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

func TestMatchUint64LessAVX2(T *testing.T) {
	for _, c := range uint64LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint64LessThanAVX2(c.slice, c.match, bits)
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
func BenchmarkMatchUint64LessGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64LessThanGeneric(a, math.MaxUint64/2, bits)
			}
		})
	}
}

func BenchmarkMatchUint64LessAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64LessThanAVX2(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchUint64LessAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64LessThanAVX2(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//

var uint64LessEqualCases = []Uint64MatchTest{
	{
		name:   "l0",
		slice:  make([]uint64, 0),
		match:  uint64LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint64LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateUint64TestCase("vec1", uint64TestSlice_0, uint64LessEqualTestMatch_0, 0, uint64LessEqualTestResult_0, 32),
	CreateUint64TestCase("l32", uint64TestSlice_1, uint64LessEqualTestMatch_1, 0, uint64LessEqualTestResult_1, 32),
	CreateUint64TestCase("l64", append(uint64TestSlice_1, uint64TestSlice_0...), uint64LessEqualTestMatch_1, 0,
		append(uint64LessEqualTestResult_1, uint64LessEqualTestResult_0...), 64),
	CreateUint64TestCase("l31", uint64TestSlice_1, uint64LessEqualTestMatch_1, 0, uint64LessEqualTestResult_1, 31),
	CreateUint64TestCase("l23", uint64TestSlice_1, uint64LessEqualTestMatch_1, 0, uint64LessEqualTestResult_1, 23),
	CreateUint64TestCase("l15", uint64TestSlice_1, uint64LessEqualTestMatch_1, 0, uint64LessEqualTestResult_1, 15),
	CreateUint64TestCase("l7", uint64TestSlice_1, uint64LessEqualTestMatch_1, 0, uint64LessEqualTestResult_1, 7),
	// with extreme values
	CreateUint64TestCase("ext32", uint64TestSlice_2, uint64LessEqualTestMatch_2, 0, uint64LessEqualTestResult_2, 32),
	CreateUint64TestCase("ext31", uint64TestSlice_2, uint64LessEqualTestMatch_2, 0, uint64LessEqualTestResult_2, 31),
}

func TestMatchUint64LessEqualGeneric(T *testing.T) {
	for _, c := range uint64LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint64LessThanEqualGeneric(c.slice, c.match, bits)
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

func TestMatchUint64LessEqualAVX2(T *testing.T) {
	for _, c := range uint64LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint64LessThanEqualAVX2(c.slice, c.match, bits)
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
func BenchmarkMatchUint64LessEqualGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64LessThanEqualGeneric(a, math.MaxUint64/2, bits)
			}
		})
	}
}

func BenchmarkMatchUint64LessEqualAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64LessThanEqualAVX2(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchUint64LessEqualAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64LessThanEqualAVX2(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
//

var uint64GreaterCases = []Uint64MatchTest{
	{
		name:   "l0",
		slice:  make([]uint64, 0),
		match:  uint64GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint64GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateUint64TestCase("vec1", uint64TestSlice_0, uint64GreaterTestMatch_0, 0, uint64GreaterTestResult_0, 32),
	CreateUint64TestCase("l32", uint64TestSlice_1, uint64GreaterTestMatch_1, 0, uint64GreaterTestResult_1, 32),
	CreateUint64TestCase("l64", append(uint64TestSlice_1, uint64TestSlice_0...), uint64GreaterTestMatch_1, 0,
		append(uint64GreaterTestResult_1, uint64GreaterTestResult_0...), 64),
	CreateUint64TestCase("l31", uint64TestSlice_1, uint64GreaterTestMatch_1, 0, uint64GreaterTestResult_1, 31),
	CreateUint64TestCase("l23", uint64TestSlice_1, uint64GreaterTestMatch_1, 0, uint64GreaterTestResult_1, 23),
	CreateUint64TestCase("l15", uint64TestSlice_1, uint64GreaterTestMatch_1, 0, uint64GreaterTestResult_1, 15),
	CreateUint64TestCase("l7", uint64TestSlice_1, uint64GreaterTestMatch_1, 0, uint64GreaterTestResult_1, 7),
	// with extreme values
	CreateUint64TestCase("ext32", uint64TestSlice_2, uint64GreaterTestMatch_2, 0, uint64GreaterTestResult_2, 32),
	CreateUint64TestCase("ext31", uint64TestSlice_2, uint64GreaterTestMatch_2, 0, uint64GreaterTestResult_2, 31),
}

func TestMatchUint64GreaterGeneric(T *testing.T) {
	for _, c := range uint64GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint64GreaterThanGeneric(c.slice, c.match, bits)
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

func TestMatchUint64GreaterAVX2(T *testing.T) {
	for _, c := range uint64GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint64GreaterThanAVX2(c.slice, c.match, bits)
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
func BenchmarkMatchUint64GreaterGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64GreaterThanGeneric(a, math.MaxUint64/2, bits)
			}
		})
	}
}

func BenchmarkMatchUint64GreaterAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64GreaterThanAVX2(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchUint64GreaterAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64GreaterThanAVX2(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//

var uint64GreaterEqualCases = []Uint64MatchTest{
	{
		name:   "l0",
		slice:  make([]uint64, 0),
		match:  uint64GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint64GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateUint64TestCase("vec1", uint64TestSlice_0, uint64GreaterEqualTestMatch_0, 0, uint64GreaterEqualTestResult_0, 32),
	CreateUint64TestCase("l32", uint64TestSlice_1, uint64GreaterEqualTestMatch_1, 0, uint64GreaterEqualTestResult_1, 32),
	CreateUint64TestCase("l64", append(uint64TestSlice_1, uint64TestSlice_0...), uint64GreaterEqualTestMatch_1, 0,
		append(uint64GreaterEqualTestResult_1, uint64GreaterEqualTestResult_0...), 64),
	CreateUint64TestCase("l31", uint64TestSlice_1, uint64GreaterEqualTestMatch_1, 0, uint64GreaterEqualTestResult_1, 31),
	CreateUint64TestCase("l23", uint64TestSlice_1, uint64GreaterEqualTestMatch_1, 0, uint64GreaterEqualTestResult_1, 23),
	CreateUint64TestCase("l15", uint64TestSlice_1, uint64GreaterEqualTestMatch_1, 0, uint64GreaterEqualTestResult_1, 15),
	CreateUint64TestCase("l7", uint64TestSlice_1, uint64GreaterEqualTestMatch_1, 0, uint64GreaterEqualTestResult_1, 7),
	// with extreme values
	CreateUint64TestCase("ext32", uint64TestSlice_2, uint64GreaterEqualTestMatch_2, 0, uint64GreaterEqualTestResult_2, 32),
	CreateUint64TestCase("ext31", uint64TestSlice_2, uint64GreaterEqualTestMatch_2, 0, uint64GreaterEqualTestResult_2, 31),
}

func TestMatchUint64GreaterEqualGeneric(T *testing.T) {
	for _, c := range uint64GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint64GreaterThanEqualGeneric(c.slice, c.match, bits)
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

func TestMatchUint64GreaterEqualAVX2(T *testing.T) {
	for _, c := range uint64GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint64GreaterThanEqualAVX2(c.slice, c.match, bits)
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
func BenchmarkMatchUint64GreaterEqualGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64GreaterThanEqualGeneric(a, math.MaxUint64/2, bits)
			}
		})
	}
}

func BenchmarkMatchUint64GreaterEqualAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64GreaterThanEqualAVX2(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
func BenchmarkMatchUint64GreaterEqualAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64GreaterThanEqualAVX2(a, math.MaxUint64/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
//

var uint64BetweenCases = []Uint64MatchTest{
	{
		name:   "l0",
		slice:  make([]uint64, 0),
		match:  uint64BetweenTestMatch_1,
		match2:  uint64BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint64BetweenTestMatch_1,
		match2:  uint64BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	},
	CreateUint64TestCase("vec1", uint64TestSlice_0, uint64BetweenTestMatch_0, uint64BetweenTestMatch_0b, uint64BetweenTestResult_0, 32),
	CreateUint64TestCase("l32", uint64TestSlice_1, uint64BetweenTestMatch_1, uint64BetweenTestMatch_1b, uint64BetweenTestResult_1, 32),
	CreateUint64TestCase("l64", append(uint64TestSlice_1, uint64TestSlice_0...), uint64BetweenTestMatch_1, uint64BetweenTestMatch_1b,
		append(uint64BetweenTestResult_1, uint64BetweenTestResult_0...), 64),
	CreateUint64TestCase("l31", uint64TestSlice_1, uint64BetweenTestMatch_1, uint64BetweenTestMatch_1b, uint64BetweenTestResult_1, 31),
	CreateUint64TestCase("l23", uint64TestSlice_1, uint64BetweenTestMatch_1, uint64BetweenTestMatch_1b, uint64BetweenTestResult_1, 23),
	CreateUint64TestCase("l15", uint64TestSlice_1, uint64BetweenTestMatch_1, uint64BetweenTestMatch_1b, uint64BetweenTestResult_1, 15),
	CreateUint64TestCase("l7", uint64TestSlice_1, uint64BetweenTestMatch_1, uint64BetweenTestMatch_1b, uint64BetweenTestResult_1, 7),
	// with extreme values
	CreateUint64TestCase("ext32", uint64TestSlice_2, uint64BetweenTestMatch_2, uint64BetweenTestMatch_1b, uint64BetweenTestResult_2, 32),
	CreateUint64TestCase("ext31", uint64TestSlice_2, uint64BetweenTestMatch_2, uint64BetweenTestMatch_1b, uint64BetweenTestResult_2, 31),
}

func TestMatchUint64BetweenGeneric(T *testing.T) {
	for _, c := range uint64BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint64BetweenGeneric(c.slice, c.match, c.match2, bits)
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

func TestMatchUint64BetweenAVX2(T *testing.T) {
	for _, c := range uint64BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint64BetweenAVX2(c.slice, c.match, c.match2, bits)
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
// BenchmarkMatchUint64BetweenGeneric/32-8     	30000000      47.3 ns/op	5417.18 MB/s
// BenchmarkMatchUint64BetweenGeneric/128-8    	10000000     159 ns/op	6436.91 MB/s
// BenchmarkMatchUint64BetweenGeneric/1024-8   	 1000000    1201 ns/op	6820.23 MB/s
// BenchmarkMatchUint64BetweenGeneric/4096-8   	  300000    4937 ns/op	6636.40 MB/s
// BenchmarkMatchUint64BetweenGeneric/65536-8  	   20000   79233 ns/op	6616.96 MB/s
// BenchmarkMatchUint64BetweenGeneric/131072-8 	   10000  161598 ns/op	6488.79 MB/s
func BenchmarkMatchUint64BetweenGeneric(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64BetweenGeneric(a, 5, 10, bits)
			}
		})
	}
}

// BenchmarkMatchUint64BetweenAVX2/32-8      100000000     14.8 ns/op	17284.10 MB/s
// BenchmarkMatchUint64BetweenAVX2/128-8      30000000     48.9 ns/op	20953.59 MB/s
// BenchmarkMatchUint64BetweenAVX2/1024-8      5000000    370 ns/op	22089.64 MB/s
// BenchmarkMatchUint64BetweenAVX2/4096-8      1000000   1629 ns/op	20114.61 MB/s
// BenchmarkMatchUint64BetweenAVX2/65536-8       50000  29559 ns/op	17736.52 MB/s
// BenchmarkMatchUint64BetweenAVX2/131072-8      20000  58059 ns/op	18060.42 MB/s
func BenchmarkMatchUint64BetweenAVX2(B *testing.B) {
	for _, n := range []int{32, 128, 1024, 4096, 64 * 1024, 128 * 1024} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64BetweenAVX2(a, 5, 10, bits)
			}
		})
	}
}

// force scalar codepath by making last block <32 entries
// BenchmarkMatchUint64BetweenAVX2Scalar/31-8     	50000000     38.2 ns/op	6492.97 MB/s
// BenchmarkMatchUint64BetweenAVX2Scalar/127-8    	20000000     70.6 ns/op	14397.10 MB/s
// BenchmarkMatchUint64BetweenAVX2Scalar/1023-8   	 5000000    389 ns/op	21000.09 MB/s
// BenchmarkMatchUint64BetweenAVX2Scalar/4095-8   	 1000000   1624 ns/op	20161.18 MB/s
// BenchmarkMatchUint64BetweenAVX2Scalar/65535-8  	   50000  28713 ns/op	18258.82 MB/s
// BenchmarkMatchUint64BetweenAVX2Scalar/131071-8 	   20000  58733 ns/op	17853.05 MB/s
func BenchmarkMatchUint64BetweenAVX2Scalar(B *testing.B) {
	for _, n := range []int{32 - 1, 128 - 1, 1024 - 1, 4096 - 1, 64*1024 - 1, 128*1024 - 1} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n * Uint64Size))
			for i := 0; i < B.N; i++ {
				matchUint64BetweenAVX2(a, 5, 10, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Uint64 Slice
//
func TestUniqueUint64(T *testing.T) {
	a := randUint64Slice(1000, 5)
	b := UniqueUint64Slice(a)
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

func BenchmarkUniqueUint64(B *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint64Slice(n, 5)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				UniqueUint64Slice(a)
			}
		})
	}
}

func TestUint64SliceContains(T *testing.T) {
	// nil slice
	if Uint64Slice(nil).Contains(1) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Uint64Slice([]uint64{}).Contains(1) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Uint64Slice([]uint64{1}).Contains(1) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Uint64Slice([]uint64{1}).Contains(2) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !Uint64Slice([]uint64{1, 3, 5, 7, 11, 13}).Contains(1) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Uint64Slice([]uint64{1, 3, 5, 7, 11, 13}).Contains(5) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Uint64Slice([]uint64{1, 3, 5, 7, 11, 13}).Contains(13) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Uint64Slice([]uint64{1, 3, 5, 7, 11, 13}).Contains(0) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Uint64Slice([]uint64{1, 3, 5, 7, 11, 13}).Contains(2) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Uint64Slice([]uint64{1, 3, 5, 7, 11, 13}).Contains(14) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkUint64SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Uint64Slice(randUint64Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(rand.Uint64())
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Uint64Slice(randUint64Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(a[rand.Intn(len(a))])
			}
		})
	}
}

func TestUint64SliceContainsRange(T *testing.T) {
	type VecTestRange struct {
		Name  string
		From  uint64
		To    uint64
		Match bool
	}

	type VecTestcase struct {
		Slice  []uint64
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
			Slice: []uint64{},
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: 0, To: 2, Match: false},
			},
		},
		// 1-element slice
		VecTestcase{
			Slice: []uint64{3},
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
			Slice: []uint64{3},
			Ranges: []VecTestRange{
				VecTestRange{Name: "BCD", From: 3, To: 3, Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		VecTestcase{
			Slice: []uint64{3, 5, 7, 11, 13},
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
			Slice: []uint64{
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
			if want, got := r.Match, Uint64Slice(v.Slice).ContainsRange(r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkUint64SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Uint64Slice(randUint64Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := rand.Uint64(), rand.Uint64()
				if min > max {
					min, max = max, min
				}
				a.ContainsRange(min, max)
			}
		})
	}
}
