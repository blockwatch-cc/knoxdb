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
	uint16TestSlice_0 = []uint16{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}

	uint16EqualTestMatch_0  uint16 = 5
	uint16EqualTestResult_0        = []byte{0x6a, 0x1e, 0x48, 0x2c}

	uint16NotEqualTestMatch_0  uint16 = 5
	uint16NotEqualTestResult_0        = []byte{0x95, 0xe1, 0xb7, 0xd3}

	uint16LessTestMatch_0  uint16 = 5
	uint16LessTestResult_0        = []byte{0x05, 0x21, 0x27, 0x01}

	uint16LessEqualTestMatch_0  uint16 = 5
	uint16LessEqualTestResult_0        = []byte{0x6f, 0x3f, 0x6f, 0x2d}

	uint16GreaterTestMatch_0  uint16 = 5
	uint16GreaterTestResult_0        = []byte{0x90, 0xc0, 0x90, 0xd2}

	uint16GreaterEqualTestMatch_0  uint16 = 5
	uint16GreaterEqualTestResult_0        = []byte{0xfa, 0xde, 0xd8, 0xfe}

	uint16BetweenTestMatch_0  uint16 = 5
	uint16BetweenTestMatch_0b uint16 = 10
	uint16BetweenTestResult_0        = []byte{0xfa, 0x1e, 0xd8, 0x2c}

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

	uint16EqualTestResult_1        = []byte{0x41, 0x42, 0xc4, 0x0e}
	uint16EqualTestMatch_1  uint16 = 5

	uint16NotEqualTestResult_1        = []byte{0xbe, 0xbd, 0x3b, 0xf1}
	uint16NotEqualTestMatch_1  uint16 = 5

	uint16LessTestResult_1        = []byte{0x0e, 0x00, 0x00, 0x00}
	uint16LessTestMatch_1  uint16 = 5

	uint16LessEqualTestResult_1        = []byte{0x4f, 0x42, 0xc4, 0x0e}
	uint16LessEqualTestMatch_1  uint16 = 5

	uint16GreaterTestResult_1        = []byte{0xb0, 0xbd, 0x3b, 0xf1}
	uint16GreaterTestMatch_1  uint16 = 5

	uint16GreaterEqualTestResult_1        = []byte{0xf1, 0xff, 0xff, 0xff}
	uint16GreaterEqualTestMatch_1  uint16 = 5

	uint16BetweenTestResult_1        = []byte{0xf1, 0x42, 0xc4, 0x0e}
	uint16BetweenTestMatch_1  uint16 = 5
	uint16BetweenTestMatch_1b uint16 = 10

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
	uint16EqualTestResult_2        = []byte{0x88, 0x88, 0x88, 0x88}
	uint16EqualTestMatch_2  uint16 = math.MaxUint16

	uint16NotEqualTestResult_2        = []byte{0x77, 0x77, 0x77, 0x77}
	uint16NotEqualTestMatch_2  uint16 = math.MaxUint16

	uint16LessTestResult_2        = []byte{0x77, 0x77, 0x77, 0x77}
	uint16LessTestMatch_2  uint16 = math.MaxUint16

	uint16LessEqualTestResult_2        = []byte{0xff, 0xff, 0xff, 0xff}
	uint16LessEqualTestMatch_2  uint16 = math.MaxUint16

	uint16GreaterTestResult_2        = []byte{0x00, 0x00, 0x00, 0x00}
	uint16GreaterTestMatch_2  uint16 = math.MaxUint16

	uint16GreaterEqualTestResult_2        = []byte{0x88, 0x88, 0x88, 0x88}
	uint16GreaterEqualTestMatch_2  uint16 = math.MaxUint16

	uint16BetweenTestResult_2        = []byte{0xcc, 0xcc, 0xcc, 0xcc}
	uint16BetweenTestMatch_2  uint16 = math.MaxUint8
	uint16BetweenTestMatch_2b uint16 = math.MaxUint16
)

func randUint16Slice(n, u int) []uint16 {
	s := make([]uint16, n*u)
	for i := 0; i < n; i++ {
		s[i] = uint16(rand.Intn(math.MaxUint16 + 1))
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

// creates an uint16 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - match, match2: are only copied to the resulting test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateUint16TestCase(name string, slice []uint16, match, match2 uint16, result []byte, length int) Uint16MatchTest {
	if len(slice)%8 != 0 {
		panic("CreateUint16TestCase: length of slice has to be a multiple of 8")
	}
	if len(result) != bitFieldLen(len(slice)) {
		panic("CreateUint16TestCase: length of slice and length of result does not match")
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []uint16
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
	return Uint16MatchTest{
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

var uint16EqualCases = []Uint16MatchTest{
	{
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
	},
	CreateUint16TestCase("vec1", uint16TestSlice_0, uint16EqualTestMatch_0, 0, uint16EqualTestResult_0, 32),
	CreateUint16TestCase("vec2", uint16TestSlice_0, uint16EqualTestMatch_0, 0, uint16EqualTestResult_0, 256),
	CreateUint16TestCase("l32", uint16TestSlice_1, uint16EqualTestMatch_1, 0, uint16EqualTestResult_1, 32),
	CreateUint16TestCase("l64", append(uint16TestSlice_1, uint16TestSlice_0...), uint16EqualTestMatch_1, 0,
		append(uint16EqualTestResult_1, uint16EqualTestResult_0...), 64),
	CreateUint16TestCase("l128", append(uint16TestSlice_1, uint16TestSlice_0...), uint16EqualTestMatch_1, 0,
		append(uint16EqualTestResult_1, uint16EqualTestResult_0...), 128),
	CreateUint16TestCase("l256", append(uint16TestSlice_1, uint16TestSlice_0...), uint16EqualTestMatch_1, 0,
		append(uint16EqualTestResult_1, uint16EqualTestResult_0...), 256),
	CreateUint16TestCase("l512", append(uint16TestSlice_1, uint16TestSlice_0...), uint16EqualTestMatch_1, 0,
		append(uint16EqualTestResult_1, uint16EqualTestResult_0...), 512),
	CreateUint16TestCase("l511", append(uint16TestSlice_1, uint16TestSlice_0...), uint16EqualTestMatch_1, 0,
		append(uint16EqualTestResult_1, uint16EqualTestResult_0...), 511),
	CreateUint16TestCase("l255", append(uint16TestSlice_1, uint16TestSlice_0...), uint16EqualTestMatch_1, 0,
		append(uint16EqualTestResult_1, uint16EqualTestResult_0...), 255),
	CreateUint16TestCase("l127", uint16TestSlice_1, uint16EqualTestMatch_1, 0, uint16EqualTestResult_1, 127),
	CreateUint16TestCase("l63", uint16TestSlice_1, uint16EqualTestMatch_1, 0, uint16EqualTestResult_1, 63),
	CreateUint16TestCase("l31", uint16TestSlice_1, uint16EqualTestMatch_1, 0, uint16EqualTestResult_1, 31),
	CreateUint16TestCase("l23", uint16TestSlice_1, uint16EqualTestMatch_1, 0, uint16EqualTestResult_1, 23),
	CreateUint16TestCase("l15", uint16TestSlice_1, uint16EqualTestMatch_1, 0, uint16EqualTestResult_1, 15),
	CreateUint16TestCase("l7", uint16TestSlice_1, uint16EqualTestMatch_1, 0, uint16EqualTestResult_1, 7),
	// with extreme values
	CreateUint16TestCase("ext256", uint16TestSlice_2, uint16EqualTestMatch_2, 0, uint16EqualTestResult_2, 256),
	CreateUint16TestCase("ext32", uint16TestSlice_2, uint16EqualTestMatch_2, 0, uint16EqualTestResult_2, 32),
	CreateUint16TestCase("ext31", uint16TestSlice_2, uint16EqualTestMatch_2, 0, uint16EqualTestResult_2, 31),
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

// -----------------------------------------------------------------------------
// Equal benchmarks
//
func BenchmarkMatchUint16EqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16EqualGeneric(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
//

var uint16NotEqualCases = []Uint16MatchTest{
	{
		name:   "l0",
		slice:  make([]uint16, 0),
		match:  uint16NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint16NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateUint16TestCase("vec1", uint16TestSlice_0, uint16NotEqualTestMatch_0, 0, uint16NotEqualTestResult_0, 32),
	CreateUint16TestCase("vec2", uint16TestSlice_0, uint16NotEqualTestMatch_0, 0, uint16NotEqualTestResult_0, 256),
	CreateUint16TestCase("l32", uint16TestSlice_1, uint16NotEqualTestMatch_1, 0, uint16NotEqualTestResult_1, 32),
	CreateUint16TestCase("l64", append(uint16TestSlice_1, uint16TestSlice_0...), uint16NotEqualTestMatch_1, 0,
		append(uint16NotEqualTestResult_1, uint16NotEqualTestResult_0...), 64),
	CreateUint16TestCase("l128", append(uint16TestSlice_1, uint16TestSlice_0...), uint16NotEqualTestMatch_1, 0,
		append(uint16NotEqualTestResult_1, uint16NotEqualTestResult_0...), 128),
	CreateUint16TestCase("l256", append(uint16TestSlice_1, uint16TestSlice_0...), uint16NotEqualTestMatch_1, 0,
		append(uint16NotEqualTestResult_1, uint16NotEqualTestResult_0...), 256),
	CreateUint16TestCase("l512", append(uint16TestSlice_1, uint16TestSlice_0...), uint16NotEqualTestMatch_1, 0,
		append(uint16NotEqualTestResult_1, uint16NotEqualTestResult_0...), 512),
	CreateUint16TestCase("l511", append(uint16TestSlice_1, uint16TestSlice_0...), uint16NotEqualTestMatch_1, 0,
		append(uint16NotEqualTestResult_1, uint16NotEqualTestResult_0...), 511),
	CreateUint16TestCase("l255", append(uint16TestSlice_1, uint16TestSlice_0...), uint16NotEqualTestMatch_1, 0,
		append(uint16NotEqualTestResult_1, uint16NotEqualTestResult_0...), 255),
	CreateUint16TestCase("l127", uint16TestSlice_1, uint16NotEqualTestMatch_1, 0, uint16NotEqualTestResult_1, 127),
	CreateUint16TestCase("l63", uint16TestSlice_1, uint16NotEqualTestMatch_1, 0, uint16NotEqualTestResult_1, 63),
	CreateUint16TestCase("l31", uint16TestSlice_1, uint16NotEqualTestMatch_1, 0, uint16NotEqualTestResult_1, 31),
	CreateUint16TestCase("l23", uint16TestSlice_1, uint16NotEqualTestMatch_1, 0, uint16NotEqualTestResult_1, 23),
	CreateUint16TestCase("l15", uint16TestSlice_1, uint16NotEqualTestMatch_1, 0, uint16NotEqualTestResult_1, 15),
	CreateUint16TestCase("l7", uint16TestSlice_1, uint16NotEqualTestMatch_1, 0, uint16NotEqualTestResult_1, 7),
	// with extreme values
	CreateUint16TestCase("ext256", uint16TestSlice_2, uint16NotEqualTestMatch_2, 0, uint16NotEqualTestResult_2, 256),
	CreateUint16TestCase("ext32", uint16TestSlice_2, uint16NotEqualTestMatch_2, 0, uint16NotEqualTestResult_2, 32),
	CreateUint16TestCase("ext31", uint16TestSlice_2, uint16NotEqualTestMatch_2, 0, uint16NotEqualTestResult_2, 31),
}

func TestMatchUint16NotEqualGeneric(T *testing.T) {
	for _, c := range uint16NotEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint16NotEqualGeneric(c.slice, c.match, bits)
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
// NotEqual benchmarks
//
func BenchmarkMatchUint16NotEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16NotEqualGeneric(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
//

var uint16LessCases = []Uint16MatchTest{
	{
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
	},
	CreateUint16TestCase("vec1", uint16TestSlice_0, uint16LessTestMatch_0, 0, uint16LessTestResult_0, 32),
	CreateUint16TestCase("vec2", uint16TestSlice_0, uint16LessTestMatch_0, 0, uint16LessTestResult_0, 256),
	CreateUint16TestCase("l32", uint16TestSlice_1, uint16LessTestMatch_1, 0, uint16LessTestResult_1, 32),
	CreateUint16TestCase("l64", append(uint16TestSlice_1, uint16TestSlice_0...), uint16LessTestMatch_1, 0,
		append(uint16LessTestResult_1, uint16LessTestResult_0...), 64),
	CreateUint16TestCase("l128", append(uint16TestSlice_1, uint16TestSlice_0...), uint16LessTestMatch_1, 0,
		append(uint16LessTestResult_1, uint16LessTestResult_0...), 128),
	CreateUint16TestCase("l256", append(uint16TestSlice_1, uint16TestSlice_0...), uint16LessTestMatch_1, 0,
		append(uint16LessTestResult_1, uint16LessTestResult_0...), 256),
	CreateUint16TestCase("l512", append(uint16TestSlice_1, uint16TestSlice_0...), uint16LessTestMatch_1, 0,
		append(uint16LessTestResult_1, uint16LessTestResult_0...), 512),
	CreateUint16TestCase("l511", append(uint16TestSlice_1, uint16TestSlice_0...), uint16LessTestMatch_1, 0,
		append(uint16LessTestResult_1, uint16LessTestResult_0...), 511),
	CreateUint16TestCase("l255", append(uint16TestSlice_1, uint16TestSlice_0...), uint16LessTestMatch_1, 0,
		append(uint16LessTestResult_1, uint16LessTestResult_0...), 255),
	CreateUint16TestCase("l127", uint16TestSlice_1, uint16LessTestMatch_1, 0, uint16LessTestResult_1, 127),
	CreateUint16TestCase("l63", uint16TestSlice_1, uint16LessTestMatch_1, 0, uint16LessTestResult_1, 63),
	CreateUint16TestCase("l31", uint16TestSlice_1, uint16LessTestMatch_1, 0, uint16LessTestResult_1, 31),
	CreateUint16TestCase("l23", uint16TestSlice_1, uint16LessTestMatch_1, 0, uint16LessTestResult_1, 23),
	CreateUint16TestCase("l15", uint16TestSlice_1, uint16LessTestMatch_1, 0, uint16LessTestResult_1, 15),
	CreateUint16TestCase("l7", uint16TestSlice_1, uint16LessTestMatch_1, 0, uint16LessTestResult_1, 7),
	// with extreme values
	CreateUint16TestCase("ext256", uint16TestSlice_2, uint16LessTestMatch_2, 0, uint16LessTestResult_2, 256),
	CreateUint16TestCase("ext32", uint16TestSlice_2, uint16LessTestMatch_2, 0, uint16LessTestResult_2, 32),
	CreateUint16TestCase("ext31", uint16TestSlice_2, uint16LessTestMatch_2, 0, uint16LessTestResult_2, 31),
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

// -----------------------------------------------------------------------------
// Less benchmarks
//
func BenchmarkMatchUint16LessGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16LessThanGeneric(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//

var uint16LessEqualCases = []Uint16MatchTest{
	{
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
	},
	CreateUint16TestCase("vec1", uint16TestSlice_0, uint16LessEqualTestMatch_0, 0, uint16LessEqualTestResult_0, 32),
	CreateUint16TestCase("vec2", uint16TestSlice_0, uint16LessEqualTestMatch_0, 0, uint16LessEqualTestResult_0, 256),
	CreateUint16TestCase("l32", uint16TestSlice_1, uint16LessEqualTestMatch_1, 0, uint16LessEqualTestResult_1, 32),
	CreateUint16TestCase("l64", append(uint16TestSlice_1, uint16TestSlice_0...), uint16LessEqualTestMatch_1, 0,
		append(uint16LessEqualTestResult_1, uint16LessEqualTestResult_0...), 64),
	CreateUint16TestCase("l128", append(uint16TestSlice_1, uint16TestSlice_0...), uint16LessEqualTestMatch_1, 0,
		append(uint16LessEqualTestResult_1, uint16LessEqualTestResult_0...), 128),
	CreateUint16TestCase("l256", append(uint16TestSlice_1, uint16TestSlice_0...), uint16LessEqualTestMatch_1, 0,
		append(uint16LessEqualTestResult_1, uint16LessEqualTestResult_0...), 256),
	CreateUint16TestCase("l512", append(uint16TestSlice_1, uint16TestSlice_0...), uint16LessEqualTestMatch_1, 0,
		append(uint16LessEqualTestResult_1, uint16LessEqualTestResult_0...), 512),
	CreateUint16TestCase("l511", append(uint16TestSlice_1, uint16TestSlice_0...), uint16LessEqualTestMatch_1, 0,
		append(uint16LessEqualTestResult_1, uint16LessEqualTestResult_0...), 511),
	CreateUint16TestCase("l255", append(uint16TestSlice_1, uint16TestSlice_0...), uint16LessEqualTestMatch_1, 0,
		append(uint16LessEqualTestResult_1, uint16LessEqualTestResult_0...), 255),
	CreateUint16TestCase("l127", uint16TestSlice_1, uint16LessEqualTestMatch_1, 0, uint16LessEqualTestResult_1, 127),
	CreateUint16TestCase("l63", uint16TestSlice_1, uint16LessEqualTestMatch_1, 0, uint16LessEqualTestResult_1, 63),
	CreateUint16TestCase("l31", uint16TestSlice_1, uint16LessEqualTestMatch_1, 0, uint16LessEqualTestResult_1, 31),
	CreateUint16TestCase("l23", uint16TestSlice_1, uint16LessEqualTestMatch_1, 0, uint16LessEqualTestResult_1, 23),
	CreateUint16TestCase("l15", uint16TestSlice_1, uint16LessEqualTestMatch_1, 0, uint16LessEqualTestResult_1, 15),
	CreateUint16TestCase("l7", uint16TestSlice_1, uint16LessEqualTestMatch_1, 0, uint16LessEqualTestResult_1, 7),
	// with extreme values
	CreateUint16TestCase("ext256", uint16TestSlice_2, uint16LessEqualTestMatch_2, 0, uint16LessEqualTestResult_2, 256),
	CreateUint16TestCase("ext32", uint16TestSlice_2, uint16LessEqualTestMatch_2, 0, uint16LessEqualTestResult_2, 32),
	CreateUint16TestCase("ext31", uint16TestSlice_2, uint16LessEqualTestMatch_2, 0, uint16LessEqualTestResult_2, 31),
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

// -----------------------------------------------------------------------------
// Less equal benchmarks
//
func BenchmarkMatchUint16LessEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16LessThanEqualGeneric(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
//

var uint16GreaterCases = []Uint16MatchTest{
	{
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
	},
	CreateUint16TestCase("vec1", uint16TestSlice_0, uint16GreaterTestMatch_0, 0, uint16GreaterTestResult_0, 32),
	CreateUint16TestCase("vec2", uint16TestSlice_0, uint16GreaterTestMatch_0, 0, uint16GreaterTestResult_0, 256),
	CreateUint16TestCase("l32", uint16TestSlice_1, uint16GreaterTestMatch_1, 0, uint16GreaterTestResult_1, 32),
	CreateUint16TestCase("l64", append(uint16TestSlice_1, uint16TestSlice_0...), uint16GreaterTestMatch_1, 0,
		append(uint16GreaterTestResult_1, uint16GreaterTestResult_0...), 64),
	CreateUint16TestCase("l128", append(uint16TestSlice_1, uint16TestSlice_0...), uint16GreaterTestMatch_1, 0,
		append(uint16GreaterTestResult_1, uint16GreaterTestResult_0...), 128),
	CreateUint16TestCase("l256", append(uint16TestSlice_1, uint16TestSlice_0...), uint16GreaterTestMatch_1, 0,
		append(uint16GreaterTestResult_1, uint16GreaterTestResult_0...), 256),
	CreateUint16TestCase("l512", append(uint16TestSlice_1, uint16TestSlice_0...), uint16GreaterTestMatch_1, 0,
		append(uint16GreaterTestResult_1, uint16GreaterTestResult_0...), 512),
	CreateUint16TestCase("l511", append(uint16TestSlice_1, uint16TestSlice_0...), uint16GreaterTestMatch_1, 0,
		append(uint16GreaterTestResult_1, uint16GreaterTestResult_0...), 511),
	CreateUint16TestCase("l255", append(uint16TestSlice_1, uint16TestSlice_0...), uint16GreaterTestMatch_1, 0,
		append(uint16GreaterTestResult_1, uint16GreaterTestResult_0...), 255),
	CreateUint16TestCase("l127", uint16TestSlice_1, uint16GreaterTestMatch_1, 0, uint16GreaterTestResult_1, 127),
	CreateUint16TestCase("l63", uint16TestSlice_1, uint16GreaterTestMatch_1, 0, uint16GreaterTestResult_1, 63),
	CreateUint16TestCase("l31", uint16TestSlice_1, uint16GreaterTestMatch_1, 0, uint16GreaterTestResult_1, 31),
	CreateUint16TestCase("l23", uint16TestSlice_1, uint16GreaterTestMatch_1, 0, uint16GreaterTestResult_1, 23),
	CreateUint16TestCase("l15", uint16TestSlice_1, uint16GreaterTestMatch_1, 0, uint16GreaterTestResult_1, 15),
	CreateUint16TestCase("l7", uint16TestSlice_1, uint16GreaterTestMatch_1, 0, uint16GreaterTestResult_1, 7),
	// with extreme values
	CreateUint16TestCase("ext256", uint16TestSlice_2, uint16GreaterTestMatch_2, 0, uint16GreaterTestResult_2, 256),
	CreateUint16TestCase("ext32", uint16TestSlice_2, uint16GreaterTestMatch_2, 0, uint16GreaterTestResult_2, 32),
	CreateUint16TestCase("ext31", uint16TestSlice_2, uint16GreaterTestMatch_2, 0, uint16GreaterTestResult_2, 31),
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

// -----------------------------------------------------------------------------
// Greater benchmarks
//
func BenchmarkMatchUint16GreaterGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16GreaterThanGeneric(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//

var uint16GreaterEqualCases = []Uint16MatchTest{
	{
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
	},
	CreateUint16TestCase("vec1", uint16TestSlice_0, uint16GreaterEqualTestMatch_0, 0, uint16GreaterEqualTestResult_0, 32),
	CreateUint16TestCase("vec2", uint16TestSlice_0, uint16GreaterEqualTestMatch_0, 0, uint16GreaterEqualTestResult_0, 256),
	CreateUint16TestCase("l32", uint16TestSlice_1, uint16GreaterEqualTestMatch_1, 0, uint16GreaterEqualTestResult_1, 32),
	CreateUint16TestCase("l64", append(uint16TestSlice_1, uint16TestSlice_0...), uint16GreaterEqualTestMatch_1, 0,
		append(uint16GreaterEqualTestResult_1, uint16GreaterEqualTestResult_0...), 64),
	CreateUint16TestCase("l128", append(uint16TestSlice_1, uint16TestSlice_0...), uint16GreaterEqualTestMatch_1, 0,
		append(uint16GreaterEqualTestResult_1, uint16GreaterEqualTestResult_0...), 128),
	CreateUint16TestCase("l256", append(uint16TestSlice_1, uint16TestSlice_0...), uint16GreaterEqualTestMatch_1, 0,
		append(uint16GreaterEqualTestResult_1, uint16GreaterEqualTestResult_0...), 256),
	CreateUint16TestCase("l512", append(uint16TestSlice_1, uint16TestSlice_0...), uint16GreaterEqualTestMatch_1, 0,
		append(uint16GreaterEqualTestResult_1, uint16GreaterEqualTestResult_0...), 512),
	CreateUint16TestCase("l511", append(uint16TestSlice_1, uint16TestSlice_0...), uint16GreaterEqualTestMatch_1, 0,
		append(uint16GreaterEqualTestResult_1, uint16GreaterEqualTestResult_0...), 511),
	CreateUint16TestCase("l255", append(uint16TestSlice_1, uint16TestSlice_0...), uint16GreaterEqualTestMatch_1, 0,
		append(uint16GreaterEqualTestResult_1, uint16GreaterEqualTestResult_0...), 255),
	CreateUint16TestCase("l127", uint16TestSlice_1, uint16GreaterEqualTestMatch_1, 0, uint16GreaterEqualTestResult_1, 127),
	CreateUint16TestCase("l63", uint16TestSlice_1, uint16GreaterEqualTestMatch_1, 0, uint16GreaterEqualTestResult_1, 63),
	CreateUint16TestCase("l31", uint16TestSlice_1, uint16GreaterEqualTestMatch_1, 0, uint16GreaterEqualTestResult_1, 31),
	CreateUint16TestCase("l23", uint16TestSlice_1, uint16GreaterEqualTestMatch_1, 0, uint16GreaterEqualTestResult_1, 23),
	CreateUint16TestCase("l15", uint16TestSlice_1, uint16GreaterEqualTestMatch_1, 0, uint16GreaterEqualTestResult_1, 15),
	CreateUint16TestCase("l7", uint16TestSlice_1, uint16GreaterEqualTestMatch_1, 0, uint16GreaterEqualTestResult_1, 7),
	// with extreme values
	CreateUint16TestCase("ext256", uint16TestSlice_2, uint16GreaterEqualTestMatch_2, 0, uint16GreaterEqualTestResult_2, 256),
	CreateUint16TestCase("ext32", uint16TestSlice_2, uint16GreaterEqualTestMatch_2, 0, uint16GreaterEqualTestResult_2, 32),
	CreateUint16TestCase("ext31", uint16TestSlice_2, uint16GreaterEqualTestMatch_2, 0, uint16GreaterEqualTestResult_2, 31),
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

// -----------------------------------------------------------------------------
// Greater equal benchmarks
//
func BenchmarkMatchUint16GreaterEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16GreaterThanEqualGeneric(a, math.MaxUint16/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
//
var uint16BetweenCases = []Uint16MatchTest{
	{
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
	},
	CreateUint16TestCase("vec1", uint16TestSlice_0, uint16BetweenTestMatch_0, uint16BetweenTestMatch_0b, uint16BetweenTestResult_0, 32),
	CreateUint16TestCase("vec2", uint16TestSlice_0, uint16BetweenTestMatch_0, uint16BetweenTestMatch_0b, uint16BetweenTestResult_0, 256),
	CreateUint16TestCase("l32", uint16TestSlice_1, uint16BetweenTestMatch_1, uint16BetweenTestMatch_1b, uint16BetweenTestResult_1, 32),
	CreateUint16TestCase("l64", append(uint16TestSlice_1, uint16TestSlice_0...), uint16BetweenTestMatch_1, uint16BetweenTestMatch_1b,
		append(uint16BetweenTestResult_1, uint16BetweenTestResult_0...), 64),
	CreateUint16TestCase("l128", append(uint16TestSlice_1, uint16TestSlice_0...), uint16BetweenTestMatch_1, uint16BetweenTestMatch_1b,
		append(uint16BetweenTestResult_1, uint16BetweenTestResult_0...), 128),
	CreateUint16TestCase("l256", append(uint16TestSlice_1, uint16TestSlice_0...), uint16BetweenTestMatch_1, uint16BetweenTestMatch_1b,
		append(uint16BetweenTestResult_1, uint16BetweenTestResult_0...), 256),
	CreateUint16TestCase("l512", append(uint16TestSlice_1, uint16TestSlice_0...), uint16BetweenTestMatch_1, uint16BetweenTestMatch_1b,
		append(uint16BetweenTestResult_1, uint16BetweenTestResult_0...), 512),
	CreateUint16TestCase("l511", append(uint16TestSlice_1, uint16TestSlice_0...), uint16BetweenTestMatch_1, uint16BetweenTestMatch_1b,
		append(uint16BetweenTestResult_1, uint16BetweenTestResult_0...), 511),
	CreateUint16TestCase("l255", append(uint16TestSlice_1, uint16TestSlice_0...), uint16BetweenTestMatch_1, uint16BetweenTestMatch_1b,
		append(uint16BetweenTestResult_1, uint16BetweenTestResult_0...), 255),
	CreateUint16TestCase("l127", uint16TestSlice_1, uint16BetweenTestMatch_1, uint16BetweenTestMatch_1b, uint16BetweenTestResult_1, 127),
	CreateUint16TestCase("l63", uint16TestSlice_1, uint16BetweenTestMatch_1, uint16BetweenTestMatch_1b, uint16BetweenTestResult_1, 63),
	CreateUint16TestCase("l31", uint16TestSlice_1, uint16BetweenTestMatch_1, uint16BetweenTestMatch_1b, uint16BetweenTestResult_1, 31),
	CreateUint16TestCase("l23", uint16TestSlice_1, uint16BetweenTestMatch_1, uint16BetweenTestMatch_1b, uint16BetweenTestResult_1, 23),
	CreateUint16TestCase("l15", uint16TestSlice_1, uint16BetweenTestMatch_1, uint16BetweenTestMatch_1b, uint16BetweenTestResult_1, 15),
	CreateUint16TestCase("l7", uint16TestSlice_1, uint16BetweenTestMatch_1, uint16BetweenTestMatch_1b, uint16BetweenTestResult_1, 7),
	// with extreme values
	CreateUint16TestCase("ext256", uint16TestSlice_2, uint16BetweenTestMatch_2, uint16BetweenTestMatch_2b, uint16BetweenTestResult_2, 256),
	CreateUint16TestCase("ext32", uint16TestSlice_2, uint16BetweenTestMatch_2, uint16BetweenTestMatch_2b, uint16BetweenTestResult_2, 32),
	CreateUint16TestCase("ext31", uint16TestSlice_2, uint16BetweenTestMatch_2, uint16BetweenTestMatch_2b, uint16BetweenTestResult_2, 31),
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

// -----------------------------------------------------------------------------
// Between benchmarks
//
func BenchmarkMatchUint16BetweenGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint16Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint16Size))
			for i := 0; i < B.N; i++ {
				matchUint16BetweenGeneric(a, math.MaxUint16/4, math.MaxUint16/2, bits)
			}
		})
	}
}

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
	if Uint16.Contains(nil, 1) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Uint16.Contains([]uint16{}, 1) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Uint16.Contains([]uint16{1}, 1) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Uint16.Contains([]uint16{1}, 2) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !Uint16.Contains([]uint16{1, 3, 5, 7, 11, 13}, 1) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Uint16.Contains([]uint16{1, 3, 5, 7, 11, 13}, 5) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Uint16.Contains([]uint16{1, 3, 5, 7, 11, 13}, 13) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Uint16.Contains([]uint16{1, 3, 5, 7, 11, 13}, 0) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Uint16.Contains([]uint16{1, 3, 5, 7, 11, 13}, 2) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Uint16.Contains([]uint16{1, 3, 5, 7, 11, 13}, 14) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkUint16SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Uint16.Sort(randUint16Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				Uint16.Contains(a, uint16(rand.Intn(math.MaxUint16+1)))
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Uint16.Sort(randUint16Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				Uint16.Contains(a, a[rand.Intn(len(a))])
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
			if want, got := r.Match, Uint16.ContainsRange(v.Slice, r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkUint16SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Uint16.Sort(randUint16Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := uint16(rand.Intn(math.MaxUint16+1)), uint16(rand.Intn(math.MaxUint16+1))
				if min > max {
					min, max = max, min
				}
				Uint16.ContainsRange(a, min, max)
			}
		})
	}
}
