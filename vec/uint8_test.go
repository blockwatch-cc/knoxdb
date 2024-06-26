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
	uint8TestSlice_0 = []uint8{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	uint8EqualTestMatch_0  uint8 = 5
	uint8EqualTestResult_0       = []byte{0x6a, 0x1e, 0x48, 0x2c}

	uint8NotEqualTestMatch_0  uint8 = 5
	uint8NotEqualTestResult_0       = []byte{0x95, 0xe1, 0xb7, 0xd3}

	uint8LessTestMatch_0  uint8 = 5
	uint8LessTestResult_0       = []byte{0x05, 0x21, 0x27, 0x01}

	uint8LessEqualTestMatch_0  uint8 = 5
	uint8LessEqualTestResult_0       = []byte{0x6f, 0x3f, 0x6f, 0x2d}

	uint8GreaterTestMatch_0  uint8 = 5
	uint8GreaterTestResult_0       = []byte{0x90, 0xc0, 0x90, 0xd2}

	uint8GreaterEqualTestMatch_0  uint8 = 5
	uint8GreaterEqualTestResult_0       = []byte{0xfa, 0xde, 0xd8, 0xfe}

	uint8BetweenTestMatch_0  uint8 = 5
	uint8BetweenTestMatch_0b uint8 = 10
	uint8BetweenTestResult_0       = []byte{0xfa, 0x1e, 0xd8, 0x2c}

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

	uint8EqualTestResult_1       = []byte{0x41, 0x42, 0xc4, 0x0e}
	uint8EqualTestMatch_1  uint8 = 5

	uint8NotEqualTestResult_1       = []byte{0xbe, 0xbd, 0x3b, 0xf1}
	uint8NotEqualTestMatch_1  uint8 = 5

	uint8LessTestResult_1       = []byte{0x0e, 0x00, 0x00, 0x00}
	uint8LessTestMatch_1  uint8 = 5

	uint8LessEqualTestResult_1       = []byte{0x4f, 0x42, 0xc4, 0x0e}
	uint8LessEqualTestMatch_1  uint8 = 5

	uint8GreaterTestResult_1       = []byte{0xb0, 0xbd, 0x3b, 0xf1}
	uint8GreaterTestMatch_1  uint8 = 5

	uint8GreaterEqualTestResult_1       = []byte{0xf1, 0xff, 0xff, 0xff}
	uint8GreaterEqualTestMatch_1  uint8 = 5

	uint8BetweenTestResult_1       = []byte{0xf1, 0x42, 0xc4, 0x0e}
	uint8BetweenTestMatch_1  uint8 = 5
	uint8BetweenTestMatch_1b uint8 = 10

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
	uint8EqualTestResult_2       = []byte{0x88, 0x88, 0x88, 0x88}
	uint8EqualTestMatch_2  uint8 = math.MaxUint8

	uint8NotEqualTestResult_2       = []byte{0x77, 0x77, 0x77, 0x77}
	uint8NotEqualTestMatch_2  uint8 = math.MaxUint8

	uint8LessTestResult_2       = []byte{0x77, 0x77, 0x77, 0x77}
	uint8LessTestMatch_2  uint8 = math.MaxUint8

	uint8LessEqualTestResult_2       = []byte{0xff, 0xff, 0xff, 0xff}
	uint8LessEqualTestMatch_2  uint8 = math.MaxUint8

	uint8GreaterTestResult_2       = []byte{0x00, 0x00, 0x00, 0x00}
	uint8GreaterTestMatch_2  uint8 = math.MaxUint8

	uint8GreaterEqualTestResult_2       = []byte{0x88, 0x88, 0x88, 0x88}
	uint8GreaterEqualTestMatch_2  uint8 = math.MaxUint8

	uint8BetweenTestResult_2       = []byte{0xcc, 0xcc, 0xcc, 0xcc}
	uint8BetweenTestMatch_2  uint8 = math.MaxInt8
	uint8BetweenTestMatch_2b uint8 = math.MaxUint8
)

func randUint8Slice(n, u int) []uint8 {
	s := make([]uint8, n*u)
	for i := 0; i < n; i++ {
		s[i] = uint8(rand.Intn(math.MaxUint8 + 1))
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

// creates an uint8 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - match, match2: are only copied to the resulting test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateUint8TestCase(name string, slice []uint8, match, match2 uint8, result []byte, length int) Uint8MatchTest {
	if len(slice)%8 != 0 {
		panic("CreateUint8TestCase: length of slice has to be a multiple of 8")
	}
	if len(result) != bitFieldLen(len(slice)) {
		panic("CreateUint8TestCase: length of slice and length of result does not match")
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []uint8
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
	return Uint8MatchTest{
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

var uint8EqualCases = []Uint8MatchTest{
	{
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
	},
	CreateUint8TestCase("vec1", uint8TestSlice_0, uint8EqualTestMatch_0, 0, uint8EqualTestResult_0, 32),
	CreateUint8TestCase("vec2", uint8TestSlice_0, uint8EqualTestMatch_0, 0, uint8EqualTestResult_0, 512),
	CreateUint8TestCase("l32", uint8TestSlice_1, uint8EqualTestMatch_1, 0, uint8EqualTestResult_1, 32),
	CreateUint8TestCase("l64", append(uint8TestSlice_1, uint8TestSlice_0...), uint8EqualTestMatch_1, 0,
		append(uint8EqualTestResult_1, uint8EqualTestResult_0...), 64),
	CreateUint8TestCase("l128", append(uint8TestSlice_1, uint8TestSlice_0...), uint8EqualTestMatch_1, 0,
		append(uint8EqualTestResult_1, uint8EqualTestResult_0...), 128),
	CreateUint8TestCase("l256", append(uint8TestSlice_1, uint8TestSlice_0...), uint8EqualTestMatch_1, 0,
		append(uint8EqualTestResult_1, uint8EqualTestResult_0...), 256),
	CreateUint8TestCase("l512", append(uint8TestSlice_1, uint8TestSlice_0...), uint8EqualTestMatch_1, 0,
		append(uint8EqualTestResult_1, uint8EqualTestResult_0...), 512),
	CreateUint8TestCase("l1024", append(uint8TestSlice_1, uint8TestSlice_0...), uint8EqualTestMatch_1, 0,
		append(uint8EqualTestResult_1, uint8EqualTestResult_0...), 1024),
	CreateUint8TestCase("l1023", append(uint8TestSlice_1, uint8TestSlice_0...), uint8EqualTestMatch_1, 0,
		append(uint8EqualTestResult_1, uint8EqualTestResult_0...), 1023),
	CreateUint8TestCase("l511", append(uint8TestSlice_1, uint8TestSlice_0...), uint8EqualTestMatch_1, 0,
		append(uint8EqualTestResult_1, uint8EqualTestResult_0...), 511),
	CreateUint8TestCase("l255", append(uint8TestSlice_1, uint8TestSlice_0...), uint8EqualTestMatch_1, 0,
		append(uint8EqualTestResult_1, uint8EqualTestResult_0...), 255),
	CreateUint8TestCase("l127", uint8TestSlice_1, uint8EqualTestMatch_1, 0, uint8EqualTestResult_1, 127),
	CreateUint8TestCase("l63", uint8TestSlice_1, uint8EqualTestMatch_1, 0, uint8EqualTestResult_1, 63),
	CreateUint8TestCase("l31", uint8TestSlice_1, uint8EqualTestMatch_1, 0, uint8EqualTestResult_1, 31),
	CreateUint8TestCase("l23", uint8TestSlice_1, uint8EqualTestMatch_1, 0, uint8EqualTestResult_1, 23),
	CreateUint8TestCase("l15", uint8TestSlice_1, uint8EqualTestMatch_1, 0, uint8EqualTestResult_1, 15),
	CreateUint8TestCase("l7", uint8TestSlice_1, uint8EqualTestMatch_1, 0, uint8EqualTestResult_1, 7),
	// with extreme values
	CreateUint8TestCase("ext512", uint8TestSlice_2, uint8EqualTestMatch_2, 0, uint8EqualTestResult_2, 512),
	CreateUint8TestCase("ext32", uint8TestSlice_2, uint8EqualTestMatch_2, 0, uint8EqualTestResult_2, 32),
	CreateUint8TestCase("ext31", uint8TestSlice_2, uint8EqualTestMatch_2, 0, uint8EqualTestResult_2, 31),
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

// -----------------------------------------------------------------------------
// Equal benchmarks
//
func BenchmarkMatchUint8EqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8EqualGeneric(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
//

var uint8NotEqualCases = []Uint8MatchTest{
	{
		name:   "l0",
		slice:  make([]uint8, 0),
		match:  uint8NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint8NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateUint8TestCase("vec1", uint8TestSlice_0, uint8NotEqualTestMatch_0, 0, uint8NotEqualTestResult_0, 32),
	CreateUint8TestCase("vec2", uint8TestSlice_0, uint8NotEqualTestMatch_0, 0, uint8NotEqualTestResult_0, 512),
	CreateUint8TestCase("l32", uint8TestSlice_1, uint8NotEqualTestMatch_1, 0, uint8NotEqualTestResult_1, 32),
	CreateUint8TestCase("l64", append(uint8TestSlice_1, uint8TestSlice_0...), uint8NotEqualTestMatch_1, 0,
		append(uint8NotEqualTestResult_1, uint8NotEqualTestResult_0...), 64),
	CreateUint8TestCase("l128", append(uint8TestSlice_1, uint8TestSlice_0...), uint8NotEqualTestMatch_1, 0,
		append(uint8NotEqualTestResult_1, uint8NotEqualTestResult_0...), 128),
	CreateUint8TestCase("l256", append(uint8TestSlice_1, uint8TestSlice_0...), uint8NotEqualTestMatch_1, 0,
		append(uint8NotEqualTestResult_1, uint8NotEqualTestResult_0...), 256),
	CreateUint8TestCase("l512", append(uint8TestSlice_1, uint8TestSlice_0...), uint8NotEqualTestMatch_1, 0,
		append(uint8NotEqualTestResult_1, uint8NotEqualTestResult_0...), 512),
	CreateUint8TestCase("l1024", append(uint8TestSlice_1, uint8TestSlice_0...), uint8NotEqualTestMatch_1, 0,
		append(uint8NotEqualTestResult_1, uint8NotEqualTestResult_0...), 1024),
	CreateUint8TestCase("l1023", append(uint8TestSlice_1, uint8TestSlice_0...), uint8NotEqualTestMatch_1, 0,
		append(uint8NotEqualTestResult_1, uint8NotEqualTestResult_0...), 1023),
	CreateUint8TestCase("l511", append(uint8TestSlice_1, uint8TestSlice_0...), uint8NotEqualTestMatch_1, 0,
		append(uint8NotEqualTestResult_1, uint8NotEqualTestResult_0...), 511),
	CreateUint8TestCase("l255", append(uint8TestSlice_1, uint8TestSlice_0...), uint8NotEqualTestMatch_1, 0,
		append(uint8NotEqualTestResult_1, uint8NotEqualTestResult_0...), 255),
	CreateUint8TestCase("l127", uint8TestSlice_1, uint8NotEqualTestMatch_1, 0, uint8NotEqualTestResult_1, 127),
	CreateUint8TestCase("l63", uint8TestSlice_1, uint8NotEqualTestMatch_1, 0, uint8NotEqualTestResult_1, 63),
	CreateUint8TestCase("l31", uint8TestSlice_1, uint8NotEqualTestMatch_1, 0, uint8NotEqualTestResult_1, 31),
	CreateUint8TestCase("l23", uint8TestSlice_1, uint8NotEqualTestMatch_1, 0, uint8NotEqualTestResult_1, 23),
	CreateUint8TestCase("l15", uint8TestSlice_1, uint8NotEqualTestMatch_1, 0, uint8NotEqualTestResult_1, 15),
	CreateUint8TestCase("l7", uint8TestSlice_1, uint8NotEqualTestMatch_1, 0, uint8NotEqualTestResult_1, 7),
	// with extreme values
	CreateUint8TestCase("ext512", uint8TestSlice_2, uint8NotEqualTestMatch_2, 0, uint8NotEqualTestResult_2, 512),
	CreateUint8TestCase("ext32", uint8TestSlice_2, uint8NotEqualTestMatch_2, 0, uint8NotEqualTestResult_2, 32),
	CreateUint8TestCase("ext31", uint8TestSlice_2, uint8NotEqualTestMatch_2, 0, uint8NotEqualTestResult_2, 31),
}

func TestMatchUint8NotEqualGeneric(T *testing.T) {
	for _, c := range uint8NotEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint8NotEqualGeneric(c.slice, c.match, bits)
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
func BenchmarkMatchUint8NotEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8NotEqualGeneric(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
//

var uint8LessCases = []Uint8MatchTest{
	{
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
	},
	CreateUint8TestCase("vec1", uint8TestSlice_0, uint8LessTestMatch_0, 0, uint8LessTestResult_0, 32),
	CreateUint8TestCase("vec2", uint8TestSlice_0, uint8LessTestMatch_0, 0, uint8LessTestResult_0, 512),
	CreateUint8TestCase("l32", uint8TestSlice_1, uint8LessTestMatch_1, 0, uint8LessTestResult_1, 32),
	CreateUint8TestCase("l64", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessTestMatch_1, 0,
		append(uint8LessTestResult_1, uint8LessTestResult_0...), 64),
	CreateUint8TestCase("l128", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessTestMatch_1, 0,
		append(uint8LessTestResult_1, uint8LessTestResult_0...), 128),
	CreateUint8TestCase("l256", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessTestMatch_1, 0,
		append(uint8LessTestResult_1, uint8LessTestResult_0...), 256),
	CreateUint8TestCase("l512", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessTestMatch_1, 0,
		append(uint8LessTestResult_1, uint8LessTestResult_0...), 512),
	CreateUint8TestCase("l1024", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessTestMatch_1, 0,
		append(uint8LessTestResult_1, uint8LessTestResult_0...), 1024),
	CreateUint8TestCase("l1023", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessTestMatch_1, 0,
		append(uint8LessTestResult_1, uint8LessTestResult_0...), 1023),
	CreateUint8TestCase("l511", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessTestMatch_1, 0,
		append(uint8LessTestResult_1, uint8LessTestResult_0...), 511),
	CreateUint8TestCase("l255", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessTestMatch_1, 0,
		append(uint8LessTestResult_1, uint8LessTestResult_0...), 255),
	CreateUint8TestCase("l127", uint8TestSlice_1, uint8LessTestMatch_1, 0, uint8LessTestResult_1, 127),
	CreateUint8TestCase("l63", uint8TestSlice_1, uint8LessTestMatch_1, 0, uint8LessTestResult_1, 63),
	CreateUint8TestCase("l31", uint8TestSlice_1, uint8LessTestMatch_1, 0, uint8LessTestResult_1, 31),
	CreateUint8TestCase("l23", uint8TestSlice_1, uint8LessTestMatch_1, 0, uint8LessTestResult_1, 23),
	CreateUint8TestCase("l15", uint8TestSlice_1, uint8LessTestMatch_1, 0, uint8LessTestResult_1, 15),
	CreateUint8TestCase("l7", uint8TestSlice_1, uint8LessTestMatch_1, 0, uint8LessTestResult_1, 7),
	// with extreme values
	CreateUint8TestCase("ext512", uint8TestSlice_2, uint8LessTestMatch_2, 0, uint8LessTestResult_2, 512),
	CreateUint8TestCase("ext32", uint8TestSlice_2, uint8LessTestMatch_2, 0, uint8LessTestResult_2, 32),
	CreateUint8TestCase("ext31", uint8TestSlice_2, uint8LessTestMatch_2, 0, uint8LessTestResult_2, 31),
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

// -----------------------------------------------------------------------------
// Less benchmarks
//
func BenchmarkMatchUint8LessGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8LessThanGeneric(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//

var uint8LessEqualCases = []Uint8MatchTest{
	{
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
	},
	CreateUint8TestCase("vec1", uint8TestSlice_0, uint8LessEqualTestMatch_0, 0, uint8LessEqualTestResult_0, 32),
	CreateUint8TestCase("vec2", uint8TestSlice_0, uint8LessEqualTestMatch_0, 0, uint8LessEqualTestResult_0, 512),
	CreateUint8TestCase("l32", uint8TestSlice_1, uint8LessEqualTestMatch_1, 0, uint8LessEqualTestResult_1, 32),
	CreateUint8TestCase("l64", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessEqualTestMatch_1, 0,
		append(uint8LessEqualTestResult_1, uint8LessEqualTestResult_0...), 64),
	CreateUint8TestCase("l128", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessEqualTestMatch_1, 0,
		append(uint8LessEqualTestResult_1, uint8LessEqualTestResult_0...), 128),
	CreateUint8TestCase("l256", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessEqualTestMatch_1, 0,
		append(uint8LessEqualTestResult_1, uint8LessEqualTestResult_0...), 256),
	CreateUint8TestCase("l512", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessEqualTestMatch_1, 0,
		append(uint8LessEqualTestResult_1, uint8LessEqualTestResult_0...), 512),
	CreateUint8TestCase("l1024", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessEqualTestMatch_1, 0,
		append(uint8LessEqualTestResult_1, uint8LessEqualTestResult_0...), 1024),
	CreateUint8TestCase("l1023", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessEqualTestMatch_1, 0,
		append(uint8LessEqualTestResult_1, uint8LessEqualTestResult_0...), 1023),
	CreateUint8TestCase("l511", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessEqualTestMatch_1, 0,
		append(uint8LessEqualTestResult_1, uint8LessEqualTestResult_0...), 511),
	CreateUint8TestCase("l255", append(uint8TestSlice_1, uint8TestSlice_0...), uint8LessEqualTestMatch_1, 0,
		append(uint8LessEqualTestResult_1, uint8LessEqualTestResult_0...), 255),
	CreateUint8TestCase("l127", uint8TestSlice_1, uint8LessEqualTestMatch_1, 0, uint8LessEqualTestResult_1, 127),
	CreateUint8TestCase("l63", uint8TestSlice_1, uint8LessEqualTestMatch_1, 0, uint8LessEqualTestResult_1, 63),
	CreateUint8TestCase("l31", uint8TestSlice_1, uint8LessEqualTestMatch_1, 0, uint8LessEqualTestResult_1, 31),
	CreateUint8TestCase("l23", uint8TestSlice_1, uint8LessEqualTestMatch_1, 0, uint8LessEqualTestResult_1, 23),
	CreateUint8TestCase("l15", uint8TestSlice_1, uint8LessEqualTestMatch_1, 0, uint8LessEqualTestResult_1, 15),
	CreateUint8TestCase("l7", uint8TestSlice_1, uint8LessEqualTestMatch_1, 0, uint8LessEqualTestResult_1, 7),
	// with extreme values
	CreateUint8TestCase("ext512", uint8TestSlice_2, uint8LessEqualTestMatch_2, 0, uint8LessEqualTestResult_2, 512),
	CreateUint8TestCase("ext32", uint8TestSlice_2, uint8LessEqualTestMatch_2, 0, uint8LessEqualTestResult_2, 32),
	CreateUint8TestCase("ext31", uint8TestSlice_2, uint8LessEqualTestMatch_2, 0, uint8LessEqualTestResult_2, 31),
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

// -----------------------------------------------------------------------------
// Less equal benchmarks
//
func BenchmarkMatchUint8LessEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8LessThanEqualGeneric(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
//

var uint8GreaterCases = []Uint8MatchTest{
	{
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
	},
	CreateUint8TestCase("vec1", uint8TestSlice_0, uint8GreaterTestMatch_0, 0, uint8GreaterTestResult_0, 32),
	CreateUint8TestCase("vec2", uint8TestSlice_0, uint8GreaterTestMatch_0, 0, uint8GreaterTestResult_0, 512),
	CreateUint8TestCase("l32", uint8TestSlice_1, uint8GreaterTestMatch_1, 0, uint8GreaterTestResult_1, 32),
	CreateUint8TestCase("l64", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterTestMatch_1, 0,
		append(uint8GreaterTestResult_1, uint8GreaterTestResult_0...), 64),
	CreateUint8TestCase("l128", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterTestMatch_1, 0,
		append(uint8GreaterTestResult_1, uint8GreaterTestResult_0...), 128),
	CreateUint8TestCase("l256", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterTestMatch_1, 0,
		append(uint8GreaterTestResult_1, uint8GreaterTestResult_0...), 256),
	CreateUint8TestCase("l512", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterTestMatch_1, 0,
		append(uint8GreaterTestResult_1, uint8GreaterTestResult_0...), 512),
	CreateUint8TestCase("l1024", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterTestMatch_1, 0,
		append(uint8GreaterTestResult_1, uint8GreaterTestResult_0...), 1024),
	CreateUint8TestCase("l1023", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterTestMatch_1, 0,
		append(uint8GreaterTestResult_1, uint8GreaterTestResult_0...), 1023),
	CreateUint8TestCase("l511", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterTestMatch_1, 0,
		append(uint8GreaterTestResult_1, uint8GreaterTestResult_0...), 511),
	CreateUint8TestCase("l255", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterTestMatch_1, 0,
		append(uint8GreaterTestResult_1, uint8GreaterTestResult_0...), 255),
	CreateUint8TestCase("l127", uint8TestSlice_1, uint8GreaterTestMatch_1, 0, uint8GreaterTestResult_1, 127),
	CreateUint8TestCase("l63", uint8TestSlice_1, uint8GreaterTestMatch_1, 0, uint8GreaterTestResult_1, 63),
	CreateUint8TestCase("l31", uint8TestSlice_1, uint8GreaterTestMatch_1, 0, uint8GreaterTestResult_1, 31),
	CreateUint8TestCase("l23", uint8TestSlice_1, uint8GreaterTestMatch_1, 0, uint8GreaterTestResult_1, 23),
	CreateUint8TestCase("l15", uint8TestSlice_1, uint8GreaterTestMatch_1, 0, uint8GreaterTestResult_1, 15),
	CreateUint8TestCase("l7", uint8TestSlice_1, uint8GreaterTestMatch_1, 0, uint8GreaterTestResult_1, 7),
	// with extreme values
	CreateUint8TestCase("ext512", uint8TestSlice_2, uint8GreaterTestMatch_2, 0, uint8GreaterTestResult_2, 512),
	CreateUint8TestCase("ext32", uint8TestSlice_2, uint8GreaterTestMatch_2, 0, uint8GreaterTestResult_2, 32),
	CreateUint8TestCase("ext31", uint8TestSlice_2, uint8GreaterTestMatch_2, 0, uint8GreaterTestResult_2, 31),
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

// -----------------------------------------------------------------------------
// Greater benchmarks
//
func BenchmarkMatchUint8GreaterGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8GreaterThanGeneric(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//

var uint8GreaterEqualCases = []Uint8MatchTest{
	{
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
	},
	CreateUint8TestCase("vec1", uint8TestSlice_0, uint8GreaterEqualTestMatch_0, 0, uint8GreaterEqualTestResult_0, 32),
	CreateUint8TestCase("vec2", uint8TestSlice_0, uint8GreaterEqualTestMatch_0, 0, uint8GreaterEqualTestResult_0, 512),
	CreateUint8TestCase("l32", uint8TestSlice_1, uint8GreaterEqualTestMatch_1, 0, uint8GreaterEqualTestResult_1, 32),
	CreateUint8TestCase("l64", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterEqualTestMatch_1, 0,
		append(uint8GreaterEqualTestResult_1, uint8GreaterEqualTestResult_0...), 64),
	CreateUint8TestCase("l128", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterEqualTestMatch_1, 0,
		append(uint8GreaterEqualTestResult_1, uint8GreaterEqualTestResult_0...), 128),
	CreateUint8TestCase("l256", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterEqualTestMatch_1, 0,
		append(uint8GreaterEqualTestResult_1, uint8GreaterEqualTestResult_0...), 256),
	CreateUint8TestCase("l512", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterEqualTestMatch_1, 0,
		append(uint8GreaterEqualTestResult_1, uint8GreaterEqualTestResult_0...), 512),
	CreateUint8TestCase("l1024", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterEqualTestMatch_1, 0,
		append(uint8GreaterEqualTestResult_1, uint8GreaterEqualTestResult_0...), 1024),
	CreateUint8TestCase("l1023", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterEqualTestMatch_1, 0,
		append(uint8GreaterEqualTestResult_1, uint8GreaterEqualTestResult_0...), 1023),
	CreateUint8TestCase("l511", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterEqualTestMatch_1, 0,
		append(uint8GreaterEqualTestResult_1, uint8GreaterEqualTestResult_0...), 511),
	CreateUint8TestCase("l255", append(uint8TestSlice_1, uint8TestSlice_0...), uint8GreaterEqualTestMatch_1, 0,
		append(uint8GreaterEqualTestResult_1, uint8GreaterEqualTestResult_0...), 255),
	CreateUint8TestCase("l127", uint8TestSlice_1, uint8GreaterEqualTestMatch_1, 0, uint8GreaterEqualTestResult_1, 127),
	CreateUint8TestCase("l63", uint8TestSlice_1, uint8GreaterEqualTestMatch_1, 0, uint8GreaterEqualTestResult_1, 63),
	CreateUint8TestCase("l31", uint8TestSlice_1, uint8GreaterEqualTestMatch_1, 0, uint8GreaterEqualTestResult_1, 31),
	CreateUint8TestCase("l23", uint8TestSlice_1, uint8GreaterEqualTestMatch_1, 0, uint8GreaterEqualTestResult_1, 23),
	CreateUint8TestCase("l15", uint8TestSlice_1, uint8GreaterEqualTestMatch_1, 0, uint8GreaterEqualTestResult_1, 15),
	CreateUint8TestCase("l7", uint8TestSlice_1, uint8GreaterEqualTestMatch_1, 0, uint8GreaterEqualTestResult_1, 7),
	// with extreme values
	CreateUint8TestCase("ext512", uint8TestSlice_2, uint8GreaterEqualTestMatch_2, 0, uint8GreaterEqualTestResult_2, 512),
	CreateUint8TestCase("ext32", uint8TestSlice_2, uint8GreaterEqualTestMatch_2, 0, uint8GreaterEqualTestResult_2, 32),
	CreateUint8TestCase("ext31", uint8TestSlice_2, uint8GreaterEqualTestMatch_2, 0, uint8GreaterEqualTestResult_2, 31),
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

// -----------------------------------------------------------------------------
// Greater equal benchmarks
//
func BenchmarkMatchUint8GreaterEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8GreaterThanEqualGeneric(a, math.MaxUint8/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
//
var uint8BetweenCases = []Uint8MatchTest{
	{
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
	},
	CreateUint8TestCase("vec1", uint8TestSlice_0, uint8BetweenTestMatch_0, uint8BetweenTestMatch_0b, uint8BetweenTestResult_0, 32),
	CreateUint8TestCase("vec2", uint8TestSlice_0, uint8BetweenTestMatch_0, uint8BetweenTestMatch_0b, uint8BetweenTestResult_0, 512),
	CreateUint8TestCase("l32", uint8TestSlice_1, uint8BetweenTestMatch_1, uint8BetweenTestMatch_1b, uint8BetweenTestResult_1, 32),
	CreateUint8TestCase("l64", append(uint8TestSlice_1, uint8TestSlice_0...), uint8BetweenTestMatch_1, uint8BetweenTestMatch_1b,
		append(uint8BetweenTestResult_1, uint8BetweenTestResult_0...), 64),
	CreateUint8TestCase("l128", append(uint8TestSlice_1, uint8TestSlice_0...), uint8BetweenTestMatch_1, uint8BetweenTestMatch_1b,
		append(uint8BetweenTestResult_1, uint8BetweenTestResult_0...), 128),
	CreateUint8TestCase("l256", append(uint8TestSlice_1, uint8TestSlice_0...), uint8BetweenTestMatch_1, uint8BetweenTestMatch_1b,
		append(uint8BetweenTestResult_1, uint8BetweenTestResult_0...), 256),
	CreateUint8TestCase("l512", append(uint8TestSlice_1, uint8TestSlice_0...), uint8BetweenTestMatch_1, uint8BetweenTestMatch_1b,
		append(uint8BetweenTestResult_1, uint8BetweenTestResult_0...), 512),
	CreateUint8TestCase("l1024", append(uint8TestSlice_1, uint8TestSlice_0...), uint8BetweenTestMatch_1, uint8BetweenTestMatch_1b,
		append(uint8BetweenTestResult_1, uint8BetweenTestResult_0...), 1024),
	CreateUint8TestCase("l1023", append(uint8TestSlice_1, uint8TestSlice_0...), uint8BetweenTestMatch_1, uint8BetweenTestMatch_1b,
		append(uint8BetweenTestResult_1, uint8BetweenTestResult_0...), 1023),
	CreateUint8TestCase("l511", append(uint8TestSlice_1, uint8TestSlice_0...), uint8BetweenTestMatch_1, uint8BetweenTestMatch_1b,
		append(uint8BetweenTestResult_1, uint8BetweenTestResult_0...), 511),
	CreateUint8TestCase("l255", append(uint8TestSlice_1, uint8TestSlice_0...), uint8BetweenTestMatch_1, uint8BetweenTestMatch_1b,
		append(uint8BetweenTestResult_1, uint8BetweenTestResult_0...), 255),
	CreateUint8TestCase("l127", uint8TestSlice_1, uint8BetweenTestMatch_1, uint8BetweenTestMatch_1b, uint8BetweenTestResult_1, 127),
	CreateUint8TestCase("l63", uint8TestSlice_1, uint8BetweenTestMatch_1, uint8BetweenTestMatch_1b, uint8BetweenTestResult_1, 63),
	CreateUint8TestCase("l31", uint8TestSlice_1, uint8BetweenTestMatch_1, uint8BetweenTestMatch_1b, uint8BetweenTestResult_1, 31),
	CreateUint8TestCase("l23", uint8TestSlice_1, uint8BetweenTestMatch_1, uint8BetweenTestMatch_1b, uint8BetweenTestResult_1, 23),
	CreateUint8TestCase("l15", uint8TestSlice_1, uint8BetweenTestMatch_1, uint8BetweenTestMatch_1b, uint8BetweenTestResult_1, 15),
	CreateUint8TestCase("l7", uint8TestSlice_1, uint8BetweenTestMatch_1, uint8BetweenTestMatch_1b, uint8BetweenTestResult_1, 7),
	// with extreme values
	CreateUint8TestCase("ext512", uint8TestSlice_2, uint8BetweenTestMatch_2, uint8BetweenTestMatch_2b, uint8BetweenTestResult_2, 512),
	CreateUint8TestCase("ext32", uint8TestSlice_2, uint8BetweenTestMatch_2, uint8BetweenTestMatch_2b, uint8BetweenTestResult_2, 32),
	CreateUint8TestCase("ext31", uint8TestSlice_2, uint8BetweenTestMatch_2, uint8BetweenTestMatch_2b, uint8BetweenTestResult_2, 31),
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

// -----------------------------------------------------------------------------
// Between benchmarks
//
func BenchmarkMatchUint8BetweenGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint8Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint8Size))
			for i := 0; i < B.N; i++ {
				matchUint8BetweenGeneric(a, math.MaxUint8/4, math.MaxUint8/2, bits)
			}
		})
	}
}

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
	if Uint8.Contains(nil, 1) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Uint8.Contains([]uint8{}, 1) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Uint8.Contains([]uint8{1}, 1) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Uint8.Contains([]uint8{1}, 2) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !Uint8.Contains([]uint8{1, 3, 5, 7, 11, 13}, 1) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Uint8.Contains([]uint8{1, 3, 5, 7, 11, 13}, 5) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Uint8.Contains([]uint8{1, 3, 5, 7, 11, 13}, 13) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Uint8.Contains([]uint8{1, 3, 5, 7, 11, 13}, 0) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Uint8.Contains([]uint8{1, 3, 5, 7, 11, 13}, 2) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Uint8.Contains([]uint8{1, 3, 5, 7, 11, 13}, 14) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkUint8SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Uint8.Sort(randUint8Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				Uint8.Contains(a, uint8(rand.Intn(math.MaxUint8+1)))
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Uint8.Sort(randUint8Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				Uint8.Contains(a, a[rand.Intn(len(a))])
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
			if want, got := r.Match, Uint8.ContainsRange(v.Slice, r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkUint8SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Uint8.Sort(randUint8Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := uint8(rand.Intn(math.MaxUint8+1)), uint8(rand.Intn(math.MaxUint8+1))
				if min > max {
					min, max = max, min
				}
				Uint8.ContainsRange(a, min, max)
			}
		})
	}
}
