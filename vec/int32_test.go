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

const Int32Size = 4

func randInt32Slice(n, u int) []int32 {
	s := make([]int32, n*u)
	for i := 0; i < n; i++ {
		s[i] = rand.Int31()
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

type Int32MatchTest struct {
	name   string
	slice  []int32
	match  int32 // used for every test
	match2 int32 // used for between tests
	result []byte
	count  int64
}

var (
	int32TestSlice_0 = []int32{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	int32EqualTestMatch_0  int32 = 5
	int32EqualTestResult_0       = []byte{0x56, 0x78, 0x12, 0x34}

	int32NotEqualTestMatch_0  int32 = 5
	int32NotEqualTestResult_0       = []byte{0xa9, 0x87, 0xed, 0xcb}

	int32LessTestMatch_0  int32 = 5
	int32LessTestResult_0       = []byte{0xa0, 0x84, 0xe4, 0x80}

	int32LessEqualTestMatch_0  int32 = 5
	int32LessEqualTestResult_0       = []byte{0xf6, 0xfc, 0xf6, 0xb4}

	int32GreaterTestMatch_0  int32 = 5
	int32GreaterTestResult_0       = []byte{0x09, 0x03, 0x09, 0x4b}

	int32GreaterEqualTestMatch_0  int32 = 5
	int32GreaterEqualTestResult_0       = []byte{0x5f, 0x7b, 0x1b, 0x7f}

	int32BetweenTestMatch_0  int32 = 5
	int32BetweenTestMatch_0b int32 = 10
	int32BetweenTestResult_0       = []byte{0x5f, 0x78, 0x1b, 0x34}

	// positive values only
	int32TestSlice_1 = []int32{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 500000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}
	int32EqualTestResult_1       = []byte{0x82, 0x42, 0x23, 0x70}
	int32EqualTestMatch_1  int32 = 5

	int32NotEqualTestResult_1       = []byte{0x7d, 0xbd, 0xdc, 0x8f}
	int32NotEqualTestMatch_1  int32 = 5

	int32LessTestResult_1       = []byte{0x70, 0x00, 0x00, 0x00}
	int32LessTestMatch_1  int32 = 5

	int32LessEqualTestResult_1       = []byte{0xf2, 0x42, 0x23, 0x70}
	int32LessEqualTestMatch_1  int32 = 5

	int32GreaterTestResult_1       = []byte{0x0d, 0xbd, 0xdc, 0x8f}
	int32GreaterTestMatch_1  int32 = 5

	int32GreaterEqualTestResult_1       = []byte{0x8f, 0xff, 0xff, 0xff}
	int32GreaterEqualTestMatch_1  int32 = 5

	int32BetweenTestResult_1       = []byte{0x8f, 0x42, 0x23, 0x70}
	int32BetweenTestMatch_1  int32 = 5
	int32BetweenTestMatch_1b int32 = 10

	// negative and positive values mixed
	int32TestSlice_2 = []int32{
		-5, 2, -3, 5,
		7, 8, 9, -10,
		15, 50, 55, 500,
		1000, -500000, 113, 12,
		31, 32, 33, 34,
		35, -36, 37, 38,
		39, 40, -41, 42,
		43, 44, 45, -46,
	}
	int32EqualTestResult_2       = []byte{0x80, 0x0, 0x0, 0x0}
	int32EqualTestMatch_2  int32 = -5

	int32NotEqualTestResult_2       = []byte{0x7f, 0xff, 0xff, 0xff}
	int32NotEqualTestMatch_2  int32 = -5

	int32LessTestResult_2       = []byte{0xe1, 0x04, 0x04, 0x21}
	int32LessTestMatch_2  int32 = 5

	int32LessEqualTestResult_2       = []byte{0xf1, 0x04, 0x04, 0x21}
	int32LessEqualTestMatch_2  int32 = 5

	int32GreaterTestResult_2       = []byte{0x0e, 0xfb, 0xfb, 0xde}
	int32GreaterTestMatch_2  int32 = 5

	int32GreaterEqualTestResult_2       = []byte{0x1e, 0xfb, 0xfb, 0xde}
	int32GreaterEqualTestMatch_2  int32 = 5

	int32BetweenTestResult_2       = []byte{0x1e, 0x00, 0x00, 0x00}
	int32BetweenTestMatch_2  int32 = 5
	int32BetweenTestMatch_2b int32 = 10

	// extreme values
	int32TestSlice_3 = []int32{
		0, 0,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
		0, 0,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
		0, 0,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
		0, 0,
		math.MaxInt8, math.MinInt8,
		math.MaxInt16, math.MinInt16,
		math.MaxInt32, math.MinInt32,
	}
	int32EqualTestResult_3       = []byte{0x01, 0x01, 0x01, 0x01}
	int32EqualTestMatch_3  int32 = math.MinInt32

	int32NotEqualTestResult_3       = []byte{0xfe, 0xfe, 0xfe, 0xfe}
	int32NotEqualTestMatch_3  int32 = math.MinInt32

	int32LessTestResult_3       = []byte{0x0, 0x0, 0x0, 0x00}
	int32LessTestMatch_3  int32 = math.MinInt32

	int32LessEqualTestResult_3       = []byte{0x01, 0x01, 0x01, 0x01}
	int32LessEqualTestMatch_3  int32 = math.MinInt32

	int32GreaterTestResult_3       = []byte{0xfe, 0xfe, 0xfe, 0xfe}
	int32GreaterTestMatch_3  int32 = math.MinInt32

	int32GreaterEqualTestResult_3       = []byte{0xff, 0xff, 0xff, 0xff}
	int32GreaterEqualTestMatch_3  int32 = math.MinInt32

	int32BetweenTestResult_3       = []byte{0x0a, 0x0a, 0x0a, 0x0a}
	int32BetweenTestMatch_3  int32 = math.MaxInt16
	int32BetweenTestMatch_3b int32 = math.MaxInt32
)

// creates an uint32 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - match, match2: are only copied to the resulting test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateInt32TestCase(name string, slice []int32, match, match2 int32, result []byte, length int) Int32MatchTest {
	if len(slice)%8 != 0 {
		panic("CreateInt32TestCase: length of slice has to be a multiple of 8")
	}
	if len(result) != bitFieldLen(len(slice)) {
		panic("CreateInt32TestCase: length of slice and length of result does not match")
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []int32
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
	return Int32MatchTest{
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
var int32EqualCases = []Int32MatchTest{
	{
		name:   "l0",
		slice:  make([]int32, 0),
		match:  int32EqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int32EqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt32TestCase("vec1", int32TestSlice_0, int32EqualTestMatch_0, 0, int32EqualTestResult_0, 32),
	CreateInt32TestCase("vec2", int32TestSlice_0, int32EqualTestMatch_0, 0, int32EqualTestResult_0, 64),
	CreateInt32TestCase("l32", int32TestSlice_1, int32EqualTestMatch_1, 0, int32EqualTestResult_1, 32),
	CreateInt32TestCase("l64", append(int32TestSlice_1, int32TestSlice_0...), int32EqualTestMatch_1, 0,
		append(int32EqualTestResult_1, int32EqualTestResult_0...), 64),
	CreateInt32TestCase("l128", append(int32TestSlice_1, int32TestSlice_0...), int32EqualTestMatch_1, 0,
		append(int32EqualTestResult_1, int32EqualTestResult_0...), 128),
	CreateInt32TestCase("l127", int32TestSlice_1, int32EqualTestMatch_1, 0, int32EqualTestResult_1, 127),
	CreateInt32TestCase("l63", int32TestSlice_1, int32EqualTestMatch_1, 0, int32EqualTestResult_1, 63),
	CreateInt32TestCase("l31", int32TestSlice_1, int32EqualTestMatch_1, 0, int32EqualTestResult_1, 31),
	CreateInt32TestCase("l23", int32TestSlice_1, int32EqualTestMatch_1, 0, int32EqualTestResult_1, 23),
	CreateInt32TestCase("l15", int32TestSlice_1, int32EqualTestMatch_1, 0, int32EqualTestResult_1, 15),
	CreateInt32TestCase("l7", int32TestSlice_1, int32EqualTestMatch_1, 0, int32EqualTestResult_1, 7),
	CreateInt32TestCase("neg64", int32TestSlice_2, int32EqualTestMatch_2, 0, int32EqualTestResult_2, 64),
	CreateInt32TestCase("neg32", int32TestSlice_2, int32EqualTestMatch_2, 0, int32EqualTestResult_2, 32),
	CreateInt32TestCase("neg31", int32TestSlice_2, int32EqualTestMatch_2, 0, int32EqualTestResult_2, 31),
	CreateInt32TestCase("ext64", int32TestSlice_3, int32EqualTestMatch_3, 0, int32EqualTestResult_3, 64),
	CreateInt32TestCase("ext32", int32TestSlice_3, int32EqualTestMatch_3, 0, int32EqualTestResult_3, 32),
	CreateInt32TestCase("ext31", int32TestSlice_3, int32EqualTestMatch_3, 0, int32EqualTestResult_3, 31),
}

func TestMatchInt32EqualGeneric(T *testing.T) {
	for _, c := range int32EqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt32EqualGeneric(c.slice, c.match, bits)
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

/*func TestMatchInt32EqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range int32EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt32EqualAVX2(c.slice, c.match, bits)
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
func TestMatchInt32EqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range int32EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt32EqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt32EqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32EqualGeneric(a, math.MaxInt32/2, bits)
			}
		})
	}
}

/*func BenchmarkMatchInt32EqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32EqualAVX2(a, math.MaxInt32/2, bits)
			}
		})
	}
}*/

func BenchmarkMatchInt32EqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32EqualAVX512(a, math.MaxInt32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
//
var int32NotEqualCases = []Int32MatchTest{
	{
		name:   "l0",
		slice:  make([]int32, 0),
		match:  int32NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int32NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt32TestCase("vec1", int32TestSlice_0, int32NotEqualTestMatch_0, 0, int32NotEqualTestResult_0, 32),
	CreateInt32TestCase("vec2", int32TestSlice_0, int32NotEqualTestMatch_0, 0, int32NotEqualTestResult_0, 64),
	CreateInt32TestCase("l32", int32TestSlice_1, int32NotEqualTestMatch_1, 0, int32NotEqualTestResult_1, 32),
	CreateInt32TestCase("l64", append(int32TestSlice_1, int32TestSlice_0...), int32NotEqualTestMatch_1, 0,
		append(int32NotEqualTestResult_1, int32NotEqualTestResult_0...), 64),
	CreateInt32TestCase("l128", append(int32TestSlice_1, int32TestSlice_0...), int32NotEqualTestMatch_1, 0,
		append(int32NotEqualTestResult_1, int32NotEqualTestResult_0...), 128),
	CreateInt32TestCase("l127", int32TestSlice_1, int32NotEqualTestMatch_1, 0, int32NotEqualTestResult_1, 127),
	CreateInt32TestCase("l63", int32TestSlice_1, int32NotEqualTestMatch_1, 0, int32NotEqualTestResult_1, 63),
	CreateInt32TestCase("l31", int32TestSlice_1, int32NotEqualTestMatch_1, 0, int32NotEqualTestResult_1, 31),
	CreateInt32TestCase("l23", int32TestSlice_1, int32NotEqualTestMatch_1, 0, int32NotEqualTestResult_1, 23),
	CreateInt32TestCase("l15", int32TestSlice_1, int32NotEqualTestMatch_1, 0, int32NotEqualTestResult_1, 15),
	CreateInt32TestCase("l7", int32TestSlice_1, int32NotEqualTestMatch_1, 0, int32NotEqualTestResult_1, 7),
	CreateInt32TestCase("neg64", int32TestSlice_2, int32NotEqualTestMatch_2, 0, int32NotEqualTestResult_2, 64),
	CreateInt32TestCase("neg32", int32TestSlice_2, int32NotEqualTestMatch_2, 0, int32NotEqualTestResult_2, 32),
	CreateInt32TestCase("neg31", int32TestSlice_2, int32NotEqualTestMatch_2, 0, int32NotEqualTestResult_2, 31),
	CreateInt32TestCase("ext64", int32TestSlice_3, int32NotEqualTestMatch_3, 0, int32NotEqualTestResult_3, 64),
	CreateInt32TestCase("ext32", int32TestSlice_3, int32NotEqualTestMatch_3, 0, int32NotEqualTestResult_3, 32),
	CreateInt32TestCase("ext31", int32TestSlice_3, int32NotEqualTestMatch_3, 0, int32NotEqualTestResult_3, 31),
}

func TestMatchInt32NotEqualGeneric(T *testing.T) {
	for _, c := range int32NotEqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt32NotEqualGeneric(c.slice, c.match, bits)
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

/*func TestMatchInt32NotEqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range int32NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt32NotEqualAVX2(c.slice, c.match, bits)
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
func TestMatchInt32NotEqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range int32NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt32NotEqualAVX512(c.slice, c.match, bits)
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
//
func BenchmarkMatchInt32NotEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32NotEqualGeneric(a, math.MaxInt32/2, bits)
			}
		})
	}
}

/*func BenchmarkMatchInt32NotEqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32NotEqualAVX2(a, math.MaxInt32/2, bits)
			}
		})
	}
}*/

func BenchmarkMatchInt32NotEqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32NotEqualAVX512(a, math.MaxInt32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
//
var int32LessCases = []Int32MatchTest{
	{
		name:   "l0",
		slice:  make([]int32, 0),
		match:  int32LessTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int32LessTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt32TestCase("vec1", int32TestSlice_0, int32LessTestMatch_0, 0, int32LessTestResult_0, 32),
	CreateInt32TestCase("vec2", int32TestSlice_0, int32LessTestMatch_0, 0, int32LessTestResult_0, 64),
	CreateInt32TestCase("l32", int32TestSlice_1, int32LessTestMatch_1, 0, int32LessTestResult_1, 32),
	CreateInt32TestCase("l64", append(int32TestSlice_1, int32TestSlice_0...), int32LessTestMatch_1, 0,
		append(int32LessTestResult_1, int32LessTestResult_0...), 64),
	CreateInt32TestCase("l128", append(int32TestSlice_1, int32TestSlice_0...), int32LessTestMatch_1, 0,
		append(int32LessTestResult_1, int32LessTestResult_0...), 128),
	CreateInt32TestCase("l127", int32TestSlice_1, int32LessTestMatch_1, 0, int32LessTestResult_1, 127),
	CreateInt32TestCase("l63", int32TestSlice_1, int32LessTestMatch_1, 0, int32LessTestResult_1, 63),
	CreateInt32TestCase("l31", int32TestSlice_1, int32LessTestMatch_1, 0, int32LessTestResult_1, 31),
	CreateInt32TestCase("l23", int32TestSlice_1, int32LessTestMatch_1, 0, int32LessTestResult_1, 23),
	CreateInt32TestCase("l15", int32TestSlice_1, int32LessTestMatch_1, 0, int32LessTestResult_1, 15),
	CreateInt32TestCase("l7", int32TestSlice_1, int32LessTestMatch_1, 0, int32LessTestResult_1, 7),
	CreateInt32TestCase("neg64", int32TestSlice_2, int32LessTestMatch_2, 0, int32LessTestResult_2, 64),
	CreateInt32TestCase("neg32", int32TestSlice_2, int32LessTestMatch_2, 0, int32LessTestResult_2, 32),
	CreateInt32TestCase("neg31", int32TestSlice_2, int32LessTestMatch_2, 0, int32LessTestResult_2, 31),
	CreateInt32TestCase("ext64", int32TestSlice_3, int32LessTestMatch_3, 0, int32LessTestResult_3, 64),
	CreateInt32TestCase("ext32", int32TestSlice_3, int32LessTestMatch_3, 0, int32LessTestResult_3, 32),
	CreateInt32TestCase("ext31", int32TestSlice_3, int32LessTestMatch_3, 0, int32LessTestResult_3, 31),
}

func TestMatchInt32LessGeneric(T *testing.T) {
	for _, c := range int32LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt32LessThanGeneric(c.slice, c.match, bits)
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

/*func TestMatchInt32LessAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range int32LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt32LessThanAVX2(c.slice, c.match, bits)
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
}*/

func TestMatchInt32LessAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range int32LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt32LessThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt32LessGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32LessThanGeneric(a, math.MaxInt32/2, bits)
			}
		})
	}
}

/*func BenchmarkMatchInt32LessAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32LessThanAVX2(a, math.MaxInt32/2, bits)
			}
		})
	}
}*/

func BenchmarkMatchInt32LessAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32LessThanAVX512(a, math.MaxInt32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//
var int32LessEqualCases = []Int32MatchTest{
	{
		name:   "l0",
		slice:  make([]int32, 0),
		match:  int32LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int32LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt32TestCase("vec1", int32TestSlice_0, int32LessEqualTestMatch_0, 0, int32LessEqualTestResult_0, 32),
	CreateInt32TestCase("vec2", int32TestSlice_0, int32LessEqualTestMatch_0, 0, int32LessEqualTestResult_0, 64),
	CreateInt32TestCase("l32", int32TestSlice_1, int32LessEqualTestMatch_1, 0, int32LessEqualTestResult_1, 32),
	CreateInt32TestCase("l64", append(int32TestSlice_1, int32TestSlice_0...), int32LessEqualTestMatch_1, 0,
		append(int32LessEqualTestResult_1, int32LessEqualTestResult_0...), 64),
	CreateInt32TestCase("l128", append(int32TestSlice_1, int32TestSlice_0...), int32LessEqualTestMatch_1, 0,
		append(int32LessEqualTestResult_1, int32LessEqualTestResult_0...), 128),
	CreateInt32TestCase("l127", int32TestSlice_1, int32LessEqualTestMatch_1, 0, int32LessEqualTestResult_1, 127),
	CreateInt32TestCase("l63", int32TestSlice_1, int32LessEqualTestMatch_1, 0, int32LessEqualTestResult_1, 63),
	CreateInt32TestCase("l31", int32TestSlice_1, int32LessEqualTestMatch_1, 0, int32LessEqualTestResult_1, 31),
	CreateInt32TestCase("l23", int32TestSlice_1, int32LessEqualTestMatch_1, 0, int32LessEqualTestResult_1, 23),
	CreateInt32TestCase("l15", int32TestSlice_1, int32LessEqualTestMatch_1, 0, int32LessEqualTestResult_1, 15),
	CreateInt32TestCase("l7", int32TestSlice_1, int32LessEqualTestMatch_1, 0, int32LessEqualTestResult_1, 7),
	CreateInt32TestCase("neg64", int32TestSlice_2, int32LessEqualTestMatch_2, 0, int32LessEqualTestResult_2, 64),
	CreateInt32TestCase("neg32", int32TestSlice_2, int32LessEqualTestMatch_2, 0, int32LessEqualTestResult_2, 32),
	CreateInt32TestCase("neg31", int32TestSlice_2, int32LessEqualTestMatch_2, 0, int32LessEqualTestResult_2, 31),
	CreateInt32TestCase("ext64", int32TestSlice_3, int32LessEqualTestMatch_3, 0, int32LessEqualTestResult_3, 64),
	CreateInt32TestCase("ext32", int32TestSlice_3, int32LessEqualTestMatch_3, 0, int32LessEqualTestResult_3, 32),
	CreateInt32TestCase("ext31", int32TestSlice_3, int32LessEqualTestMatch_3, 0, int32LessEqualTestResult_3, 31),
}

func TestMatchInt32LessEqualGeneric(T *testing.T) {
	for _, c := range int32LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt32LessThanEqualGeneric(c.slice, c.match, bits)
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

/*func TestMatchInt32LessEqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range int32LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt32LessThanEqualAVX2(c.slice, c.match, bits)
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
func TestMatchInt32LessEqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range int32LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt32LessThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt32LessEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32LessThanEqualGeneric(a, math.MaxInt32/2, bits)
			}
		})
	}
}

/*func BenchmarkMatchInt32LessEqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32LessThanEqualAVX2(a, math.MaxInt32/2, bits)
			}
		})
	}
}*/

func BenchmarkMatchInt32LessEqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32LessThanEqualAVX512(a, math.MaxInt32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
//
var int32GreaterCases = []Int32MatchTest{
	{
		name:   "l0",
		slice:  make([]int32, 0),
		match:  int32GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int32GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt32TestCase("vec1", int32TestSlice_0, int32GreaterTestMatch_0, 0, int32GreaterTestResult_0, 32),
	CreateInt32TestCase("vec2", int32TestSlice_0, int32GreaterTestMatch_0, 0, int32GreaterTestResult_0, 64),
	CreateInt32TestCase("l32", int32TestSlice_1, int32GreaterTestMatch_1, 0, int32GreaterTestResult_1, 32),
	CreateInt32TestCase("l64", append(int32TestSlice_1, int32TestSlice_0...), int32GreaterTestMatch_1, 0,
		append(int32GreaterTestResult_1, int32GreaterTestResult_0...), 64),
	CreateInt32TestCase("l128", append(int32TestSlice_1, int32TestSlice_0...), int32GreaterTestMatch_1, 0,
		append(int32GreaterTestResult_1, int32GreaterTestResult_0...), 128),
	CreateInt32TestCase("l127", int32TestSlice_1, int32GreaterTestMatch_1, 0, int32GreaterTestResult_1, 127),
	CreateInt32TestCase("l63", int32TestSlice_1, int32GreaterTestMatch_1, 0, int32GreaterTestResult_1, 63),
	CreateInt32TestCase("l31", int32TestSlice_1, int32GreaterTestMatch_1, 0, int32GreaterTestResult_1, 31),
	CreateInt32TestCase("l23", int32TestSlice_1, int32GreaterTestMatch_1, 0, int32GreaterTestResult_1, 23),
	CreateInt32TestCase("l15", int32TestSlice_1, int32GreaterTestMatch_1, 0, int32GreaterTestResult_1, 15),
	CreateInt32TestCase("l7", int32TestSlice_1, int32GreaterTestMatch_1, 0, int32GreaterTestResult_1, 7),
	CreateInt32TestCase("neg64", int32TestSlice_2, int32GreaterTestMatch_2, 0, int32GreaterTestResult_2, 64),
	CreateInt32TestCase("neg32", int32TestSlice_2, int32GreaterTestMatch_2, 0, int32GreaterTestResult_2, 32),
	CreateInt32TestCase("neg31", int32TestSlice_2, int32GreaterTestMatch_2, 0, int32GreaterTestResult_2, 31),
	CreateInt32TestCase("ext64", int32TestSlice_3, int32GreaterTestMatch_3, 0, int32GreaterTestResult_3, 64),
	CreateInt32TestCase("ext32", int32TestSlice_3, int32GreaterTestMatch_3, 0, int32GreaterTestResult_3, 32),
	CreateInt32TestCase("ext31", int32TestSlice_3, int32GreaterTestMatch_3, 0, int32GreaterTestResult_3, 31),
}

func TestMatchInt32GreaterGeneric(T *testing.T) {
	for _, c := range int32GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt32GreaterThanGeneric(c.slice, c.match, bits)
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

/*func TestMatchInt32GreaterAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range int32GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt32GreaterThanAVX2(c.slice, c.match, bits)
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
}*/

func TestMatchInt32GreaterAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range int32GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt32GreaterThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt32GreaterGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32GreaterThanGeneric(a, math.MaxInt32/2, bits)
			}
		})
	}
}

/*func BenchmarkMatchInt32GreaterAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32GreaterThanAVX2(a, math.MaxInt32/2, bits)
			}
		})
	}
}*/

func BenchmarkMatchInt32GreaterAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32GreaterThanAVX512(a, math.MaxInt32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//
var int32GreaterEqualCases = []Int32MatchTest{
	{
		name:   "l0",
		slice:  make([]int32, 0),
		match:  int32GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int32GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt32TestCase("vec1", int32TestSlice_0, int32GreaterEqualTestMatch_0, 0, int32GreaterEqualTestResult_0, 32),
	CreateInt32TestCase("vec2", int32TestSlice_0, int32GreaterEqualTestMatch_0, 0, int32GreaterEqualTestResult_0, 64),
	CreateInt32TestCase("l32", int32TestSlice_1, int32GreaterEqualTestMatch_1, 0, int32GreaterEqualTestResult_1, 32),
	CreateInt32TestCase("l64", append(int32TestSlice_1, int32TestSlice_0...), int32GreaterEqualTestMatch_1, 0,
		append(int32GreaterEqualTestResult_1, int32GreaterEqualTestResult_0...), 64),
	CreateInt32TestCase("l128", append(int32TestSlice_1, int32TestSlice_0...), int32GreaterEqualTestMatch_1, 0,
		append(int32GreaterEqualTestResult_1, int32GreaterEqualTestResult_0...), 128),
	CreateInt32TestCase("l127", int32TestSlice_1, int32GreaterEqualTestMatch_1, 0, int32GreaterEqualTestResult_1, 127),
	CreateInt32TestCase("l63", int32TestSlice_1, int32GreaterEqualTestMatch_1, 0, int32GreaterEqualTestResult_1, 63),
	CreateInt32TestCase("l31", int32TestSlice_1, int32GreaterEqualTestMatch_1, 0, int32GreaterEqualTestResult_1, 31),
	CreateInt32TestCase("l23", int32TestSlice_1, int32GreaterEqualTestMatch_1, 0, int32GreaterEqualTestResult_1, 23),
	CreateInt32TestCase("l15", int32TestSlice_1, int32GreaterEqualTestMatch_1, 0, int32GreaterEqualTestResult_1, 15),
	CreateInt32TestCase("l7", int32TestSlice_1, int32GreaterEqualTestMatch_1, 0, int32GreaterEqualTestResult_1, 7),
	CreateInt32TestCase("neg64", int32TestSlice_2, int32GreaterEqualTestMatch_2, 0, int32GreaterEqualTestResult_2, 64),
	CreateInt32TestCase("neg32", int32TestSlice_2, int32GreaterEqualTestMatch_2, 0, int32GreaterEqualTestResult_2, 32),
	CreateInt32TestCase("neg31", int32TestSlice_2, int32GreaterEqualTestMatch_2, 0, int32GreaterEqualTestResult_2, 31),
	CreateInt32TestCase("ext64", int32TestSlice_3, int32GreaterEqualTestMatch_3, 0, int32GreaterEqualTestResult_3, 64),
	CreateInt32TestCase("ext32", int32TestSlice_3, int32GreaterEqualTestMatch_3, 0, int32GreaterEqualTestResult_3, 32),
	CreateInt32TestCase("ext31", int32TestSlice_3, int32GreaterEqualTestMatch_3, 0, int32GreaterEqualTestResult_3, 31),
}

func TestMatchInt32GreaterEqualGeneric(T *testing.T) {
	for _, c := range int32GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt32GreaterThanEqualGeneric(c.slice, c.match, bits)
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

/*func TestMatchInt32GreaterEqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range int32GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt32GreaterThanEqualAVX2(c.slice, c.match, bits)
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
}*/

func TestMatchInt32GreaterEqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range int32GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt32GreaterThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt32GreaterEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32GreaterThanEqualGeneric(a, math.MaxInt32/2, bits)
			}
		})
	}
}

/*func BenchmarkMatchInt32GreaterEqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32GreaterThanEqualAVX2(a, math.MaxInt32/2, bits)
			}
		})
	}
}*/

func BenchmarkMatchInt32GreaterEqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32GreaterThanEqualAVX512(a, math.MaxInt32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
//
var int32BetweenCases = []Int32MatchTest{
	{
		name:   "l0",
		slice:  make([]int32, 0),
		match:  int32BetweenTestMatch_1,
		match2: int32BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  int32BetweenTestMatch_1,
		match2: int32BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	},
	CreateInt32TestCase("vec1", int32TestSlice_0, int32BetweenTestMatch_0, int32BetweenTestMatch_0b, int32BetweenTestResult_0, 32),
	CreateInt32TestCase("vec2", int32TestSlice_0, int32BetweenTestMatch_0, int32BetweenTestMatch_0b, int32BetweenTestResult_0, 64),
	CreateInt32TestCase("l32", int32TestSlice_1, int32BetweenTestMatch_1, int32BetweenTestMatch_1b, int32BetweenTestResult_1, 32),
	CreateInt32TestCase("l64", append(int32TestSlice_1, int32TestSlice_0...), int32BetweenTestMatch_1, int32BetweenTestMatch_1b,
		append(int32BetweenTestResult_1, int32BetweenTestResult_0...), 64),
	CreateInt32TestCase("l128", append(int32TestSlice_1, int32TestSlice_0...), int32BetweenTestMatch_1, int32BetweenTestMatch_1b,
		append(int32BetweenTestResult_1, int32BetweenTestResult_0...), 128),
	CreateInt32TestCase("l127", int32TestSlice_1, int32BetweenTestMatch_1, int32BetweenTestMatch_1b, int32BetweenTestResult_1, 127),
	CreateInt32TestCase("l63", int32TestSlice_1, int32BetweenTestMatch_1, int32BetweenTestMatch_1b, int32BetweenTestResult_1, 63),
	CreateInt32TestCase("l31", int32TestSlice_1, int32BetweenTestMatch_1, int32BetweenTestMatch_1b, int32BetweenTestResult_1, 31),
	CreateInt32TestCase("l23", int32TestSlice_1, int32BetweenTestMatch_1, int32BetweenTestMatch_1b, int32BetweenTestResult_1, 23),
	CreateInt32TestCase("l15", int32TestSlice_1, int32BetweenTestMatch_1, int32BetweenTestMatch_1b, int32BetweenTestResult_1, 15),
	CreateInt32TestCase("l7", int32TestSlice_1, int32BetweenTestMatch_1, int32BetweenTestMatch_1b, int32BetweenTestResult_1, 7),
	CreateInt32TestCase("neg64", int32TestSlice_2, int32BetweenTestMatch_2, int32BetweenTestMatch_2b, int32BetweenTestResult_2, 64),
	CreateInt32TestCase("neg32", int32TestSlice_2, int32BetweenTestMatch_2, int32BetweenTestMatch_2b, int32BetweenTestResult_2, 32),
	CreateInt32TestCase("neg31", int32TestSlice_2, int32BetweenTestMatch_2, int32BetweenTestMatch_2b, int32BetweenTestResult_2, 31),
	CreateInt32TestCase("ext64", int32TestSlice_3, int32BetweenTestMatch_3, int32BetweenTestMatch_3b, int32BetweenTestResult_3, 64),
	CreateInt32TestCase("ext32", int32TestSlice_3, int32BetweenTestMatch_3, int32BetweenTestMatch_3b, int32BetweenTestResult_3, 32),
	CreateInt32TestCase("ext31", int32TestSlice_3, int32BetweenTestMatch_3, int32BetweenTestMatch_3b, int32BetweenTestResult_3, 31),
}

func TestMatchInt32BetweenGeneric(T *testing.T) {
	for _, c := range int32BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt32BetweenGeneric(c.slice, c.match, c.match2, bits)
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

/*func TestMatchInt32BetweenAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range int32BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt32BetweenAVX2(c.slice, c.match, c.match2, bits)
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
}*/

func TestMatchInt32BetweenAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range int32BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt32BetweenAVX512(c.slice, c.match, c.match2, bits)
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
func BenchmarkMatchInt32BetweenGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32BetweenGeneric(a, math.MaxInt32/4, math.MaxInt32/2, bits)
			}
		})
	}
}

/*func BenchmarkMatchInt32BetweenAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32BetweenAVX2(a, math.MaxInt32/4, math.MaxInt32/2, bits)
			}
		})
	}
}
*/
func BenchmarkMatchInt32BetweenAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				matchInt32BetweenAVX512(a, math.MaxInt32/4, math.MaxInt32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Int32 Slice
//
func TestUniqueInt32(T *testing.T) {
	a := randInt32Slice(1000, 5)
	b := UniqueInt32Slice(a)
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

func BenchmarkUniqueInt32(B *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt32Slice(n, 5)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				UniqueInt32Slice(a)
			}
		})
	}
}

func TestInt32SliceContains(T *testing.T) {
	// nil slice
	if Int32Slice(nil).Contains(1) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Int32Slice([]int32{}).Contains(1) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Int32Slice([]int32{1}).Contains(1) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Int32Slice([]int32{1}).Contains(2) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !Int32Slice([]int32{-1, 3, 5, 7, 11, 13}).Contains(-1) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Int32Slice([]int32{-1, 3, 5, 7, 11, 13}).Contains(5) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Int32Slice([]int32{-1, 3, 5, 7, 11, 13}).Contains(13) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Int32Slice([]int32{-1, 3, 5, 7, 11, 13}).Contains(0) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Int32Slice([]int32{-1, 3, 5, 7, 11, 13}).Contains(2) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Int32Slice([]int32{-1, 3, 5, 7, 11, 13}).Contains(14) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkInt32SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Int32Slice(randInt32Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(rand.Int31())
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Int32Slice(randInt32Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(a[rand.Intn(len(a))])
			}
		})
	}
}

func TestInt32SliceContainsRange(T *testing.T) {
	type VecTestRange struct {
		Name  string
		From  int32
		To    int32
		Match bool
	}

	type VecTestcase struct {
		Slice  []int32
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
			Slice: []int32{},
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: 0, To: 2, Match: false},
			},
		},
		// 1-element slice
		VecTestcase{
			Slice: []int32{3},
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
			Slice: []int32{3},
			Ranges: []VecTestRange{
				VecTestRange{Name: "BCD", From: 3, To: 3, Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		VecTestcase{
			Slice: []int32{3, 5, 7, 11, 13},
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
			Slice: []int32{
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
			if want, got := r.Match, Int32Slice(v.Slice).ContainsRange(r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkInt32SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Int32Slice(randInt32Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := rand.Int31(), rand.Int31()
				if min > max {
					min, max = max, min
				}
				a.ContainsRange(min, max)
			}
		})
	}
}
