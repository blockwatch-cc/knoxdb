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

const Int128Size = 16

func randInt128Slice(n, u int) Int128Slice {
	s := make([]Int128, n*u)
	for i := 0; i < n; i++ {
		s[i][0] = uint64(rand.Int63())
		s[i][0] = rand.Uint64()
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

func splitInt128Slice(src []Int128) ([]int64, []uint64) {
	res0 := make([]int64, len(src))
	res1 := make([]uint64, len(src))
	for i, v := range src {
		res0[i] = int64(v[0])
		res1[i] = v[1]
	}
	return res0, res1
}

type Int128MatchTest struct {
	name   string
	slice  []Int128
	match  Int128 // used for every test
	match2 Int128 // used for between tests
	result []byte
	count  int64
}

var (
	// positive values only
	Int128TestSlice_1 = []Int128{
		{2, 5}, {2, 2}, {2, 3}, {2, 4},
		{2, 7}, {2, 8}, {2, 5}, {2, 10},
		{1, 5}, {1, 2}, {1, 3}, {1, 4},
		{1, 7}, {1, 8}, {1, 5}, {1, 10},
		{3, 5}, {3, 2}, {3, 3}, {3, 4},
		{3, 7}, {3, 8}, {3, 5}, {3, 10},
		{2, 5}, {0, 2}, {10, 3}, {0, 40},
		{2, 0}, {2, 10}, {2, 10}, {2, 5},
	}
	Int128EqualTestResult_1 = []byte{0x41, 0x00, 0x00, 0x81}
	Int128EqualTestMatch_1  = Int128From2Int64(2, 5)

	Int128NotEqualTestResult_1 = []byte{0xbe, 0xff, 0xff, 0x7e}
	Int128NotEqualTestMatch_1  = Int128From2Int64(2, 5)

	Int128LessTestResult_1 = []byte{0x0e, 0xff, 0x00, 0x1a}
	Int128LessTestMatch_1  = Int128From2Int64(2, 5)

	Int128LessEqualTestResult_1 = []byte{0x4f, 0xff, 0x00, 0x9b}
	Int128LessEqualTestMatch_1  = Int128From2Int64(2, 5)

	Int128GreaterTestResult_1 = []byte{0xb0, 0x00, 0xff, 0x64}
	Int128GreaterTestMatch_1  = Int128From2Int64(2, 5)

	Int128GreaterEqualTestResult_1 = []byte{0xf1, 0x00, 0xff, 0xe5}
	Int128GreaterEqualTestMatch_1  = Int128From2Int64(2, 5)

	Int128BetweenTestResult_1 = []byte{0xf1, 0x00, 0x00, 0xe1}
	Int128BetweenTestMatch_1  = Int128From2Int64(2, 5)
	Int128BetweenTestMatch_1b = Int128From2Int64(2, 10)

	// negative and positive values mixed
	Int128TestSlice_2 = []Int128{
		Int128From2Int64(-2, -5), Int128From2Int64(-2, -4), Int128From2Int64(-2, -3), Int128From2Int64(-2, -2),
		Int128From2Int64(-2, -7), Int128From2Int64(-2, -8), Int128From2Int64(-2, -5), Int128From2Int64(-2, -10),
		Int128From2Int64(-1, -5), Int128From2Int64(-1, -4), Int128From2Int64(-1, -3), Int128From2Int64(-1, -2),
		Int128From2Int64(-1, -7), Int128From2Int64(-1, -8), Int128From2Int64(-1, -5), Int128From2Int64(-1, -10),
		Int128From2Int64(-3, -5), Int128From2Int64(-3, -4), Int128From2Int64(-3, -3), Int128From2Int64(-3, -2),
		Int128From2Int64(-3, -7), Int128From2Int64(-3, -8), Int128From2Int64(-3, -5), Int128From2Int64(-3, -10),
		Int128From2Int64(2, -5), Int128From2Int64(2, -4), Int128From2Int64(2, -3), Int128From2Int64(2, -2),
		Int128From2Int64(2, -7), Int128From2Int64(2, -8), Int128From2Int64(2, -5), Int128From2Int64(2, 10),
	}
	Int128EqualTestResult_2 = []byte{0x41, 0x0, 0x0, 0x0}
	Int128EqualTestMatch_2  = Int128From2Int64(-2, -5)

	Int128NotEqualTestResult_2 = []byte{0xbe, 0xff, 0xff, 0xff}
	Int128NotEqualTestMatch_2  = Int128From2Int64(-2, -5)

	Int128LessTestResult_2 = []byte{0xb0, 0x00, 0xff, 0x00}
	Int128LessTestMatch_2  = Int128From2Int64(-2, -5)

	Int128LessEqualTestResult_2 = []byte{0xf1, 0x00, 0xff, 0x00}
	Int128LessEqualTestMatch_2  = Int128From2Int64(-2, -5)

	Int128GreaterTestResult_2 = []byte{0x0e, 0xff, 0x00, 0xff}
	Int128GreaterTestMatch_2  = Int128From2Int64(-2, -5)

	Int128GreaterEqualTestResult_2 = []byte{0x4f, 0xff, 0x00, 0xff}
	Int128GreaterEqualTestMatch_2  = Int128From2Int64(-2, -5)

	Int128BetweenTestResult_2 = []byte{0xf1, 0x00, 0x00, 0x00}
	Int128BetweenTestMatch_2  = Int128From2Int64(-2, -10)
	Int128BetweenTestMatch_2b = Int128From2Int64(-2, -5)

	// extreme values
	Int128TestSlice_3 = []Int128{
		Int128FromInt64(math.MaxInt16), Int128FromInt64(math.MinInt16),
		Int128FromInt64(math.MaxInt32), Int128FromInt64(math.MinInt32),
		Int128FromInt64(math.MaxInt64), Int128FromInt64(math.MinInt64),
		MaxInt128, MinInt128,
		Int128FromInt64(math.MaxInt16), Int128FromInt64(math.MinInt16),
		Int128FromInt64(math.MaxInt32), Int128FromInt64(math.MinInt32),
		Int128FromInt64(math.MaxInt64), Int128FromInt64(math.MinInt64),
		MaxInt128, MinInt128,
		Int128FromInt64(math.MaxInt16), Int128FromInt64(math.MinInt16),
		Int128FromInt64(math.MaxInt32), Int128FromInt64(math.MinInt32),
		Int128FromInt64(math.MaxInt64), Int128FromInt64(math.MinInt64),
		MaxInt128, MinInt128,
		Int128FromInt64(math.MaxInt16), Int128FromInt64(math.MinInt16),
		Int128FromInt64(math.MaxInt32), Int128FromInt64(math.MinInt32),
		Int128FromInt64(math.MaxInt64), Int128FromInt64(math.MinInt64),
		MaxInt128, MinInt128,
	}
	Int128EqualTestResult_3 = []byte{0x80, 0x80, 0x80, 0x80}
	Int128EqualTestMatch_3  = MinInt128

	Int128NotEqualTestResult_3 = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	Int128NotEqualTestMatch_3  = MinInt128

	Int128LessTestResult_3 = []byte{0x0, 0x0, 0x0, 0x0}
	Int128LessTestMatch_3  = MinInt128

	Int128LessEqualTestResult_3 = []byte{0x80, 0x80, 0x80, 0x80}
	Int128LessEqualTestMatch_3  = MinInt128

	Int128GreaterTestResult_3 = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	Int128GreaterTestMatch_3  = MinInt128

	Int128GreaterEqualTestResult_3 = []byte{0xff, 0xff, 0xff, 0xff}
	Int128GreaterEqualTestMatch_3  = MinInt128

	Int128BetweenTestResult_3 = []byte{0x50, 0x50, 0x50, 0x50}
	Int128BetweenTestMatch_3  = Int128FromInt64(math.MaxInt64)
	Int128BetweenTestMatch_3b = MaxInt128
)

// creates an Int128 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - match, match2: are only copied to the resulting test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateInt128TestCase(name string, slice []Int128, match, match2 Int128, result []byte, length int) Int128MatchTest {
	if len(slice)%8 != 0 {
		panic("CreateInt128TestCase: length of slice has to be a multiple of 8")
	}
	if len(result) != bitFieldLen(len(slice)) {
		panic("CreateInt128TestCase: length of slice and length of result does not match")
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []Int128
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
	return Int128MatchTest{
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
var Int128EqualCases = []Int128MatchTest{
	{
		name:   "l0",
		slice:  make([]Int128, 0),
		match:  Int128EqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  Int128EqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt128TestCase("l32", Int128TestSlice_1, Int128EqualTestMatch_1, ZeroInt128, Int128EqualTestResult_1, 32),
	CreateInt128TestCase("l64", Int128TestSlice_1, Int128EqualTestMatch_1, ZeroInt128, Int128EqualTestResult_1, 64),
	CreateInt128TestCase("l128", Int128TestSlice_1, Int128EqualTestMatch_1, ZeroInt128, Int128EqualTestResult_1, 128),
	/*	CreateInt128TestCase("l127", Int128TestSlice_1, Int128EqualTestMatch_1, ZeroInt128, Int128EqualTestResult_1, 127),
		CreateInt128TestCase("l63", Int128TestSlice_1, Int128EqualTestMatch_1, ZeroInt128, Int128EqualTestResult_1, 63),
		CreateInt128TestCase("l31", Int128TestSlice_1, Int128EqualTestMatch_1, ZeroInt128, Int128EqualTestResult_1, 31),
		CreateInt128TestCase("l23", Int128TestSlice_1, Int128EqualTestMatch_1, ZeroInt128, Int128EqualTestResult_1, 23),
		CreateInt128TestCase("l15", Int128TestSlice_1, Int128EqualTestMatch_1, ZeroInt128, Int128EqualTestResult_1, 15),
		CreateInt128TestCase("l7", Int128TestSlice_1, Int128EqualTestMatch_1, ZeroInt128, Int128EqualTestResult_1, 7),*/
	CreateInt128TestCase("neg64", Int128TestSlice_2, Int128EqualTestMatch_2, ZeroInt128, Int128EqualTestResult_2, 64),
	CreateInt128TestCase("neg32", Int128TestSlice_2, Int128EqualTestMatch_2, ZeroInt128, Int128EqualTestResult_2, 32),
	//	CreateInt128TestCase("neg31", Int128TestSlice_2, Int128EqualTestMatch_2, ZeroInt128, Int128EqualTestResult_2, 31),
	CreateInt128TestCase("ext64", Int128TestSlice_3, Int128EqualTestMatch_3, ZeroInt128, Int128EqualTestResult_3, 64),
	CreateInt128TestCase("ext32", Int128TestSlice_3, Int128EqualTestMatch_3, ZeroInt128, Int128EqualTestResult_3, 32),
	//	CreateInt128TestCase("ext31", Int128TestSlice_3, Int128EqualTestMatch_3, ZeroInt128, Int128EqualTestResult_3, 31),
}

func TestMatchInt128EqualGeneric(T *testing.T) {
	for _, c := range Int128EqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt128EqualGeneric(c.slice, c.match, bits, nil)
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

func TestMatchInt128EqualAVX2Easy(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		src0, src1 := splitInt128Slice(c.slice)
		cnt := matchInt128EqualAVX2Easy(src0, src1, c.match, bits)
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

func TestMatchInt128EqualAVX2New(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		src0, src1 := splitInt128Slice(c.slice)
		cnt := matchInt128EqualAVX2New(src0, src1, c.match, bits)
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

func TestMatchInt128EqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		//src0, src1 := splitInt128Slice(c.slice)
		cnt := matchInt128EqualAVX2(c.slice, c.match, bits)
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

/*
func TestMatchInt128EqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range Int128EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt128EqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt128EqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		mask := fillBitset(nil, len(a), 0xff)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128EqualGeneric(a, MaxInt128.Div64(2), bits, mask)
			}
		})
	}
}

func BenchmarkMatchInt128EqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128EqualAVX2(a, MaxInt128.Div64(2), bits)
			}
		})
	}
}

func BenchmarkMatchInt128EqualAVX2Easy(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		src0, src1 := splitInt128Slice(a)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128EqualAVX2Easy(src0, src1, MaxInt128.Div64(2), bits)
			}
		})
	}
}

func BenchmarkMatchInt128EqualAVX2New(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		src0, src1 := splitInt128Slice(a)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128EqualAVX2New(src0, src1, MaxInt128.Div64(2), bits)
			}
		})
	}
}

/*
func BenchmarkMatchInt128EqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int128(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128EqualAVX512(a, math.MaxInt128/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Not Equal Testcases
//

var Int128NotEqualCases = []Int128MatchTest{
	{
		name:   "l0",
		slice:  make([]Int128, 0),
		match:  Int128NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  Int128NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt128TestCase("l32", Int128TestSlice_1, Int128NotEqualTestMatch_1, ZeroInt128, Int128NotEqualTestResult_1, 32),
	CreateInt128TestCase("l64", Int128TestSlice_1, Int128NotEqualTestMatch_1, ZeroInt128, Int128NotEqualTestResult_1, 64),
	CreateInt128TestCase("l128", Int128TestSlice_1, Int128NotEqualTestMatch_1, ZeroInt128, Int128NotEqualTestResult_1, 128),
	CreateInt128TestCase("l127", Int128TestSlice_1, Int128NotEqualTestMatch_1, ZeroInt128, Int128NotEqualTestResult_1, 127),
	CreateInt128TestCase("l63", Int128TestSlice_1, Int128NotEqualTestMatch_1, ZeroInt128, Int128NotEqualTestResult_1, 63),
	CreateInt128TestCase("l31", Int128TestSlice_1, Int128NotEqualTestMatch_1, ZeroInt128, Int128NotEqualTestResult_1, 31),
	CreateInt128TestCase("l23", Int128TestSlice_1, Int128NotEqualTestMatch_1, ZeroInt128, Int128NotEqualTestResult_1, 23),
	CreateInt128TestCase("l15", Int128TestSlice_1, Int128NotEqualTestMatch_1, ZeroInt128, Int128NotEqualTestResult_1, 15),
	CreateInt128TestCase("l7", Int128TestSlice_1, Int128NotEqualTestMatch_1, ZeroInt128, Int128NotEqualTestResult_1, 7),
	CreateInt128TestCase("neg64", Int128TestSlice_2, Int128NotEqualTestMatch_2, ZeroInt128, Int128NotEqualTestResult_2, 64),
	CreateInt128TestCase("neg32", Int128TestSlice_2, Int128NotEqualTestMatch_2, ZeroInt128, Int128NotEqualTestResult_2, 32),
	CreateInt128TestCase("neg31", Int128TestSlice_2, Int128NotEqualTestMatch_2, ZeroInt128, Int128NotEqualTestResult_2, 31),
	CreateInt128TestCase("ext64", Int128TestSlice_3, Int128NotEqualTestMatch_3, ZeroInt128, Int128NotEqualTestResult_3, 64),
	CreateInt128TestCase("ext32", Int128TestSlice_3, Int128NotEqualTestMatch_3, ZeroInt128, Int128NotEqualTestResult_3, 32),
	CreateInt128TestCase("ext31", Int128TestSlice_3, Int128NotEqualTestMatch_3, ZeroInt128, Int128NotEqualTestResult_3, 31),
}

func TestMatchInt128NotEqualGeneric(T *testing.T) {
	for _, c := range Int128NotEqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt128NotEqualGeneric(c.slice, c.match, bits, nil)
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
func TestMatchInt128NotEqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt128NotEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt128NotEqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range Int128NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt128NotEqualAVX512(c.slice, c.match, bits)
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
// Not Equal benchmarks
//
func BenchmarkMatchInt128NotEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128NotEqualGeneric(a, MaxInt128.Div64(2), bits, nil)
			}
		})
	}
}

/*
func BenchmarkMatchInt128NotEqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int128(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128NotEqualAVX2(a, math.MaxInt128/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt128NotEqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int128(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128NotEqualAVX512(a, math.MaxInt128/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Less Testcases
//
var Int128LessCases = []Int128MatchTest{
	{
		name:   "l0",
		slice:  make([]Int128, 0),
		match:  Int128LessTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  Int128LessTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt128TestCase("l32", Int128TestSlice_1, Int128LessTestMatch_1, ZeroInt128, Int128LessTestResult_1, 32),
	CreateInt128TestCase("l64", Int128TestSlice_1, Int128LessTestMatch_1, ZeroInt128, Int128LessTestResult_1, 64),
	CreateInt128TestCase("l128", Int128TestSlice_1, Int128LessTestMatch_1, ZeroInt128, Int128LessTestResult_1, 128),
	/*CreateInt128TestCase("l127", Int128TestSlice_1, Int128LessTestMatch_1, ZeroInt128, Int128LessTestResult_1, 127),
	CreateInt128TestCase("l63", Int128TestSlice_1, Int128LessTestMatch_1, ZeroInt128, Int128LessTestResult_1, 63),
	CreateInt128TestCase("l31", Int128TestSlice_1, Int128LessTestMatch_1, ZeroInt128, Int128LessTestResult_1, 31),
	CreateInt128TestCase("l23", Int128TestSlice_1, Int128LessTestMatch_1, ZeroInt128, Int128LessTestResult_1, 23),
	CreateInt128TestCase("l15", Int128TestSlice_1, Int128LessTestMatch_1, ZeroInt128, Int128LessTestResult_1, 15),
	CreateInt128TestCase("l7", Int128TestSlice_1, Int128LessTestMatch_1, ZeroInt128, Int128LessTestResult_1, 7),*/
	CreateInt128TestCase("neg64", Int128TestSlice_2, Int128LessTestMatch_2, ZeroInt128, Int128LessTestResult_2, 64),
	CreateInt128TestCase("neg32", Int128TestSlice_2, Int128LessTestMatch_2, ZeroInt128, Int128LessTestResult_2, 32),
	//CreateInt128TestCase("neg31", Int128TestSlice_2, Int128LessTestMatch_2, ZeroInt128, Int128LessTestResult_2, 31),
	CreateInt128TestCase("ext64", Int128TestSlice_3, Int128LessTestMatch_3, ZeroInt128, Int128LessTestResult_3, 64),
	CreateInt128TestCase("ext32", Int128TestSlice_3, Int128LessTestMatch_3, ZeroInt128, Int128LessTestResult_3, 32),
	//CreateInt128TestCase("ext31", Int128TestSlice_3, Int128LessTestMatch_3, ZeroInt128, Int128LessTestResult_3, 31),
}

func TestMatchInt128LessGeneric(T *testing.T) {
	for _, c := range Int128LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt128LessThanGeneric(c.slice, c.match, bits, nil)
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

func TestMatchInt128LessAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		//src0, src1 := splitInt128Slice(c.slice)
		cnt := matchInt128LessThanAVX2(c.slice, c.match, bits)
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

func TestMatchInt128LessAVX2Easy(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		src0, src1 := splitInt128Slice(c.slice)
		cnt := matchInt128LessThanAVX2Easy(src0, src1, c.match, bits)
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

func TestMatchInt128LessAVX2New(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		src0, src1 := splitInt128Slice(c.slice)
		cnt := matchInt128LessThanAVX2New(src0, src1, c.match, bits)
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

/*
func TestMatchInt128LessAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range Int128LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt128LessThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt128LessGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128LessThanGeneric(a, MaxInt128.Div64(2), bits, nil)
			}
		})
	}
}

func BenchmarkMatchInt128LessAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128LessThanAVX2(a, MaxInt128.Div64(2), bits)
			}
		})
	}
}

func BenchmarkMatchInt128LessAVX2Easy(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		src0, src1 := splitInt128Slice(a)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128LessThanAVX2Easy(src0, src1, MaxInt128.Div64(2), bits)
			}
		})
	}
}

func BenchmarkMatchInt128LessAVX2New(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		src0, src1 := splitInt128Slice(a)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128LessThanAVX2New(src0, src1, MaxInt128.Div64(2), bits)
			}
		})
	}
}

/*
func BenchmarkMatchInt128LessAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int128(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128LessThanAVX512(a, math.MaxInt128/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Less Equal Testcases
//
var Int128LessEqualCases = []Int128MatchTest{
	{
		name:   "l0",
		slice:  make([]Int128, 0),
		match:  Int128LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  Int128LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt128TestCase("l32", Int128TestSlice_1, Int128LessEqualTestMatch_1, ZeroInt128, Int128LessEqualTestResult_1, 32),
	CreateInt128TestCase("l64", Int128TestSlice_1, Int128LessEqualTestMatch_1, ZeroInt128, Int128LessEqualTestResult_1, 64),
	CreateInt128TestCase("l128", Int128TestSlice_1, Int128LessEqualTestMatch_1, ZeroInt128, Int128LessEqualTestResult_1, 128),
	CreateInt128TestCase("l127", Int128TestSlice_1, Int128LessEqualTestMatch_1, ZeroInt128, Int128LessEqualTestResult_1, 127),
	CreateInt128TestCase("l63", Int128TestSlice_1, Int128LessEqualTestMatch_1, ZeroInt128, Int128LessEqualTestResult_1, 63),
	CreateInt128TestCase("l31", Int128TestSlice_1, Int128LessEqualTestMatch_1, ZeroInt128, Int128LessEqualTestResult_1, 31),
	CreateInt128TestCase("l23", Int128TestSlice_1, Int128LessEqualTestMatch_1, ZeroInt128, Int128LessEqualTestResult_1, 23),
	CreateInt128TestCase("l15", Int128TestSlice_1, Int128LessEqualTestMatch_1, ZeroInt128, Int128LessEqualTestResult_1, 15),
	CreateInt128TestCase("l7", Int128TestSlice_1, Int128LessEqualTestMatch_1, ZeroInt128, Int128LessEqualTestResult_1, 7),
	CreateInt128TestCase("neg64", Int128TestSlice_2, Int128LessEqualTestMatch_2, ZeroInt128, Int128LessEqualTestResult_2, 64),
	CreateInt128TestCase("neg32", Int128TestSlice_2, Int128LessEqualTestMatch_2, ZeroInt128, Int128LessEqualTestResult_2, 32),
	CreateInt128TestCase("neg31", Int128TestSlice_2, Int128LessEqualTestMatch_2, ZeroInt128, Int128LessEqualTestResult_2, 31),
	CreateInt128TestCase("ext64", Int128TestSlice_3, Int128LessEqualTestMatch_3, ZeroInt128, Int128LessEqualTestResult_3, 64),
	CreateInt128TestCase("ext32", Int128TestSlice_3, Int128LessEqualTestMatch_3, ZeroInt128, Int128LessEqualTestResult_3, 32),
	CreateInt128TestCase("ext31", Int128TestSlice_3, Int128LessEqualTestMatch_3, ZeroInt128, Int128LessEqualTestResult_3, 31),
}

func TestMatchInt128LessEqualGeneric(T *testing.T) {
	for _, c := range Int128LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt128LessThanEqualGeneric(c.slice, c.match, bits, nil)
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
func TestMatchInt128LessEqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt128LessThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt128LessEqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range Int128LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt128LessThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt128LessEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128LessThanEqualGeneric(a, MaxInt128.Div64(2), bits, nil)
			}
		})
	}
}

/*
func BenchmarkMatchInt128LessEqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int128(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128LessThanEqualAVX2(a, math.MaxInt128/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt128LessEqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int128(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128LessThanEqualAVX512(a, math.MaxInt128/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Greater Testcases
//
var Int128GreaterCases = []Int128MatchTest{
	{
		name:   "l0",
		slice:  make([]Int128, 0),
		match:  Int128GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  Int128GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt128TestCase("l32", Int128TestSlice_1, Int128GreaterTestMatch_1, ZeroInt128, Int128GreaterTestResult_1, 32),
	CreateInt128TestCase("l64", Int128TestSlice_1, Int128GreaterTestMatch_1, ZeroInt128, Int128GreaterTestResult_1, 64),
	CreateInt128TestCase("l128", Int128TestSlice_1, Int128GreaterTestMatch_1, ZeroInt128, Int128GreaterTestResult_1, 128),
	CreateInt128TestCase("l127", Int128TestSlice_1, Int128GreaterTestMatch_1, ZeroInt128, Int128GreaterTestResult_1, 127),
	CreateInt128TestCase("l63", Int128TestSlice_1, Int128GreaterTestMatch_1, ZeroInt128, Int128GreaterTestResult_1, 63),
	CreateInt128TestCase("l31", Int128TestSlice_1, Int128GreaterTestMatch_1, ZeroInt128, Int128GreaterTestResult_1, 31),
	CreateInt128TestCase("l23", Int128TestSlice_1, Int128GreaterTestMatch_1, ZeroInt128, Int128GreaterTestResult_1, 23),
	CreateInt128TestCase("l15", Int128TestSlice_1, Int128GreaterTestMatch_1, ZeroInt128, Int128GreaterTestResult_1, 15),
	CreateInt128TestCase("l7", Int128TestSlice_1, Int128GreaterTestMatch_1, ZeroInt128, Int128GreaterTestResult_1, 7),
	CreateInt128TestCase("neg64", Int128TestSlice_2, Int128GreaterTestMatch_2, ZeroInt128, Int128GreaterTestResult_2, 64),
	CreateInt128TestCase("neg32", Int128TestSlice_2, Int128GreaterTestMatch_2, ZeroInt128, Int128GreaterTestResult_2, 32),
	CreateInt128TestCase("neg31", Int128TestSlice_2, Int128GreaterTestMatch_2, ZeroInt128, Int128GreaterTestResult_2, 31),
	CreateInt128TestCase("ext64", Int128TestSlice_3, Int128GreaterTestMatch_3, ZeroInt128, Int128GreaterTestResult_3, 64),
	CreateInt128TestCase("ext32", Int128TestSlice_3, Int128GreaterTestMatch_3, ZeroInt128, Int128GreaterTestResult_3, 32),
	CreateInt128TestCase("ext31", Int128TestSlice_3, Int128GreaterTestMatch_3, ZeroInt128, Int128GreaterTestResult_3, 31),
}

func TestMatchInt128GreaterGeneric(T *testing.T) {
	for _, c := range Int128GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt128GreaterThanGeneric(c.slice, c.match, bits, nil)
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
func TestMatchInt128GreaterAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt128GreaterThanAVX2(c.slice, c.match, bits)
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

func TestMatchInt128GreaterAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range Int128GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt128GreaterThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt128GreaterGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128GreaterThanGeneric(a, MaxInt128.Div64(2), bits, nil)
			}
		})
	}
}

/*
func BenchmarkMatchInt128GreaterAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int128(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128GreaterThanAVX2(a, math.MaxInt128/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt128GreaterAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int128(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128GreaterThanAVX512(a, math.MaxInt128/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Greater Equal Testcases
//
var Int128GreaterEqualCases = []Int128MatchTest{
	{
		name:   "l0",
		slice:  make([]Int128, 0),
		match:  Int128GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  Int128GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt128TestCase("l32", Int128TestSlice_1, Int128GreaterEqualTestMatch_1, ZeroInt128, Int128GreaterEqualTestResult_1, 32),
	CreateInt128TestCase("l64", Int128TestSlice_1, Int128GreaterEqualTestMatch_1, ZeroInt128, Int128GreaterEqualTestResult_1, 64),
	CreateInt128TestCase("l128", Int128TestSlice_1, Int128GreaterEqualTestMatch_1, ZeroInt128, Int128GreaterEqualTestResult_1, 128),
	CreateInt128TestCase("l127", Int128TestSlice_1, Int128GreaterEqualTestMatch_1, ZeroInt128, Int128GreaterEqualTestResult_1, 127),
	CreateInt128TestCase("l63", Int128TestSlice_1, Int128GreaterEqualTestMatch_1, ZeroInt128, Int128GreaterEqualTestResult_1, 63),
	CreateInt128TestCase("l31", Int128TestSlice_1, Int128GreaterEqualTestMatch_1, ZeroInt128, Int128GreaterEqualTestResult_1, 31),
	CreateInt128TestCase("l23", Int128TestSlice_1, Int128GreaterEqualTestMatch_1, ZeroInt128, Int128GreaterEqualTestResult_1, 23),
	CreateInt128TestCase("l15", Int128TestSlice_1, Int128GreaterEqualTestMatch_1, ZeroInt128, Int128GreaterEqualTestResult_1, 15),
	CreateInt128TestCase("l7", Int128TestSlice_1, Int128GreaterEqualTestMatch_1, ZeroInt128, Int128GreaterEqualTestResult_1, 7),
	CreateInt128TestCase("neg64", Int128TestSlice_2, Int128GreaterEqualTestMatch_2, ZeroInt128, Int128GreaterEqualTestResult_2, 64),
	CreateInt128TestCase("neg32", Int128TestSlice_2, Int128GreaterEqualTestMatch_2, ZeroInt128, Int128GreaterEqualTestResult_2, 32),
	CreateInt128TestCase("neg31", Int128TestSlice_2, Int128GreaterEqualTestMatch_2, ZeroInt128, Int128GreaterEqualTestResult_2, 31),
	CreateInt128TestCase("ext64", Int128TestSlice_3, Int128GreaterEqualTestMatch_3, ZeroInt128, Int128GreaterEqualTestResult_3, 64),
	CreateInt128TestCase("ext32", Int128TestSlice_3, Int128GreaterEqualTestMatch_3, ZeroInt128, Int128GreaterEqualTestResult_3, 32),
	CreateInt128TestCase("ext31", Int128TestSlice_3, Int128GreaterEqualTestMatch_3, ZeroInt128, Int128GreaterEqualTestResult_3, 31),
}

func TestMatchInt128GreaterEqualGeneric(T *testing.T) {
	for _, c := range Int128GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt128GreaterThanEqualGeneric(c.slice, c.match, bits, nil)
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
func TestMatchInt128GreaterEqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt128GreaterThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt128GreaterEqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range Int128GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt128GreaterThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt128GreaterEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128GreaterThanEqualGeneric(a, MaxInt128.Div64(2), bits, nil)
			}
		})
	}
}

/*
func BenchmarkMatchInt128GreaterEqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int128(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128GreaterThanEqualAVX2(a, math.MaxInt128/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt128GreaterEqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int128(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128GreaterThanEqualAVX512(a, math.MaxInt128/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Between Testcases
//
var Int128BetweenCases = []Int128MatchTest{
	{
		name:   "l0",
		slice:  make([]Int128, 0),
		match:  Int128BetweenTestMatch_1,
		match2: Int128BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  Int128BetweenTestMatch_1,
		match2: Int128BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	},
	CreateInt128TestCase("l32", Int128TestSlice_1, Int128BetweenTestMatch_1, Int128BetweenTestMatch_1b, Int128BetweenTestResult_1, 32),
	CreateInt128TestCase("l64", Int128TestSlice_1, Int128BetweenTestMatch_1, Int128BetweenTestMatch_1b, Int128BetweenTestResult_1, 64),
	CreateInt128TestCase("l128", Int128TestSlice_1, Int128BetweenTestMatch_1, Int128BetweenTestMatch_1b, Int128BetweenTestResult_1, 128),
	CreateInt128TestCase("l127", Int128TestSlice_1, Int128BetweenTestMatch_1, Int128BetweenTestMatch_1b, Int128BetweenTestResult_1, 127),
	CreateInt128TestCase("l63", Int128TestSlice_1, Int128BetweenTestMatch_1, Int128BetweenTestMatch_1b, Int128BetweenTestResult_1, 63),
	CreateInt128TestCase("l31", Int128TestSlice_1, Int128BetweenTestMatch_1, Int128BetweenTestMatch_1b, Int128BetweenTestResult_1, 31),
	CreateInt128TestCase("l23", Int128TestSlice_1, Int128BetweenTestMatch_1, Int128BetweenTestMatch_1b, Int128BetweenTestResult_1, 23),
	CreateInt128TestCase("l15", Int128TestSlice_1, Int128BetweenTestMatch_1, Int128BetweenTestMatch_1b, Int128BetweenTestResult_1, 15),
	CreateInt128TestCase("l7", Int128TestSlice_1, Int128BetweenTestMatch_1, Int128BetweenTestMatch_1b, Int128BetweenTestResult_1, 7),
	CreateInt128TestCase("neg64", Int128TestSlice_2, Int128BetweenTestMatch_2, Int128BetweenTestMatch_2b, Int128BetweenTestResult_2, 64),
	CreateInt128TestCase("neg32", Int128TestSlice_2, Int128BetweenTestMatch_2, Int128BetweenTestMatch_2b, Int128BetweenTestResult_2, 32),
	CreateInt128TestCase("neg31", Int128TestSlice_2, Int128BetweenTestMatch_2, Int128BetweenTestMatch_2b, Int128BetweenTestResult_2, 31),
	CreateInt128TestCase("ext64", Int128TestSlice_3, Int128BetweenTestMatch_3, Int128BetweenTestMatch_3b, Int128BetweenTestResult_3, 64),
	CreateInt128TestCase("ext32", Int128TestSlice_3, Int128BetweenTestMatch_3, Int128BetweenTestMatch_3b, Int128BetweenTestResult_3, 32),
	CreateInt128TestCase("ext31", Int128TestSlice_3, Int128BetweenTestMatch_3, Int128BetweenTestMatch_3b, Int128BetweenTestResult_3, 31),
}

func TestMatchInt128BetweenGeneric(T *testing.T) {
	for _, c := range Int128BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt128BetweenGeneric(c.slice, c.match, c.match2, bits, nil)
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
func TestMatchInt128BetweenAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int128BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt128BetweenAVX2(c.slice, c.match, c.match2, bits)
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

func TestMatchInt128BetweenAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range Int128BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt128BetweenAVX512(c.slice, c.match, c.match2, bits)
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
func BenchmarkMatchInt128BetweenGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128BetweenGeneric(a, MaxInt128.Div64(4), MaxInt128.Div64(2), bits, nil)
			}
		})
	}
}

/*
func BenchmarkMatchInt128BetweenAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int128(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128BetweenAVX2(a, math.MaxInt128/4, math.MaxInt128/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt128BetweenAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt128Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int128(n.l * Int128Size))
			for i := 0; i < B.N; i++ {
				matchInt128BetweenAVX512(a, math.MaxInt128/4, math.MaxInt128/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------
// Int128 Slice
//
func TestUniqueInt128(T *testing.T) {
	a := randInt128Slice(1000, 5)
	b := UniqueInt128Slice(a)
	for i, _ := range b {
		// slice must be sorted and unique
		if i > 0 && b[i-1].Gt(b[i]) {
			T.Errorf("result is unsorted at pos %d", i)
		}
		if i > 0 && b[i-1].Eq(b[i]) {
			T.Errorf("result is not unique at pos %d", i)
		}
	}
}

func BenchmarkUniqueInt128(B *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt128Slice(n, 5)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				UniqueInt128Slice(a)
			}
		})
	}
}

func TestInt128SliceContains(T *testing.T) {
	// nil slice

	if Int128Slice(nil).Contains(Int128FromInt64(1)) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if (Int128Slice{}).Contains(Int128FromInt64(1)) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !(Int128Slice{Int128FromInt64(1)}).Contains(Int128FromInt64(1)) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if (Int128Slice{Int128FromInt64(1)}).Contains(Int128FromInt64(2)) {
		T.Errorf("1-element slice found wrong match")
	}

	slice := Int128Slice{Int128FromInt64(-1),
		Int128FromInt64(3),
		Int128FromInt64(5),
		Int128FromInt64(7),
		Int128FromInt64(11),
		Int128FromInt64(13),
	}

	// n-element slice positive first element
	if !slice.Contains(Int128FromInt64(-1)) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !slice.Contains(Int128FromInt64(5)) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !slice.Contains(Int128FromInt64(13)) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if slice.Contains(Int128FromInt64(0)) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if slice.Contains(Int128FromInt64(2)) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if slice.Contains(Int128FromInt64(14)) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkInt128SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := randInt128Slice(n, 1).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(Int128FromInt64(rand.Int63()))
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := randInt128Slice(n, 1).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(a[rand.Intn(len(a))])
			}
		})
	}
}

func TestInt128SliceContainsRange(T *testing.T) {
	type VecTestRange struct {
		Name  string
		From  Int128
		To    Int128
		Match bool
	}

	type VecTestcase struct {
		Slice  Int128Slice
		Ranges []VecTestRange
	}

	var tests = []VecTestcase{
		// nil slice
		VecTestcase{
			Slice: nil,
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: Int128FromInt64(0), To: Int128FromInt64(2), Match: false},
			},
		},
		// empty slice
		VecTestcase{
			Slice: Int128Slice{},
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: Int128FromInt64(0), To: Int128FromInt64(2), Match: false},
			},
		},
		// 1-element slice
		VecTestcase{
			Slice: Int128Slice{Int128FromInt64(3)},
			Ranges: []VecTestRange{
				VecTestRange{Name: "A", From: Int128FromInt64(0), To: Int128FromInt64(2), Match: false},   // Case A
				VecTestRange{Name: "B1", From: Int128FromInt64(1), To: Int128FromInt64(3), Match: true},   // Case B.1, D1
				VecTestRange{Name: "B3", From: Int128FromInt64(3), To: Int128FromInt64(4), Match: true},   // Case B.3, D3
				VecTestRange{Name: "E", From: Int128FromInt64(15), To: Int128FromInt64(16), Match: false}, // Case E
				VecTestRange{Name: "F", From: Int128FromInt64(1), To: Int128FromInt64(4), Match: true},    // Case F
			},
		},
		// 1-element slice, from == to
		VecTestcase{
			Slice: Int128Slice{Int128FromInt64(3)},
			Ranges: []VecTestRange{
				VecTestRange{Name: "BCD", From: Int128FromInt64(3), To: Int128FromInt64(3), Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		VecTestcase{
			Slice: Int128Slice{Int128FromInt64(3), Int128FromInt64(5), Int128FromInt64(7), Int128FromInt64(11), Int128FromInt64(13)},
			Ranges: []VecTestRange{
				VecTestRange{Name: "A", From: Int128FromInt64(0), To: Int128FromInt64(2), Match: false},    // Case A
				VecTestRange{Name: "B1a", From: Int128FromInt64(1), To: Int128FromInt64(3), Match: true},   // Case B.1
				VecTestRange{Name: "B1b", From: Int128FromInt64(3), To: Int128FromInt64(3), Match: true},   // Case B.1
				VecTestRange{Name: "B2a", From: Int128FromInt64(1), To: Int128FromInt64(4), Match: true},   // Case B.2
				VecTestRange{Name: "B2b", From: Int128FromInt64(1), To: Int128FromInt64(5), Match: true},   // Case B.2
				VecTestRange{Name: "B3a", From: Int128FromInt64(3), To: Int128FromInt64(4), Match: true},   // Case B.3
				VecTestRange{Name: "B3b", From: Int128FromInt64(3), To: Int128FromInt64(5), Match: true},   // Case B.3
				VecTestRange{Name: "C1a", From: Int128FromInt64(4), To: Int128FromInt64(5), Match: true},   // Case C.1
				VecTestRange{Name: "C1b", From: Int128FromInt64(4), To: Int128FromInt64(6), Match: true},   // Case C.1
				VecTestRange{Name: "C1c", From: Int128FromInt64(4), To: Int128FromInt64(7), Match: true},   // Case C.1
				VecTestRange{Name: "C1d", From: Int128FromInt64(5), To: Int128FromInt64(5), Match: true},   // Case C.1
				VecTestRange{Name: "C2a", From: Int128FromInt64(8), To: Int128FromInt64(8), Match: false},  // Case C.2
				VecTestRange{Name: "C2b", From: Int128FromInt64(8), To: Int128FromInt64(10), Match: false}, // Case C.2
				VecTestRange{Name: "D1a", From: Int128FromInt64(11), To: Int128FromInt64(13), Match: true}, // Case D.1
				VecTestRange{Name: "D1b", From: Int128FromInt64(12), To: Int128FromInt64(13), Match: true}, // Case D.1
				VecTestRange{Name: "D2", From: Int128FromInt64(12), To: Int128FromInt64(14), Match: true},  // Case D.2
				VecTestRange{Name: "D3a", From: Int128FromInt64(13), To: Int128FromInt64(13), Match: true}, // Case D.3
				VecTestRange{Name: "D3b", From: Int128FromInt64(13), To: Int128FromInt64(14), Match: true}, // Case D.3
				VecTestRange{Name: "E", From: Int128FromInt64(15), To: Int128FromInt64(16), Match: false},  // Case E
				VecTestRange{Name: "Fa", From: Int128FromInt64(0), To: Int128FromInt64(16), Match: true},   // Case F
				VecTestRange{Name: "Fb", From: Int128FromInt64(0), To: Int128FromInt64(13), Match: true},   // Case F
				VecTestRange{Name: "Fc", From: Int128FromInt64(3), To: Int128FromInt64(13), Match: true},   // Case F
			},
		},
		// real-word testcase
		VecTestcase{
			Slice: Int128Slice{
				Int128FromInt64(699421), Int128FromInt64(1374016), Int128FromInt64(1692360), Int128FromInt64(1797909),
				Int128FromInt64(1809339), Int128FromInt64(2552208), Int128FromInt64(2649552), Int128FromInt64(2740915),
				Int128FromInt64(2769610), Int128FromInt64(3043393),
			},
			Ranges: []VecTestRange{
				VecTestRange{Name: "1", From: Int128FromInt64(2785281), To: Int128FromInt64(2818048), Match: false},
				VecTestRange{Name: "2", From: Int128FromInt64(2818049), To: Int128FromInt64(2850816), Match: false},
				VecTestRange{Name: "3", From: Int128FromInt64(2850817), To: Int128FromInt64(2883584), Match: false},
				VecTestRange{Name: "4", From: Int128FromInt64(2883585), To: Int128FromInt64(2916352), Match: false},
				VecTestRange{Name: "5", From: Int128FromInt64(2916353), To: Int128FromInt64(2949120), Match: false},
				VecTestRange{Name: "6", From: Int128FromInt64(2949121), To: Int128FromInt64(2981888), Match: false},
				VecTestRange{Name: "7", From: Int128FromInt64(2981889), To: Int128FromInt64(3014656), Match: false},
				VecTestRange{Name: "8", From: Int128FromInt64(3014657), To: Int128FromInt64(3047424), Match: true},
			},
		},
	}

	for i, v := range tests {
		for _, r := range v.Ranges {
			if want, got := r.Match, v.Slice.ContainsRange(r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkInt128SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt128Slice(n, 1).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := rand.Int63(), rand.Int63()
				if min > max {
					min, max = max, min
				}
				a.ContainsRange(Int128FromInt64(min), Int128FromInt64(max))
			}
		})
	}
}
