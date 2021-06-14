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

const Int256Size = 32

func randInt256Slice(n, u int) Int256Slice {
	s := make([]Int256, n*u)
	for i := 0; i < n; i++ {
		s[i][0] = rand.Uint64()
		s[i][1] = rand.Uint64()
		s[i][2] = rand.Uint64()
		s[i][3] = rand.Uint64()
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

func splitInt256Slice(src []Int256) ([]int64, []uint64, []uint64, []uint64) {
	res0 := make([]int64, len(src))
	res1 := make([]uint64, len(src))
	res2 := make([]uint64, len(src))
	res3 := make([]uint64, len(src))
	for i, v := range src {
		res0[i] = int64(v[0])
		res1[i] = v[1]
		res2[i] = v[2]
		res3[i] = v[3]
	}
	return res0, res1, res2, res3
}

type Int256MatchTest struct {
	name   string
	slice  []Int256
	match  Int256 // used for every test
	match2 Int256 // used for between tests
	result []byte
	count  int64
}

var (
	MaxInt256Half  = Int256{math.MaxInt64 / 2, math.MaxUint64, math.MaxUint64, math.MaxUint64}
	MaxInt256Quart = Int256{math.MaxInt64 / 4, math.MaxUint64, math.MaxUint64, math.MaxUint64}

	// positive values only
	Int256TestSlice_1 = []Int256{
		{0, 0, 2, 5}, {0, 0, 2, 2}, {0, 0, 2, 3}, {0, 0, 2, 4},
		{0, 0, 2, 7}, {0, 0, 2, 8}, {0, 0, 2, 5}, {0, 0, 2, 10},
		{0, 0, 1, 5}, {0, 0, 1, 2}, {0, 0, 1, 3}, {0, 0, 1, 4},
		{0, 0, 1, 7}, {0, 0, 1, 8}, {0, 0, 1, 5}, {0, 0, 1, 10},
		{0, 0, 3, 5}, {0, 0, 3, 2}, {0, 0, 3, 3}, {0, 0, 3, 4},
		{0, 0, 3, 7}, {0, 0, 3, 8}, {0, 0, 3, 5}, {0, 0, 3, 10},
		{0, 0, 2, 5}, {0, 0, 0, 2}, {0, 0, 10, 3}, {0, 0, 0, 40},
		{0, 0, 2, 0}, {0, 0, 2, 10}, {0, 0, 2, 10}, {0, 0, 2, 5},
	}
	Int256EqualTestResult_1 = []byte{0x41, 0x00, 0x00, 0x81}
	Int256EqualTestMatch_1  = Int256From2Int64(2, 5)

	Int256NotEqualTestResult_1 = []byte{0xbe, 0xff, 0xff, 0x7e}
	Int256NotEqualTestMatch_1  = Int256From2Int64(2, 5)

	Int256LessTestResult_1 = []byte{0x0e, 0xff, 0x00, 0x1a}
	Int256LessTestMatch_1  = Int256From2Int64(2, 5)

	Int256LessEqualTestResult_1 = []byte{0x4f, 0xff, 0x00, 0x9b}
	Int256LessEqualTestMatch_1  = Int256From2Int64(2, 5)

	Int256GreaterTestResult_1 = []byte{0xb0, 0x00, 0xff, 0x64}
	Int256GreaterTestMatch_1  = Int256From2Int64(2, 5)

	Int256GreaterEqualTestResult_1 = []byte{0xf1, 0x00, 0xff, 0xe5}
	Int256GreaterEqualTestMatch_1  = Int256From2Int64(2, 5)

	Int256BetweenTestResult_1 = []byte{0xf1, 0x00, 0x00, 0xe1}
	Int256BetweenTestMatch_1  = Int256From2Int64(2, 5)
	Int256BetweenTestMatch_1b = Int256From2Int64(2, 10)

	// negative and positive values mixed
	Int256TestSlice_2 = []Int256{
		Int256From2Int64(-2, -5), Int256From2Int64(-2, -4), Int256From2Int64(-2, -3), Int256From2Int64(-2, -2),
		Int256From2Int64(-2, -7), Int256From2Int64(-2, -8), Int256From2Int64(-2, -5), Int256From2Int64(-2, -10),
		Int256From2Int64(-1, -5), Int256From2Int64(-1, -4), Int256From2Int64(-1, -3), Int256From2Int64(-1, -2),
		Int256From2Int64(-1, -7), Int256From2Int64(-1, -8), Int256From2Int64(-1, -5), Int256From2Int64(-1, -10),
		Int256From2Int64(-3, -5), Int256From2Int64(-3, -4), Int256From2Int64(-3, -3), Int256From2Int64(-3, -2),
		Int256From2Int64(-3, -7), Int256From2Int64(-3, -8), Int256From2Int64(-3, -5), Int256From2Int64(-3, -10),
		Int256From2Int64(2, -5), Int256From2Int64(2, -4), Int256From2Int64(2, -3), Int256From2Int64(2, -2),
		Int256From2Int64(2, -7), Int256From2Int64(2, -8), Int256From2Int64(2, -5), Int256From2Int64(2, 10),
	}
	Int256EqualTestResult_2 = []byte{0x41, 0x0, 0x0, 0x0}
	Int256EqualTestMatch_2  = Int256From2Int64(-2, -5)

	Int256NotEqualTestResult_2 = []byte{0xbe, 0xff, 0xff, 0xff}
	Int256NotEqualTestMatch_2  = Int256From2Int64(-2, -5)

	Int256LessTestResult_2 = []byte{0xb0, 0x00, 0xff, 0x00}
	Int256LessTestMatch_2  = Int256From2Int64(-2, -5)

	Int256LessEqualTestResult_2 = []byte{0xf1, 0x00, 0xff, 0x00}
	Int256LessEqualTestMatch_2  = Int256From2Int64(-2, -5)

	Int256GreaterTestResult_2 = []byte{0x0e, 0xff, 0x00, 0xff}
	Int256GreaterTestMatch_2  = Int256From2Int64(-2, -5)

	Int256GreaterEqualTestResult_2 = []byte{0x4f, 0xff, 0x00, 0xff}
	Int256GreaterEqualTestMatch_2  = Int256From2Int64(-2, -5)

	Int256BetweenTestResult_2 = []byte{0xf1, 0x00, 0x00, 0x00}
	Int256BetweenTestMatch_2  = Int256From2Int64(-2, -10)
	Int256BetweenTestMatch_2b = Int256From2Int64(-2, -5)

	// extreme values
	Int256TestSlice_3 = []Int256{
		Int256FromInt64(math.MaxInt32), Int256FromInt64(math.MinInt32),
		Int256FromInt64(math.MaxInt64), Int256FromInt64(math.MinInt64),
		Int256FromInt128(MaxInt128), Int256FromInt128(MinInt128),
		MaxInt256, MinInt256,
		Int256FromInt64(math.MaxInt32), Int256FromInt64(math.MinInt32),
		Int256FromInt64(math.MaxInt64), Int256FromInt64(math.MinInt64),
		Int256FromInt128(MaxInt128), Int256FromInt128(MinInt128),
		MaxInt256, MinInt256,
		Int256FromInt64(math.MaxInt32), Int256FromInt64(math.MinInt32),
		Int256FromInt64(math.MaxInt64), Int256FromInt64(math.MinInt64),
		Int256FromInt128(MaxInt128), Int256FromInt128(MinInt128),
		MaxInt256, MinInt256,
		Int256FromInt64(math.MaxInt32), Int256FromInt64(math.MinInt32),
		Int256FromInt64(math.MaxInt64), Int256FromInt64(math.MinInt64),
		Int256FromInt128(MaxInt128), Int256FromInt128(MinInt128),
		MaxInt256, MinInt256,
	}
	Int256EqualTestResult_3 = []byte{0x80, 0x80, 0x80, 0x80}
	Int256EqualTestMatch_3  = MinInt256

	Int256NotEqualTestResult_3 = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	Int256NotEqualTestMatch_3  = MinInt256

	Int256LessTestResult_3 = []byte{0x0, 0x0, 0x0, 0x0}
	Int256LessTestMatch_3  = MinInt256

	Int256LessEqualTestResult_3 = []byte{0x80, 0x80, 0x80, 0x80}
	Int256LessEqualTestMatch_3  = MinInt256

	Int256GreaterTestResult_3 = []byte{0x7f, 0x7f, 0x7f, 0x7f}
	Int256GreaterTestMatch_3  = MinInt256

	Int256GreaterEqualTestResult_3 = []byte{0xff, 0xff, 0xff, 0xff}
	Int256GreaterEqualTestMatch_3  = MinInt256

	Int256BetweenTestResult_3 = []byte{0x50, 0x50, 0x50, 0x50}
	Int256BetweenTestMatch_3  = Int256FromInt128(MaxInt128)
	Int256BetweenTestMatch_3b = MaxInt256
)

// creates an Int256 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - match, match2: are only copied to the resulting test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateInt256TestCase(name string, slice []Int256, match, match2 Int256, result []byte, length int) Int256MatchTest {
	if len(slice)%8 != 0 {
		panic("CreateInt256TestCase: length of slice has to be a multiple of 8")
	}
	if len(result) != bitFieldLen(len(slice)) {
		panic("CreateInt256TestCase: length of slice and length of result does not match")
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []Int256
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
	return Int256MatchTest{
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
var Int256EqualCases = []Int256MatchTest{
	{
		name:   "l0",
		slice:  make([]Int256, 0),
		match:  Int256EqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  Int256EqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
    CreateInt256TestCase("l32", Int256TestSlice_1, Int256EqualTestMatch_1, ZeroInt256, Int256EqualTestResult_1, 32),
	CreateInt256TestCase("l64", Int256TestSlice_1, Int256EqualTestMatch_1, ZeroInt256, Int256EqualTestResult_1, 64),
	CreateInt256TestCase("l128", Int256TestSlice_1, Int256EqualTestMatch_1, ZeroInt256, Int256EqualTestResult_1, 128),
	/*	CreateInt256TestCase("l127", Int256TestSlice_1, Int256EqualTestMatch_1, ZeroInt256, Int256EqualTestResult_1, 127),
		CreateInt256TestCase("l63", Int256TestSlice_1, Int256EqualTestMatch_1, ZeroInt256, Int256EqualTestResult_1, 63),
		CreateInt256TestCase("l31", Int256TestSlice_1, Int256EqualTestMatch_1, ZeroInt256, Int256EqualTestResult_1, 31),
		CreateInt256TestCase("l23", Int256TestSlice_1, Int256EqualTestMatch_1, ZeroInt256, Int256EqualTestResult_1, 23),
		CreateInt256TestCase("l15", Int256TestSlice_1, Int256EqualTestMatch_1, ZeroInt256, Int256EqualTestResult_1, 15),
		CreateInt256TestCase("l7", Int256TestSlice_1, Int256EqualTestMatch_1, ZeroInt256, Int256EqualTestResult_1, 7),*/
	CreateInt256TestCase("neg64", Int256TestSlice_2, Int256EqualTestMatch_2, ZeroInt256, Int256EqualTestResult_2, 64),
	CreateInt256TestCase("neg32", Int256TestSlice_2, Int256EqualTestMatch_2, ZeroInt256, Int256EqualTestResult_2, 32),
	//	CreateInt256TestCase("neg31", Int256TestSlice_2, Int256EqualTestMatch_2, ZeroInt256, Int256EqualTestResult_2, 31),
	CreateInt256TestCase("ext64", Int256TestSlice_3, Int256EqualTestMatch_3, ZeroInt256, Int256EqualTestResult_3, 64),
	CreateInt256TestCase("ext32", Int256TestSlice_3, Int256EqualTestMatch_3, ZeroInt256, Int256EqualTestResult_3, 32),
	//	CreateInt256TestCase("ext31", Int256TestSlice_3, Int256EqualTestMatch_3, ZeroInt256, Int256EqualTestResult_3, 31),
}

func TestMatchInt256EqualGeneric(T *testing.T) {
	for _, c := range Int256EqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt256EqualGeneric(c.slice, c.match, bits, nil)
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

func TestMatchInt256EqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int256EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		src0, src1, src2, src3 := splitInt256Slice(c.slice)
		cnt := matchInt256EqualAVX2(src0, src1, src2, src3, c.match, bits)
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
func TestMatchInt256EqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range Int256EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt256EqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt256EqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		mask := fillBitset(nil, len(a), 0xff)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256EqualGeneric(a, MaxInt256Half, bits, mask)
			}
		})
	}
}

func BenchmarkMatchInt256EqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		src0, src1, src2, src3 := splitInt256Slice(a)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256EqualAVX2(src0, src1, src2, src3, MaxInt256Half, bits)
			}
		})
	}
}

/*
func BenchmarkMatchInt256EqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int256(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256EqualAVX512(a, math.MaxInt256/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Not Equal Testcases
//

var Int256NotEqualCases = []Int256MatchTest{
	{
		name:   "l0",
		slice:  make([]Int256, 0),
		match:  Int256NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  Int256NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt256TestCase("l32", Int256TestSlice_1, Int256NotEqualTestMatch_1, ZeroInt256, Int256NotEqualTestResult_1, 32),
	CreateInt256TestCase("l64", Int256TestSlice_1, Int256NotEqualTestMatch_1, ZeroInt256, Int256NotEqualTestResult_1, 64),
	CreateInt256TestCase("l128", Int256TestSlice_1, Int256NotEqualTestMatch_1, ZeroInt256, Int256NotEqualTestResult_1, 128),
	CreateInt256TestCase("l127", Int256TestSlice_1, Int256NotEqualTestMatch_1, ZeroInt256, Int256NotEqualTestResult_1, 127),
	CreateInt256TestCase("l63", Int256TestSlice_1, Int256NotEqualTestMatch_1, ZeroInt256, Int256NotEqualTestResult_1, 63),
	CreateInt256TestCase("l31", Int256TestSlice_1, Int256NotEqualTestMatch_1, ZeroInt256, Int256NotEqualTestResult_1, 31),
	CreateInt256TestCase("l23", Int256TestSlice_1, Int256NotEqualTestMatch_1, ZeroInt256, Int256NotEqualTestResult_1, 23),
	CreateInt256TestCase("l15", Int256TestSlice_1, Int256NotEqualTestMatch_1, ZeroInt256, Int256NotEqualTestResult_1, 15),
	CreateInt256TestCase("l7", Int256TestSlice_1, Int256NotEqualTestMatch_1, ZeroInt256, Int256NotEqualTestResult_1, 7),
	CreateInt256TestCase("neg64", Int256TestSlice_2, Int256NotEqualTestMatch_2, ZeroInt256, Int256NotEqualTestResult_2, 64),
	CreateInt256TestCase("neg32", Int256TestSlice_2, Int256NotEqualTestMatch_2, ZeroInt256, Int256NotEqualTestResult_2, 32),
	CreateInt256TestCase("neg31", Int256TestSlice_2, Int256NotEqualTestMatch_2, ZeroInt256, Int256NotEqualTestResult_2, 31),
	CreateInt256TestCase("ext64", Int256TestSlice_3, Int256NotEqualTestMatch_3, ZeroInt256, Int256NotEqualTestResult_3, 64),
	CreateInt256TestCase("ext32", Int256TestSlice_3, Int256NotEqualTestMatch_3, ZeroInt256, Int256NotEqualTestResult_3, 32),
	CreateInt256TestCase("ext31", Int256TestSlice_3, Int256NotEqualTestMatch_3, ZeroInt256, Int256NotEqualTestResult_3, 31),
}

func TestMatchInt256NotEqualGeneric(T *testing.T) {
	for _, c := range Int256NotEqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt256NotEqualGeneric(c.slice, c.match, bits, nil)
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
func TestMatchInt256NotEqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int256NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt256NotEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt256NotEqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range Int256NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt256NotEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt256NotEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256NotEqualGeneric(a, MaxInt256Half, bits, nil)
			}
		})
	}
}

/*
func BenchmarkMatchInt256NotEqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int256(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256NotEqualAVX2(a, math.MaxInt256/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt256NotEqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int256(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256NotEqualAVX512(a, math.MaxInt256/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Less Testcases
//
var Int256LessCases = []Int256MatchTest{
	{
		name:   "l0",
		slice:  make([]Int256, 0),
		match:  Int256LessTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  Int256LessTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt256TestCase("l32", Int256TestSlice_1, Int256LessTestMatch_1, ZeroInt256, Int256LessTestResult_1, 32),
	CreateInt256TestCase("l64", Int256TestSlice_1, Int256LessTestMatch_1, ZeroInt256, Int256LessTestResult_1, 64),
	CreateInt256TestCase("l128", Int256TestSlice_1, Int256LessTestMatch_1, ZeroInt256, Int256LessTestResult_1, 128),
	//CreateInt256TestCase("l127", Int256TestSlice_1, Int256LessTestMatch_1, ZeroInt256, Int256LessTestResult_1, 127),
	//CreateInt256TestCase("l63", Int256TestSlice_1, Int256LessTestMatch_1, ZeroInt256, Int256LessTestResult_1, 63),
	//CreateInt256TestCase("l31", Int256TestSlice_1, Int256LessTestMatch_1, ZeroInt256, Int256LessTestResult_1, 31),
	//CreateInt256TestCase("l23", Int256TestSlice_1, Int256LessTestMatch_1, ZeroInt256, Int256LessTestResult_1, 23),
	//CreateInt256TestCase("l15", Int256TestSlice_1, Int256LessTestMatch_1, ZeroInt256, Int256LessTestResult_1, 15),
	//CreateInt256TestCase("l7", Int256TestSlice_1, Int256LessTestMatch_1, ZeroInt256, Int256LessTestResult_1, 7),
	CreateInt256TestCase("neg64", Int256TestSlice_2, Int256LessTestMatch_2, ZeroInt256, Int256LessTestResult_2, 64),
	CreateInt256TestCase("neg32", Int256TestSlice_2, Int256LessTestMatch_2, ZeroInt256, Int256LessTestResult_2, 32),
	//CreateInt256TestCase("neg31", Int256TestSlice_2, Int256LessTestMatch_2, ZeroInt256, Int256LessTestResult_2, 31),
	CreateInt256TestCase("ext64", Int256TestSlice_3, Int256LessTestMatch_3, ZeroInt256, Int256LessTestResult_3, 64),
	CreateInt256TestCase("ext32", Int256TestSlice_3, Int256LessTestMatch_3, ZeroInt256, Int256LessTestResult_3, 32),
	//CreateInt256TestCase("ext31", Int256TestSlice_3, Int256LessTestMatch_3, ZeroInt256, Int256LessTestResult_3, 31),
}

func TestMatchInt256LessGeneric(T *testing.T) {
	for _, c := range Int256LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt256LessThanGeneric(c.slice, c.match, bits, nil)
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

func TestMatchInt256LessAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int256LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		src0, src1, src2, src3 := splitInt256Slice(c.slice)
		cnt := matchInt256LessThanAVX2(src0, src1, src2, src3, c.match, bits)
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
func TestMatchInt256LessAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range Int256LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt256LessThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt256LessGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256LessThanGeneric(a, MaxInt256Half, bits, nil)
			}
		})
	}
}

func BenchmarkMatchInt256LessAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		src0, src1, src2, src3 := splitInt256Slice(a)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256LessThanAVX2(src0, src1, src2, src3, MaxInt256Half, bits)
			}
		})
	}
}
/*
func BenchmarkMatchInt256LessAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int256(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256LessThanAVX512(a, math.MaxInt256/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Less Equal Testcases
//
var Int256LessEqualCases = []Int256MatchTest{
	{
		name:   "l0",
		slice:  make([]Int256, 0),
		match:  Int256LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  Int256LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt256TestCase("l32", Int256TestSlice_1, Int256LessEqualTestMatch_1, ZeroInt256, Int256LessEqualTestResult_1, 32),
	CreateInt256TestCase("l64", Int256TestSlice_1, Int256LessEqualTestMatch_1, ZeroInt256, Int256LessEqualTestResult_1, 64),
	CreateInt256TestCase("l128", Int256TestSlice_1, Int256LessEqualTestMatch_1, ZeroInt256, Int256LessEqualTestResult_1, 128),
	CreateInt256TestCase("l127", Int256TestSlice_1, Int256LessEqualTestMatch_1, ZeroInt256, Int256LessEqualTestResult_1, 127),
	CreateInt256TestCase("l63", Int256TestSlice_1, Int256LessEqualTestMatch_1, ZeroInt256, Int256LessEqualTestResult_1, 63),
	CreateInt256TestCase("l31", Int256TestSlice_1, Int256LessEqualTestMatch_1, ZeroInt256, Int256LessEqualTestResult_1, 31),
	CreateInt256TestCase("l23", Int256TestSlice_1, Int256LessEqualTestMatch_1, ZeroInt256, Int256LessEqualTestResult_1, 23),
	CreateInt256TestCase("l15", Int256TestSlice_1, Int256LessEqualTestMatch_1, ZeroInt256, Int256LessEqualTestResult_1, 15),
	CreateInt256TestCase("l7", Int256TestSlice_1, Int256LessEqualTestMatch_1, ZeroInt256, Int256LessEqualTestResult_1, 7),
	CreateInt256TestCase("neg64", Int256TestSlice_2, Int256LessEqualTestMatch_2, ZeroInt256, Int256LessEqualTestResult_2, 64),
	CreateInt256TestCase("neg32", Int256TestSlice_2, Int256LessEqualTestMatch_2, ZeroInt256, Int256LessEqualTestResult_2, 32),
	CreateInt256TestCase("neg31", Int256TestSlice_2, Int256LessEqualTestMatch_2, ZeroInt256, Int256LessEqualTestResult_2, 31),
	CreateInt256TestCase("ext64", Int256TestSlice_3, Int256LessEqualTestMatch_3, ZeroInt256, Int256LessEqualTestResult_3, 64),
	CreateInt256TestCase("ext32", Int256TestSlice_3, Int256LessEqualTestMatch_3, ZeroInt256, Int256LessEqualTestResult_3, 32),
	CreateInt256TestCase("ext31", Int256TestSlice_3, Int256LessEqualTestMatch_3, ZeroInt256, Int256LessEqualTestResult_3, 31),
}

func TestMatchInt256LessEqualGeneric(T *testing.T) {
	for _, c := range Int256LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt256LessThanEqualGeneric(c.slice, c.match, bits, nil)
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
func TestMatchInt256LessEqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int256LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt256LessThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt256LessEqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range Int256LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt256LessThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt256LessEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256LessThanEqualGeneric(a, MaxInt256Half, bits, nil)
			}
		})
	}
}

/*
func BenchmarkMatchInt256LessEqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int256(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256LessThanEqualAVX2(a, math.MaxInt256/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt256LessEqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int256(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256LessThanEqualAVX512(a, math.MaxInt256/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Greater Testcases
//
var Int256GreaterCases = []Int256MatchTest{
	{
		name:   "l0",
		slice:  make([]Int256, 0),
		match:  Int256GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  Int256GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt256TestCase("l32", Int256TestSlice_1, Int256GreaterTestMatch_1, ZeroInt256, Int256GreaterTestResult_1, 32),
	CreateInt256TestCase("l64", Int256TestSlice_1, Int256GreaterTestMatch_1, ZeroInt256, Int256GreaterTestResult_1, 64),
	CreateInt256TestCase("l128", Int256TestSlice_1, Int256GreaterTestMatch_1, ZeroInt256, Int256GreaterTestResult_1, 128),
	CreateInt256TestCase("l127", Int256TestSlice_1, Int256GreaterTestMatch_1, ZeroInt256, Int256GreaterTestResult_1, 127),
	CreateInt256TestCase("l63", Int256TestSlice_1, Int256GreaterTestMatch_1, ZeroInt256, Int256GreaterTestResult_1, 63),
	CreateInt256TestCase("l31", Int256TestSlice_1, Int256GreaterTestMatch_1, ZeroInt256, Int256GreaterTestResult_1, 31),
	CreateInt256TestCase("l23", Int256TestSlice_1, Int256GreaterTestMatch_1, ZeroInt256, Int256GreaterTestResult_1, 23),
	CreateInt256TestCase("l15", Int256TestSlice_1, Int256GreaterTestMatch_1, ZeroInt256, Int256GreaterTestResult_1, 15),
	CreateInt256TestCase("l7", Int256TestSlice_1, Int256GreaterTestMatch_1, ZeroInt256, Int256GreaterTestResult_1, 7),
	CreateInt256TestCase("neg64", Int256TestSlice_2, Int256GreaterTestMatch_2, ZeroInt256, Int256GreaterTestResult_2, 64),
	CreateInt256TestCase("neg32", Int256TestSlice_2, Int256GreaterTestMatch_2, ZeroInt256, Int256GreaterTestResult_2, 32),
	CreateInt256TestCase("neg31", Int256TestSlice_2, Int256GreaterTestMatch_2, ZeroInt256, Int256GreaterTestResult_2, 31),
	CreateInt256TestCase("ext64", Int256TestSlice_3, Int256GreaterTestMatch_3, ZeroInt256, Int256GreaterTestResult_3, 64),
	CreateInt256TestCase("ext32", Int256TestSlice_3, Int256GreaterTestMatch_3, ZeroInt256, Int256GreaterTestResult_3, 32),
	CreateInt256TestCase("ext31", Int256TestSlice_3, Int256GreaterTestMatch_3, ZeroInt256, Int256GreaterTestResult_3, 31),
}

func TestMatchInt256GreaterGeneric(T *testing.T) {
	for _, c := range Int256GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt256GreaterThanGeneric(c.slice, c.match, bits, nil)
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
func TestMatchInt256GreaterAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int256GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt256GreaterThanAVX2(c.slice, c.match, bits)
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

func TestMatchInt256GreaterAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range Int256GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt256GreaterThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt256GreaterGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256GreaterThanGeneric(a, MaxInt256Half, bits, nil)
			}
		})
	}
}

/*
func BenchmarkMatchInt256GreaterAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int256(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256GreaterThanAVX2(a, math.MaxInt256/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt256GreaterAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int256(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256GreaterThanAVX512(a, math.MaxInt256/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Greater Equal Testcases
//
var Int256GreaterEqualCases = []Int256MatchTest{
	{
		name:   "l0",
		slice:  make([]Int256, 0),
		match:  Int256GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  Int256GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateInt256TestCase("l32", Int256TestSlice_1, Int256GreaterEqualTestMatch_1, ZeroInt256, Int256GreaterEqualTestResult_1, 32),
	CreateInt256TestCase("l64", Int256TestSlice_1, Int256GreaterEqualTestMatch_1, ZeroInt256, Int256GreaterEqualTestResult_1, 64),
	CreateInt256TestCase("l128", Int256TestSlice_1, Int256GreaterEqualTestMatch_1, ZeroInt256, Int256GreaterEqualTestResult_1, 128),
	CreateInt256TestCase("l127", Int256TestSlice_1, Int256GreaterEqualTestMatch_1, ZeroInt256, Int256GreaterEqualTestResult_1, 127),
	CreateInt256TestCase("l63", Int256TestSlice_1, Int256GreaterEqualTestMatch_1, ZeroInt256, Int256GreaterEqualTestResult_1, 63),
	CreateInt256TestCase("l31", Int256TestSlice_1, Int256GreaterEqualTestMatch_1, ZeroInt256, Int256GreaterEqualTestResult_1, 31),
	CreateInt256TestCase("l23", Int256TestSlice_1, Int256GreaterEqualTestMatch_1, ZeroInt256, Int256GreaterEqualTestResult_1, 23),
	CreateInt256TestCase("l15", Int256TestSlice_1, Int256GreaterEqualTestMatch_1, ZeroInt256, Int256GreaterEqualTestResult_1, 15),
	CreateInt256TestCase("l7", Int256TestSlice_1, Int256GreaterEqualTestMatch_1, ZeroInt256, Int256GreaterEqualTestResult_1, 7),
	CreateInt256TestCase("neg64", Int256TestSlice_2, Int256GreaterEqualTestMatch_2, ZeroInt256, Int256GreaterEqualTestResult_2, 64),
	CreateInt256TestCase("neg32", Int256TestSlice_2, Int256GreaterEqualTestMatch_2, ZeroInt256, Int256GreaterEqualTestResult_2, 32),
	CreateInt256TestCase("neg31", Int256TestSlice_2, Int256GreaterEqualTestMatch_2, ZeroInt256, Int256GreaterEqualTestResult_2, 31),
	CreateInt256TestCase("ext64", Int256TestSlice_3, Int256GreaterEqualTestMatch_3, ZeroInt256, Int256GreaterEqualTestResult_3, 64),
	CreateInt256TestCase("ext32", Int256TestSlice_3, Int256GreaterEqualTestMatch_3, ZeroInt256, Int256GreaterEqualTestResult_3, 32),
	CreateInt256TestCase("ext31", Int256TestSlice_3, Int256GreaterEqualTestMatch_3, ZeroInt256, Int256GreaterEqualTestResult_3, 31),
}

func TestMatchInt256GreaterEqualGeneric(T *testing.T) {
	for _, c := range Int256GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt256GreaterThanEqualGeneric(c.slice, c.match, bits, nil)
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
func TestMatchInt256GreaterEqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int256GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt256GreaterThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchInt256GreaterEqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range Int256GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt256GreaterThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchInt256GreaterEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256GreaterThanEqualGeneric(a, MaxInt256Half, bits, nil)
			}
		})
	}
}

/*
func BenchmarkMatchInt256GreaterEqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int256(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256GreaterThanEqualAVX2(a, math.MaxInt256/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt256GreaterEqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int256(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256GreaterThanEqualAVX512(a, math.MaxInt256/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------------
// Between Testcases
//
var Int256BetweenCases = []Int256MatchTest{
	{
		name:   "l0",
		slice:  make([]Int256, 0),
		match:  Int256BetweenTestMatch_1,
		match2: Int256BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  Int256BetweenTestMatch_1,
		match2: Int256BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	},
	CreateInt256TestCase("l32", Int256TestSlice_1, Int256BetweenTestMatch_1, Int256BetweenTestMatch_1b, Int256BetweenTestResult_1, 32),
	CreateInt256TestCase("l64", Int256TestSlice_1, Int256BetweenTestMatch_1, Int256BetweenTestMatch_1b, Int256BetweenTestResult_1, 64),
	CreateInt256TestCase("l128", Int256TestSlice_1, Int256BetweenTestMatch_1, Int256BetweenTestMatch_1b, Int256BetweenTestResult_1, 128),
	CreateInt256TestCase("l127", Int256TestSlice_1, Int256BetweenTestMatch_1, Int256BetweenTestMatch_1b, Int256BetweenTestResult_1, 127),
	CreateInt256TestCase("l63", Int256TestSlice_1, Int256BetweenTestMatch_1, Int256BetweenTestMatch_1b, Int256BetweenTestResult_1, 63),
	CreateInt256TestCase("l31", Int256TestSlice_1, Int256BetweenTestMatch_1, Int256BetweenTestMatch_1b, Int256BetweenTestResult_1, 31),
	CreateInt256TestCase("l23", Int256TestSlice_1, Int256BetweenTestMatch_1, Int256BetweenTestMatch_1b, Int256BetweenTestResult_1, 23),
	CreateInt256TestCase("l15", Int256TestSlice_1, Int256BetweenTestMatch_1, Int256BetweenTestMatch_1b, Int256BetweenTestResult_1, 15),
	CreateInt256TestCase("l7", Int256TestSlice_1, Int256BetweenTestMatch_1, Int256BetweenTestMatch_1b, Int256BetweenTestResult_1, 7),
	CreateInt256TestCase("neg64", Int256TestSlice_2, Int256BetweenTestMatch_2, Int256BetweenTestMatch_2b, Int256BetweenTestResult_2, 64),
	CreateInt256TestCase("neg32", Int256TestSlice_2, Int256BetweenTestMatch_2, Int256BetweenTestMatch_2b, Int256BetweenTestResult_2, 32),
	CreateInt256TestCase("neg31", Int256TestSlice_2, Int256BetweenTestMatch_2, Int256BetweenTestMatch_2b, Int256BetweenTestResult_2, 31),
	CreateInt256TestCase("ext64", Int256TestSlice_3, Int256BetweenTestMatch_3, Int256BetweenTestMatch_3b, Int256BetweenTestResult_3, 64),
	CreateInt256TestCase("ext32", Int256TestSlice_3, Int256BetweenTestMatch_3, Int256BetweenTestMatch_3b, Int256BetweenTestResult_3, 32),
	CreateInt256TestCase("ext31", Int256TestSlice_3, Int256BetweenTestMatch_3, Int256BetweenTestMatch_3b, Int256BetweenTestResult_3, 31),
}

func TestMatchInt256BetweenGeneric(T *testing.T) {
	for _, c := range Int256BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchInt256BetweenGeneric(c.slice, c.match, c.match2, bits, nil)
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
func TestMatchInt256BetweenAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range Int256BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt256BetweenAVX2(c.slice, c.match, c.match2, bits)
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

func TestMatchInt256BetweenAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range Int256BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchInt256BetweenAVX512(c.slice, c.match, c.match2, bits)
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
func BenchmarkMatchInt256BetweenGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256BetweenGeneric(a, MaxInt256Quart, MaxInt256Half, bits, nil)
			}
		})
	}
}

/*
func BenchmarkMatchInt256BetweenAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int256(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256BetweenAVX2(a, math.MaxInt256/4, math.MaxInt256/2, bits)
			}
		})
	}
}

func BenchmarkMatchInt256BetweenAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randInt256Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(Int256(n.l * Int256Size))
			for i := 0; i < B.N; i++ {
				matchInt256BetweenAVX512(a, math.MaxInt256/4, math.MaxInt256/2, bits)
			}
		})
	}
}
*/
// -----------------------------------------------------------------------
// Int256 Slice
//
func TestUniqueInt256(T *testing.T) {
	a := randInt256Slice(1000, 5)
	b := UniqueInt256Slice(a)
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

func BenchmarkUniqueInt256(B *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt256Slice(n, 5)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				UniqueInt256Slice(a)
			}
		})
	}
}

func TestInt256SliceContains(T *testing.T) {
	// nil slice

	if Int256Slice(nil).Contains(Int256FromInt64(1)) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if (Int256Slice{}).Contains(Int256FromInt64(1)) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !(Int256Slice{Int256FromInt64(1)}).Contains(Int256FromInt64(1)) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if (Int256Slice{Int256FromInt64(1)}).Contains(Int256FromInt64(2)) {
		T.Errorf("1-element slice found wrong match")
	}

	slice := Int256Slice{Int256FromInt64(-1),
		Int256FromInt64(3),
		Int256FromInt64(5),
		Int256FromInt64(7),
		Int256FromInt64(11),
		Int256FromInt64(13),
	}

	// n-element slice positive first element
	if !slice.Contains(Int256FromInt64(-1)) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !slice.Contains(Int256FromInt64(5)) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !slice.Contains(Int256FromInt64(13)) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if slice.Contains(Int256FromInt64(0)) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if slice.Contains(Int256FromInt64(2)) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if slice.Contains(Int256FromInt64(14)) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkInt256SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := randInt256Slice(n, 1).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(Int256FromInt64(rand.Int63()))
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := randInt256Slice(n, 1).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(a[rand.Intn(len(a))])
			}
		})
	}
}

func TestInt256SliceContainsRange(T *testing.T) {
	type VecTestRange struct {
		Name  string
		From  Int256
		To    Int256
		Match bool
	}

	type VecTestcase struct {
		Slice  Int256Slice
		Ranges []VecTestRange
	}

	var tests = []VecTestcase{
		// nil slice
		VecTestcase{
			Slice: nil,
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: Int256FromInt64(0), To: Int256FromInt64(2), Match: false},
			},
		},
		// empty slice
		VecTestcase{
			Slice: Int256Slice{},
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: Int256FromInt64(0), To: Int256FromInt64(2), Match: false},
			},
		},
		// 1-element slice
		VecTestcase{
			Slice: Int256Slice{Int256FromInt64(3)},
			Ranges: []VecTestRange{
				VecTestRange{Name: "A", From: Int256FromInt64(0), To: Int256FromInt64(2), Match: false},   // Case A
				VecTestRange{Name: "B1", From: Int256FromInt64(1), To: Int256FromInt64(3), Match: true},   // Case B.1, D1
				VecTestRange{Name: "B3", From: Int256FromInt64(3), To: Int256FromInt64(4), Match: true},   // Case B.3, D3
				VecTestRange{Name: "E", From: Int256FromInt64(15), To: Int256FromInt64(16), Match: false}, // Case E
				VecTestRange{Name: "F", From: Int256FromInt64(1), To: Int256FromInt64(4), Match: true},    // Case F
			},
		},
		// 1-element slice, from == to
		VecTestcase{
			Slice: Int256Slice{Int256FromInt64(3)},
			Ranges: []VecTestRange{
				VecTestRange{Name: "BCD", From: Int256FromInt64(3), To: Int256FromInt64(3), Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		VecTestcase{
			Slice: Int256Slice{Int256FromInt64(3), Int256FromInt64(5), Int256FromInt64(7), Int256FromInt64(11), Int256FromInt64(13)},
			Ranges: []VecTestRange{
				VecTestRange{Name: "A", From: Int256FromInt64(0), To: Int256FromInt64(2), Match: false},    // Case A
				VecTestRange{Name: "B1a", From: Int256FromInt64(1), To: Int256FromInt64(3), Match: true},   // Case B.1
				VecTestRange{Name: "B1b", From: Int256FromInt64(3), To: Int256FromInt64(3), Match: true},   // Case B.1
				VecTestRange{Name: "B2a", From: Int256FromInt64(1), To: Int256FromInt64(4), Match: true},   // Case B.2
				VecTestRange{Name: "B2b", From: Int256FromInt64(1), To: Int256FromInt64(5), Match: true},   // Case B.2
				VecTestRange{Name: "B3a", From: Int256FromInt64(3), To: Int256FromInt64(4), Match: true},   // Case B.3
				VecTestRange{Name: "B3b", From: Int256FromInt64(3), To: Int256FromInt64(5), Match: true},   // Case B.3
				VecTestRange{Name: "C1a", From: Int256FromInt64(4), To: Int256FromInt64(5), Match: true},   // Case C.1
				VecTestRange{Name: "C1b", From: Int256FromInt64(4), To: Int256FromInt64(6), Match: true},   // Case C.1
				VecTestRange{Name: "C1c", From: Int256FromInt64(4), To: Int256FromInt64(7), Match: true},   // Case C.1
				VecTestRange{Name: "C1d", From: Int256FromInt64(5), To: Int256FromInt64(5), Match: true},   // Case C.1
				VecTestRange{Name: "C2a", From: Int256FromInt64(8), To: Int256FromInt64(8), Match: false},  // Case C.2
				VecTestRange{Name: "C2b", From: Int256FromInt64(8), To: Int256FromInt64(10), Match: false}, // Case C.2
				VecTestRange{Name: "D1a", From: Int256FromInt64(11), To: Int256FromInt64(13), Match: true}, // Case D.1
				VecTestRange{Name: "D1b", From: Int256FromInt64(12), To: Int256FromInt64(13), Match: true}, // Case D.1
				VecTestRange{Name: "D2", From: Int256FromInt64(12), To: Int256FromInt64(14), Match: true},  // Case D.2
				VecTestRange{Name: "D3a", From: Int256FromInt64(13), To: Int256FromInt64(13), Match: true}, // Case D.3
				VecTestRange{Name: "D3b", From: Int256FromInt64(13), To: Int256FromInt64(14), Match: true}, // Case D.3
				VecTestRange{Name: "E", From: Int256FromInt64(15), To: Int256FromInt64(16), Match: false},  // Case E
				VecTestRange{Name: "Fa", From: Int256FromInt64(0), To: Int256FromInt64(16), Match: true},   // Case F
				VecTestRange{Name: "Fb", From: Int256FromInt64(0), To: Int256FromInt64(13), Match: true},   // Case F
				VecTestRange{Name: "Fc", From: Int256FromInt64(3), To: Int256FromInt64(13), Match: true},   // Case F
			},
		},
		// real-word testcase
		VecTestcase{
			Slice: Int256Slice{
				Int256FromInt64(699421), Int256FromInt64(1374016), Int256FromInt64(1692360), Int256FromInt64(1797909),
				Int256FromInt64(1809339), Int256FromInt64(2552208), Int256FromInt64(2649552), Int256FromInt64(2740915),
				Int256FromInt64(2769610), Int256FromInt64(3043393),
			},
			Ranges: []VecTestRange{
				VecTestRange{Name: "1", From: Int256FromInt64(2785281), To: Int256FromInt64(2818048), Match: false},
				VecTestRange{Name: "2", From: Int256FromInt64(2818049), To: Int256FromInt64(2850816), Match: false},
				VecTestRange{Name: "3", From: Int256FromInt64(2850817), To: Int256FromInt64(2883584), Match: false},
				VecTestRange{Name: "4", From: Int256FromInt64(2883585), To: Int256FromInt64(2916352), Match: false},
				VecTestRange{Name: "5", From: Int256FromInt64(2916353), To: Int256FromInt64(2949120), Match: false},
				VecTestRange{Name: "6", From: Int256FromInt64(2949121), To: Int256FromInt64(2981888), Match: false},
				VecTestRange{Name: "7", From: Int256FromInt64(2981889), To: Int256FromInt64(3014656), Match: false},
				VecTestRange{Name: "8", From: Int256FromInt64(3014657), To: Int256FromInt64(3047424), Match: true},
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

func BenchmarkInt256SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randInt256Slice(n, 1).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := rand.Int63(), rand.Int63()
				if min > max {
					min, max = max, min
				}
				a.ContainsRange(Int256FromInt64(min), Int256FromInt64(max))
			}
		})
	}
}
