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

const Uint32Size = 4

type Uint32MatchTest struct {
	name   string
	slice  []uint32
	match  uint32 // used for every test
	match2 uint32 // used for between tests
	result []byte
	count  int64
}

var (
	uint32TestSlice_0 = []uint32{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	uint32EqualTestMatch_0  uint32 = 5
	uint32EqualTestResult_0        = []byte{0x56, 0x78, 0x12, 0x34}

	uint32NotEqualTestMatch_0  uint32 = 5
	uint32NotEqualTestResult_0        = []byte{0xa9, 0x87, 0xed, 0xcb}

	uint32LessTestMatch_0  uint32 = 5
	uint32LessTestResult_0        = []byte{0xa0, 0x84, 0xe4, 0x80}

	uint32LessEqualTestMatch_0  uint32 = 5
	uint32LessEqualTestResult_0        = []byte{0xf6, 0xfc, 0xf6, 0xb4}

	uint32GreaterTestMatch_0  uint32 = 5
	uint32GreaterTestResult_0        = []byte{0x09, 0x03, 0x09, 0x4b}

	uint32GreaterEqualTestMatch_0  uint32 = 5
	uint32GreaterEqualTestResult_0        = []byte{0x5f, 0x7b, 0x1b, 0x7f}

	uint32BetweenTestMatch_0  uint32 = 5
	uint32BetweenTestMatch_0b uint32 = 10
	uint32BetweenTestResult_0        = []byte{0x5f, 0x78, 0x1b, 0x34}

	uint32TestSlice_1 = []uint32{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 500000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}

	uint32EqualTestResult_1        = []byte{0x82, 0x42, 0x23, 0x70}
	uint32EqualTestMatch_1  uint32 = 5

	uint32NotEqualTestResult_1        = []byte{0x7d, 0xbd, 0xdc, 0x8f}
	uint32NotEqualTestMatch_1  uint32 = 5

	uint32LessTestResult_1        = []byte{0x70, 0x00, 0x00, 0x00}
	uint32LessTestMatch_1  uint32 = 5

	uint32LessEqualTestResult_1        = []byte{0xf2, 0x42, 0x23, 0x70}
	uint32LessEqualTestMatch_1  uint32 = 5

	uint32GreaterTestResult_1        = []byte{0x0d, 0xbd, 0xdc, 0x8f}
	uint32GreaterTestMatch_1  uint32 = 5

	uint32GreaterEqualTestResult_1        = []byte{0x8f, 0xff, 0xff, 0xff}
	uint32GreaterEqualTestMatch_1  uint32 = 5

	uint32BetweenTestResult_1        = []byte{0x8f, 0x42, 0x23, 0x70}
	uint32BetweenTestMatch_1  uint32 = 5
	uint32BetweenTestMatch_1b uint32 = 10

	// extreme values
	uint32TestSlice_2 = []uint32{
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
		0, math.MaxUint8, math.MaxUint16, math.MaxUint32,
	}

	uint32EqualTestResult_2        = []byte{0x11, 0x11, 0x11, 0x11}
	uint32EqualTestMatch_2  uint32 = math.MaxUint32

	uint32NotEqualTestResult_2        = []byte{0xee, 0xee, 0xee, 0xee}
	uint32NotEqualTestMatch_2  uint32 = math.MaxUint32

	uint32LessTestResult_2        = []byte{0xee, 0xee, 0xee, 0xee}
	uint32LessTestMatch_2  uint32 = math.MaxUint32

	uint32LessEqualTestResult_2        = []byte{0xff, 0xff, 0xff, 0xff}
	uint32LessEqualTestMatch_2  uint32 = math.MaxUint32

	uint32GreaterTestResult_2        = []byte{0x00, 0x00, 0x00, 0x00}
	uint32GreaterTestMatch_2  uint32 = math.MaxUint32

	uint32GreaterEqualTestResult_2        = []byte{0x11, 0x11, 0x11, 0x11}
	uint32GreaterEqualTestMatch_2  uint32 = math.MaxUint32

	uint32BetweenTestResult_2        = []byte{0x33, 0x33, 0x33, 0x33}
	uint32BetweenTestMatch_2  uint32 = math.MaxUint16
	uint32BetweenTestMatch_2b uint32 = math.MaxUint32
)

func randUint32Slice(n, u int) []uint32 {
	s := make([]uint32, n*u)
	for i := 0; i < n; i++ {
		s[i] = rand.Uint32()
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

// creates an uint32 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - match, match2: are only copied to the resulting test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateUint32TestCase(name string, slice []uint32, match, match2 uint32, result []byte, length int) Uint32MatchTest {
	if len(slice)%8 != 0 {
		panic("CreateUint32TestCase: length of slice has to be a multiple of 8")
	}
	if len(result) != bitFieldLen(len(slice)) {
		panic("CreateUint32TestCase: length of slice and length of result does not match")
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []uint32
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
	return Uint32MatchTest{
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

var uint32EqualCases = []Uint32MatchTest{
	{
		name:   "l0",
		slice:  make([]uint32, 0),
		match:  uint32EqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint32EqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateUint32TestCase("vec1", uint32TestSlice_0, uint32EqualTestMatch_0, 0, uint32EqualTestResult_0, 32),
	CreateUint32TestCase("vec2", uint32TestSlice_0, uint32EqualTestMatch_0, 0, uint32EqualTestResult_0, 64),
	CreateUint32TestCase("l32", uint32TestSlice_1, uint32EqualTestMatch_1, 0, uint32EqualTestResult_1, 32),
	CreateUint32TestCase("l64", append(uint32TestSlice_1, uint32TestSlice_0...), uint32EqualTestMatch_1, 0,
		append(uint32EqualTestResult_1, uint32EqualTestResult_0...), 64),
	CreateUint32TestCase("l128", append(uint32TestSlice_1, uint32TestSlice_0...), uint32EqualTestMatch_1, 0,
		append(uint32EqualTestResult_1, uint32EqualTestResult_0...), 128),
	CreateUint32TestCase("l127", uint32TestSlice_1, uint32EqualTestMatch_1, 0, uint32EqualTestResult_1, 127),
	CreateUint32TestCase("l63", uint32TestSlice_1, uint32EqualTestMatch_1, 0, uint32EqualTestResult_1, 63),
	CreateUint32TestCase("l31", uint32TestSlice_1, uint32EqualTestMatch_1, 0, uint32EqualTestResult_1, 31),
	CreateUint32TestCase("l23", uint32TestSlice_1, uint32EqualTestMatch_1, 0, uint32EqualTestResult_1, 23),
	CreateUint32TestCase("l15", uint32TestSlice_1, uint32EqualTestMatch_1, 0, uint32EqualTestResult_1, 15),
	CreateUint32TestCase("l7", uint32TestSlice_1, uint32EqualTestMatch_1, 0, uint32EqualTestResult_1, 7),
	// with extreme values
	CreateUint32TestCase("ext64", uint32TestSlice_2, uint32EqualTestMatch_2, 0, uint32EqualTestResult_2, 64),
	CreateUint32TestCase("ext32", uint32TestSlice_2, uint32EqualTestMatch_2, 0, uint32EqualTestResult_2, 32),
	CreateUint32TestCase("ext31", uint32TestSlice_2, uint32EqualTestMatch_2, 0, uint32EqualTestResult_2, 31),
}

func TestMatchUint32EqualGeneric(T *testing.T) {
	for _, c := range uint32EqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint32EqualGeneric(c.slice, c.match, bits)
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

//func TestMatchUint32EqualAVX2(T *testing.T) {
//if !useAVX2 {
//T.SkipNow()
//}
//for _, c := range uint32EqualCases {
//// pre-allocate the result slice and fill with poison
//l := bitFieldLen(len(c.slice))
//bits := make([]byte, l+32)
//for i, _ := range bits {
//bits[i] = 0xfa
//}
//bits = bits[:l]
//cnt := matchUint32EqualAVX2(c.slice, c.match, bits)
//if got, want := len(bits), len(c.result); got != want {
//T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
//}
//if got, want := cnt, c.count; got != want {
//T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
//}
//if bytes.Compare(bits, c.result) != 0 {
//T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
//}
//if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
//T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
//}
//}
//}

func TestMatchUint32EqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range uint32EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint32EqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint32EqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32EqualGeneric(a, math.MaxUint32/2, bits)
			}
		})
	}
}

//func BenchmarkMatchUint32EqualAVX2(B *testing.B) {
//if !useAVX2 {
//B.SkipNow()
//}
//for _, n := range vecBenchmarkSizes {
//a := randUint32Slice(n.l, 1)
//bits := make([]byte, bitFieldLen(len(a)))
//B.Run(n.name, func(B *testing.B) {
//B.SetBytes(int64(n.l * Uint32Size))
//for i := 0; i < B.N; i++ {
//matchUint32EqualAVX2(a, math.MaxUint32/2, bits)
//}
//})
//}
//}

func BenchmarkMatchUint32EqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32EqualAVX512(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// NotEqual Testcases
//

var uint32NotEqualCases = []Uint32MatchTest{
	{
		name:   "l0",
		slice:  make([]uint32, 0),
		match:  uint32NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint32NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateUint32TestCase("vec1", uint32TestSlice_0, uint32NotEqualTestMatch_0, 0, uint32NotEqualTestResult_0, 32),
	CreateUint32TestCase("vec2", uint32TestSlice_0, uint32NotEqualTestMatch_0, 0, uint32NotEqualTestResult_0, 64),
	CreateUint32TestCase("l32", uint32TestSlice_1, uint32NotEqualTestMatch_1, 0, uint32NotEqualTestResult_1, 32),
	CreateUint32TestCase("l64", append(uint32TestSlice_1, uint32TestSlice_0...), uint32NotEqualTestMatch_1, 0,
		append(uint32NotEqualTestResult_1, uint32NotEqualTestResult_0...), 64),
	CreateUint32TestCase("l128", append(uint32TestSlice_1, uint32TestSlice_0...), uint32NotEqualTestMatch_1, 0,
		append(uint32NotEqualTestResult_1, uint32NotEqualTestResult_0...), 128),
	CreateUint32TestCase("l127", uint32TestSlice_1, uint32NotEqualTestMatch_1, 0, uint32NotEqualTestResult_1, 127),
	CreateUint32TestCase("l63", uint32TestSlice_1, uint32NotEqualTestMatch_1, 0, uint32NotEqualTestResult_1, 63),
	CreateUint32TestCase("l31", uint32TestSlice_1, uint32NotEqualTestMatch_1, 0, uint32NotEqualTestResult_1, 31),
	CreateUint32TestCase("l23", uint32TestSlice_1, uint32NotEqualTestMatch_1, 0, uint32NotEqualTestResult_1, 23),
	CreateUint32TestCase("l15", uint32TestSlice_1, uint32NotEqualTestMatch_1, 0, uint32NotEqualTestResult_1, 15),
	CreateUint32TestCase("l7", uint32TestSlice_1, uint32NotEqualTestMatch_1, 0, uint32NotEqualTestResult_1, 7),
	// with extreme values
	CreateUint32TestCase("ext64", uint32TestSlice_2, uint32NotEqualTestMatch_2, 0, uint32NotEqualTestResult_2, 64),
	CreateUint32TestCase("ext32", uint32TestSlice_2, uint32NotEqualTestMatch_2, 0, uint32NotEqualTestResult_2, 32),
	CreateUint32TestCase("ext31", uint32TestSlice_2, uint32NotEqualTestMatch_2, 0, uint32NotEqualTestResult_2, 31),
}

func TestMatchUint32NotEqualGeneric(T *testing.T) {
	for _, c := range uint32NotEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint32NotEqualGeneric(c.slice, c.match, bits)
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

//func TestMatchUint32NotEqualAVX2(T *testing.T) {
//if !useAVX2 {
//T.SkipNow()
//}
//for _, c := range uint32NotEqualCases {
//// pre-allocate the result slice and fill with poison
//l := bitFieldLen(len(c.slice))
//bits := make([]byte, l+32)
//for i, _ := range bits {
//bits[i] = 0xfa
//}
//bits = bits[:l]
//cnt := matchUint32NotEqualAVX2(c.slice, c.match, bits)
//if got, want := len(bits), len(c.result); got != want {
//T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
//}
//if got, want := cnt, c.count; got != want {
//T.Errorf("%s: unexpected result bit count %d, expected %d", c.name, got, want)
//}
//if bytes.Compare(bits, c.result) != 0 {
//T.Errorf("%s: unexpected result %x, expected %x", c.name, bits, c.result)
//}
//if bytes.Compare(bits[l:l+32], bytes.Repeat([]byte{0xfa}, 32)) != 0 {
//T.Errorf("%s: result boundary violation %x", c.name, bits[l:l+32])
//}
//}
//}

func TestMatchUint32NotEqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range uint32NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint32NotEqualAVX512(c.slice, c.match, bits)
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
// NotEqual benchmarks
//
func BenchmarkMatchUint32NotEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32NotEqualGeneric(a, math.MaxUint32/2, bits)
			}
		})
	}
}

/*func BenchmarkMatchUint32NotEqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32NotEqualAVX2(a, math.MaxUint32/2, bits)
			}
		})
	}
}
*/
func BenchmarkMatchUint32NotEqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32NotEqualAVX512(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
//

var uint32LessCases = []Uint32MatchTest{
	{
		name:   "l0",
		slice:  make([]uint32, 0),
		match:  uint32LessTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint32LessTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateUint32TestCase("vec1", uint32TestSlice_0, uint32LessTestMatch_0, 0, uint32LessTestResult_0, 32),
	CreateUint32TestCase("vec2", uint32TestSlice_0, uint32LessTestMatch_0, 0, uint32LessTestResult_0, 64),
	CreateUint32TestCase("l32", uint32TestSlice_1, uint32LessTestMatch_1, 0, uint32LessTestResult_1, 32),
	CreateUint32TestCase("l64", append(uint32TestSlice_1, uint32TestSlice_0...), uint32LessTestMatch_1, 0,
		append(uint32LessTestResult_1, uint32LessTestResult_0...), 64),
	CreateUint32TestCase("l128", append(uint32TestSlice_1, uint32TestSlice_0...), uint32LessTestMatch_1, 0,
		append(uint32LessTestResult_1, uint32LessTestResult_0...), 128),
	CreateUint32TestCase("l127", uint32TestSlice_1, uint32LessTestMatch_1, 0, uint32LessTestResult_1, 127),
	CreateUint32TestCase("l63", uint32TestSlice_1, uint32LessTestMatch_1, 0, uint32LessTestResult_1, 63),
	CreateUint32TestCase("l31", uint32TestSlice_1, uint32LessTestMatch_1, 0, uint32LessTestResult_1, 31),
	CreateUint32TestCase("l23", uint32TestSlice_1, uint32LessTestMatch_1, 0, uint32LessTestResult_1, 23),
	CreateUint32TestCase("l15", uint32TestSlice_1, uint32LessTestMatch_1, 0, uint32LessTestResult_1, 15),
	CreateUint32TestCase("l7", uint32TestSlice_1, uint32LessTestMatch_1, 0, uint32LessTestResult_1, 7),
	// with extreme values
	CreateUint32TestCase("ext64", uint32TestSlice_2, uint32LessTestMatch_2, 0, uint32LessTestResult_2, 64),
	CreateUint32TestCase("ext32", uint32TestSlice_2, uint32LessTestMatch_2, 0, uint32LessTestResult_2, 32),
	CreateUint32TestCase("ext31", uint32TestSlice_2, uint32LessTestMatch_2, 0, uint32LessTestResult_2, 31),
}

func TestMatchUint32LessGeneric(T *testing.T) {
	for _, c := range uint32LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint32LessThanGeneric(c.slice, c.match, bits)
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

/*func TestMatchUint32LessAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range uint32LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint32LessThanAVX2(c.slice, c.match, bits)
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
func TestMatchUint32LessAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range uint32LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint32LessThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint32LessGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32LessThanGeneric(a, math.MaxUint32/2, bits)
			}
		})
	}
}

/*func BenchmarkMatchUint32LessAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32LessThanAVX2(a, math.MaxUint32/2, bits)
			}
		})
	}
}*/

func BenchmarkMatchUint32LessAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32LessThanAVX512(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//

var uint32LessEqualCases = []Uint32MatchTest{
	{
		name:   "l0",
		slice:  make([]uint32, 0),
		match:  uint32LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint32LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateUint32TestCase("vec1", uint32TestSlice_0, uint32LessEqualTestMatch_0, 0, uint32LessEqualTestResult_0, 32),
	CreateUint32TestCase("vec2", uint32TestSlice_0, uint32LessEqualTestMatch_0, 0, uint32LessEqualTestResult_0, 64),
	CreateUint32TestCase("l32", uint32TestSlice_1, uint32LessEqualTestMatch_1, 0, uint32LessEqualTestResult_1, 32),
	CreateUint32TestCase("l64", append(uint32TestSlice_1, uint32TestSlice_0...), uint32LessEqualTestMatch_1, 0,
		append(uint32LessEqualTestResult_1, uint32LessEqualTestResult_0...), 64),
	CreateUint32TestCase("l128", append(uint32TestSlice_1, uint32TestSlice_0...), uint32LessEqualTestMatch_1, 0,
		append(uint32LessEqualTestResult_1, uint32LessEqualTestResult_0...), 128),
	CreateUint32TestCase("l127", uint32TestSlice_1, uint32LessEqualTestMatch_1, 0, uint32LessEqualTestResult_1, 127),
	CreateUint32TestCase("l63", uint32TestSlice_1, uint32LessEqualTestMatch_1, 0, uint32LessEqualTestResult_1, 63),
	CreateUint32TestCase("l31", uint32TestSlice_1, uint32LessEqualTestMatch_1, 0, uint32LessEqualTestResult_1, 31),
	CreateUint32TestCase("l23", uint32TestSlice_1, uint32LessEqualTestMatch_1, 0, uint32LessEqualTestResult_1, 23),
	CreateUint32TestCase("l15", uint32TestSlice_1, uint32LessEqualTestMatch_1, 0, uint32LessEqualTestResult_1, 15),
	CreateUint32TestCase("l7", uint32TestSlice_1, uint32LessEqualTestMatch_1, 0, uint32LessEqualTestResult_1, 7),
	// with extreme values
	CreateUint32TestCase("ext64", uint32TestSlice_2, uint32LessEqualTestMatch_2, 0, uint32LessEqualTestResult_2, 64),
	CreateUint32TestCase("ext32", uint32TestSlice_2, uint32LessEqualTestMatch_2, 0, uint32LessEqualTestResult_2, 32),
	CreateUint32TestCase("ext31", uint32TestSlice_2, uint32LessEqualTestMatch_2, 0, uint32LessEqualTestResult_2, 31),
}

func TestMatchUint32LessEqualGeneric(T *testing.T) {
	for _, c := range uint32LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint32LessThanEqualGeneric(c.slice, c.match, bits)
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

/*func TestMatchUint32LessEqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range uint32LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint32LessThanEqualAVX2(c.slice, c.match, bits)
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
func TestMatchUint32LessEqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range uint32LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint32LessThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint32LessEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32LessThanEqualGeneric(a, math.MaxUint32/2, bits)
			}
		})
	}
}

/*func BenchmarkMatchUint32LessEqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32LessThanEqualAVX2(a, math.MaxUint32/2, bits)
			}
		})
	}
}*/

func BenchmarkMatchUint32LessEqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32LessThanEqualAVX512(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
//

var uint32GreaterCases = []Uint32MatchTest{
	{
		name:   "l0",
		slice:  make([]uint32, 0),
		match:  uint32GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint32GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateUint32TestCase("vec1", uint32TestSlice_0, uint32GreaterTestMatch_0, 0, uint32GreaterTestResult_0, 32),
	CreateUint32TestCase("vec2", uint32TestSlice_0, uint32GreaterTestMatch_0, 0, uint32GreaterTestResult_0, 64),
	CreateUint32TestCase("l32", uint32TestSlice_1, uint32GreaterTestMatch_1, 0, uint32GreaterTestResult_1, 32),
	CreateUint32TestCase("l64", append(uint32TestSlice_1, uint32TestSlice_0...), uint32GreaterTestMatch_1, 0,
		append(uint32GreaterTestResult_1, uint32GreaterTestResult_0...), 64),
	CreateUint32TestCase("l128", append(uint32TestSlice_1, uint32TestSlice_0...), uint32GreaterTestMatch_1, 0,
		append(uint32GreaterTestResult_1, uint32GreaterTestResult_0...), 128),
	CreateUint32TestCase("l127", uint32TestSlice_1, uint32GreaterTestMatch_1, 0, uint32GreaterTestResult_1, 127),
	CreateUint32TestCase("l63", uint32TestSlice_1, uint32GreaterTestMatch_1, 0, uint32GreaterTestResult_1, 63),
	CreateUint32TestCase("l31", uint32TestSlice_1, uint32GreaterTestMatch_1, 0, uint32GreaterTestResult_1, 31),
	CreateUint32TestCase("l23", uint32TestSlice_1, uint32GreaterTestMatch_1, 0, uint32GreaterTestResult_1, 23),
	CreateUint32TestCase("l15", uint32TestSlice_1, uint32GreaterTestMatch_1, 0, uint32GreaterTestResult_1, 15),
	CreateUint32TestCase("l7", uint32TestSlice_1, uint32GreaterTestMatch_1, 0, uint32GreaterTestResult_1, 7),
	// with extreme values
	CreateUint32TestCase("ext64", uint32TestSlice_2, uint32GreaterTestMatch_2, 0, uint32GreaterTestResult_2, 64),
	CreateUint32TestCase("ext32", uint32TestSlice_2, uint32GreaterTestMatch_2, 0, uint32GreaterTestResult_2, 32),
	CreateUint32TestCase("ext31", uint32TestSlice_2, uint32GreaterTestMatch_2, 0, uint32GreaterTestResult_2, 31),
}

func TestMatchUint32GreaterGeneric(T *testing.T) {
	for _, c := range uint32GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint32GreaterThanGeneric(c.slice, c.match, bits)
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

/*func TestMatchUint32GreaterAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range uint32GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint32GreaterThanAVX2(c.slice, c.match, bits)
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

func TestMatchUint32GreaterAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range uint32GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint32GreaterThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint32GreaterGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32GreaterThanGeneric(a, math.MaxUint32/2, bits)
			}
		})
	}
}

/*func BenchmarkMatchUint32GreaterAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32GreaterThanAVX2(a, math.MaxUint32/2, bits)
			}
		})
	}
}*/

func BenchmarkMatchUint32GreaterAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32GreaterThanAVX512(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//

var uint32GreaterEqualCases = []Uint32MatchTest{
	{
		name:   "l0",
		slice:  make([]uint32, 0),
		match:  uint32GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint32GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateUint32TestCase("vec1", uint32TestSlice_0, uint32GreaterEqualTestMatch_0, 0, uint32GreaterEqualTestResult_0, 32),
	CreateUint32TestCase("vec2", uint32TestSlice_0, uint32GreaterEqualTestMatch_0, 0, uint32GreaterEqualTestResult_0, 64),
	CreateUint32TestCase("l32", uint32TestSlice_1, uint32GreaterEqualTestMatch_1, 0, uint32GreaterEqualTestResult_1, 32),
	CreateUint32TestCase("l64", append(uint32TestSlice_1, uint32TestSlice_0...), uint32GreaterEqualTestMatch_1, 0,
		append(uint32GreaterEqualTestResult_1, uint32GreaterEqualTestResult_0...), 64),
	CreateUint32TestCase("l128", append(uint32TestSlice_1, uint32TestSlice_0...), uint32GreaterEqualTestMatch_1, 0,
		append(uint32GreaterEqualTestResult_1, uint32GreaterEqualTestResult_0...), 128),
	CreateUint32TestCase("l127", uint32TestSlice_1, uint32GreaterEqualTestMatch_1, 0, uint32GreaterEqualTestResult_1, 127),
	CreateUint32TestCase("l63", uint32TestSlice_1, uint32GreaterEqualTestMatch_1, 0, uint32GreaterEqualTestResult_1, 63),
	CreateUint32TestCase("l31", uint32TestSlice_1, uint32GreaterEqualTestMatch_1, 0, uint32GreaterEqualTestResult_1, 31),
	CreateUint32TestCase("l23", uint32TestSlice_1, uint32GreaterEqualTestMatch_1, 0, uint32GreaterEqualTestResult_1, 23),
	CreateUint32TestCase("l15", uint32TestSlice_1, uint32GreaterEqualTestMatch_1, 0, uint32GreaterEqualTestResult_1, 15),
	CreateUint32TestCase("l7", uint32TestSlice_1, uint32GreaterEqualTestMatch_1, 0, uint32GreaterEqualTestResult_1, 7),
	// with extreme values
	CreateUint32TestCase("ext64", uint32TestSlice_2, uint32GreaterEqualTestMatch_2, 0, uint32GreaterEqualTestResult_2, 64),
	CreateUint32TestCase("ext32", uint32TestSlice_2, uint32GreaterEqualTestMatch_2, 0, uint32GreaterEqualTestResult_2, 32),
	CreateUint32TestCase("ext31", uint32TestSlice_2, uint32GreaterEqualTestMatch_2, 0, uint32GreaterEqualTestResult_2, 31),
}

func TestMatchUint32GreaterEqualGeneric(T *testing.T) {
	for _, c := range uint32GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint32GreaterThanEqualGeneric(c.slice, c.match, bits)
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

/*func TestMatchUint32GreaterEqualAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range uint32GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint32GreaterThanEqualAVX2(c.slice, c.match, bits)
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
func TestMatchUint32GreaterEqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range uint32GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint32GreaterThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchUint32GreaterEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32GreaterThanEqualGeneric(a, math.MaxUint32/2, bits)
			}
		})
	}
}

/*func BenchmarkMatchUint32GreaterEqualAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32GreaterThanEqualAVX2(a, math.MaxUint32/2, bits)
			}
		})
	}
}*/

func BenchmarkMatchUint32GreaterEqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32GreaterThanEqualAVX512(a, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
//
var uint32BetweenCases = []Uint32MatchTest{
	{
		name:   "l0",
		slice:  make([]uint32, 0),
		match:  uint32BetweenTestMatch_1,
		match2: uint32BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  uint32BetweenTestMatch_1,
		match2: uint32BetweenTestMatch_1b,
		result: []byte{},
		count:  0,
	},
	CreateUint32TestCase("vec1", uint32TestSlice_0, uint32BetweenTestMatch_0, uint32BetweenTestMatch_0b, uint32BetweenTestResult_0, 32),
	CreateUint32TestCase("vec2", uint32TestSlice_0, uint32BetweenTestMatch_0, uint32BetweenTestMatch_0b, uint32BetweenTestResult_0, 64),
	CreateUint32TestCase("l32", uint32TestSlice_1, uint32BetweenTestMatch_1, uint32BetweenTestMatch_1b, uint32BetweenTestResult_1, 32),
	CreateUint32TestCase("l64", append(uint32TestSlice_1, uint32TestSlice_0...), uint32BetweenTestMatch_1, uint32BetweenTestMatch_1b,
		append(uint32BetweenTestResult_1, uint32BetweenTestResult_0...), 64),
	CreateUint32TestCase("l128", append(uint32TestSlice_1, uint32TestSlice_0...), uint32BetweenTestMatch_1, uint32BetweenTestMatch_1b,
		append(uint32BetweenTestResult_1, uint32BetweenTestResult_0...), 128),
	CreateUint32TestCase("l127", uint32TestSlice_1, uint32BetweenTestMatch_1, uint32BetweenTestMatch_1b, uint32BetweenTestResult_1, 127),
	CreateUint32TestCase("l63", uint32TestSlice_1, uint32BetweenTestMatch_1, uint32BetweenTestMatch_1b, uint32BetweenTestResult_1, 63),
	CreateUint32TestCase("l31", uint32TestSlice_1, uint32BetweenTestMatch_1, uint32BetweenTestMatch_1b, uint32BetweenTestResult_1, 31),
	CreateUint32TestCase("l23", uint32TestSlice_1, uint32BetweenTestMatch_1, uint32BetweenTestMatch_1b, uint32BetweenTestResult_1, 23),
	CreateUint32TestCase("l15", uint32TestSlice_1, uint32BetweenTestMatch_1, uint32BetweenTestMatch_1b, uint32BetweenTestResult_1, 15),
	CreateUint32TestCase("l7", uint32TestSlice_1, uint32BetweenTestMatch_1, uint32BetweenTestMatch_1b, uint32BetweenTestResult_1, 7),
	// with extreme values
	CreateUint32TestCase("ext64", uint32TestSlice_2, uint32BetweenTestMatch_2, uint32BetweenTestMatch_2b, uint32BetweenTestResult_2, 64),
	CreateUint32TestCase("ext32", uint32TestSlice_2, uint32BetweenTestMatch_2, uint32BetweenTestMatch_2b, uint32BetweenTestResult_2, 32),
	CreateUint32TestCase("ext31", uint32TestSlice_2, uint32BetweenTestMatch_2, uint32BetweenTestMatch_2b, uint32BetweenTestResult_2, 31),
}

func TestMatchUint32BetweenGeneric(T *testing.T) {
	for _, c := range uint32BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchUint32BetweenGeneric(c.slice, c.match, c.match2, bits)
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

/*func TestMatchUint32BetweenAVX2(T *testing.T) {
	if !useAVX2 {
		T.SkipNow()
	}
	for _, c := range uint32BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint32BetweenAVX2(c.slice, c.match, c.match2, bits)
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
func TestMatchUint32BetweenAVX512(T *testing.T) {
	if !useAVX512_F {
		T.SkipNow()
	}
	for _, c := range uint32BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchUint32BetweenAVX512(c.slice, c.match, c.match2, bits)
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
func BenchmarkMatchUint32BetweenGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32BetweenGeneric(a, math.MaxUint32/4, math.MaxUint32/2, bits)
			}
		})
	}
}

/*func BenchmarkMatchUint32BetweenAVX2(B *testing.B) {
	if !useAVX2 {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32BetweenAVX2(a, math.MaxUint32/4, math.MaxUint32/2, bits)
			}
		})
	}
}
*/
func BenchmarkMatchUint32BetweenAVX512(B *testing.B) {
	if !useAVX512_F {
		B.SkipNow()
	}
	for _, n := range vecBenchmarkSizes {
		a := randUint32Slice(n.l, 1)
		bits := make([]byte, bitFieldLen(len(a)))
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Uint32Size))
			for i := 0; i < B.N; i++ {
				matchUint32BetweenAVX512(a, math.MaxUint32/4, math.MaxUint32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Uint32 Slice
//
func TestUniqueUint32(T *testing.T) {
	a := randUint32Slice(1000, 5)
	b := UniqueUint32Slice(a)
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

func BenchmarkUniqueUint32(B *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := randUint32Slice(n, 5)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				UniqueUint32Slice(a)
			}
		})
	}
}

func TestUint32SliceContains(T *testing.T) {
	// nil slice
	if Uint32Slice(nil).Contains(1) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Uint32Slice([]uint32{}).Contains(1) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Uint32Slice([]uint32{1}).Contains(1) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Uint32Slice([]uint32{1}).Contains(2) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !Uint32Slice([]uint32{1, 3, 5, 7, 11, 13}).Contains(1) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Uint32Slice([]uint32{1, 3, 5, 7, 11, 13}).Contains(5) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Uint32Slice([]uint32{1, 3, 5, 7, 11, 13}).Contains(13) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Uint32Slice([]uint32{1, 3, 5, 7, 11, 13}).Contains(0) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Uint32Slice([]uint32{1, 3, 5, 7, 11, 13}).Contains(2) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Uint32Slice([]uint32{1, 3, 5, 7, 11, 13}).Contains(14) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkUint32SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Uint32Slice(randUint32Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(rand.Uint32())
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Uint32Slice(randUint32Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				a.Contains(a[rand.Intn(len(a))])
			}
		})
	}
}

func TestUint32SliceContainsRange(T *testing.T) {
	type VecTestRange struct {
		Name  string
		From  uint32
		To    uint32
		Match bool
	}

	type VecTestcase struct {
		Slice  []uint32
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
			Slice: []uint32{},
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: 0, To: 2, Match: false},
			},
		},
		// 1-element slice
		VecTestcase{
			Slice: []uint32{3},
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
			Slice: []uint32{3},
			Ranges: []VecTestRange{
				VecTestRange{Name: "BCD", From: 3, To: 3, Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		VecTestcase{
			Slice: []uint32{3, 5, 7, 11, 13},
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
			Slice: []uint32{
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
			if want, got := r.Match, Uint32Slice(v.Slice).ContainsRange(r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkUint32SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Uint32Slice(randUint32Slice(n, 1)).Sort()
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := rand.Uint32(), rand.Uint32()
				if min > max {
					min, max = max, min
				}
				a.ContainsRange(min, max)
			}
		})
	}
}
