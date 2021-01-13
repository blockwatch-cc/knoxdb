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

const Float32Size = 4

func randFloat32Slice(n, u int) []float32 {
	s := make([]float32, n*u)
	for i := 0; i < n; i++ {
		s[i] = rand.Float32()
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

type Float32MatchTest struct {
	name   string
	slice  []float32
	match  float32
	match2 float32
	result []byte
	count  int64
}

var (
	float32TestSlice_0 = []float32{
		0, 5, 3, 5, // Y1
		7, 5, 5, 9, // Y2
		3, 5, 5, 5, // Y3
		5, 0, 113, 12, // Y4

		4, 2, 3, 5, // Y5
		7, 3, 5, 9, // Y6
		3, 13, 5, 5, // Y7
		42, 5, 113, 12, // Y8
	}
	float32EqualTestMatch_0  float32 = 5
	float32EqualTestResult_0         = []byte{0x56, 0x78, 0x12, 0x34}

	float32NotEqualTestMatch_0  float32 = 5
	float32NotEqualTestResult_0         = []byte{0xa9, 0x87, 0xed, 0xcb}

	float32LessTestMatch_0  float32 = 5
	float32LessTestResult_0         = []byte{0xa0, 0x84, 0xe4, 0x80}

	float32LessEqualTestMatch_0  float32 = 5
	float32LessEqualTestResult_0         = []byte{0xf6, 0xfc, 0xf6, 0xb4}

	float32GreaterTestMatch_0  float32 = 5
	float32GreaterTestResult_0         = []byte{0x09, 0x03, 0x09, 0x4b}

	float32GreaterEqualTestMatch_0  float32 = 5
	float32GreaterEqualTestResult_0         = []byte{0x5f, 0x7b, 0x1b, 0x7f}

	float32BetweenTestMatch_0  float32 = 5
	float32BetweenTestMatch_0b float32 = 10
	float32BetweenTestResult_0         = []byte{0x5f, 0x78, 0x1b, 0x34}

	// positive int values only
	float32TestSlice_1 = []float32{
		5, 2, 3, 4,
		7, 8, 5, 10,
		15, 5, 55, 500,
		1000, 500000, 5, 113,
		31, 32, 5, 34,
		35, 36, 5, 5,
		43, 5, 5, 5,
		39, 40, 41, 42,
	}
	float32EqualTestResult_1         = []byte{0x82, 0x42, 0x23, 0x70}
	float32EqualTestMatch_1  float32 = 5

	float32NotEqualTestResult_1         = []byte{0x7d, 0xbd, 0xdc, 0x8f}
	float32NotEqualTestMatch_1  float32 = 5

	float32LessTestResult_1         = []byte{0x70, 0x00, 0x00, 0x00}
	float32LessTestMatch_1  float32 = 5

	float32LessEqualTestResult_1         = []byte{0xf2, 0x42, 0x23, 0x70}
	float32LessEqualTestMatch_1  float32 = 5

	float32GreaterTestResult_1         = []byte{0x0d, 0xbd, 0xdc, 0x8f}
	float32GreaterTestMatch_1  float32 = 5

	float32GreaterEqualTestResult_1         = []byte{0x8f, 0xff, 0xff, 0xff}
	float32GreaterEqualTestMatch_1  float32 = 5

	float32BetweenTestResult_1         = []byte{0x8f, 0x42, 0x23, 0x70}
	float32BetweenTestMatch_1  float32 = 5
	float32BetweenTestMatch_1b float32 = 10

	// negative and positive values mixed
	float32TestSlice_2 = []float32{
		-5.12, 2.5, -3.1, 5.45,
		7.125, 8.2, 9.4, -10.25,
		15.25, 50.25, 55.25, 500.25,
		1000.25, -500000.25, 113.25, 12.25,
		31.25, 32.25, 33.25, 34.25,
		35, -36, 37.25, 38.25,
		39.25, 40.25, -41.25, 42.25,
		43.25, 44.25, 45.25, -46.25,
	}
	float32EqualTestResult_2         = []byte{0x80, 0x0, 0x0, 0x0}
	float32EqualTestMatch_2  float32 = -5.12

	float32NotEqualTestResult_2         = []byte{0x7f, 0xff, 0xff, 0xff}
	float32NotEqualTestMatch_2  float32 = -5.12

	float32LessTestResult_2         = []byte{0x01, 0x04, 0x04, 0x21}
	float32LessTestMatch_2  float32 = -5.12

	float32LessEqualTestResult_2         = []byte{0x81, 0x04, 0x04, 0x21}
	float32LessEqualTestMatch_2  float32 = -5.12

	float32GreaterTestResult_2         = []byte{0x7e, 0xfb, 0xfb, 0xde}
	float32GreaterTestMatch_2  float32 = -5.12

	float32GreaterEqualTestResult_2         = []byte{0xfe, 0xfb, 0xfb, 0xde}
	float32GreaterEqualTestMatch_2  float32 = -5.12

	float32BetweenTestResult_2         = []byte{0xfe, 0x00, 0x00, 0x00}
	float32BetweenTestMatch_2  float32 = -5.12
	float32BetweenTestMatch_2b float32 = 10

	// extreme values
	float32TestSlice_3 = []float32{
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		math.MaxFloat32 / 2, math.SmallestNonzeroFloat32,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
	}
	float32EqualTestResult_3         = []byte{0x22, 0x22, 0x22, 0x22}
	float32EqualTestMatch_3  float32 = math.MaxFloat32

	float32NotEqualTestResult_3         = []byte{0xdd, 0xdd, 0xdd, 0xdd}
	float32NotEqualTestMatch_3  float32 = math.MaxFloat32

	float32LessTestResult_3         = []byte{0xdd, 0xdd, 0xdd, 0xdd}
	float32LessTestMatch_3  float32 = math.MaxFloat32

	float32LessEqualTestResult_3         = []byte{0xff, 0xff, 0xff, 0xff}
	float32LessEqualTestMatch_3  float32 = math.MaxFloat32

	float32GreaterTestResult_3         = []byte{0x0, 0x0, 0x0, 0x0}
	float32GreaterTestMatch_3  float32 = math.MaxFloat32

	float32GreaterEqualTestResult_3         = []byte{0x22, 0x22, 0x22, 0x22}
	float32GreaterEqualTestMatch_3  float32 = math.MaxFloat32

	float32BetweenTestResult_3         = []byte{0xaa, 0xaa, 0xaa, 0xaa}
	float32BetweenTestMatch_3  float32 = math.MaxFloat32 / 2
	float32BetweenTestMatch_3b float32 = math.MaxFloat32

	// NaN/Inf values
	float32TestSlice_4 = []float32{
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
		float32(math.Inf(-1)), float32(math.Inf(0)),
		float32(math.NaN()), float32(math.NaN()),
	}
	float32EqualTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float32EqualTestMatch_4  float32 = float32(math.NaN())

	float32NotEqualTestResult_4         = []byte{0xff, 0xff, 0xff, 0xff}
	float32NotEqualTestMatch_4  float32 = float32(math.NaN())

	float32LessTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float32LessTestMatch_4  float32 = float32(math.NaN())

	float32LessEqualTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float32LessEqualTestMatch_4  float32 = float32(math.NaN())

	float32GreaterTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float32GreaterTestMatch_4  float32 = float32(math.NaN())

	float32GreaterEqualTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float32GreaterEqualTestMatch_4  float32 = float32(math.NaN())

	float32BetweenTestResult_4         = []byte{0x0, 0x0, 0x0, 0x0}
	float32BetweenTestMatch_4  float32 = float32(math.NaN())
	float32BetweenTestMatch_4b float32 = float32(math.NaN())
)

// creates an uint64 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - match, match2: are only copied to the resulting test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateFloat32TestCase(name string, slice []float32, match, match2 float32, result []byte, length int) Float32MatchTest {
	if len(slice)%8 != 0 {
		panic("CreateUint64TestCase: length of slice has to be a multiple of 8")
	}
	if len(result) != bitFieldLen(len(slice)) {
		panic("CreateUint64TestCase: length of slice and length of result does not match")
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []float32
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
	return Float32MatchTest{
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
var float32EqualCases = []Float32MatchTest{
	{
		name:   "l0",
		slice:  make([]float32, 0),
		match:  float32EqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  float32EqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateFloat32TestCase("vec1", float32TestSlice_0, float32EqualTestMatch_0, 0, float32EqualTestResult_0, 32),
	CreateFloat32TestCase("vec2", float32TestSlice_0, float32EqualTestMatch_0, 0, float32EqualTestResult_0, 128),
	CreateFloat32TestCase("l32", float32TestSlice_1, float32EqualTestMatch_1, 0, float32EqualTestResult_1, 32),
	CreateFloat32TestCase("l64", append(float32TestSlice_1, float32TestSlice_0...), float32EqualTestMatch_1, 0,
		append(float32EqualTestResult_1, float32EqualTestResult_0...), 64),
	CreateFloat32TestCase("l128", append(float32TestSlice_1, float32TestSlice_0...), float32EqualTestMatch_1, 0,
		append(float32EqualTestResult_1, float32EqualTestResult_0...), 128),
	CreateFloat32TestCase("l256", append(float32TestSlice_1, float32TestSlice_0...), float32EqualTestMatch_1, 0,
		append(float32EqualTestResult_1, float32EqualTestResult_0...), 256),
	CreateFloat32TestCase("l255", append(float32TestSlice_1, float32TestSlice_0...), float32EqualTestMatch_1, 0,
		append(float32EqualTestResult_1, float32EqualTestResult_0...), 255),
	CreateFloat32TestCase("l127", append(float32TestSlice_1, float32TestSlice_0...), float32EqualTestMatch_1, 0,
		append(float32EqualTestResult_1, float32EqualTestResult_0...), 127),
	CreateFloat32TestCase("l63", float32TestSlice_1, float32EqualTestMatch_1, 0, float32EqualTestResult_1, 63),
	CreateFloat32TestCase("l31", float32TestSlice_1, float32EqualTestMatch_1, 0, float32EqualTestResult_1, 31),
	CreateFloat32TestCase("l23", float32TestSlice_1, float32EqualTestMatch_1, 0, float32EqualTestResult_1, 23),
	CreateFloat32TestCase("l15", float32TestSlice_1, float32EqualTestMatch_1, 0, float32EqualTestResult_1, 15),
	CreateFloat32TestCase("l7", float32TestSlice_1, float32EqualTestMatch_1, 0, float32EqualTestResult_1, 7),
	CreateFloat32TestCase("neg128", float32TestSlice_2, float32EqualTestMatch_2, 0, float32EqualTestResult_2, 128),
	CreateFloat32TestCase("neg32", float32TestSlice_2, float32EqualTestMatch_2, 0, float32EqualTestResult_2, 32),
	CreateFloat32TestCase("neg31", float32TestSlice_2, float32EqualTestMatch_2, 0, float32EqualTestResult_2, 31),
	CreateFloat32TestCase("ext128", float32TestSlice_3, float32EqualTestMatch_3, 0, float32EqualTestResult_3, 128),
	CreateFloat32TestCase("ext32", float32TestSlice_3, float32EqualTestMatch_3, 0, float32EqualTestResult_3, 32),
	CreateFloat32TestCase("ext31", float32TestSlice_3, float32EqualTestMatch_3, 0, float32EqualTestResult_3, 31),
	CreateFloat32TestCase("nan128", float32TestSlice_4, float32EqualTestMatch_4, 0, float32EqualTestResult_4, 128),
	CreateFloat32TestCase("nan32", float32TestSlice_4, float32EqualTestMatch_4, 0, float32EqualTestResult_4, 32),
	CreateFloat32TestCase("nan31", float32TestSlice_4, float32EqualTestMatch_4, 0, float32EqualTestResult_4, 31),
}

func TestMatchFloat32EqualGeneric(T *testing.T) {
	for _, c := range float32EqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchFloat32EqualGeneric(c.slice, c.match, bits)
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

func TestMatchFloat32EqualAVX2(T *testing.T) {
	for _, c := range float32EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat32EqualAVX2(c.slice, c.match, bits)
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

func TestMatchFloat32EqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat32EqualAVX512.")
	}
	for _, c := range float32EqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat32EqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchFloat32EqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32EqualGeneric(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func BenchmarkMatchFloat32EqualAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32EqualAVX2(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func BenchmarkMatchFloat32EqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32EqualAVX512.")
	}
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32EqualAVX512(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Not Equal Testcases
//
var float32NotEqualCases = []Float32MatchTest{
	{
		name:   "l0",
		slice:  make([]float32, 0),
		match:  float32NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  float32NotEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateFloat32TestCase("vec1", float32TestSlice_0, float32NotEqualTestMatch_0, 0, float32NotEqualTestResult_0, 32),
	CreateFloat32TestCase("vec2", float32TestSlice_0, float32NotEqualTestMatch_0, 0, float32NotEqualTestResult_0, 128),
	CreateFloat32TestCase("l32", float32TestSlice_1, float32NotEqualTestMatch_1, 0, float32NotEqualTestResult_1, 32),
	CreateFloat32TestCase("l64", append(float32TestSlice_1, float32TestSlice_0...), float32NotEqualTestMatch_1, 0,
		append(float32NotEqualTestResult_1, float32NotEqualTestResult_0...), 64),
	CreateFloat32TestCase("l128", append(float32TestSlice_1, float32TestSlice_0...), float32NotEqualTestMatch_1, 0,
		append(float32NotEqualTestResult_1, float32NotEqualTestResult_0...), 128),
	CreateFloat32TestCase("l256", append(float32TestSlice_1, float32TestSlice_0...), float32NotEqualTestMatch_1, 0,
		append(float32NotEqualTestResult_1, float32NotEqualTestResult_0...), 256),
	CreateFloat32TestCase("l255", append(float32TestSlice_1, float32TestSlice_0...), float32NotEqualTestMatch_1, 0,
		append(float32NotEqualTestResult_1, float32NotEqualTestResult_0...), 255),
	CreateFloat32TestCase("l127", append(float32TestSlice_1, float32TestSlice_0...), float32NotEqualTestMatch_1, 0,
		append(float32NotEqualTestResult_1, float32NotEqualTestResult_0...), 127),
	CreateFloat32TestCase("l63", float32TestSlice_1, float32NotEqualTestMatch_1, 0, float32NotEqualTestResult_1, 63),
	CreateFloat32TestCase("l31", float32TestSlice_1, float32NotEqualTestMatch_1, 0, float32NotEqualTestResult_1, 31),
	CreateFloat32TestCase("l23", float32TestSlice_1, float32NotEqualTestMatch_1, 0, float32NotEqualTestResult_1, 23),
	CreateFloat32TestCase("l15", float32TestSlice_1, float32NotEqualTestMatch_1, 0, float32NotEqualTestResult_1, 15),
	CreateFloat32TestCase("l7", float32TestSlice_1, float32NotEqualTestMatch_1, 0, float32NotEqualTestResult_1, 7),
	CreateFloat32TestCase("neg128", float32TestSlice_2, float32NotEqualTestMatch_2, 0, float32NotEqualTestResult_2, 128),
	CreateFloat32TestCase("neg32", float32TestSlice_2, float32NotEqualTestMatch_2, 0, float32NotEqualTestResult_2, 32),
	CreateFloat32TestCase("neg31", float32TestSlice_2, float32NotEqualTestMatch_2, 0, float32NotEqualTestResult_2, 31),
	CreateFloat32TestCase("ext128", float32TestSlice_3, float32NotEqualTestMatch_3, 0, float32NotEqualTestResult_3, 128),
	CreateFloat32TestCase("ext32", float32TestSlice_3, float32NotEqualTestMatch_3, 0, float32NotEqualTestResult_3, 32),
	CreateFloat32TestCase("ext31", float32TestSlice_3, float32NotEqualTestMatch_3, 0, float32NotEqualTestResult_3, 31),
	CreateFloat32TestCase("nan128", float32TestSlice_4, float32NotEqualTestMatch_4, 0, float32NotEqualTestResult_4, 128),
	CreateFloat32TestCase("nan32", float32TestSlice_4, float32NotEqualTestMatch_4, 0, float32NotEqualTestResult_4, 32),
	CreateFloat32TestCase("nan31", float32TestSlice_4, float32NotEqualTestMatch_4, 0, float32NotEqualTestResult_4, 31),
}

func TestMatchFloat32NotEqualGeneric(T *testing.T) {
	for _, c := range float32NotEqualCases {
		// pre-allocate the result slice and fill with poison
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchFloat32NotEqualGeneric(c.slice, c.match, bits)
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

func TestMatchFloat32NotEqualAVX2(T *testing.T) {
	for _, c := range float32NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat32NotEqualAVX2(c.slice, c.match, bits)
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

func TestMatchFloat32NotEqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat32NotEqualAVX512.")
	}
	for _, c := range float32NotEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat32NotEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchFloat32NotEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32NotEqualGeneric(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func BenchmarkMatchFloat32NotEqualAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32NotEqualAVX2(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func BenchmarkMatchFloat32NotEqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32NotEqualAVX512.")
	}
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32NotEqualAVX512(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Testcases
//
var float32LessCases = []Float32MatchTest{
	{
		name:   "l0",
		slice:  make([]float32, 0),
		match:  float32LessTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  float32LessTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateFloat32TestCase("vec1", float32TestSlice_0, float32LessTestMatch_0, 0, float32LessTestResult_0, 32),
	CreateFloat32TestCase("vec2", float32TestSlice_0, float32LessTestMatch_0, 0, float32LessTestResult_0, 128),
	CreateFloat32TestCase("l32", float32TestSlice_1, float32LessTestMatch_1, 0, float32LessTestResult_1, 32),
	CreateFloat32TestCase("l64", append(float32TestSlice_1, float32TestSlice_0...), float32LessTestMatch_1, 0,
		append(float32LessTestResult_1, float32LessTestResult_0...), 64),
	CreateFloat32TestCase("l128", append(float32TestSlice_1, float32TestSlice_0...), float32LessTestMatch_1, 0,
		append(float32LessTestResult_1, float32LessTestResult_0...), 128),
	CreateFloat32TestCase("l256", append(float32TestSlice_1, float32TestSlice_0...), float32LessTestMatch_1, 0,
		append(float32LessTestResult_1, float32LessTestResult_0...), 256),
	CreateFloat32TestCase("l255", append(float32TestSlice_1, float32TestSlice_0...), float32LessTestMatch_1, 0,
		append(float32LessTestResult_1, float32LessTestResult_0...), 255),
	CreateFloat32TestCase("l127", append(float32TestSlice_1, float32TestSlice_0...), float32LessTestMatch_1, 0,
		append(float32LessTestResult_1, float32LessTestResult_0...), 127),
	CreateFloat32TestCase("l63", float32TestSlice_1, float32LessTestMatch_1, 0, float32LessTestResult_1, 63),
	CreateFloat32TestCase("l31", float32TestSlice_1, float32LessTestMatch_1, 0, float32LessTestResult_1, 31),
	CreateFloat32TestCase("l23", float32TestSlice_1, float32LessTestMatch_1, 0, float32LessTestResult_1, 23),
	CreateFloat32TestCase("l15", float32TestSlice_1, float32LessTestMatch_1, 0, float32LessTestResult_1, 15),
	CreateFloat32TestCase("l7", float32TestSlice_1, float32LessTestMatch_1, 0, float32LessTestResult_1, 7),
	CreateFloat32TestCase("neg128", float32TestSlice_2, float32LessTestMatch_2, 0, float32LessTestResult_2, 128),
	CreateFloat32TestCase("neg32", float32TestSlice_2, float32LessTestMatch_2, 0, float32LessTestResult_2, 32),
	CreateFloat32TestCase("neg31", float32TestSlice_2, float32LessTestMatch_2, 0, float32LessTestResult_2, 31),
	CreateFloat32TestCase("ext128", float32TestSlice_3, float32LessTestMatch_3, 0, float32LessTestResult_3, 128),
	CreateFloat32TestCase("ext32", float32TestSlice_3, float32LessTestMatch_3, 0, float32LessTestResult_3, 32),
	CreateFloat32TestCase("ext31", float32TestSlice_3, float32LessTestMatch_3, 0, float32LessTestResult_3, 31),
	CreateFloat32TestCase("nan128", float32TestSlice_4, float32LessTestMatch_4, 0, float32LessTestResult_4, 128),
	CreateFloat32TestCase("nan32", float32TestSlice_4, float32LessTestMatch_4, 0, float32LessTestResult_4, 32),
	CreateFloat32TestCase("nan31", float32TestSlice_4, float32LessTestMatch_4, 0, float32LessTestResult_4, 31),
}

func TestMatchFloat32LessGeneric(T *testing.T) {
	for _, c := range float32LessCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchFloat32LessThanGeneric(c.slice, c.match, bits)
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

func TestMatchFloat32LessAVX2(T *testing.T) {
	for _, c := range float32LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat32LessThanAVX2(c.slice, c.match, bits)
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

func TestMatchFloat32LessAVX512(T *testing.T) {
	if !useAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat32LessAVX512.")
	}
	for _, c := range float32LessCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat32LessThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchFloat32LessGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32LessThanGeneric(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func BenchmarkMatchFloat32LessAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32LessThanAVX2(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func BenchmarkMatchFloat32LessAVX512(B *testing.B) {
	if !useAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32LessAVX512.")
	}
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32LessThanAVX512(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Less Equal Testcases
//
var float32LessEqualCases = []Float32MatchTest{
	{
		name:   "l0",
		slice:  make([]float32, 0),
		match:  float32LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  float32LessEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateFloat32TestCase("vec1", float32TestSlice_0, float32LessEqualTestMatch_0, 0, float32LessEqualTestResult_0, 32),
	CreateFloat32TestCase("vec2", float32TestSlice_0, float32LessEqualTestMatch_0, 0, float32LessEqualTestResult_0, 128),
	CreateFloat32TestCase("l32", float32TestSlice_1, float32LessEqualTestMatch_1, 0, float32LessEqualTestResult_1, 32),
	CreateFloat32TestCase("l64", append(float32TestSlice_1, float32TestSlice_0...), float32LessEqualTestMatch_1, 0,
		append(float32LessEqualTestResult_1, float32LessEqualTestResult_0...), 64),
	CreateFloat32TestCase("l128", append(float32TestSlice_1, float32TestSlice_0...), float32LessEqualTestMatch_1, 0,
		append(float32LessEqualTestResult_1, float32LessEqualTestResult_0...), 128),
	CreateFloat32TestCase("l256", append(float32TestSlice_1, float32TestSlice_0...), float32LessEqualTestMatch_1, 0,
		append(float32LessEqualTestResult_1, float32LessEqualTestResult_0...), 256),
	CreateFloat32TestCase("l255", append(float32TestSlice_1, float32TestSlice_0...), float32LessEqualTestMatch_1, 0,
		append(float32LessEqualTestResult_1, float32LessEqualTestResult_0...), 255),
	CreateFloat32TestCase("l127", append(float32TestSlice_1, float32TestSlice_0...), float32LessEqualTestMatch_1, 0,
		append(float32LessEqualTestResult_1, float32LessEqualTestResult_0...), 127),
	CreateFloat32TestCase("l63", float32TestSlice_1, float32LessEqualTestMatch_1, 0, float32LessEqualTestResult_1, 63),
	CreateFloat32TestCase("l31", float32TestSlice_1, float32LessEqualTestMatch_1, 0, float32LessEqualTestResult_1, 31),
	CreateFloat32TestCase("l23", float32TestSlice_1, float32LessEqualTestMatch_1, 0, float32LessEqualTestResult_1, 23),
	CreateFloat32TestCase("l15", float32TestSlice_1, float32LessEqualTestMatch_1, 0, float32LessEqualTestResult_1, 15),
	CreateFloat32TestCase("l7", float32TestSlice_1, float32LessEqualTestMatch_1, 0, float32LessEqualTestResult_1, 7),
	CreateFloat32TestCase("neg128", float32TestSlice_2, float32LessEqualTestMatch_2, 0, float32LessEqualTestResult_2, 128),
	CreateFloat32TestCase("neg32", float32TestSlice_2, float32LessEqualTestMatch_2, 0, float32LessEqualTestResult_2, 32),
	CreateFloat32TestCase("neg31", float32TestSlice_2, float32LessEqualTestMatch_2, 0, float32LessEqualTestResult_2, 31),
	CreateFloat32TestCase("ext128", float32TestSlice_3, float32LessEqualTestMatch_3, 0, float32LessEqualTestResult_3, 128),
	CreateFloat32TestCase("ext32", float32TestSlice_3, float32LessEqualTestMatch_3, 0, float32LessEqualTestResult_3, 32),
	CreateFloat32TestCase("ext31", float32TestSlice_3, float32LessEqualTestMatch_3, 0, float32LessEqualTestResult_3, 31),
	CreateFloat32TestCase("nan128", float32TestSlice_4, float32LessEqualTestMatch_4, 0, float32LessEqualTestResult_4, 128),
	CreateFloat32TestCase("nan32", float32TestSlice_4, float32LessEqualTestMatch_4, 0, float32LessEqualTestResult_4, 32),
	CreateFloat32TestCase("nan31", float32TestSlice_4, float32LessEqualTestMatch_4, 0, float32LessEqualTestResult_4, 31),
}

func TestMatchFloat32LessEqualGeneric(T *testing.T) {
	for _, c := range float32LessEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchFloat32LessThanEqualGeneric(c.slice, c.match, bits)
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

func TestMatchFloat32LessEqualAVX2(T *testing.T) {
	for _, c := range float32LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat32LessThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchFloat32LessEqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat32LessEqualAVX512.")
	}
	for _, c := range float32LessEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat32LessThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchFloat32LessEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32LessThanEqualGeneric(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func BenchmarkMatchFloat32LessEqualAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32LessThanEqualAVX2(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func BenchmarkMatchFloat32LessEqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32LessEqualAVX512.")
	}
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32LessThanEqualAVX512(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Testcases
//
var float32GreaterCases = []Float32MatchTest{
	{
		name:   "l0",
		slice:  make([]float32, 0),
		match:  float32GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  float32GreaterTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateFloat32TestCase("vec1", float32TestSlice_0, float32GreaterTestMatch_0, 0, float32GreaterTestResult_0, 32),
	CreateFloat32TestCase("vec2", float32TestSlice_0, float32GreaterTestMatch_0, 0, float32GreaterTestResult_0, 128),
	CreateFloat32TestCase("l32", float32TestSlice_1, float32GreaterTestMatch_1, 0, float32GreaterTestResult_1, 32),
	CreateFloat32TestCase("l64", append(float32TestSlice_1, float32TestSlice_0...), float32GreaterTestMatch_1, 0,
		append(float32GreaterTestResult_1, float32GreaterTestResult_0...), 64),
	CreateFloat32TestCase("l128", append(float32TestSlice_1, float32TestSlice_0...), float32GreaterTestMatch_1, 0,
		append(float32GreaterTestResult_1, float32GreaterTestResult_0...), 128),
	CreateFloat32TestCase("l256", append(float32TestSlice_1, float32TestSlice_0...), float32GreaterTestMatch_1, 0,
		append(float32GreaterTestResult_1, float32GreaterTestResult_0...), 256),
	CreateFloat32TestCase("l255", append(float32TestSlice_1, float32TestSlice_0...), float32GreaterTestMatch_1, 0,
		append(float32GreaterTestResult_1, float32GreaterTestResult_0...), 255),
	CreateFloat32TestCase("l127", append(float32TestSlice_1, float32TestSlice_0...), float32GreaterTestMatch_1, 0,
		append(float32GreaterTestResult_1, float32GreaterTestResult_0...), 127),
	CreateFloat32TestCase("l63", float32TestSlice_1, float32GreaterTestMatch_1, 0, float32GreaterTestResult_1, 63),
	CreateFloat32TestCase("l31", float32TestSlice_1, float32GreaterTestMatch_1, 0, float32GreaterTestResult_1, 31),
	CreateFloat32TestCase("l23", float32TestSlice_1, float32GreaterTestMatch_1, 0, float32GreaterTestResult_1, 23),
	CreateFloat32TestCase("l15", float32TestSlice_1, float32GreaterTestMatch_1, 0, float32GreaterTestResult_1, 15),
	CreateFloat32TestCase("l7", float32TestSlice_1, float32GreaterTestMatch_1, 0, float32GreaterTestResult_1, 7),
	CreateFloat32TestCase("neg128", float32TestSlice_2, float32GreaterTestMatch_2, 0, float32GreaterTestResult_2, 128),
	CreateFloat32TestCase("neg32", float32TestSlice_2, float32GreaterTestMatch_2, 0, float32GreaterTestResult_2, 32),
	CreateFloat32TestCase("neg31", float32TestSlice_2, float32GreaterTestMatch_2, 0, float32GreaterTestResult_2, 31),
	CreateFloat32TestCase("ext128", float32TestSlice_3, float32GreaterTestMatch_3, 0, float32GreaterTestResult_3, 128),
	CreateFloat32TestCase("ext32", float32TestSlice_3, float32GreaterTestMatch_3, 0, float32GreaterTestResult_3, 32),
	CreateFloat32TestCase("ext31", float32TestSlice_3, float32GreaterTestMatch_3, 0, float32GreaterTestResult_3, 31),
	CreateFloat32TestCase("nan128", float32TestSlice_4, float32GreaterTestMatch_4, 0, float32GreaterTestResult_4, 128),
	CreateFloat32TestCase("nan32", float32TestSlice_4, float32GreaterTestMatch_4, 0, float32GreaterTestResult_4, 32),
	CreateFloat32TestCase("nan31", float32TestSlice_4, float32GreaterTestMatch_4, 0, float32GreaterTestResult_4, 31),
}

func TestMatchFloat32GreaterGeneric(T *testing.T) {
	for _, c := range float32GreaterCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchFloat32GreaterThanGeneric(c.slice, c.match, bits)
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

func TestMatchFloat32GreaterAVX2(T *testing.T) {
	for _, c := range float32GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat32GreaterThanAVX2(c.slice, c.match, bits)
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

func TestMatchFloat32GreaterAVX512(T *testing.T) {
	if !useAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat32GreaterAVX512.")
	}
	for _, c := range float32GreaterCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat32GreaterThanAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchFloat32GreaterGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32GreaterThanGeneric(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func BenchmarkMatchFloat32GreaterAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32GreaterThanAVX2(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func BenchmarkMatchFloat32GreaterAVX512(B *testing.B) {
	if !useAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32GreaterAVX512.")
	}
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32GreaterThanAVX512(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Greater Equal Testcases
//
var float32GreaterEqualCases = []Float32MatchTest{
	{
		name:   "l0",
		slice:  make([]float32, 0),
		match:  float32GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  float32GreaterEqualTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateFloat32TestCase("vec1", float32TestSlice_0, float32GreaterEqualTestMatch_0, 0, float32GreaterEqualTestResult_0, 32),
	CreateFloat32TestCase("vec2", float32TestSlice_0, float32GreaterEqualTestMatch_0, 0, float32GreaterEqualTestResult_0, 128),
	CreateFloat32TestCase("l32", float32TestSlice_1, float32GreaterEqualTestMatch_1, 0, float32GreaterEqualTestResult_1, 32),
	CreateFloat32TestCase("l64", append(float32TestSlice_1, float32TestSlice_0...), float32GreaterEqualTestMatch_1, 0,
		append(float32GreaterEqualTestResult_1, float32GreaterEqualTestResult_0...), 64),
	CreateFloat32TestCase("l128", append(float32TestSlice_1, float32TestSlice_0...), float32GreaterEqualTestMatch_1, 0,
		append(float32GreaterEqualTestResult_1, float32GreaterEqualTestResult_0...), 128),
	CreateFloat32TestCase("l256", append(float32TestSlice_1, float32TestSlice_0...), float32GreaterEqualTestMatch_1, 0,
		append(float32GreaterEqualTestResult_1, float32GreaterEqualTestResult_0...), 256),
	CreateFloat32TestCase("l255", append(float32TestSlice_1, float32TestSlice_0...), float32GreaterEqualTestMatch_1, 0,
		append(float32GreaterEqualTestResult_1, float32GreaterEqualTestResult_0...), 255),
	CreateFloat32TestCase("l127", append(float32TestSlice_1, float32TestSlice_0...), float32GreaterEqualTestMatch_1, 0,
		append(float32GreaterEqualTestResult_1, float32GreaterEqualTestResult_0...), 127),
	CreateFloat32TestCase("l63", float32TestSlice_1, float32GreaterEqualTestMatch_1, 0, float32GreaterEqualTestResult_1, 63),
	CreateFloat32TestCase("l31", float32TestSlice_1, float32GreaterEqualTestMatch_1, 0, float32GreaterEqualTestResult_1, 31),
	CreateFloat32TestCase("l23", float32TestSlice_1, float32GreaterEqualTestMatch_1, 0, float32GreaterEqualTestResult_1, 23),
	CreateFloat32TestCase("l15", float32TestSlice_1, float32GreaterEqualTestMatch_1, 0, float32GreaterEqualTestResult_1, 15),
	CreateFloat32TestCase("l7", float32TestSlice_1, float32GreaterEqualTestMatch_1, 0, float32GreaterEqualTestResult_1, 7),
	CreateFloat32TestCase("neg128", float32TestSlice_2, float32GreaterEqualTestMatch_2, 0, float32GreaterEqualTestResult_2, 128),
	CreateFloat32TestCase("neg32", float32TestSlice_2, float32GreaterEqualTestMatch_2, 0, float32GreaterEqualTestResult_2, 32),
	CreateFloat32TestCase("neg31", float32TestSlice_2, float32GreaterEqualTestMatch_2, 0, float32GreaterEqualTestResult_2, 31),
	CreateFloat32TestCase("ext128", float32TestSlice_3, float32GreaterEqualTestMatch_3, 0, float32GreaterEqualTestResult_3, 128),
	CreateFloat32TestCase("ext32", float32TestSlice_3, float32GreaterEqualTestMatch_3, 0, float32GreaterEqualTestResult_3, 32),
	CreateFloat32TestCase("ext31", float32TestSlice_3, float32GreaterEqualTestMatch_3, 0, float32GreaterEqualTestResult_3, 31),
	CreateFloat32TestCase("nan128", float32TestSlice_4, float32GreaterEqualTestMatch_4, 0, float32GreaterEqualTestResult_4, 128),
	CreateFloat32TestCase("nan32", float32TestSlice_4, float32GreaterEqualTestMatch_4, 0, float32GreaterEqualTestResult_4, 32),
	CreateFloat32TestCase("nan31", float32TestSlice_4, float32GreaterEqualTestMatch_4, 0, float32GreaterEqualTestResult_4, 31),
}

func TestMatchFloat32GreaterEqualGeneric(T *testing.T) {
	for _, c := range float32GreaterEqualCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchFloat32GreaterThanEqualGeneric(c.slice, c.match, bits)
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

func TestMatchFloat32GreaterEqualAVX2(T *testing.T) {
	for _, c := range float32GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat32GreaterThanEqualAVX2(c.slice, c.match, bits)
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

func TestMatchFloat32GreaterEqualAVX512(T *testing.T) {
	if !useAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat32GreaterEqualAVX512.")
	}
	for _, c := range float32GreaterEqualCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat32GreaterThanEqualAVX512(c.slice, c.match, bits)
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
func BenchmarkMatchFloat32GreaterEqualGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32GreaterThanEqualGeneric(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func BenchmarkMatchFloat32GreaterEqualAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32GreaterThanEqualAVX2(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

func BenchmarkMatchFloat32GreaterEqualAVX512(B *testing.B) {
	if !useAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32GreaterEqualAVX512.")
	}
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32GreaterThanEqualAVX512(a, math.MaxFloat32/2, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Between Testcases
//
var float32BetweenCases = []Float32MatchTest{
	{
		name:   "l0",
		slice:  make([]float32, 0),
		match:  float32BetweenTestMatch_1,
		result: []byte{},
		count:  0,
	}, {
		name:   "nil",
		slice:  nil,
		match:  float32BetweenTestMatch_1,
		result: []byte{},
		count:  0,
	},
	CreateFloat32TestCase("vec1", float32TestSlice_0, float32BetweenTestMatch_0, float32BetweenTestMatch_0b, float32BetweenTestResult_0, 32),
	CreateFloat32TestCase("vec2", float32TestSlice_0, float32BetweenTestMatch_0, float32BetweenTestMatch_0b, float32BetweenTestResult_0, 128),
	CreateFloat32TestCase("l32", float32TestSlice_1, float32BetweenTestMatch_1, float32BetweenTestMatch_1b, float32BetweenTestResult_1, 32),
	CreateFloat32TestCase("l64", append(float32TestSlice_1, float32TestSlice_0...), float32BetweenTestMatch_1, float32BetweenTestMatch_1b,
		append(float32BetweenTestResult_1, float32BetweenTestResult_0...), 64),
	CreateFloat32TestCase("l128", append(float32TestSlice_1, float32TestSlice_0...), float32BetweenTestMatch_1, float32BetweenTestMatch_1b,
		append(float32BetweenTestResult_1, float32BetweenTestResult_0...), 128),
	CreateFloat32TestCase("l256", append(float32TestSlice_1, float32TestSlice_0...), float32BetweenTestMatch_1, float32BetweenTestMatch_1b,
		append(float32BetweenTestResult_1, float32BetweenTestResult_0...), 256),
	CreateFloat32TestCase("l255", append(float32TestSlice_1, float32TestSlice_0...), float32BetweenTestMatch_1, float32BetweenTestMatch_1b,
		append(float32BetweenTestResult_1, float32BetweenTestResult_0...), 255),
	CreateFloat32TestCase("l127", append(float32TestSlice_1, float32TestSlice_0...), float32BetweenTestMatch_1, float32BetweenTestMatch_1b,
		append(float32BetweenTestResult_1, float32BetweenTestResult_0...), 127),
	CreateFloat32TestCase("l63", float32TestSlice_1, float32BetweenTestMatch_1, float32BetweenTestMatch_1b, float32BetweenTestResult_1, 63),
	CreateFloat32TestCase("l31", float32TestSlice_1, float32BetweenTestMatch_1, float32BetweenTestMatch_1b, float32BetweenTestResult_1, 31),
	CreateFloat32TestCase("l23", float32TestSlice_1, float32BetweenTestMatch_1, float32BetweenTestMatch_1b, float32BetweenTestResult_1, 23),
	CreateFloat32TestCase("l15", float32TestSlice_1, float32BetweenTestMatch_1, float32BetweenTestMatch_1b, float32BetweenTestResult_1, 15),
	CreateFloat32TestCase("l7", float32TestSlice_1, float32BetweenTestMatch_1, float32BetweenTestMatch_1b, float32BetweenTestResult_1, 7),
	CreateFloat32TestCase("neg128", float32TestSlice_2, float32BetweenTestMatch_2, float32BetweenTestMatch_2b, float32BetweenTestResult_2, 128),
	CreateFloat32TestCase("neg32", float32TestSlice_2, float32BetweenTestMatch_2, float32BetweenTestMatch_2b, float32BetweenTestResult_2, 32),
	CreateFloat32TestCase("neg31", float32TestSlice_2, float32BetweenTestMatch_2, float32BetweenTestMatch_2b, float32BetweenTestResult_2, 31),
	CreateFloat32TestCase("ext128", float32TestSlice_3, float32BetweenTestMatch_3, float32BetweenTestMatch_3b, float32BetweenTestResult_3, 128),
	CreateFloat32TestCase("ext32", float32TestSlice_3, float32BetweenTestMatch_3, float32BetweenTestMatch_3b, float32BetweenTestResult_3, 32),
	CreateFloat32TestCase("ext31", float32TestSlice_3, float32BetweenTestMatch_3, float32BetweenTestMatch_3b, float32BetweenTestResult_3, 31),
	CreateFloat32TestCase("nan128", float32TestSlice_4, float32BetweenTestMatch_4, float32BetweenTestMatch_4b, float32BetweenTestResult_4, 128),
	CreateFloat32TestCase("nan32", float32TestSlice_4, float32BetweenTestMatch_4, float32BetweenTestMatch_4b, float32BetweenTestResult_4, 32),
	CreateFloat32TestCase("nan31", float32TestSlice_4, float32BetweenTestMatch_4, float32BetweenTestMatch_4b, float32BetweenTestResult_4, 31),
}

func TestMatchFloat32BetweenGeneric(T *testing.T) {
	for _, c := range float32BetweenCases {
		// pre-allocate the result slice
		bits := make([]byte, bitFieldLen(len(c.slice)))
		cnt := matchFloat32BetweenGeneric(c.slice, c.match, c.match2, bits)
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

func TestMatchFloat32BetweenAVX2(T *testing.T) {
	for _, c := range float32BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat32BetweenAVX2(c.slice, c.match, c.match2, bits)
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

func TestMatchFloat32BetweenAVX512(T *testing.T) {
	if !useAVX512_F {
		T.Skip("AVX512F not available. Skipping TestMatchFloat32BetweenAVX512.")
	}
	for _, c := range float32BetweenCases {
		// pre-allocate the result slice and fill with poison
		l := bitFieldLen(len(c.slice))
		bits := make([]byte, l+32)
		for i, _ := range bits {
			bits[i] = 0xfa
		}
		bits = bits[:l]
		cnt := matchFloat32BetweenAVX512(c.slice, c.match, c.match2, bits)
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
func BenchmarkMatchFloat32BetweenGeneric(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32BetweenGeneric(a, 5, math.MaxFloat32/2, bits)
			}
		})
	}
}

func BenchmarkMatchFloat32BetweenAVX2(B *testing.B) {
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32BetweenAVX2(a, 5, math.MaxFloat32/2, bits)
			}
		})
	}
}

func BenchmarkMatchFloat32BetweenAVX512(B *testing.B) {
	if !useAVX512_F {
		B.Skip("AVX512F not available. Skipping BenchmarkMatchFloat32BetweenAVX512.")
	}
	for _, n := range vecBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			a := randFloat32Slice(n.l, 1)
			bits := make([]byte, bitFieldLen(len(a)))
			B.ResetTimer()
			B.SetBytes(int64(n.l * Float32Size))
			for i := 0; i < B.N; i++ {
				matchFloat32BetweenAVX512(a, 5, 10, bits)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Float32 Slice
//
func TestFloat32SliceContains(T *testing.T) {
	// nil slice
	if Float32.Contains(nil, 1) {
		T.Errorf("nil slice cannot contain value")
	}

	// empty slice
	if Float32.Contains([]float32{}, 1) {
		T.Errorf("empty slice cannot contain value")
	}

	// 1-element slice positive
	if !Float32.Contains([]float32{1}, 1) {
		T.Errorf("1-element slice value not found")
	}

	// 1-element slice negative
	if Float32.Contains([]float32{1}, 2) {
		T.Errorf("1-element slice found wrong match")
	}

	// n-element slice positive first element
	if !Float32.Contains([]float32{1, 3, 5, 7, 11, 13}, 1) {
		T.Errorf("N-element first slice value not found")
	}

	// n-element slice positive middle element
	if !Float32.Contains([]float32{1, 3, 5, 7, 11, 13}, 5) {
		T.Errorf("N-element middle slice value not found")
	}

	// n-element slice positive last element
	if !Float32.Contains([]float32{1, 3, 5, 7, 11, 13}, 13) {
		T.Errorf("N-element last slice value not found")
	}

	// n-element slice negative before
	if Float32.Contains([]float32{1, 3, 5, 7, 11, 13}, 0) {
		T.Errorf("N-element before slice value wrong match")
	}

	// n-element slice negative middle
	if Float32.Contains([]float32{1, 3, 5, 7, 11, 13}, 2) {
		T.Errorf("N-element middle slice value wrong match")
	}

	// n-element slice negative after
	if Float32.Contains([]float32{1, 3, 5, 7, 11, 13}, 14) {
		T.Errorf("N-element after slice value wrong match")
	}
}

func BenchmarkFloat32SliceContains(B *testing.B) {
	cases := []int{10, 1000, 1000000}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-neg", n), func(B *testing.B) {
			a := Float32.Sort(randFloat32Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				Float32.Contains(a, rand.Float32())
			}
		})
	}
	for _, n := range cases {
		B.Run(fmt.Sprintf("%d-pos", n), func(B *testing.B) {
			a := Float32.Sort(randFloat32Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				Float32.Contains(a, a[rand.Intn(len(a))])
			}
		})
	}
}

func TestFloat32SliceContainsRange(T *testing.T) {
	type VecTestRange struct {
		Name  string
		From  float32
		To    float32
		Match bool
	}

	type VecTestcase struct {
		Slice  []float32
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
			Slice: []float32{},
			Ranges: []VecTestRange{
				VecTestRange{Name: "X", From: 0, To: 2, Match: false},
			},
		},
		// 1-element slice
		VecTestcase{
			Slice: []float32{3},
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
			Slice: []float32{3},
			Ranges: []VecTestRange{
				VecTestRange{Name: "BCD", From: 3, To: 3, Match: true}, // Case B.3, C.1, D.1
			},
		},
		// N-element slice
		VecTestcase{
			Slice: []float32{3, 5, 7, 11, 13},
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
			Slice: []float32{
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
			if want, got := r.Match, Float32.ContainsRange(v.Slice, r.From, r.To); want != got {
				T.Errorf("case %d/%s want=%t got=%t", i, r.Name, want, got)
			}
		}
	}
}

func BenchmarkFloat32SliceContainsRange(B *testing.B) {
	for _, n := range []int{10, 1000, 1000000} {
		B.Run(fmt.Sprintf("%d", n), func(B *testing.B) {
			a := Float32.Sort(randFloat32Slice(n, 1))
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				min, max := rand.Float32(), rand.Float32()
				if min > max {
					min, max = max, min
				}
				Float32.ContainsRange(a, min, max)
			}
		})
	}
}
