// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package compress

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
)

type benchmarkSize struct {
	name string
	l    int
}

var benchmarkSizes = []benchmarkSize{
	{"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"64K", 64 * 1024},
	{"128K", 128 * 1024},
	{"1M", 1024 * 1024},
	{"128M", 128 * 1024 * 1024},
}

const Int64Size = 8
const Int32Size = 4
const Int16Size = 2
const Int8Size = 1

type Int64Test struct {
	name   string
	slice  []int64
	result []int64
}

type Int32Test struct {
	name   string
	slice  []int32
	result []int32
}

type Int16Test struct {
	name   string
	slice  []int16
	result []int16
}

type Int8Test struct {
	name   string
	slice  []int8
	result []int8
}

var (
	int64DecodedSlice = []int64{
		1, 3, 6, 10,
		15, 21, 28, 20,
		13, 25, 24, 22,
		19, 15, 10, 0,
	}

	int64DeltaEncoded = []int64{
		1, 2, 3, 4,
		5, 6, 7, -8,
		-7, 12, -1, -2,
		-3, -4, -5, -10,
	}

	int64ZzDeltaEncoded = []int64{
		2, 4, 6, 8,
		10, 12, 14, 15,
		13, 24, 1, 3,
		5, 7, 9, 19,
	}
)

func randInt64Slice(n, u int) []int64 {
	s := make([]int64, n*u)
	for i := 0; i < n; i++ {
		s[i] = rand.Int63()
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

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

func randInt16Slice(n, u int) []int16 {
	s := make([]int16, n*u)
	for i := 0; i < n; i++ {
		s[i] = int16(rand.Intn(math.MaxInt16 + 1))
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

func randInt8Slice(n, u int) []int8 {
	s := make([]int8, n*u)
	for i := 0; i < n; i++ {
		s[i] = int8(rand.Intn(math.MaxInt8 + 1))
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

// creates an int64 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateInt64TestCase(name string, slice []int64, result []int64, length int) Int64Test {
	if len(result) != len(slice) {
		panic("CreateInt64TestCase: length of slice and length of result does not match")
	}

	// create new slices by concat of given slices
	// we make it a little bit longer check buffer overruns
	var new_slice []int64
	var new_result []int64
	var l int = length
	for l > 0 {
		new_slice = append(new_slice, slice...)
		new_result = append(new_result, result...)
		l -= len(slice)
	}

	return Int64Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

// creates an int32 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateInt32TestCase(name string, slice []int64, result []int64, length int) Int32Test {
	if len(result) != len(slice) {
		panic("CreateInt32TestCase: length of slice and length of result does not match")
	}

	// create new slices by concat of given slices
	// we make it a little bit longer check buffer overruns
	slice32 := make([]int32, len(slice))
	for i, v := range slice {
		slice32[i] = int32(v)
	}
	result32 := make([]int32, len(result))
	for i, v := range result {
		result32[i] = int32(v)
	}
	var new_slice []int32
	var new_result []int32
	var l int = length
	for l > 0 {
		new_slice = append(new_slice, slice32...)
		new_result = append(new_result, result32...)
		l -= len(slice)
	}

	return Int32Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

// creates an int16 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateInt16TestCase(name string, slice []int64, result []int64, length int) Int16Test {
	if len(result) != len(slice) {
		panic("CreateInt16TestCase: length of slice and length of result does not match")
	}

	// create new slices by concat of given slices
	// we make it a little bit longer check buffer overruns
	slice16 := make([]int16, len(slice))
	for i, v := range slice {
		slice16[i] = int16(v)
	}
	result16 := make([]int16, len(result))
	for i, v := range result {
		result16[i] = int16(v)
	}
	var new_slice []int16
	var new_result []int16
	var l int = length
	for l > 0 {
		new_slice = append(new_slice, slice16...)
		new_result = append(new_result, result16...)
		l -= len(slice)
	}

	return Int16Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

// creates an int8 test case from the given slice
// Parameters:
//  - name: desired name of the test case
//  - slice: the slice for constructing the test case
//  - result: result for the given slice
//  - len: desired length of the test case
func CreateInt8TestCase(name string, slice []int64, result []int64, length int) Int8Test {
	if len(result) != len(slice) {
		panic("CreateInt8TestCase: length of slice and length of result does not match")
	}

	// create new slices by concat of given slices
	// we make it a little bit longer check buffer overruns
	slice8 := make([]int8, len(slice))
	for i, v := range slice {
		slice8[i] = int8(v)
	}
	result8 := make([]int8, len(result))
	for i, v := range result {
		result8[i] = int8(v)
	}
	var new_slice []int8
	var new_result []int8
	var l int = length
	for l > 0 {
		new_slice = append(new_slice, slice8...)
		new_result = append(new_result, result8...)
		l -= len(slice)
	}

	return Int8Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

// --------------- zzDeltaDecodeInt64 --------------------------------------------------------------

var zzDeltaDecodeInt64Cases = []Int64Test{
	{
		name:   "l0",
		slice:  make([]int64, 0),
		result: []int64{},
	},
	CreateInt64TestCase("l3", int64ZzDeltaEncoded, int64DecodedSlice, 3),
	CreateInt64TestCase("l4", int64ZzDeltaEncoded, int64DecodedSlice, 4),
	CreateInt64TestCase("l7", int64ZzDeltaEncoded, int64DecodedSlice, 7),
	CreateInt64TestCase("l8", int64ZzDeltaEncoded, int64DecodedSlice, 8),
	CreateInt64TestCase("l15", int64ZzDeltaEncoded, int64DecodedSlice, 15),
	CreateInt64TestCase("l16", int64ZzDeltaEncoded, int64DecodedSlice, 16),
}

func TestZzDeltaDecodeInt64Generic(T *testing.T) {
	for _, c := range zzDeltaDecodeInt64Cases {
		slice := make([]int64, len(c.slice))
		copy(slice, c.slice)
		zzDeltaDecodeInt64Generic(slice)
		if got, want := len(slice), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(slice, c.result) {
			T.Errorf("%s: unexpected result %v, expected %v", c.name, slice, c.result)
		}
	}
}

func BenchmarkZzDeltaDecodeInt64Generic(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.l, 1)
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int64Size))
			for i := 0; i < B.N; i++ {
				zzDeltaDecodeInt64Generic(a)
			}
		})
	}
}

// --------------- zzDeltaDecodeInt32 --------------------------------------------------------------

var zzDeltaDecodeInt32Cases = []Int32Test{
	{
		name:   "l0",
		slice:  make([]int32, 0),
		result: []int32{},
	},
	CreateInt32TestCase("l3", int64ZzDeltaEncoded, int64DecodedSlice, 3),
	CreateInt32TestCase("l4", int64ZzDeltaEncoded, int64DecodedSlice, 4),
	CreateInt32TestCase("l7", int64ZzDeltaEncoded, int64DecodedSlice, 7),
	CreateInt32TestCase("l8", int64ZzDeltaEncoded, int64DecodedSlice, 8),
	CreateInt32TestCase("l15", int64ZzDeltaEncoded, int64DecodedSlice, 15),
	CreateInt32TestCase("l16", int64ZzDeltaEncoded, int64DecodedSlice, 16),
}

func TestZzDeltaDecodeInt32Generic(T *testing.T) {
	for _, c := range zzDeltaDecodeInt32Cases {
		slice := make([]int32, len(c.slice))
		copy(slice, c.slice)
		zzDeltaDecodeInt32Generic(slice)
		if got, want := len(slice), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(slice, c.result) {
			T.Errorf("%s: unexpected result %v, expected %v", c.name, slice, c.result)
		}
	}
}

func BenchmarkZzDeltaDecodeInt32Generic(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt32Slice(n.l, 1)
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				zzDeltaDecodeInt32Generic(a)
			}
		})
	}
}

// --------------- zzDeltaDecodeInt16 --------------------------------------------------------------

var zzDeltaDecodeInt16Cases = []Int16Test{
	{
		name:   "l0",
		slice:  make([]int16, 0),
		result: []int16{},
	},
	CreateInt16TestCase("l3", int64ZzDeltaEncoded, int64DecodedSlice, 3),
	CreateInt16TestCase("l4", int64ZzDeltaEncoded, int64DecodedSlice, 4),
	CreateInt16TestCase("l7", int64ZzDeltaEncoded, int64DecodedSlice, 7),
	CreateInt16TestCase("l8", int64ZzDeltaEncoded, int64DecodedSlice, 8),
	CreateInt16TestCase("l15", int64ZzDeltaEncoded, int64DecodedSlice, 15),
	CreateInt16TestCase("l16", int64ZzDeltaEncoded, int64DecodedSlice, 16),
	CreateInt16TestCase("l31", int64ZzDeltaEncoded, int64DecodedSlice, 31),
	CreateInt16TestCase("l32", int64ZzDeltaEncoded, int64DecodedSlice, 32),
}

func TestZzDeltaDecodeInt16Generic(T *testing.T) {
	for _, c := range zzDeltaDecodeInt16Cases {
		slice := make([]int16, len(c.slice))
		copy(slice, c.slice)
		zzDeltaDecodeInt16Generic(slice)
		if got, want := len(slice), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(slice, c.result) {
			T.Errorf("%s: unexpected result %v, expected %v", c.name, slice, c.result)
		}
	}
}

func BenchmarkZzDeltaDecodeInt16Generic(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt16Slice(n.l, 1)
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int16Size))
			for i := 0; i < B.N; i++ {
				zzDeltaDecodeInt16Generic(a)
			}
		})
	}
}

// --------------- zzDeltaDecodeInt8 --------------------------------------------------------------

var zzDeltaDecodeInt8Cases = []Int8Test{
	{
		name:   "l0",
		slice:  make([]int8, 0),
		result: []int8{},
	},
	CreateInt8TestCase("l3", int64ZzDeltaEncoded, int64DecodedSlice, 3),
	CreateInt8TestCase("l4", int64ZzDeltaEncoded, int64DecodedSlice, 4),
	CreateInt8TestCase("l7", int64ZzDeltaEncoded, int64DecodedSlice, 7),
	CreateInt8TestCase("l8", int64ZzDeltaEncoded, int64DecodedSlice, 8),
	CreateInt8TestCase("l15", int64ZzDeltaEncoded, int64DecodedSlice, 15),
	CreateInt8TestCase("l16", int64ZzDeltaEncoded, int64DecodedSlice, 16),
}

func TestZzDeltaDecodeInt8Generic(T *testing.T) {
	for _, c := range zzDeltaDecodeInt8Cases {
		slice := make([]int8, len(c.slice))
		copy(slice, c.slice)
		zzDeltaDecodeInt8Generic(slice)
		if got, want := len(slice), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(slice, c.result) {
			T.Errorf("%s: unexpected result %v, expected %v", c.name, slice, c.result)
		}
	}
}

func BenchmarkZzDeltaDecodeInt8Generic(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt8Slice(n.l, 1)
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int8Size))
			for i := 0; i < B.N; i++ {
				zzDeltaDecodeInt8Generic(a)
			}
		})
	}
}

// ---------------- zzDeltaEncodeInt64 -------------------------------------------------------------

var zzDeltaEncodeInt64Cases = []Int64Test{
	{
		name:   "l0",
		slice:  make([]int64, 0),
		result: []int64{},
	},
	CreateInt64TestCase("l3", int64DecodedSlice, int64ZzDeltaEncoded, 3),
	CreateInt64TestCase("l4", int64DecodedSlice, int64ZzDeltaEncoded, 4),
	CreateInt64TestCase("l7", int64DecodedSlice, int64ZzDeltaEncoded, 7),
	CreateInt64TestCase("l8", int64DecodedSlice, int64ZzDeltaEncoded, 8),
}

func TestZzDeltaEncodeUint64Generic(T *testing.T) {
	for _, c := range zzDeltaEncodeInt64Cases {
		slice := make([]int64, len(c.slice))
		copy(slice, c.slice)
		zzDeltaEncodeUint64Generic(ReintepretInt64ToUint64Slice(slice))
		if got, want := len(slice), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(slice, c.result) {
			T.Errorf("%s: unexpected result %v, expected %v", c.name, slice, c.result)
		}
	}
}

// --------------- zzDecodeInt64 --------------------------------------------------------------

var zzDecodeInt64Cases = []Int64Test{
	{
		name:   "l0",
		slice:  make([]int64, 0),
		result: []int64{},
	},
	CreateInt64TestCase("l3", int64ZzDeltaEncoded, int64DeltaEncoded, 3),
	CreateInt64TestCase("l4", int64ZzDeltaEncoded, int64DeltaEncoded, 4),
	CreateInt64TestCase("l7", int64ZzDeltaEncoded, int64DeltaEncoded, 7),
	CreateInt64TestCase("l8", int64ZzDeltaEncoded, int64DeltaEncoded, 8),
}

func TestZzDecodeInt64Generic(T *testing.T) {
	for _, c := range zzDecodeInt64Cases {
		slice := make([]int64, len(c.slice))
		copy(slice, c.slice)
		zzDecodeInt64Generic(slice)
		if got, want := len(slice), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(slice, c.result) {
			T.Errorf("%s: unexpected result %v, expected %v", c.name, slice, c.result)
		}
	}
}

func BenchmarkZzDecodeInt64Generic(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.l, 1)
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int64Size))
			for i := 0; i < B.N; i++ {
				zzDecodeInt64Generic(a)
			}
		})
	}
}

// ----------------- deltaDecodeInt64 ------------------------------------------------------------

var deltaDecodeInt64Cases = []Int64Test{
	{
		name:   "l0",
		slice:  make([]int64, 0),
		result: []int64{},
	},
	CreateInt64TestCase("l3", int64DeltaEncoded, int64DecodedSlice, 3),
	CreateInt64TestCase("l4", int64DeltaEncoded, int64DecodedSlice, 4),
	CreateInt64TestCase("l7", int64DeltaEncoded, int64DecodedSlice, 7),
	CreateInt64TestCase("l8", int64DeltaEncoded, int64DecodedSlice, 8),
}

func TestDeltaDecodeInt64Generic(T *testing.T) {
	for _, c := range deltaDecodeInt64Cases {
		slice := make([]int64, len(c.slice))
		copy(slice, c.slice)
		deltaDecodeInt64Generic(slice)
		if got, want := len(slice), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(slice, c.result) {
			T.Errorf("%s: unexpected result %v, expected %v", c.name, slice, c.result)
		}
	}
}

func BenchmarkDeltaDecodeInt64Generic(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt64Slice(n.l, 1)
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int64Size))
			for i := 0; i < B.N; i++ {
				deltaDecodeInt64Generic(a)
			}
		})
	}
}

// ----------------- deltaDecodeInt32 ------------------------------------------------------------

var deltaDecodeInt32Cases = []Int32Test{
	{
		name:   "l0",
		slice:  make([]int32, 0),
		result: []int32{},
	},
	CreateInt32TestCase("l3", int64DeltaEncoded, int64DecodedSlice, 3),
	CreateInt32TestCase("l4", int64DeltaEncoded, int64DecodedSlice, 4),
	CreateInt32TestCase("l7", int64DeltaEncoded, int64DecodedSlice, 7),
	CreateInt32TestCase("l8", int64DeltaEncoded, int64DecodedSlice, 8),
}

func TestDeltaDecodeInt32Generic(T *testing.T) {
	for _, c := range deltaDecodeInt32Cases {
		slice := make([]int32, len(c.slice))
		copy(slice, c.slice)
		deltaDecodeInt32Generic(slice)
		if got, want := len(slice), len(c.result); got != want {
			T.Errorf("%s: unexpected result length %d, expected %d", c.name, got, want)
		}
		if !reflect.DeepEqual(slice, c.result) {
			T.Errorf("%s: unexpected result %v, expected %v", c.name, slice, c.result)
		}
	}
}

func BenchmarkDeltaDecodeInt32Generic(B *testing.B) {
	for _, n := range benchmarkSizes {
		a := randInt32Slice(n.l, 1)
		B.Run(n.name, func(B *testing.B) {
			B.SetBytes(int64(n.l * Int32Size))
			for i := 0; i < B.N; i++ {
				deltaDecodeInt32Generic(a)
			}
		})
	}
}
