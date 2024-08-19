// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package tests

import (
	"golang.org/x/exp/slices"
	"math"
	"math/rand"
)

const Int64Size = 8
const Int32Size = 4
const Int16Size = 2
const Int8Size = 1

type Int64Test struct {
	Name   string
	Slice  []int64
	Result []int64
}

type Int32Test struct {
	Name   string
	Slice  []int32
	Result []int32
}

type Int16Test struct {
	Name   string
	Slice  []int16
	Result []int16
}

type Int8Test struct {
	Name   string
	Slice  []int8
	Result []int8
}

var (
	int64DecodedSlice = []int64{
		1, 3, 6, 10,
		15, 21, 28, 20,
		13, 25, 24, 22,
		19, 15, 10, 4,
		0,
	}

	int32DecodedSlice = []int32{
		1, 3, 6, 10,
		15, 21, 28, 20,
		13, 25, 24, 22,
		19, 15, 10, 4,
		0,
	}

	int16DecodedSlice = []int16{
		1, 3, 6, 10,
		15, 21, 28, 20,
		13, 25, 24, 22,
		19, 15, 10, 4,
		0,
	}

	int8DecodedSlice = []int8{
		1, 3, 6, 10,
		15, 21, 28, 20,
		13, 25, 24, 22,
		19, 15, 10, 4,
		0,
	}

	// int64DeltaEncoded = []int64{
	//  1, 2, 3, 4,
	//  5, 6, 7, -8,
	//  -7, 12, -1, -2,
	//  -3, -4, -5, -6,
	//  -4,
	// }

	int64ZzDeltaEncoded = []int64{
		2, 4, 6, 8,
		10, 12, 14, 15,
		13, 24, 1, 3,
		5, 7, 9, 11,
		7,
	}

	int32ZzDeltaEncoded = []int32{
		2, 4, 6, 8,
		10, 12, 14, 15,
		13, 24, 1, 3,
		5, 7, 9, 11,
		7,
	}

	int16ZzDeltaEncoded = []int16{
		2, 4, 6, 8,
		10, 12, 14, 15,
		13, 24, 1, 3,
		5, 7, 9, 11,
		7,
	}

	int8ZzDeltaEncoded = []int8{
		2, 4, 6, 8,
		10, 12, 14, 15,
		13, 24, 1, 3,
		5, 7, 9, 11,
		7,
	}
)

// creates an int64 test case from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateInt64TestCase(name string, slice, result []int64, length int) Int64Test {
	if len(result) != len(slice) {
		panic("CreateInt64TestCase: length of slice and length of result does not match")
	}

	return Int64Test{
		Name:   name,
		Slice:  slices.Clone(slice)[:length],
		Result: slices.Clone(result)[:length],
	}
}

// creates an int32 test case from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateInt32TestCase(name string, slice, result []int32, length int) Int32Test {
	if len(result) != len(slice) {
		panic("CreateInt32TestCase: length of slice and length of result does not match")
	}

	return Int32Test{
		Name:   name,
		Slice:  slices.Clone(slice)[:length],
		Result: slices.Clone(result)[:length],
	}
}

// creates an int16 test case from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateInt16TestCase(name string, slice, result []int16, length int) Int16Test {
	if len(result) != len(slice) {
		panic("CreateInt16TestCase: length of slice and length of result does not match")
	}

	return Int16Test{
		Name:   name,
		Slice:  slices.Clone(slice)[:length],
		Result: slices.Clone(result)[:length],
	}
}

// creates an int8 test case from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateInt8TestCase(name string, slice, result []int8, length int) Int8Test {
	if len(result) != len(slice) {
		panic("CreateInt8TestCase: length of slice and length of result does not match")
	}

	return Int8Test{
		Name:   name,
		Slice:  slices.Clone(slice)[:length],
		Result: slices.Clone(result)[:length],
	}
}

func RandInt64Slice(n, u int) []int64 {
	s := make([]int64, n*u)
	for i := 0; i < n; i++ {
		s[i] = rand.Int63()
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

func RandInt32Slice(n, u int) []int32 {
	s := make([]int32, n*u)
	for i := 0; i < n; i++ {
		s[i] = rand.Int31()
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

func RandInt16Slice(n, u int) []int16 {
	s := make([]int16, n*u)
	for i := 0; i < n; i++ {
		s[i] = int16(rand.Intn(math.MaxInt16 + 1))
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}

func RandInt8Slice(n, u int) []int8 {
	s := make([]int8, n*u)
	for i := 0; i < n; i++ {
		s[i] = int8(rand.Intn(math.MaxInt8 + 1))
	}
	for i := 1; i < u; i++ {
		copy(s[i*n:], s[:n])
	}
	return s
}
