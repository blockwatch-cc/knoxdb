// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package xxhash

import (
	"encoding/binary"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func RandUints[T constraints.Unsigned](sz int) []T {
	s := make([]T, sz)
	for i := 0; i < sz; i++ {
		s[i] = T(rand.Uint64())
	}
	return s
}

type XXHash32Uint32Test struct {
	name   string
	slice  []uint32
	result []uint32
}

type XXHash32Int32Test struct {
	name   string
	slice  []int32
	result []uint32
}

type XXHash32Uint64Test struct {
	name   string
	slice  []uint64
	result []uint32
}

type XXHash32Int64Test struct {
	name   string
	slice  []int64
	result []uint32
}

var (
	xxhash32Input = [][]byte{
		{0, 1, 2, 3, 4, 5, 6, 7},
		{1, 2, 3, 4, 5, 6, 7, 8},
		{2, 3, 4, 5, 6, 7, 8, 9},
		{3, 4, 5, 6, 7, 8, 9, 10},
		{4, 5, 6, 7, 8, 9, 10, 11},
		{5, 6, 7, 8, 9, 10, 11, 12},
		{6, 7, 8, 9, 10, 11, 12, 13},
		{7, 8, 9, 10, 11, 12, 13, 14},
	}
	/* reference values are calculatetd with xxhash library v0.8.0
	 * https://github.com/Cyan4973/xxHash */
	xxhash32Uint32Result = []uint32{2154372710, 4271296924, 2572881654, 3610179124,
		1767988938, 2757935525, 3225940163, 3594529143}
	xxhash32Uint64Result = []uint32{2746060985, 339348840, 1725762203, 1251338271,
		1114514114, 1889681329, 3683323844, 2797893054}
)

// creates an XXHash32 test case for uint32 input date from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateXXHash32Uint32TestCase(name string, input [][]byte, result []uint32, length int) XXHash32Uint32Test {
	if len(result) != len(input) {
		panic("CreateXXHash32Uint32TestCase: length of slice and length of result does not match")
	}

	// Create input slice from bytes
	slice := make([]uint32, len(input))
	for i, v := range input {
		slice[i] = binary.LittleEndian.Uint32(v[0:4])
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
	var new_result []uint32
	l = length
	for l > 0 {
		new_result = append(new_result, result...)
		l -= len(result)
	}

	return XXHash32Uint32Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

// creates an XXHash32 test case for int32 input date from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateXXHash32Int32TestCase(name string, input [][]byte, result []uint32, length int) XXHash32Int32Test {
	if len(result) != len(input) {
		panic("CreateXXHash32Int32TestCase: length of slice and length of result does not match")
	}

	// Create input slice from bytes
	slice := make([]int32, len(input))
	for i, v := range input {
		slice[i] = int32(binary.LittleEndian.Uint32(v[0:4]))
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
	var new_result []uint32
	l = length
	for l > 0 {
		new_result = append(new_result, result...)
		l -= len(result)
	}

	return XXHash32Int32Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

// creates an XXHash32 test case for uint64 input date from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateXXHash32Uint64TestCase(name string, input [][]byte, result []uint32, length int) XXHash32Uint64Test {
	if len(result) != len(input) {
		panic("CreateXXHash32Uint64TestCase: length of slice and length of result does not match")
	}

	// Create input slice from bytes
	slice := make([]uint64, len(input))
	for i, v := range input {
		slice[i] = binary.LittleEndian.Uint64(v[0:8])
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []uint64
	var l int = length
	for l > 0 {
		new_slice = append(new_slice, slice...)
		l -= len(slice)
	}

	// create new result by concat of given result
	var new_result []uint32
	l = length
	for l > 0 {
		new_result = append(new_result, result...)
		l -= len(result)
	}

	return XXHash32Uint64Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

// creates an XXHash32 test case for int64 input date from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateXXHash32Int64TestCase(name string, input [][]byte, result []uint32, length int) XXHash32Int64Test {
	if len(result) != len(input) {
		panic("CreateXXHash32Int64TestCase: length of slice and length of result does not match")
	}

	// Create input slice from bytes
	slice := make([]int64, len(input))
	for i, v := range input {
		slice[i] = int64(binary.LittleEndian.Uint64(v[0:8]))
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []int64
	var l int = length
	for l > 0 {
		new_slice = append(new_slice, slice...)
		l -= len(slice)
	}

	// create new result by concat of given result
	var new_result []uint32
	l = length
	for l > 0 {
		new_result = append(new_result, result...)
		l -= len(result)
	}

	return XXHash32Int64Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

/*************** xxhash32Uint32 *******************************************************/

func TestXXHash32Uint32(t *testing.T) {
	for i, c := range xxhash32Input {
		require.Equal(t, xxhash32Uint32Result[i], Hash32u32(binary.LittleEndian.Uint32(c[0:4]), 0), "input %d: %x", i, c[0:4])
	}
}

var xxhash32Uint32Cases = []XXHash32Uint32Test{
	{
		name:   "l0",
		slice:  make([]uint32, 0),
		result: []uint32{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint32{},
	},
	CreateXXHash32Uint32TestCase("l7", xxhash32Input, xxhash32Uint32Result, 7),
	CreateXXHash32Uint32TestCase("l8", xxhash32Input, xxhash32Uint32Result, 8),
	CreateXXHash32Uint32TestCase("l15", xxhash32Input, xxhash32Uint32Result, 15),
	CreateXXHash32Uint32TestCase("l16", xxhash32Input, xxhash32Uint32Result, 16),
	CreateXXHash32Uint32TestCase("l31", xxhash32Input, xxhash32Uint32Result, 31),
	CreateXXHash32Uint32TestCase("l32", xxhash32Input, xxhash32Uint32Result, 32),
}

func TestXXHash32Uint32Generic(t *testing.T) {
	for _, c := range xxhash32Uint32Cases {
		// pre-allocate the result slice
		res := x32_u32_purego(c.slice, make([]uint32, len(c.slice)), 0)
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func BenchmarkXXHash32Uint32Generic(b *testing.B) {
	for _, n := range hashBenchmarkSizes {
		a := RandUints[uint32](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(int64(n.l * 4))
			for range b.N {
				x32_u32_purego(a, res, 0)
			}
		})
	}
}

/*************** xxhash32Uint64 *******************************************************/

func TestXXHash32Uint64(t *testing.T) {
	for i, c := range xxhash32Input {
		require.Equal(t, xxhash32Uint64Result[i], Hash32u64(binary.LittleEndian.Uint64(c[0:8]), 0), "input %d: %x", i, c[0:8])
	}
}

var xxhash32Uint64Cases = []XXHash32Uint64Test{
	{
		name:   "l0",
		slice:  make([]uint64, 0),
		result: []uint32{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint32{},
	},
	CreateXXHash32Uint64TestCase("l7", xxhash32Input, xxhash32Uint64Result, 7),
	CreateXXHash32Uint64TestCase("l8", xxhash32Input, xxhash32Uint64Result, 8),
	CreateXXHash32Uint64TestCase("l15", xxhash32Input, xxhash32Uint64Result, 15),
	CreateXXHash32Uint64TestCase("l16", xxhash32Input, xxhash32Uint64Result, 16),
	CreateXXHash32Uint64TestCase("l31", xxhash32Input, xxhash32Uint64Result, 31),
	CreateXXHash32Uint64TestCase("l32", xxhash32Input, xxhash32Uint64Result, 32),
}

func TestXXHash32Uint64Generic(t *testing.T) {
	for _, c := range xxhash32Uint64Cases {
		// pre-allocate the result slice
		res := x32_u64_purego(c.slice, make([]uint32, len(c.slice)), 0)
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func BenchmarkXXHash32Uint64Generic(b *testing.B) {
	for _, n := range hashBenchmarkSizes {
		a := RandUints[uint64](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for range b.N {
				x32_u64_purego(a, res, 0)
			}
		})
	}
}
