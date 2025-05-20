// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package xxhash

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

type XXHash64Uint32Test struct {
	name   string
	slice  []uint32
	result []uint64
}

// type XXHash64Int32Test struct {
//  name   string
//  slice  []int32
//  result []uint64
// }

type XXHash64Uint64Test struct {
	name   string
	slice  []uint64
	result []uint64
}

// type XXHash64Int64Test struct {
//  name   string
//  slice  []int64
//  result []uint64
// }

var (
	xxhash64Input = [][]byte{
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
	xxhash64Uint32Result = []uint64{18432908232848821278, 6063570110359613137, 873772980599321746, 5856652436104769068,
		5752797560547662665, 16833853067498898772, 3015398042591893023, 11282460491355425862}
	xxhash64Uint64Result = []uint64{9820687458478070669, 9316896406413536788, 13085766782279498260, 1636669749266472520,
		7694617266880998282, 738958588033515616, 8444214855924868781, 5257069345255417428}
)

// creates an XXHash64 test case for uint32 input date from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateXXHash64Uint32TestCase(name string, input [][]byte, result []uint64, length int) XXHash64Uint32Test {
	if len(result) != len(input) {
		panic("CreateXXHash64Uint32TestCase: length of slice and length of result does not match")
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
	var new_result []uint64
	l = length
	for l > 0 {
		new_result = append(new_result, result...)
		l -= len(result)
	}

	return XXHash64Uint32Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

// creates an XXHash64 test case for uint64 input date from the given slice
// Parameters:
//   - name: desired name of the test case
//   - slice: the slice for constructing the test case
//   - result: result for the given slice
//   - len: desired length of the test case
func CreateXXHash64Uint64TestCase(name string, input [][]byte, result []uint64, length int) XXHash64Uint64Test {
	if len(result) != len(input) {
		panic("CreateXXHash64Uint64TestCase: length of slice and length of result does not match")
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
	var new_result []uint64
	l = length
	for l > 0 {
		new_result = append(new_result, result...)
		l -= len(result)
	}

	return XXHash64Uint64Test{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

/*************** xxhash64Uint32 *******************************************************/

func TestXXHash64Uint32(t *testing.T) {
	for i, c := range xxhash64Input {
		require.Equal(t, xxhash64Uint32Result[i], Hash64u32(binary.LittleEndian.Uint32(c[0:4])), "input %d: %x", i, c[0:4])
	}
}

var xxhash64Uint32Cases = []XXHash64Uint32Test{
	{
		name:   "l0",
		slice:  make([]uint32, 0),
		result: []uint64{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint64{},
	},
	CreateXXHash64Uint32TestCase("l3", xxhash64Input, xxhash64Uint32Result, 3),
	CreateXXHash64Uint32TestCase("l4", xxhash64Input, xxhash64Uint32Result, 4),
	CreateXXHash64Uint32TestCase("l7", xxhash64Input, xxhash64Uint32Result, 7),
	CreateXXHash64Uint32TestCase("l8", xxhash64Input, xxhash64Uint32Result, 8),
	CreateXXHash64Uint32TestCase("l15", xxhash64Input, xxhash64Uint32Result, 15),
	CreateXXHash64Uint32TestCase("l16", xxhash64Input, xxhash64Uint32Result, 16),
}

func TestXXHash64Uint32Generic(t *testing.T) {
	for _, c := range xxhash64Uint32Cases {
		// pre-allocate the result slice
		res := x64_u32_purego(c.slice, make([]uint64, len(c.slice)))
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func BenchmarkXXHash64Uint32Generic(b *testing.B) {
	for _, n := range hashBenchmarkSizes {
		a := RandUints[uint32](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(4 * int64(n.l))
			for range b.N {
				x64_u32_purego(a, res)
			}
		})
	}
}

/*************** xxhash64Uint64 *******************************************************/

func TestXXHash64Uint64(t *testing.T) {
	for i, c := range xxhash64Input {
		require.Equal(t, xxhash64Uint64Result[i], Hash64u64(binary.LittleEndian.Uint64(c[0:8])), "input %d: %x", i, c[0:8])
	}
}

var xxhash64Uint64Cases = []XXHash64Uint64Test{
	{
		name:   "l0",
		slice:  make([]uint64, 0),
		result: []uint64{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint64{},
	},
	CreateXXHash64Uint64TestCase("l3", xxhash64Input, xxhash64Uint64Result, 3),
	CreateXXHash64Uint64TestCase("l4", xxhash64Input, xxhash64Uint64Result, 4),
	CreateXXHash64Uint64TestCase("l7", xxhash64Input, xxhash64Uint64Result, 7),
	CreateXXHash64Uint64TestCase("l8", xxhash64Input, xxhash64Uint64Result, 8),
	CreateXXHash64Uint64TestCase("l15", xxhash64Input, xxhash64Uint64Result, 15),
	CreateXXHash64Uint64TestCase("l16", xxhash64Input, xxhash64Uint64Result, 16),
}

func TestXXHash64Uint64Generic(t *testing.T) {
	for _, c := range xxhash64Uint64Cases {
		// pre-allocate the result slice
		res := x64_u64_purego(c.slice, make([]uint64, len(c.slice)))
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func BenchmarkXXHash64Uint64Generic(b *testing.B) {
	for _, n := range hashBenchmarkSizes {
		a := RandUints[uint64](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for range b.N {
				x64_u64_purego(a, res)
			}
		})
	}
}
