// Copyright (c) 2021-2026 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc, alex@blockwatch.cc

package hash

import (
	"encoding/binary"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

var (
	xxh3Input = [][]byte{
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
	xxh3Uint32Result = []uint64{6979084321315492338, 10992015174800262690, 9198932749014320068, 284606709437413655,
		9636445692175435800, 10506574136472534422, 15288656668032338727, 17931165511542358483}
	xxh3Uint64Result = []uint64{4187271766389786872, 1653410307359580823, 10968988069148854349, 18394629982161883682,
		7288085727936083465, 17701208102331325482, 17779176444116337920, 9817807099013809187}
)

type xxhTest[T uint32 | uint64] struct {
	name   string
	slice  []T
	result []uint64
}

// creates an XXHash64 test case for uint32 input date from the given slice
// Parameters:
//   - name: desired name of the test case
//   - input: inbut buffers for constructing the test case
//   - result: expected results for the given input
//   - len: desired length of the test case
func makeTestCase[T uint32 | uint64](name string, input [][]byte, result []uint64, length int) xxhTest[T] {
	if len(result) != len(input) {
		panic("makeTestCase: mismatched input and result length")
	}

	// Create input slice from bytes
	slice := make([]T, len(input))
	switch any(T(0)).(type) {
	case uint64:
		for i, v := range input {
			slice[i] = T(binary.LittleEndian.Uint64(v[0:8]))
		}
	case uint32:
		for i, v := range input {
			slice[i] = T(binary.LittleEndian.Uint32(v[0:4]))
		}
	}

	// create new slice by concat of given slice
	// we make it a little bit longer check buffer overruns
	var new_slice []T
	l := length
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

	return xxhTest[T]{
		name:   name,
		slice:  new_slice[:length],
		result: new_result[:length],
	}
}

/*************** xxh3Uint32 *******************************************************/

func TestXXH3Uint32(t *testing.T) {
	for i, c := range xxh3Input {
		require.Equal(t, xxh3Uint32Result[i], xxh3_u32(binary.LittleEndian.Uint32(c[0:4])), "input %d: %x", i, c[0:4])
	}
}

var xxh3Uint32Cases = []xxhTest[uint32]{
	{
		name:   "l0",
		slice:  make([]uint32, 0),
		result: []uint64{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint64{},
	},
	makeTestCase[uint32]("l3", xxh3Input, xxh3Uint32Result, 3),
	makeTestCase[uint32]("l4", xxh3Input, xxh3Uint32Result, 4),
	makeTestCase[uint32]("l7", xxh3Input, xxh3Uint32Result, 7),
	makeTestCase[uint32]("l8", xxh3Input, xxh3Uint32Result, 8),
	makeTestCase[uint32]("l15", xxh3Input, xxh3Uint32Result, 15),
	makeTestCase[uint32]("l16", xxh3Input, xxh3Uint32Result, 16),
}

func TestXXH3Uint32Generic(t *testing.T) {
	for _, c := range xxh3Uint32Cases {
		t.Run(c.name, func(t *testing.T) {
			// pre-allocate the result slice
			res := xxh3_u32_purego(c.slice, make([]uint64, len(c.slice)))
			require.Equal(t, len(c.result), len(res), "len")
			require.Equal(t, c.result, res, "result")
		})
	}
}

func BenchmarkXXH3Uint32Generic(b *testing.B) {
	for _, n := range BenchmarkSizes {
		a := util.RandUints[uint32](n.N)
		res := make([]uint64, n.N)
		b.Run(n.Name, func(b *testing.B) {
			b.SetBytes(4 * int64(n.N))
			for range b.N {
				xxh3_u32_purego(a, res)
			}
		})
	}
}

/*************** xxh3Uint64 *******************************************************/

func TestXXH3Uint64(t *testing.T) {
	for i, c := range xxh3Input {
		require.Equal(t, xxh3Uint64Result[i], xxh3_u64(binary.LittleEndian.Uint64(c[0:8])), "input %d: %x", i, c[0:8])
	}
}

var xxh3Uint64Cases = []xxhTest[uint64]{
	{
		name:   "l0",
		slice:  make([]uint64, 0),
		result: []uint64{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint64{},
	},
	makeTestCase[uint64]("l3", xxh3Input, xxh3Uint64Result, 3),
	makeTestCase[uint64]("l4", xxh3Input, xxh3Uint64Result, 4),
	makeTestCase[uint64]("l7", xxh3Input, xxh3Uint64Result, 7),
	makeTestCase[uint64]("l8", xxh3Input, xxh3Uint64Result, 8),
	makeTestCase[uint64]("l15", xxh3Input, xxh3Uint64Result, 15),
	makeTestCase[uint64]("l16", xxh3Input, xxh3Uint64Result, 16),
	makeTestCase[uint64]("l31", xxh3Input, xxh3Uint64Result, 31),
	makeTestCase[uint64]("l32", xxh3Input, xxh3Uint64Result, 32),
}

func TestXXH3Uint64Generic(t *testing.T) {
	for _, c := range xxh3Uint64Cases {
		t.Run(c.name, func(t *testing.T) {
			// pre-allocate the result slice
			res := xxh3_u64_purego(c.slice, make([]uint64, len(c.slice)))
			require.Equal(t, len(c.result), len(res), "len")
			require.Equal(t, c.result, res, "result")
		})
	}
}

func BenchmarkXXH3Uint64Generic(b *testing.B) {
	for _, n := range BenchmarkSizes {
		a := util.RandUints[uint64](n.N)
		res := make([]uint64, n.N)
		b.Run(n.Name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.N))
			for range b.N {
				xxh3_u64_purego(a, res)
			}
		})
	}
}
