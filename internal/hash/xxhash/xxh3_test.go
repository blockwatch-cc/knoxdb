// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package xxhash

import (
	"encoding/binary"
	"testing"

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

/*************** xxh3Uint32 *******************************************************/

func TestXXH3Uint32(t *testing.T) {
	for i, c := range xxh3Input {
		require.Equal(t, xxh3Uint32Result[i], XXH3u32(binary.LittleEndian.Uint32(c[0:4])), "input %d: %x", i, c[0:4])
	}
}

var xxh3Uint32Cases = []XXHash64Uint32Test{
	{
		name:   "l0",
		slice:  make([]uint32, 0),
		result: []uint64{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint64{},
	},
	CreateXXHash64Uint32TestCase("l3", xxh3Input, xxh3Uint32Result, 3),
	CreateXXHash64Uint32TestCase("l4", xxh3Input, xxh3Uint32Result, 4),
	CreateXXHash64Uint32TestCase("l7", xxh3Input, xxh3Uint32Result, 7),
	CreateXXHash64Uint32TestCase("l8", xxh3Input, xxh3Uint32Result, 8),
	CreateXXHash64Uint32TestCase("l15", xxh3Input, xxh3Uint32Result, 15),
	CreateXXHash64Uint32TestCase("l16", xxh3Input, xxh3Uint32Result, 16),
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
	for _, n := range hashBenchmarkSizes {
		a := RandUints[uint32](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(4 * int64(n.l))
			for range b.N {
				xxh3_u32_purego(a, res)
			}
		})
	}
}

/*************** xxh3Uint64 *******************************************************/

func TestXXH3Uint64(t *testing.T) {
	for i, c := range xxh3Input {
		require.Equal(t, xxh3Uint64Result[i], XXH3u64(binary.LittleEndian.Uint64(c[0:8])), "input %d: %x", i, c[0:8])
	}
}

var xxh3Uint64Cases = []XXHash64Uint64Test{
	{
		name:   "l0",
		slice:  make([]uint64, 0),
		result: []uint64{},
	}, {
		name:   "nil",
		slice:  nil,
		result: []uint64{},
	},
	CreateXXHash64Uint64TestCase("l3", xxh3Input, xxh3Uint64Result, 3),
	CreateXXHash64Uint64TestCase("l4", xxh3Input, xxh3Uint64Result, 4),
	CreateXXHash64Uint64TestCase("l7", xxh3Input, xxh3Uint64Result, 7),
	CreateXXHash64Uint64TestCase("l8", xxh3Input, xxh3Uint64Result, 8),
	CreateXXHash64Uint64TestCase("l15", xxh3Input, xxh3Uint64Result, 15),
	CreateXXHash64Uint64TestCase("l16", xxh3Input, xxh3Uint64Result, 16),
	CreateXXHash64Uint64TestCase("l31", xxh3Input, xxh3Uint64Result, 31),
	CreateXXHash64Uint64TestCase("l32", xxh3Input, xxh3Uint64Result, 32),
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
	for _, n := range hashBenchmarkSizes {
		a := RandUints[uint64](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for range b.N {
				xxh3_u64_purego(a, res)
			}
		})
	}
}
