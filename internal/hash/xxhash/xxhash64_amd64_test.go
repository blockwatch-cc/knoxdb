// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package xxhash

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cpu"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

/*************** xxhash64Uint32 *******************************************************/

func TestXXHash64Uint32SliceAVX2(t *testing.T) {
	if !cpu.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range xxhash64Uint32Cases {
		// pre-allocate the result slice
		res := x64_u32_avx2(c.slice, make([]uint64, len(c.slice)))
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func TestXXHash64Uint32SliceAVX512(t *testing.T) {
	if !cpu.UseAVX512_DQ {
		t.SkipNow()
	}
	for _, c := range xxhash64Uint32Cases {
		// pre-allocate the result slice
		res := x64_u32_avx512(c.slice, make([]uint64, len(c.slice)))
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func BenchmarkXXHash64Uint32SliceAVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint32](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(4 * int64(n.l))
			for range b.N {
				x64_u32_avx2(a, res)
			}
		})
	}
}

func BenchmarkXXHash64Uint32SliceAVX512(b *testing.B) {
	if !cpu.UseAVX512_DQ {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint32](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(4 * int64(n.l))
			for range b.N {
				x64_u32_avx512(a, res)
			}
		})
	}
}

/*************** xxhash64Uint64 *******************************************************/

func TestXXHash64Uint64SliceAVX2(t *testing.T) {
	if !cpu.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range xxhash64Uint64Cases {
		// pre-allocate the result slice
		res := x64_u64_avx2(c.slice, make([]uint64, len(c.slice)))
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func TestXXHash64Uint64SliceAVX512(t *testing.T) {
	if !cpu.UseAVX512_DQ {
		t.SkipNow()
	}
	for _, c := range xxhash64Uint64Cases {
		// pre-allocate the result slice
		res := x64_u64_avx512(c.slice, make([]uint64, len(c.slice)))
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func BenchmarkXXHash64Uint64SliceAVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint64](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for range b.N {
				x64_u64_avx2(a, res)
			}
		})
	}
}

func BenchmarkXXHash64Uint64SliceAVX512(b *testing.B) {
	if !cpu.UseAVX512_DQ {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint64](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for range b.N {
				x64_u64_avx512(a, res)
			}
		})
	}
}
