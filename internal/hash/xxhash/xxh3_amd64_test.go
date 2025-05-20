// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package xxhash

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/cpu"
	"github.com/stretchr/testify/require"
)

/*************** xxh3Uint32 *******************************************************/

func TestXXH3Uint32SliceAVX2(t *testing.T) {
	if !cpu.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range xxh3Uint32Cases {
		// pre-allocate the result slice
		res := xxh3_u32_avx2(c.slice, make([]uint64, len(c.slice)))
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func TestXXH3Uint32SliceAVX512(t *testing.T) {
	if !cpu.UseAVX512_DQ {
		t.SkipNow()
	}
	for _, c := range xxh3Uint32Cases {
		// pre-allocate the result slice
		res := xxh3_u32_avx512(c.slice, make([]uint64, len(c.slice)))
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func BenchmarkXXH3Uint32SliceAVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := RandUints[uint32](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(4 * int64(n.l))
			for range b.N {
				xxh3_u32_avx2(a, res)
			}
		})
	}
}

func BenchmarkXXH3Uint32SliceAVX512(b *testing.B) {
	if !cpu.UseAVX512_DQ {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := RandUints[uint32](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(4 * int64(n.l))
			for range b.N {
				xxh3_u32_avx512(a, res)
			}
		})
	}
}

/*************** xxh3Uint64 *******************************************************/

func TestXXH3Uint64SliceAVX2(t *testing.T) {
	if !cpu.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range xxh3Uint64Cases {
		// pre-allocate the result slice
		res := xxh3_u64_avx2(c.slice, make([]uint64, len(c.slice)))
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func TestXXH3Uint64SliceAVX512(t *testing.T) {
	if !cpu.UseAVX512_DQ {
		t.SkipNow()
	}
	for _, c := range xxh3Uint64Cases {
		// pre-allocate the result slice
		res := xxh3_u64_avx512(c.slice, make([]uint64, len(c.slice)))
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func BenchmarkXXH3Uint64SliceAVX2(b *testing.B) {
	if !cpu.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := RandUints[uint64](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for range b.N {
				xxh3_u64_avx2(a, res)
			}
		})
	}
}

func BenchmarkXXH3Uint64SliceAVX512(b *testing.B) {
	if !cpu.UseAVX512_DQ {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := RandUints[uint64](n.l)
		res := make([]uint64, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for range b.N {
				xxh3_u64_avx512(a, res)
			}
		})
	}
}
