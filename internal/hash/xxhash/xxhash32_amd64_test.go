// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package xxhash

import (
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestXXhash32Uint32AVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range xxhash32Uint32Cases {
		// pre-allocate the result slice
		res := x32_u32_avx2(c.slice, make([]uint32, len(c.slice)), 0)
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func TestXXhash32Uint32AVX512(t *testing.T) {
	if !util.UseAVX512_F {
		t.SkipNow()
	}
	for _, c := range xxhash32Uint32Cases {
		// pre-allocate the result slice
		res := x32_u32_avx512(c.slice, make([]uint32, len(c.slice)), 0)
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func BenchmarkXXHash32Uint32AVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint32](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(int64(n.l * 4))
			for range b.N {
				x32_u32_avx2(a, res, 0)
			}
		})
	}
}

func BenchmarkXXHash32Uint32AVX512(b *testing.B) {
	if !util.UseAVX512_F {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint32](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(int64(n.l * 4))
			for range b.N {
				x32_u32_avx512(a, res, 0)
			}
		})
	}
}

/*************** xxhash32Uint64 *******************************************************/

func TestXXHash32Uint64AVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.SkipNow()
	}
	for _, c := range xxhash32Uint64Cases {
		// pre-allocate the result slice
		res := x32_u64_avx2(c.slice, make([]uint32, len(c.slice)), 0)
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func TestXXHash32Uint64AVX512(t *testing.T) {
	if !util.UseAVX512_F {
		t.SkipNow()
	}
	for _, c := range xxhash32Uint64Cases {
		// pre-allocate the result slice
		res := x32_u64_avx512(c.slice, make([]uint32, len(c.slice)), 0)
		require.Equal(t, len(c.result), len(res), "len")
		require.Equal(t, c.result, res, "result")
	}
}

func BenchmarkXXHash32Uint64AVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint64](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for range b.N {
				x32_u64_avx2(a, res, 0)
			}
		})
	}
}

func BenchmarkXXHash32Uint64AVX512(b *testing.B) {
	if !util.UseAVX512_F {
		b.SkipNow()
	}
	for _, n := range hashBenchmarkSizes {
		a := util.RandUints[uint64](n.l)
		res := make([]uint32, n.l)
		b.Run(n.name, func(b *testing.B) {
			b.SetBytes(8 * int64(n.l))
			for range b.N {
				x32_u64_avx512(a, res, 0)
			}
		})
	}
}
