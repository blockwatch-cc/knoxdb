// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package xxhash

import "blockwatch.cc/knoxdb/internal/cpu"

// ------------------------
// xxhash64
//

func init() {
	switch {
	case cpu.UseAVX512_DQ:
		Vec64u32 = x64_u32_avx512
		Vec64u64 = x64_u64_avx512
	case cpu.UseAVX2:
		Vec64u32 = x64_u32_avx2
		Vec64u64 = x64_u64_avx2
	}
}

//go:noescape
func x64_u32_core_avx2(src []uint32, res []uint64)

//go:noescape
func x64_u32_core_avx512(src []uint32, res []uint64)

//go:noescape
func x64_u64_core_avx2(src []uint64, res []uint64)

//go:noescape
func x64_u64_core_avx512(src []uint64, res []uint64)

func x64_u32_avx2(src []uint32, res []uint64) []uint64 {
	len_head := len(src) & 0x7ffffffffffffffc
	x64_u32_core_avx2(src, res)
	x64_u32_purego(src[len_head:], res[len_head:])
	return res
}

func x64_u32_avx512(src []uint32, res []uint64) []uint64 {
	len_head := len(src) & 0x7ffffffffffffff8
	x64_u32_core_avx512(src, res)
	x64_u32_purego(src[len_head:], res[len_head:])
	return res
}

func x64_u64_avx2(src []uint64, res []uint64) []uint64 {
	len_head := len(src) & 0x7ffffffffffffffc
	x64_u64_core_avx2(src, res)
	x64_u64_purego(src[len_head:], res[len_head:])
	return res
}

func x64_u64_avx512(src []uint64, res []uint64) []uint64 {
	len_head := len(src) & 0x7ffffffffffffff8
	x64_u64_core_avx512(src, res)
	x64_u64_purego(src[len_head:], res[len_head:])
	return res
}
