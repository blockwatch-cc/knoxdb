// Copyright (c) 2021-2026 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc, alex@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package hash

import "blockwatch.cc/knoxdb/internal/cpu"

// ------------------------
// xxh3
//

func init() {
	switch {
	case cpu.UseAVX512_DQ:
		Vec32 = xxh3_u32_avx512
		Vec64 = xxh3_u64_avx512
	case cpu.UseAVX2:
		Vec32 = xxh3_u32_avx2
		Vec64 = xxh3_u64_avx2
	}
}

//go:noescape
func xxh3_u64_core_avx2(src []uint64, res []uint64)

//go:noescape
func xxh3_u64_core_avx512(src []uint64, res []uint64)

//go:noescape
func xxh3_u32_core_avx2(src []uint32, res []uint64)

//go:noescape
func xxh3_u32_core_avx512(src []uint32, res []uint64)

func xxh3_u32_avx2(src []uint32, res []uint64) []uint64 {
	if cap(res) < len(src) {
		res = make([]uint64, len(src))
	}
	res = res[:len(src)]
	len_head := len(src) & 0x7ffffffffffffffc
	xxh3_u32_core_avx2(src, res)
	xxh3_u32_purego(src[len_head:], res[len_head:])
	return res
}

func xxh3_u32_avx512(src []uint32, res []uint64) []uint64 {
	if cap(res) < len(src) {
		res = make([]uint64, len(src))
	}
	res = res[:len(src)]
	len_head := len(src) & 0x7ffffffffffffff8
	xxh3_u32_core_avx512(src, res)
	xxh3_u32_purego(src[len_head:], res[len_head:])
	return res
}

func xxh3_u64_avx2(src []uint64, res []uint64) []uint64 {
	if cap(res) < len(src) {
		res = make([]uint64, len(src))
	}
	res = res[:len(src)]
	len_head := len(src) & 0x7ffffffffffffffc
	xxh3_u64_core_avx2(src, res)
	xxh3_u64_purego(src[len_head:], res[len_head:])
	return res
}

func xxh3_u64_avx512(src []uint64, res []uint64) []uint64 {
	if cap(res) < len(src) {
		res = make([]uint64, len(src))
	}
	res = res[:len(src)]
	len_head := len(src) & 0x7ffffffffffffff8
	xxh3_u64_core_avx512(src, res)
	xxh3_u64_purego(src[len_head:], res[len_head:])
	return res
}
