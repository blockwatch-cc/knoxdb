// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package xxhash

import (
	"blockwatch.cc/knoxdb/pkg/util"
)

// ------------------------
// xxh3
//

func init() {
	switch {
	case util.UseAVX512_DQ:
		VecXXH3u32 = xxh3_u32_avx512
		VecXXH3u64 = xxh3_u64_avx512
	case util.UseAVX2:
		VecXXH3u32 = xxh3_u32_avx2
		VecXXH3u64 = xxh3_u64_avx2
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
	len_head := len(src) & 0x7ffffffffffffffc
	xxh3_u32_core_avx2(src, res)
	xxh3_u32_purego(src[len_head:], res[len_head:])
	return res
}

func xxh3_u32_avx512(src []uint32, res []uint64) []uint64 {
	len_head := len(src) & 0x7ffffffffffffff8
	xxh3_u32_core_avx512(src, res)
	xxh3_u32_purego(src[len_head:], res[len_head:])
	return res
}

func xxh3_u64_avx2(src []uint64, res []uint64) []uint64 {
	len_head := len(src) & 0x7ffffffffffffffc
	xxh3_u64_core_avx2(src, res)
	xxh3_u64_purego(src[len_head:], res[len_head:])
	return res
}

func xxh3_u64_avx512(src []uint64, res []uint64) []uint64 {
	len_head := len(src) & 0x7ffffffffffffff8
	xxh3_u64_core_avx512(src, res)
	xxh3_u64_purego(src[len_head:], res[len_head:])
	return res
}
