// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package xxhash

import (
	"blockwatch.cc/knoxdb/pkg/util"
)

// ------------------------
// xxhash32
//

func init() {
	switch {
	case util.UseAVX512_F:
		Vec32u32 = x32_u32_avx512
		Vec32u64 = x32_u64_avx512
	case util.UseAVX2:
		Vec32u32 = x32_u32_avx2
		Vec32u64 = x32_u64_avx2
	}
}

//go:noescape
func x32_u32_core_avx2(src []uint32, res []uint32, seed uint32)

//go:noescape
func x32_u32_core_avx512(src []uint32, res []uint32, seed uint32)

//go:noescape
func x32_u64_core_avx2(src []uint64, res []uint32, seed uint32)

//go:noescape
func x32_u64_core_avx512(src []uint64, res []uint32, seed uint32)

func x32_u32_avx2(src []uint32, res []uint32, seed uint32) []uint32 {
	len_head := len(src) & 0x7ffffffffffffff8
	x32_u32_core_avx2(src, res, seed)
	x32_u32_purego(src[len_head:], res[len_head:], seed)
	return res
}

func x32_u32_avx512(src []uint32, res []uint32, seed uint32) []uint32 {
	len_head := len(src) & 0x7ffffffffffffff0
	x32_u32_core_avx512(src, res, seed)
	x32_u32_purego(src[len_head:], res[len_head:], seed)
	return res
}

func x32_u64_avx2(src []uint64, res []uint32, seed uint32) []uint32 {
	len_head := len(src) & 0x7ffffffffffffff8
	x32_u64_core_avx2(src, res, seed)
	x32_u64_purego(src[len_head:], res[len_head:], seed)
	return res
}

func x32_u64_avx512(src []uint64, res []uint32, seed uint32) []uint32 {
	len_head := len(src) & 0x7ffffffffffffff8
	x32_u64_core_avx512(src, res, seed)
	x32_u64_purego(src[len_head:], res[len_head:], seed)
	return res
}
