// Copyright (c) 2020-2025 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc, alex@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package bloom

import (
	"blockwatch.cc/knoxdb/internal/filter"
	"blockwatch.cc/knoxdb/pkg/util"
)

func init() {
	if util.UseAVX2 {
		add_u32 = add_u32_avx2
		add_u64 = add_u64_avx2
		merge = merge_core_avx2
	}
}

//go:noescape
func add_u32_core_avx2(data []uint32, buf []byte, mask, seed uint32)

//go:noescape
func add_ui64_core_avx2(data []uint64, buf []byte, mask, seed uint32)

//go:noescape
func merge_core_avx2(dst, src []byte)

func add_u32_avx2(f *Filter, data []uint32) {
	add_u32_core_avx2(data, f.bits, f.mask, filter.XxHash32Seed)
	len_head := len(data) & 0x7ffffffffffffff8
	add_u32_purego(f, data[len_head:])
}

func add_u64_avx2(f *Filter, data []uint64) {
	add_ui64_core_avx2(data, f.bits, f.mask, filter.XxHash32Seed)
	len_head := len(data) & 0x7ffffffffffffff8
	add_u64_purego(f, data[len_head:])
}
