// Copyright (c) 2021-2026 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc, alex@blockwatch.cc

//go:build amd64 && !gccgo && !appengine

package llb

import (
	"blockwatch.cc/knoxdb/internal/cpu"
)

// Deprecation Note
// Multi-value add algos use deprecated xxhash32 instead of xxh3!
// Currently disabled in favor of hash-first-then-add structure.

func init() {
	// multi add algos (TODO: need hash update to xxh3)
	// switch {
	// case cpu.UseAVX512_CD:
	// 	llb_add_u32 = llb_add_u32_avx512
	// 	llb_add_u64 = llb_add_u64_avx512
	// case cpu.UseAVX2:
	// 	llb_add_u32 = llb_add_u32_avx2
	// 	llb_add_u64 = llb_add_u64_avx2
	// }

	// cardinality
	switch {
	case cpu.UseAVX512_F:
		llb_cardinality = llb_cardinality_avx512
	case cpu.UseAVX2:
		llb_cardinality = llb_cardinality_avx2
	}

	// merge (AVX2 only)
	if cpu.UseAVX2 {
		llb_merge = llb_merge_core_avx2
	}
}

//go:noescape
func llb_add_u32_core_avx2(f LogLogBeta, data []uint32, seed uint32)

//go:noescape
func llb_add_u32_core_avx512(f LogLogBeta, data []uint32, seed uint32)

//go:noescape
func llb_add_u64_core_avx2(f LogLogBeta, data []uint64, seed uint32)

//go:noescape
func llb_add_u64_core_avx512(f LogLogBeta, data []uint64, seed uint32)

//go:noescape
func llb_merge_core_avx2(dst, src []byte)

//go:noescape
func llb_sum_core_avx2(registers []uint8) (ret1 float64, ret2 float64)

//go:noescape
func llb_sum_core_avx512(registers []uint8) (ret1 float64, ret2 float64)

func llb_add_u32_avx2(f *LogLogBeta, data []uint32) {
	len_head := len(data) & 0x7ffffffffffffff8
	llb_add_u32_core_avx2(*f, data, 0)
	llb_add_u32_purego(f, data[len_head:])
}

func llb_add_u32_avx512(f *LogLogBeta, data []uint32) {
	len_head := len(data) & 0x7ffffffffffffff0
	llb_add_u32_core_avx512(*f, data, 0)
	llb_add_u32_purego(f, data[len_head:])
}

func llb_add_u64_avx2(f *LogLogBeta, data []uint64) {
	len_head := len(data) & 0x7ffffffffffffff8
	llb_add_u64_core_avx2(*f, data, 0)
	llb_add_u64_purego(f, data[len_head:])
}

func llb_add_u64_avx512(f *LogLogBeta, data []uint64) {
	len_head := len(data) & 0x7ffffffffffffff0
	llb_add_u64_core_avx512(*f, data, 0)
	llb_add_u64_purego(f, data[len_head:])
}

// Cardinality returns the number of unique elements added to the sketch
func llb_cardinality_avx2(llb *LogLogBeta) uint64 {
	sum, ez := llb_sum_core_avx2(llb.buf)
	m := float64(llb.m)
	return uint64(llb.alpha * m * (m - ez) / (beta(ez) + sum))
}

// Cardinality returns the number of unique elements added to the sketch
func llb_cardinality_avx512(llb *LogLogBeta) uint64 {
	sum, ez := llb_sum_core_avx512(llb.buf)
	m := float64(llb.m)
	return uint64(llb.alpha * m * (m - ez) / (beta(ez) + sum))
}
