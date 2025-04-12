// Copyright (c) 2020-2025 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc, alex@blockwatch.cc

package bloom

import (
	"blockwatch.cc/knoxdb/internal/filter"
	"blockwatch.cc/knoxdb/internal/hash/xxhash"
)

var (
	add_u8      = add_u8_purego
	add_u16     = add_u16_purego
	add_u32     = add_u32_purego
	add_u64     = add_u64_purego
	add_string  = add_string_purego
	add_strings = add_strings_purego
	merge       = merge_purego
)

func add_string_purego(f *Filter, v []byte) {
	h0, h1 := xxhash.Sum32x2(v, filter.XxHash32Seed, 0)
	f.add(f, h0, h1)
}

func add_strings_purego(f *Filter, src [][]byte) {
	for _, v := range src {
		h0, h1 := xxhash.Sum32x2(v, filter.XxHash32Seed, 0)
		f.add(f, h0, h1)
	}
}

func add_u8_purego(f *Filter, src []uint8) {
	for i := range src {
		v := uint32(src[i])
		f.add(f, xxhash.Hash32u32(v, filter.XxHash32Seed), xxhash.Hash32u32(v, 0))
	}
}

func add_u16_purego(f *Filter, src []uint16) {
	for i := range src {
		v := uint32(src[i])
		f.add(f, xxhash.Hash32u32(v, filter.XxHash32Seed), xxhash.Hash32u32(v, 0))
	}
}

func add_u32_purego(f *Filter, src []uint32) {
	for _, v := range src {
		f.add(f, xxhash.Hash32u32(v, filter.XxHash32Seed), xxhash.Hash32u32(v, 0))
	}
}

func add_u64_purego(f *Filter, src []uint64) {
	for _, v := range src {
		f.add(f, xxhash.Hash32u64(v, filter.XxHash32Seed), xxhash.Hash32u64(v, 0))
	}
}

func merge_purego(dst, src []byte) {
	// Perform union of each byte.
	for i := range dst {
		dst[i] |= src[i]
	}
}
