// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package bloom

import (
	"blockwatch.cc/knoxdb/internal/hash"
	"blockwatch.cc/knoxdb/pkg/util"
)

//go:noescape
func filterAddManyUint32AVX2Core(data []uint32, buf []byte, mask, seed uint32)

//go:noescape
func filterAddManyInt32AVX2Core(data []int32, buf []byte, mask, seed uint32)

//go:noescape
func filterAddManyUint64AVX2Core(data []uint64, buf []byte, mask, seed uint32)

//go:noescape
func filterAddManyInt64AVX2Core(data []int64, buf []byte, mask, seed uint32)

//go:noescape
func filterMergeAVX2(dst, src []byte)

func filterAddManyUint32(f *Filter, data []uint32) {
	switch {
	case util.UseAVX2:
		filterAddManyUint32AVX2Core(data, f.bits, f.mask, hash.XxHash32Seed)
		len_head := len(data) & 0x7ffffffffffffff8
		filterAddManyUint32Generic(f, data[len_head:])
	default:
		filterAddManyUint32Generic(f, data)
	}
}

func filterAddManyInt32(f *Filter, data []int32) {
	switch {
	case util.UseAVX2:
		filterAddManyInt32AVX2Core(data, f.bits, f.mask, hash.XxHash32Seed)
		len_head := len(data) & 0x7ffffffffffffff8
		filterAddManyInt32Generic(f, data[len_head:])
	default:
		filterAddManyInt32Generic(f, data)
	}
}

func filterAddManyUint64(f *Filter, data []uint64) {
	switch {
	case util.UseAVX2:
		filterAddManyUint64AVX2Core(data, f.bits, f.mask, hash.XxHash32Seed)
		len_head := len(data) & 0x7ffffffffffffff8
		filterAddManyUint64Generic(f, data[len_head:])
	default:
		filterAddManyUint64Generic(f, data)
	}
}

func filterAddManyInt64(f *Filter, data []int64) {
	switch {
	case util.UseAVX2:
		filterAddManyInt64AVX2Core(data, f.bits, f.mask, hash.XxHash32Seed)
		len_head := len(data) & 0x7ffffffffffffff8
		filterAddManyInt64Generic(f, data[len_head:])
	default:
		filterAddManyInt64Generic(f, data)
	}
}

func filterMerge(dst, src []byte) {
	switch {
	case util.UseAVX2:
		filterMergeAVX2(dst, src)
	default:
		filterMergeGeneric(dst, src)
	}
}

// func filterAddManyUint32AVX2(buf []byte, data []uint32, seed uint32) {
// 	len_head := len(data) & 0x7ffffffffffffff8
// 	filterAddManyUint32AVX2Core(buf, data, seed)
// 	filterAddManyUint32Generic(&f, data[len_head:])
// }

// func filterAddManyInt32AVX2(f Filter, data []int32, seed uint32) {
// 	len_head := len(data) & 0x7ffffffffffffff8
// 	filterAddManyInt32AVX2Core(f, data, seed)
// 	filterAddManyInt32Generic(&f, data[len_head:])
// }

// func filterAddManyUint64AVX2(f Filter, data []uint64, seed uint32) {
// 	len_head := len(data) & 0x7ffffffffffffff8
// 	filterAddManyUint64AVX2Core(f, data, seed)
// 	filterAddManyUint64Generic(&f, data[len_head:])
// }

// func filterAddManyInt64AVX2(f Filter, data []int64, seed uint32) {
// 	len_head := len(data) & 0x7ffffffffffffff8
// 	filterAddManyInt64AVX2Core(f, data, seed)
// 	filterAddManyInt64Generic(&f, data[len_head:])
// }
