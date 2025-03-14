// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package xxhashVec

import (
	"blockwatch.cc/knoxdb/pkg/util"
)

//go:noescape
func xxhash32Uint32SliceAVX2Core(src []uint32, res []uint32, seed uint32)

//go:noescape
func xxhash32Uint32SliceAVX512Core(src []uint32, res []uint32, seed uint32)

//go:noescape
func xxhash32Int32SliceAVX2Core(src []int32, res []uint32, seed uint32)

//go:noescape
func xxhash32Int32SliceAVX512Core(src []int32, res []uint32, seed uint32)

//go:noescape
func xxhash32Uint64SliceAVX2Core(src []uint64, res []uint32, seed uint32)

//go:noescape
func xxhash32Uint64SliceAVX512Core(src []uint64, res []uint32, seed uint32)

//go:noescape
func xxhash32Int64SliceAVX2Core(src []int64, res []uint32, seed uint32)

//go:noescape
func xxhash32Int64SliceAVX512Core(src []int64, res []uint32, seed uint32)

//go:noescape
func xxhash64Uint32SliceAVX2Core(src []uint32, res []uint64)

//go:noescape
func xxhash64Uint32SliceAVX512Core(src []uint32, res []uint64)

//go:noescape
func xxhash64Uint64SliceAVX2Core(src []uint64, res []uint64)

//go:noescape
func xxhash64Uint64SliceAVX512Core(src []uint64, res []uint64)

//go:noescape
func xxh3Uint32SliceAVX2Core(src []uint32, res []uint64)

//go:noescape
func xxh3Uint32SliceAVX512Core(src []uint32, res []uint64)

//go:noescape
func xxh3Uint64SliceAVX2Core(src []uint64, res []uint64)

//go:noescape
func xxh3Uint64SliceAVX512Core(src []uint64, res []uint64)

func xxhash32Uint32Slice(src []uint32, res []uint32, seed uint32) {
	switch {
	case util.UseAVX512_F:
		xxhash32Uint32SliceAVX512(src, res, seed)
	case util.UseAVX2:
		xxhash32Uint32SliceAVX2(src, res, seed)
	default:
		xxhash32Uint32SliceGeneric(src, res, seed)
	}
}

func xxhash32Int32Slice(src []int32, res []uint32, seed uint32) {
	switch {
	case util.UseAVX512_F:
		xxhash32Int32SliceAVX512(src, res, seed)
	case util.UseAVX2:
		xxhash32Int32SliceAVX2(src, res, seed)
	default:
		xxhash32Int32SliceGeneric(src, res, seed)
	}
}

func xxhash32Uint64Slice(src []uint64, res []uint32, seed uint32) {
	switch {
	case util.UseAVX512_F:
		xxhash32Uint64SliceAVX512(src, res, seed)
	case util.UseAVX2:
		xxhash32Uint64SliceAVX2(src, res, seed)
	default:
		xxhash32Uint64SliceGeneric(src, res, seed)
	}
}

func xxhash32Int64Slice(src []int64, res []uint32, seed uint32) {
	switch {
	case util.UseAVX512_F:
		xxhash32Int64SliceAVX512(src, res, seed)
	case util.UseAVX2:
		xxhash32Int64SliceAVX2(src, res, seed)
	default:
		xxhash32Int64SliceGeneric(src, res, seed)
	}
}

func xxhash64Uint8Slice(src []uint8, res []uint64) {
	switch {
	// case util.UseAVX512_DQ:
	// 	xxhash64Uint16SliceAVX512(src, res)
	// case util.UseAVX2:
	// 	xxhash64Uint16SliceAVX2(src, res)
	default:
		xxhash64Uint8SliceGeneric(src, res)
	}
}

func xxhash64Uint16Slice(src []uint16, res []uint64) {
	switch {
	// case util.UseAVX512_DQ:
	// 	xxhash64Uint16SliceAVX512(src, res)
	// case util.UseAVX2:
	// 	xxhash64Uint16SliceAVX2(src, res)
	default:
		xxhash64Uint16SliceGeneric(src, res)
	}
}

func xxhash64Uint32Slice(src []uint32, res []uint64) {
	switch {
	case util.UseAVX512_DQ:
		xxhash64Uint32SliceAVX512(src, res)
	case util.UseAVX2:
		xxhash64Uint32SliceAVX2(src, res)
	default:
		xxhash64Uint32SliceGeneric(src, res)
	}
}

func xxhash64Uint64Slice(src []uint64, res []uint64) {
	switch {
	case util.UseAVX512_DQ:
		xxhash64Uint64SliceAVX512(src, res)
	case util.UseAVX2:
		xxhash64Uint64SliceAVX2(src, res)
	default:
		xxhash64Uint64SliceGeneric(src, res)
	}
}

func xxh3Uint32Slice(src []uint32, res []uint64) {
	switch {
	case util.UseAVX512_DQ:
		xxh3Uint32SliceAVX512(src, res)
	case util.UseAVX2:
		xxh3Uint32SliceAVX2(src, res)
	default:
		xxh3Uint32SliceGeneric(src, res)
	}
}

func xxh3Uint64Slice(src []uint64, res []uint64) {
	switch {
	case util.UseAVX512_DQ:
		xxh3Uint64SliceAVX512(src, res)
	case util.UseAVX2:
		xxh3Uint64SliceAVX2(src, res)
	default:
		xxh3Uint64SliceGeneric(src, res)
	}
}

func xxhash32Uint32SliceAVX2(src []uint32, res []uint32, seed uint32) {
	len_head := len(src) & 0x7ffffffffffffff8
	xxhash32Uint32SliceAVX2Core(src, res, seed)
	xxhash32Uint32SliceGeneric(src[len_head:], res[len_head:], seed)
}

func xxhash32Uint32SliceAVX512(src []uint32, res []uint32, seed uint32) {
	len_head := len(src) & 0x7ffffffffffffff0
	xxhash32Uint32SliceAVX512Core(src, res, seed)
	xxhash32Uint32SliceGeneric(src[len_head:], res[len_head:], seed)
}

func xxhash32Int32SliceAVX2(src []int32, res []uint32, seed uint32) {
	len_head := len(src) & 0x7ffffffffffffff8
	xxhash32Int32SliceAVX2Core(src, res, seed)
	xxhash32Int32SliceGeneric(src[len_head:], res[len_head:], seed)
}

func xxhash32Int32SliceAVX512(src []int32, res []uint32, seed uint32) {
	len_head := len(src) & 0x7ffffffffffffff0
	xxhash32Int32SliceAVX512Core(src, res, seed)
	xxhash32Int32SliceGeneric(src[len_head:], res[len_head:], seed)
}

func xxhash32Uint64SliceAVX2(src []uint64, res []uint32, seed uint32) {
	len_head := len(src) & 0x7ffffffffffffff8
	xxhash32Uint64SliceAVX2Core(src, res, seed)
	xxhash32Uint64SliceGeneric(src[len_head:], res[len_head:], seed)
}

func xxhash32Uint64SliceAVX512(src []uint64, res []uint32, seed uint32) {
	len_head := len(src) & 0x7ffffffffffffff0
	xxhash32Uint64SliceAVX512Core(src, res, seed)
	xxhash32Uint64SliceGeneric(src[len_head:], res[len_head:], seed)
}

func xxhash32Int64SliceAVX2(src []int64, res []uint32, seed uint32) {
	len_head := len(src) & 0x7ffffffffffffff8
	xxhash32Int64SliceAVX2Core(src, res, seed)
	xxhash32Int64SliceGeneric(src[len_head:], res[len_head:], seed)
}

func xxhash32Int64SliceAVX512(src []int64, res []uint32, seed uint32) {
	len_head := len(src) & 0x7ffffffffffffff0
	xxhash32Int64SliceAVX512Core(src, res, seed)
	xxhash32Int64SliceGeneric(src[len_head:], res[len_head:], seed)
}

func xxhash64Uint32SliceAVX2(src []uint32, res []uint64) {
	len_head := len(src) & 0x7ffffffffffffffc
	xxhash64Uint32SliceAVX2Core(src, res)
	xxhash64Uint32SliceGeneric(src[len_head:], res[len_head:])
}

func xxhash64Uint32SliceAVX512(src []uint32, res []uint64) {
	len_head := len(src) & 0x7ffffffffffffff8
	xxhash64Uint32SliceAVX512Core(src, res)
	xxhash64Uint32SliceGeneric(src[len_head:], res[len_head:])
}

func xxhash64Uint64SliceAVX2(src []uint64, res []uint64) {
	len_head := len(src) & 0x7ffffffffffffffc
	xxhash64Uint64SliceAVX2Core(src, res)
	xxhash64Uint64SliceGeneric(src[len_head:], res[len_head:])
}

func xxhash64Uint64SliceAVX512(src []uint64, res []uint64) {
	len_head := len(src) & 0x7ffffffffffffff8
	xxhash64Uint64SliceAVX512Core(src, res)
	xxhash64Uint64SliceGeneric(src[len_head:], res[len_head:])
}

func xxh3Uint32SliceAVX2(src []uint32, res []uint64) {
	len_head := len(src) & 0x7ffffffffffffffc
	xxh3Uint32SliceAVX2Core(src, res)
	xxh3Uint32SliceGeneric(src[len_head:], res[len_head:])
}

func xxh3Uint32SliceAVX512(src []uint32, res []uint64) {
	len_head := len(src) & 0x7ffffffffffffff8
	xxh3Uint32SliceAVX512Core(src, res)
	xxh3Uint32SliceGeneric(src[len_head:], res[len_head:])
}

func xxh3Uint64SliceAVX2(src []uint64, res []uint64) {
	len_head := len(src) & 0x7ffffffffffffffc
	xxh3Uint64SliceAVX2Core(src, res)
	xxh3Uint64SliceGeneric(src[len_head:], res[len_head:])
}

func xxh3Uint64SliceAVX512(src []uint64, res []uint64) {
	len_head := len(src) & 0x7ffffffffffffff8
	xxh3Uint64SliceAVX512Core(src, res)
	xxh3Uint64SliceGeneric(src[len_head:], res[len_head:])
}
