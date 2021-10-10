// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

package xxhashVec

import (
	"blockwatch.cc/knoxdb/util"
)

//go:noescape
func xxhash32Uint32SliceAVX2Core(src []uint32, res []uint32, seed uint32)

//go:noescape
func xxhash32Uint64SliceAVX2Core(src []uint64, res []uint32, seed uint32)

// //go:noescape
// func xxhash64Uint32SliceAVX2Core(src []uint32, res []uint32)

// //go:noescape
// func xxhash64Uint64SliceAVX2Core(src []uint64, res []uint32)

// //go:noescape
// func xxh3Uint32SliceAVX2Core(src []uint32, res []uint32)

// //go:noescape
// func xxh3Uint64SliceAVX2Core(src []uint64, res []uint32)

func xxhash32Uint32Slice(src []uint32, res []uint32, seed uint32) {
	switch {
	//	case util.UseAVX512_F:
	//		xxhash32Uint32SliceAVX512(src, res, seed)
	case util.UseAVX2:
		xxhash32Uint32SliceAVX2(src, res, seed)
	default:
		xxhash32Uint32SliceGeneric(src, res, seed)
	}
}

func xxhash32Uint64Slice(src []uint64, res []uint32, seed uint32) {
	switch {
	//	case util.UseAVX512_F:
	//		xxhash32Uint64SliceAVX512(src, res, seed)
	case util.UseAVX2:
		xxhash32Uint64SliceAVX2(src, res, seed)
	default:
		xxhash32Uint64SliceGeneric(src, res, seed)
	}
}

func xxhash64Uint32Slice(src []uint32, res []uint64) {
	switch {
	//	case util.UseAVX512_F:
	//		xxhash64Uint32SliceAVX512(src, res)
	//  case util.UseAVX2:
	//      xxhash64Uint32SliceAVX2(src, res)
	default:
		xxhash64Uint32SliceGeneric(src, res)
	}
}

func xxhash64Uint64Slice(src []uint64, res []uint64) {
	switch {
	//	case util.UseAVX512_F:
	//		xxhash64Uint64SliceAVX512(src, res)
	//	case util.UseAVX2:
	//	    xxhash64Uint64SliceAVX2(src, res)
	default:
		xxhash64Uint64SliceGeneric(src, res)
	}
}

func xxh3Uint32Slice(src []uint32, res []uint64) {
	switch {
	//	case util.UseAVX512_F:
	//		xxhash32Uint32SliceAVX512(src, res)
	//	case util.UseAVX2:
	//	    xxh3Uint32SliceAVX2(src, res)
	default:
		xxh3Uint32SliceGeneric(src, res)
	}
}

func xxh3Uint64Slice(src []uint64, res []uint64) {
	switch {
	//	case util.UseAVX512_F:
	//		xxh3Uint64SliceAVX512(src, res)
	//  case util.UseAVX2:
	//	    xxh3Uint64SliceAVX2(src, res)
	default:
		xxh3Uint64SliceGeneric(src, res)
	}
}

func xxhash32Uint32SliceAVX2(src []uint32, res []uint32, seed uint32) {
	len_head := len(src) & 0x7ffffffffffffff8
	xxhash32Uint32SliceAVX2Core(src, res, seed)
	xxhash32Uint32SliceGeneric(src[len_head:], res[len_head:], seed)
}

func xxhash32Uint64SliceAVX2(src []uint64, res []uint32, seed uint32) {
	len_head := len(src) & 0x7ffffffffffffff8
	xxhash32Uint64SliceAVX2Core(src, res, seed)
	xxhash32Uint64SliceGeneric(src[len_head:], res[len_head:], seed)
}
