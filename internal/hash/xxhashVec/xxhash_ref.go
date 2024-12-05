// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package xxhashVec

func xxhash32Uint32Slice(src []uint32, res []uint32, seed uint32) {
	xxhash32Uint32SliceGeneric(src, res, seed)
}

func xxhash32Int32Slice(src []int32, res []uint32, seed uint32) {
	xxhash32Int32SliceGeneric(src, res, seed)
}

func xxhash32Uint64Slice(src []uint64, res []uint32, seed uint32) {
	xxhash32Uint64SliceGeneric(src, res, seed)
}

func xxhash32Int64Slice(src []int64, res []uint32, seed uint32) {
	xxhash32Int64SliceGeneric(src, res, seed)
}

func xxhash64Uint32Slice(src []uint32, res []uint64) {
	xxhash64Uint32SliceGeneric(src, res)
}

func xxhash64Uint64Slice(src []uint64, res []uint64) {
	xxhash64Uint64SliceGeneric(src, res)
}

func xxh3Uint32Slice(src []uint32, res []uint64) {
	xxh3Uint32SliceGeneric(src, res)
}

func xxh3Uint64Slice(src []uint64, res []uint64) {
	xxh3Uint64SliceGeneric(src, res)
}
