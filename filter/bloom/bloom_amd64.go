// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package bloom

import (
	"blockwatch.cc/knoxdb/util"
)

//go:noescape
func filterAddManyUint32AVX2Core(f Filter, data []uint32, seed uint32)

//go:noescape
func filterAddManyInt32AVX2Core(f Filter, data []int32, seed uint32)

//go:noescape
func filterAddManyUint64AVX2Core(f Filter, data []uint64, seed uint32)

//go:noescape
func filterAddManyInt64AVX2Core(f Filter, data []int64, seed uint32)

//go:noescape
func filterMergeAVX2(dst, src []byte)

func filterAddManyUint32(f *Filter, data []uint32, seed uint32) {
	switch {
	case util.UseAVX2:
		filterAddManyUint32AVX2(*f, data, seed)
	default:
		filterAddManyUint32Generic(*f, data, seed)
	}
}

func filterAddManyInt32(f *Filter, data []int32, seed uint32) {
	switch {
	case util.UseAVX2:
		filterAddManyInt32AVX2(*f, data, seed)
	default:
		filterAddManyInt32Generic(*f, data, seed)
	}
}

func filterAddManyUint64(f *Filter, data []uint64, seed uint32) {
	switch {
	case util.UseAVX2:
		filterAddManyUint64AVX2(*f, data, seed)
	default:
		filterAddManyUint64Generic(*f, data, seed)
	}
}

func filterAddManyInt64(f *Filter, data []int64, seed uint32) {
	switch {
	case util.UseAVX2:
		filterAddManyInt64AVX2(*f, data, seed)
	default:
		filterAddManyInt64Generic(*f, data, seed)
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

func filterAddManyUint32AVX2(f Filter, data []uint32, seed uint32) {
	len_head := len(data) & 0x7ffffffffffffff8
	filterAddManyUint32AVX2Core(f, data, seed)
	filterAddManyUint32Generic(f, data[len_head:], seed)
}

func filterAddManyInt32AVX2(f Filter, data []int32, seed uint32) {
	len_head := len(data) & 0x7ffffffffffffff8
	filterAddManyInt32AVX2Core(f, data, seed)
	filterAddManyInt32Generic(f, data[len_head:], seed)
}

func filterAddManyUint64AVX2(f Filter, data []uint64, seed uint32) {
	len_head := len(data) & 0x7ffffffffffffff8
	filterAddManyUint64AVX2Core(f, data, seed)
	filterAddManyUint64Generic(f, data[len_head:], seed)
}

func filterAddManyInt64AVX2(f Filter, data []int64, seed uint32) {
	len_head := len(data) & 0x7ffffffffffffff8
	filterAddManyInt64AVX2Core(f, data, seed)
	filterAddManyInt64Generic(f, data[len_head:], seed)
}
