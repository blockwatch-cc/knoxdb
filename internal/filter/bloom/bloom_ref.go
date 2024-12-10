// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package bloom

func filterAddManyUint32(f *Filter, data []uint32, seed uint32) {
	filterAddManyUint32Generic(*f, data, seed)
}

func filterAddManyInt32(f *Filter, data []int32, seed uint32) {
	filterAddManyInt32Generic(*f, data, seed)
}

func filterAddManyUint64(f *Filter, data []uint64, seed uint32) {
	filterAddManyUint64Generic(*f, data, seed)
}

func filterAddManyInt64(f *Filter, data []int64, seed uint32) {
	filterAddManyInt64Generic(*f, data, seed)
}

func filterMerge(dst, src []byte) {
	filterMergeGeneric(dst, src)
}
