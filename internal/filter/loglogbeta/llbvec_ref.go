// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package loglogbeta

func filterAddManyUint32(f *LogLogBeta, data []uint32, seed uint32) {
	filterAddManyUint32Generic(*f, data, seed)
}

func filterAddManyInt32(f *LogLogBeta, data []int32, seed uint32) {
	filterAddManyInt32Generic(*f, data, seed)
}

func filterAddManyUint64(f *LogLogBeta, data []uint64, seed uint32) {
	filterAddManyUint64Generic(*f, data, seed)
}

func filterAddManyInt64(f *LogLogBeta, data []int64, seed uint32) {
	filterAddManyInt64Generic(*f, data, seed)
}

func filterCardinality(f *LogLogBeta) uint64 {
	return filterCardinalityGeneric(*f)
}

func filterMerge(dst, src []byte) {
	filterMergeGeneric(dst, src)
}
