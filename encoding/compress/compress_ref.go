// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package compress

func zzDeltaDecodeInt64(data []int64) {
	zzDeltaDecodeInt64Generic(data)
}

func zzDeltaDecodeUint64(data []uint64) {
	zzDeltaDecodeUint64Generic(data)
}

func zzDecodeInt64(data []int64) {
	zzDecodeInt64Generic(data)
}

func zzDecodeUint64(data []uint64) {
	zzDecodeUint64Generic(data)
}

func delta8DecodeUint64(data []uint64) {
	delta8DecodeUint64Generic(data)
}

func delta8EncodeUint64(data []uint64) uint64 {
	return delta8EncodeUint64Generic(data)
}
