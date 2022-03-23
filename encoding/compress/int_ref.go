// Copyright (c) 2021 Blockwatch Data Inc.
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

func packBytes8Bit(src []uint64, buf []byte) {
	packBytes8BitGeneric(src, buf)
}

func packBytes16Bit(src []uint64, buf []byte) {
	packBytes16BitGeneric(src, buf)
}

func packBytes24Bit(src []uint64, buf []byte) {
	packBytes24BitGeneric(src, buf)
}

func packBytes32Bit(src []uint64, buf []byte) {
	packBytes32BitGeneric(src, buf)
}

func unpackBytes8Bit(src []byte, dst []uint64) {
	unpackBytes8BitGeneric(src, dst)
}

func unpackBytes16Bit(src []byte, dst []uint64) {
	unpackBytes16BitGeneric(src, dst)
}

func unpackBytes24Bit(src []byte, dst []uint64) {
	unpackBytes24BitGeneric(src, dst)
}

func unpackBytes32Bit(src []byte, dst []uint64) {
	unpackBytes32BitGeneric(src, dst)
}
