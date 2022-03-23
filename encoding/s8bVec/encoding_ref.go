// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package s8bVec

func decodeAll(dst, src []uint64) (value int, err error) {
	return decodeAllGeneric(dst, src)
}

func decodeBytesBigEndian(dst []uint64, src []byte) (value int, err error) {
	return decodeBytesBigEndianGeneric(dst, src)
}

func countBytes(b []byte) (int, error) {
	return countBytesGeneric(b)
}
