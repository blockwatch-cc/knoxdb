// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package s8b

func decodeAllUint64(dst []uint64, src []byte) (value int, err error) {
	return decodeAllUint64Generic(dst, src)
}

func decodeAllUint32(dst []uint32, src []byte) (value int, err error) {
	return decodeAllUint32Generic(dst, src)
}

func decodeAllUint16(dst []uint16, src []byte) (value int, err error) {
	return decodeAllUint16Generic(dst, src)
}

func decodeAllUint8(dst []uint8, src []byte) (value int, err error) {
	return decodeAllUint8Generic(dst, src)
}

func decodeBytesBigEndian(dst []uint64, src []byte) (value int, err error) {
	return decodeBytesBigEndianGeneric(dst, src)
}

func countValues(b []byte) (int, error) {
	return countValuesGeneric(b)
}

func countValuesBigEndian(b []byte) (int, error) {
	return countValuesBigEndianGeneric(b)
}
