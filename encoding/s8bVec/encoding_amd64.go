// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package s8bVec

import (
	"errors"

	"blockwatch.cc/knoxdb/util"
)

func init() {
	if util.UseAVX512_F {
		initUint64AVX512()
	}
	if util.UseAVX2 {
		initUint64AVX2()
		initUint32AVX2()
	}
}

func decodeBytesBigEndian(dst []uint64, src []byte) (value int, err error) {
	switch {
	//case util.UseAVX2:
	//	return decodeBytesBigEndianAVX2(dst, src)
	default:
		return decodeBytesBigEndianGeneric(dst, src)
	}
}

func decodeAllUint64(dst []uint64, src []byte) (value int, err error) {
	switch {
	case util.UseAVX2:
		return decodeAllUint64AVX2(dst, src), nil
	default:
		return decodeAllUint64Generic(dst, src)
	}
}

func decodeAllUint32(dst []uint32, src []byte) (value int, err error) {
	switch {
	case util.UseAVX2:
		return decodeAllUint32AVX2(dst, src), nil
	default:
		return decodeAllUint32Generic(dst, src)
	}
}

func decodeAllUint16(dst []uint16, src []byte) (value int, err error) {
	switch {
	//case util.UseAVX2:
	//	return decodeAllUint16AVX2(dst, src), nil
	default:
		return decodeAllUint16Generic(dst, src)
	}
}

func decodeAllUint8(dst []uint8, src []byte) (value int, err error) {
	switch {
	//case util.UseAVX2:
	//	return decodeAllUint8AVX2(dst, src), nil
	default:
		return decodeAllUint8Generic(dst, src)
	}
}

func countValues(b []byte) (int, error) {
	switch {
	case util.UseAVX2:
		return countValuesAVX2(b)
	default:
		return countValuesGeneric(b)
	}
}

func countValuesBigEndian(b []byte) (int, error) {
	switch {
	case util.UseAVX2:
		return countValuesBigEndianAVX2(b)
	default:
		return countValuesBigEndianGeneric(b)
	}
}

func countValuesAVX2(src []byte) (int, error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}
	return countValuesAVX2Core(src), nil
}

func countValuesBigEndianAVX2(src []byte) (int, error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}
	return countValuesBigEndianAVX2Core(src), nil
}
