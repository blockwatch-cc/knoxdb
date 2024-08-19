// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package avx512

import (
	"blockwatch.cc/knoxdb/util"
)

/**************************** AVX512 64bit **************************/

//go:noescape
func initUint64AVX512()

//go:noescape
func decodeUint64AVX512(dst []uint64, src []byte) (value int)

//go:noescape
func unpack1Uint64AVX512()

//go:noescape
func unpack2Uint64AVX512()

//go:noescape
func unpack3Uint64AVX512()

//go:noescape
func unpack4Uint64AVX512()

//go:noescape
func unpack5Uint64AVX512()

//go:noescape
func unpack6Uint64AVX512()

//go:noescape
func unpack7Uint64AVX512()

//go:noescape
func unpack8Uint64AVX512()

//go:noescape
func unpack10Uint64AVX512()

//go:noescape
func unpack12Uint64AVX512()

//go:noescape
func unpack15Uint64AVX512()

//go:noescape
func unpack20Uint64AVX512()

//go:noescape
func unpack30Uint64AVX512()

//go:noescape
func unpack60Uint64AVX512()

//go:noescape
func unpack120Uint64AVX512()

//go:noescape
func unpack240Uint64AVX512()

// Go exports
func DecodeUint64(dst []uint64, src []byte) (int, error) {
	return decodeUint64AVX512(dst, src), nil
}

func init() {
	if util.UseAVX512_F {
		initUint64AVX512()
	}
}
