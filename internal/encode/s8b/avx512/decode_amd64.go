// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx512

import (
	"blockwatch.cc/knoxdb/pkg/util"
)

/**************************** AVX512 64bit **************************/

//go:noescape
func initUint64AVX512()

//go:noescape
func decodeUint64AVX512(dst []uint64, src []byte) (value int)

// Go exports
func DecodeUint64(dst []uint64, src []byte) (int, error) {
	return decodeUint64AVX512(dst, src), nil
}

func init() {
	if util.UseAVX512_F {
		initUint64AVX512()
	}
}
