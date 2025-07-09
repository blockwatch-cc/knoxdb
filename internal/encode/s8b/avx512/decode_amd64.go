// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

// nolint
package avx512

import (
	"blockwatch.cc/knoxdb/internal/cpu"
	"blockwatch.cc/knoxdb/pkg/util"
)

/**************************** AVX512 64bit **************************/

//go:noescape
func initUint64AVX512()

//go:noescape
func decodeUint64AVX512(dst []uint64, src []byte, minv uint64) (value int)

// Go exports
func DecodeUint64(dst []uint64, src []byte, minv uint64) (int, error) {
	return decodeUint64AVX512(dst, src, minv), nil
}

func DecodeInt64(dst []int64, src []byte, minv int64) (int, error) {
	return decodeUint64AVX512(util.ReinterpretSlice[int64, uint64](dst), src, uint64(minv)), nil
}

func init() {
	if cpu.UseAVX512_F {
		initUint64AVX512()
	}
}

// internal jump targets, defined here to make go vet happy

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
func unpackOnesUint64AVX512()

//go:noescape
func unpackZerosUint64AVX512()
