// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package s8bVec

//go:noescape
func countValuesAVX2Core(src []byte) (count int)

//go:noescape
func countValuesBigEndianAVX2Core(src []byte) (count int)

/*************************** AVX2 64 bit ******************************/

//go:noescape
func initUint64AVX2()

//go:noescape
func decodeAllUint64AVX2(dst []uint64, src []byte) (value int)

//go:noescape
func decodeAllUint64AVX2Exit()

//go:noescape
func unpack1Uint64AVX2()

//go:noescape
func unpack2Uint64AVX2()

//go:noescape
func unpack3Uint64AVX2()

//go:noescape
func unpack4Uint64AVX2()

//go:noescape
func unpack5Uint64AVX2()

//go:noescape
func unpack6Uint64AVX2()

//go:noescape
func unpack7Uint64AVX2()

//go:noescape
func unpack8Uint64AVX2()

//go:noescape
func unpack10Uint64AVX2()

//go:noescape
func unpack12Uint64AVX2()

//go:noescape
func unpack15Uint64AVX2()

//go:noescape
func unpack20Uint64AVX2()

//go:noescape
func unpack30Uint64AVX2()

//go:noescape
func unpack60Uint64AVX2()

//go:noescape
func unpack120Uint64AVX2()

//go:noescape
func unpack240Uint64AVX2()

/************************ AVX2 32bit ***************************/

//go:noescape
func initUint32AVX2()

//go:noescape
func decodeAllUint32AVX2(dst []uint32, src []byte) (value int)

//go:noescape
func unpack1Uint32AVX2()

//go:noescape
func unpack2Uint32AVX2()

//go:noescape
func unpack3Uint32AVX2()

//go:noescape
func unpack4Uint32AVX2()

//go:noescape
func unpack5Uint32AVX2()

//go:noescape
func unpack6Uint32AVX2()

//go:noescape
func unpack7Uint32AVX2()

//go:noescape
func unpack8Uint32AVX2()

//go:noescape
func unpack10Uint32AVX2()

//go:noescape
func unpack12Uint32AVX2()

//go:noescape
func unpack15Uint32AVX2()

//go:noescape
func unpack20Uint32AVX2()

//go:noescape
func unpack30Uint32AVX2()

//go:noescape
func unpack60Uint32AVX2()

//go:noescape
func unpack120Uint32AVX2()

//go:noescape
func unpack240Uint32AVX2()

/**************************** AVX512 64bit **************************/

//go:noescape
func initUint64AVX512()

//go:noescape
func decodeAllUint64AVX512(dst, src []uint64) (value int)

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
