// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64
// +build amd64

// nolint
package avx2

//go:noescape
func countValuesAVX2Core(src []byte) (count int)

/*************************** AVX2 64 bit ******************************/

//go:noescape
func initUint64AVX2()

//go:noescape
func decodeUint64AVX2(dst []uint64, src []byte, minv uint64) (value int)

/************************ AVX2 32bit ***************************/

//go:noescape
func initUint32AVX2()

//go:noescape
func decodeUint32AVX2Core(dst []uint32, src []byte, minv uint32) (value int)

/************************ AVX2 16bit ***************************/

//go:noescape
func initUint16AVX2()

//go:noescape
func decodeUint16AVX2Core(dst []uint16, src []byte, minv uint16) (value int)

/************************ AVX2 8bit ***************************/

//go:noescape
func initUint8AVX2()

//go:noescape
func decodeUint8AVX2Core(dst []uint8, src []byte, minv uint8) (value int)

// internal jump targets, defined here to make go vet happy

//go:noescape
func decodeUint8AVX2Exit(dst []uint8, src []byte, minv uint8) (value int)

//go:noescape
func unpack1Uint8AVX2()

//go:noescape
func unpack2Uint8AVX2()

//go:noescape
func unpack3Uint8AVX2()

//go:noescape
func unpack4Uint8AVX2()

//go:noescape
func unpack5Uint8AVX2()

//go:noescape
func unpack6Uint8AVX2()

//go:noescape
func unpack7Uint8AVX2()

//go:noescape
func unpack8Uint8AVX2()

//go:noescape
func unpack10Uint8AVX2()

//go:noescape
func unpack12Uint8AVX2()

//go:noescape
func unpack15Uint8AVX2()

//go:noescape
func unpack20Uint8AVX2()

//go:noescape
func unpack30Uint8AVX2()

//go:noescape
func unpack60Uint8AVX2()

//go:noescape
func unpackOnesUint8AVX2()

//go:noescape
func unpackZerosUint8AVX2()

//go:noescape
func decodeUint16AVX2Exit(dst []uint16, src []byte, minv uint16) (value int)

//go:noescape
func unpack1Uint16AVX2()

//go:noescape
func unpack2Uint16AVX2()

//go:noescape
func unpack3Uint16AVX2()

//go:noescape
func unpack4Uint16AVX2()

//go:noescape
func unpack5Uint16AVX2()

//go:noescape
func unpack6Uint16AVX2()

//go:noescape
func unpack7Uint16AVX2()

//go:noescape
func unpack8Uint16AVX2()

//go:noescape
func unpack10Uint16AVX2()

//go:noescape
func unpack12Uint16AVX2()

//go:noescape
func unpack15Uint16AVX2()

//go:noescape
func unpack20Uint16AVX2()

//go:noescape
func unpack30Uint16AVX2()

//go:noescape
func unpack60Uint16AVX2()

//go:noescape
func unpackOnesUint16AVX2()

//go:noescape
func unpackZerosUint16AVX2()

//go:noescape
func decodeUint32AVX2Exit(dst []uint32, src []byte, minv uint32) (value int)

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
func unpackOnesUint32AVX2()

//go:noescape
func unpackZerosUint32AVX2()

//go:noescape
func decodeUint64AVX2Exit(dst []uint64, src []byte, minv uint64) (value int)

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
func unpackOnesUint64AVX2()

//go:noescape
func unpackZerosUint64AVX2()
