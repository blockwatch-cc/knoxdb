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
func decodeUint64AVX2(dst []uint64, src []byte) (value int)

/************************ AVX2 32bit ***************************/

//go:noescape
func initUint32AVX2()

//go:noescape
func decodeUint32AVX2Core(dst []uint32, src []byte) (value int)

/************************ AVX2 16bit ***************************/

//go:noescape
func initUint16AVX2()

//go:noescape
func decodeUint16AVX2Core(dst []uint16, src []byte) (value int)

/************************ AVX2 8bit ***************************/

//go:noescape
func initUint8AVX2()

//go:noescape
func decodeUint8AVX2Core(dst []uint8, src []byte) (value int)
