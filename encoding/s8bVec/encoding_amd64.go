// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package s8bVec

import (
	"errors"
	"unsafe"

	"blockwatch.cc/knoxdb/util"
)

/*************************** AVX2 ******************************/

//go:noescape
func countBytesAVX2Core(src []byte) (count int)

//go:noescape
func countBytesBigEndianAVX2Core(src []byte) (count int)

//go:noescape
func unpack1AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack2AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack3AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack4AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack5AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack6AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack7AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack8AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack10AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack12AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack15AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack20AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack30AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack60AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack120AVX2(v uint64, dst *[240]uint64)

//go:noescape
func unpack240AVX2(v uint64, dst *[240]uint64)

/**************************** AVX2 Call **************************/

//go:noescape
func initAVX2Call()

//go:noescape
func decodeAllAVX2Call(dst []uint64, src []byte) (value int)

//go:noescape
func decodeBytesBigEndianAVX2Core(dst []uint64, src []byte) (value int)

//go:noescape
func unpack1AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack2AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack3AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack4AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack5AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack6AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack7AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack8AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack10AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack12AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack15AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack20AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack30AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack60AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack120AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack240AVX2Call(v uint64, dst *[240]uint64)

/* ************************* AVX2 JMP ****************************/

//go:noescape
func initAVX2Jmp()

//go:noescape
func decodeAllAVX2Jmp(dst, src []uint64) (value int)

//go:noescape
func decodeAllAVX2JmpLoop()

//go:noescape
func decodeAllAVX2JmpRet()

//go:noescape
func decodeAllAVX2JmpExit()

//go:noescape
func unpack1AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack2AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack3AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack4AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack5AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack6AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack7AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack8AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack10AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack12AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack15AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack20AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack30AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack60AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack120AVX2Jmp(v uint64, dst *[240]uint64)

//go:noescape
func unpack240AVX2Jmp(v uint64, dst *[240]uint64)

/************************ AVX2 Opt ******************************/

//go:noescape
func decodeAllAVX2Opt(dst []uint64, src []byte) (value int)

//go:noescape
func initAVX2Opt()

//go:noescape
func decodeAllAVX2OptExit()

//go:noescape
func unpack1AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack2AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack3AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack4AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack5AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack6AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack7AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack8AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack10AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack12AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack15AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack20AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack30AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack60AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack120AVX2Opt(v uint64, dst *[240]uint64)

//go:noescape
func unpack240AVX2Opt(v uint64, dst *[240]uint64)

/************************ AVX2 32bit ***************************/

//go:noescape
func init32bitAVX2Call()

//go:noescape
func decodeAllUint32AVX2(dst []uint32, src []byte) (value int)

//go:noescape
func unpack32bit1AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack32bit2AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack32bit3AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack32bit4AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack32bit5AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack32bit6AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack32bit7AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack32bit8AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack32bit10AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack32bit12AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack32bit15AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack32bit20AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack32bit30AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack32bit60AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack32bit120AVX2Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack32bit240AVX2Call(v uint64, dst *[240]uint64)

/**************************** AVX512 Call **************************/

//go:noescape
func initAVX512Call()

//go:noescape
func countBytesAVX512Core(src []byte) (count int)

//go:noescape
func decodeAllAVX512Call(dst, src []uint64) (value int)

// //go:noescape
// func decodeBytesBigEndianAVX512Core(dst []uint64, src []byte) (value int)

//go:noescape
func unpack1AVX512Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack2AVX512Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack3AVX512Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack4AVX512Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack5AVX512Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack6AVX512Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack7AVX512Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack8AVX512Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack10AVX512Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack12AVX512Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack15AVX512Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack20AVX512Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack30AVX512Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack60AVX512Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack120AVX512Call(v uint64, dst *[240]uint64)

//go:noescape
func unpack240AVX512Call(v uint64, dst *[240]uint64)

func init() {
	if util.UseAVX512_F {
		initAVX512Call()
	}
	if util.UseAVX2 {
		initAVX2Jmp()
		initAVX2Opt()
		initAVX2Call()
		init32bitAVX2Call()
	}
}

func decodeBytesBigEndian(dst []uint64, src []byte) (value int, err error) {
	switch {
	case util.UseAVX2:
		return decodeBytesBigEndianAVX2(dst, src)
	default:
		return decodeBytesBigEndianGeneric(dst, src)
	}
}

func decodeAllUint64(dst []uint64, src []byte) (value int, err error) {
	switch {
	case util.UseAVX2:
		return decodeAllAVX2Opt(dst, src), nil
	default:
		return decodeAllUint64Generic(dst, src)
	}
}

func decodeAllUint32(dst []uint32, src []byte) (value int, err error) {
	switch {
	//case util.UseAVX2:
	//	return decodeAllUint32AVX2(dst, src), nil
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

func countBytes(b []byte) (int, error) {
	switch {
	case util.UseAVX2:
		return countBytesAVX2(b)
	default:
		return countBytesGeneric(b)
	}
}

func countBytesBigEndian(b []byte) (int, error) {
	switch {
	case util.UseAVX2:
		return countBytesBigEndianAVX2(b)
	default:
		return countBytesBigEndianGeneric(b)
	}
}

var selectorAVX2 [16]packing = [16]packing{
	{240, 0, unpack240AVX2, pack240},
	{120, 0, unpack120AVX2, pack120},
	{60, 1, unpack60AVX2, pack60},
	{30, 2, unpack30AVX2, pack30},
	{20, 3, unpack20AVX2, pack20},
	{15, 4, unpack15AVX2, pack15},
	{12, 5, unpack12AVX2, pack12},
	{10, 6, unpack10AVX2, pack10},
	{8, 7, unpack8AVX2, pack8},
	{7, 8, unpack7AVX2, pack7},
	{6, 10, unpack6AVX2, pack6},
	{5, 12, unpack5AVX2, pack5},
	{4, 15, unpack4AVX2, pack4},
	{3, 20, unpack3AVX2, pack3},
	{2, 30, unpack2AVX2, pack2},
	{1, 60, unpack1AVX2, pack1},
}

// Decode writes the uncompressed values from src to dst.  It returns the number
// of values written or an error.
//go:nocheckptr
// nocheckptr while the underlying struct layout doesn't change
func decodeAllAVX2(dst, src []uint64) (value int, err error) {
	j := 0
	for _, v := range src {
		sel := (v >> 60) & 0xf
		selectorAVX2[sel].unpack(v, (*[240]uint64)(unsafe.Pointer(&dst[j])))
		j += selector[sel].n
	}
	return j, nil
}

func decodeBytesBigEndianAVX2(dst []uint64, src []byte) (value int, err error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}
	return decodeBytesBigEndianAVX2Core(dst, src), nil
}

func countBytesAVX2(src []byte) (int, error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}
	return countBytesAVX2Core(src), nil
}

func countBytesBigEndianAVX2(src []byte) (int, error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}
	return countBytesBigEndianAVX2Core(src), nil
}

/*
func countBytesAVX512(src []byte) (int, error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}
	return countBytesAVX512Core(src), nil
}
*/
