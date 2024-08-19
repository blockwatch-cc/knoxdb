// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package avx2

import (
	"blockwatch.cc/knoxdb/internal/zip/generic"
)

// generic imports
var (
	ZigZagDecodeUint64 = generic.ZigZagDecodeUint64
	ZigZagDecodeUint32 = generic.ZigZagDecodeUint32
	ZigZagDecodeUint16 = generic.ZigZagDecodeUint16
	ZigZagDecodeUint8  = generic.ZigZagDecodeUint8
)

// ASM imports

//go:noescape
func zzDeltaDecodeInt64AVX2Core(src []int64)

//go:noescape
func zzDeltaDecodeInt32AVX2Core(src []int32)

//go:noescape
func zzDeltaDecodeInt16AVX2Core(src []int16)

//go:noescape
func zzDeltaDecodeInt8AVX2Core(src []int8)

//go:noescape
func zzDeltaDecodeUint64AVX2Core(src []uint64)

//go:noescape
func deltaDecodeTimeAVX2Core(src []uint64, mod uint64)

//go:noescape
func zzDeltaDecodeTimeAVX2Core(src []uint64, mod uint64)

// Go package exports
var (
	// not implemented yet
	// ZzDeltaEncodeUint64 = zzDeltaEncodeUint64AVX2
	// ZzDeltaEncodeUint32 = zzDeltaEncodeUint32AVX2
	// ZzDeltaEncodeUint16 = zzDeltaEncodeUint16AVX2
	// ZzDeltaEncodeUint8  = zzDeltaEncodeUint8AVX2

	ZzDeltaDecodeInt64  = zzDeltaDecodeInt64AVX2
	ZzDeltaDecodeInt32  = zzDeltaDecodeInt32AVX2
	ZzDeltaDecodeInt16  = zzDeltaDecodeInt16AVX2
	ZzDeltaDecodeInt8   = zzDeltaDecodeInt8AVX2
	ZzDeltaDecodeUint64 = zzDeltaDecodeUint64AVX2
	ZzDeltaDecodeTime   = zzDeltaDecodeTimeAVX2
	DeltaDecodeTime     = deltaDecodeTimeAVX2
)

func zzDeltaDecodeInt64AVX2(data []int64) {
	if len(data) == 0 {
		return
	}
	len_head := len(data) & 0x7ffffffffffffffc
	zzDeltaDecodeInt64AVX2Core(data)
	var prev int64
	if len_head != 0 {
		prev = data[len_head-1]
	}
	for i := len_head; i < len(data); i++ {
		prev += ZigZagDecodeUint64(uint64(data[i]))
		data[i] = prev
	}
}

func zzDeltaDecodeInt32AVX2(data []int32) {
	if len(data) == 0 {
		return
	}
	len_head := len(data) & 0x7ffffffffffffff8
	zzDeltaDecodeInt32AVX2Core(data)
	var prev int32
	if len_head != 0 {
		prev = data[len_head-1]
	}
	for i := len_head; i < len(data); i++ {
		prev += ZigZagDecodeUint32(uint32(data[i]))
		data[i] = prev
	}
}

func zzDeltaDecodeInt16AVX2(data []int16) {
	if len(data) == 0 {
		return
	}
	len_head := len(data) & 0x7ffffffffffffffc
	zzDeltaDecodeInt16AVX2Core(data)
	var prev int16
	if len_head != 0 {
		prev = data[len_head-1]
	}
	for i := len_head; i < len(data); i++ {
		prev += ZigZagDecodeUint16(uint16(data[i]))
		data[i] = prev
	}
}

func zzDeltaDecodeInt8AVX2(data []int8) {
	if len(data) == 0 {
		return
	}
	len_head := len(data) & 0x7ffffffffffffff8
	zzDeltaDecodeInt8AVX2Core(data)
	var prev int8
	if len_head != 0 {
		prev = data[len_head-1]
	}
	for i := len_head; i < len(data); i++ {
		prev += ZigZagDecodeUint8(uint8(data[i]))
		data[i] = prev
	}
}

func zzDeltaDecodeUint64AVX2(data []uint64) {
	len_head := len(data) & 0x7ffffffffffffffc
	zzDeltaDecodeUint64AVX2Core(data)
	var prev uint64
	if len_head != 0 {
		prev = data[len_head-1]
	}
	for i := len_head; i < len(data); i++ {
		prev += uint64(ZigZagDecodeUint64(data[i]))
		data[i] = prev
	}
}

func deltaDecodeTimeAVX2(data []uint64, mod uint64) {
	len_head := len(data) & 0x7ffffffffffffffc
	deltaDecodeTimeAVX2Core(data, mod)
	var prev uint64
	if len_head != 0 {
		prev = data[len_head-1]
	}
	for i := len_head; i < len(data); i++ {
		prev += data[i] * mod
		data[i] = prev
	}
}

func zzDeltaDecodeTimeAVX2(data []uint64, mod uint64) {
	len_head := len(data) & 0x7ffffffffffffffc
	zzDeltaDecodeTimeAVX2Core(data, mod)
	var prev uint64
	if len_head != 0 {
		prev = data[len_head-1]
	}
	for i := len_head; i < len(data); i++ {
		prev += uint64(ZigZagDecodeUint64(data[i]))
		data[i] = prev * mod
	}
}
