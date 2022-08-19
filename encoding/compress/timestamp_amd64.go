// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package compress

import "blockwatch.cc/knoxdb/util"

//go:noescape
func deltaDecodeTimeAVX2Core(data []uint64, mod uint64)

//go:noescape
func zzDeltaDecodeTimeAVX2Core(data []uint64, mod uint64)

func deltaDecodeTime(data []uint64, mod uint64) {
	switch {
	case util.UseAVX2:
		deltaDecodeTimeAVX2(data, mod)
	default:
		deltaDecodeTimeGeneric(data, mod)
	}
}

func zzDeltaDecodeTime(data []uint64, mod uint64) {
	switch {
	case util.UseAVX2:
		zzDeltaDecodeTimeAVX2(data, mod)
	default:
		zzDeltaDecodeTimeGeneric(data, mod)
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
		prev += uint64(ZigZagDecode(data[i]))
		data[i] = prev * mod
	}
}
