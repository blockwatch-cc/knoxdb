// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package compress

import "blockwatch.cc/knoxdb/util"

//go:noescape
func zzDecodeInt64AVX2Core(data []int64)

//go:noescape
func deltaDecodeInt64AVX2Core(data []int64)

//go:noescape
func zzdeltaDecodeInt64AVX2Core(data []int64)

//go:noescape
func zzdeltaDecodeUint64AVX2Core(data []uint64)

//go:noescape
func zzDecodeUint64AVX2Core(data []uint64)

//go:noescape
func delta8DecodeUint64AVX2Core(data []uint64)

//go:noescape
func delta8EncodeUint64AVX2Core(data []uint64) uint64

func zzDeltaEncodeUint8(data []uint8) uint8 {
	switch {
	//case util.UseAVX2:
	//	zzDeltaDecodeInt16AVX2(data)
	default:
		return zzDeltaEncodeUint8Generic(data)
	}
}

func zzDeltaEncodeUint16(data []uint16) uint16 {
	switch {
	//case util.UseAVX2:
	//	zzDeltaDecodeInt16AVX2(data)
	default:
		return zzDeltaEncodeUint16Generic(data)
	}
}

func zzDeltaEncodeUint32(data []uint32) uint32 {
	switch {
	//case util.UseAVX2:
	//	zzDeltaDecodeInt16AVX2(data)
	default:
		return zzDeltaEncodeUint32Generic(data)
	}
}

func zzDeltaEncodeUint64(data []uint64) uint64 {
	switch {
	//case util.UseAVX2:
	//	zzDeltaDecodeInt16AVX2(data)
	default:
		return zzDeltaEncodeUint64Generic(data)
	}
}

func zzDeltaDecodeInt64(data []int64) {
	switch {
	case util.UseAVX2:
		zzDeltaDecodeInt64AVX2(data)
	default:
		zzDeltaDecodeInt64Generic(data)
	}
}

func zzDeltaDecodeInt32(data []int32) {
	switch {
	//case util.UseAVX2:
	//	zzDeltaDecodeInt32AVX2(data)
	default:
		zzDeltaDecodeInt32Generic(data)
	}
}

func zzDeltaDecodeInt16(data []int16) {
	switch {
	//case util.UseAVX2:
	//	zzDeltaDecodeInt16AVX2(data)
	default:
		zzDeltaDecodeInt16Generic(data)
	}
}

func zzDeltaDecodeInt8(data []int8) {
	switch {
	//case util.UseAVX2:
	//	zzDeltaDecodeInt8AVX2(data)
	default:
		zzDeltaDecodeInt8Generic(data)
	}
}

func zzDeltaDecodeUint64(data []uint64) {
	switch {
	case util.UseAVX2:
		zzDeltaDecodeUint64AVX2(data)
	default:
		zzDeltaDecodeUint64Generic(data)
	}
}

func zzDecodeInt64(data []int64) {
	switch {
	case util.UseAVX2:
		zzDecodeInt64AVX2(data)
	default:
		zzDecodeInt64Generic(data)
	}
}

func zzDecodeUint64(data []uint64) {
	switch {
	case util.UseAVX2:
		zzDecodeUint64AVX2(data)
	default:
		zzDecodeUint64Generic(data)
	}
}

func delta8DecodeUint64(data []uint64) {
	switch {
	case util.UseAVX2:
		delta8DecodeUint64AVX2(data)
	default:
		delta8DecodeUint64Generic(data)
	}
}

func delta8EncodeUint64(data []uint64) uint64 {
	switch {
	case util.UseAVX2:
		return delta8EncodeUint64AVX2(data)
	default:
		return delta8EncodeUint64Generic(data)
	}
}

func zzDeltaDecodeInt64AVX2(data []int64) {
	if len(data) == 0 {
		return
	}
	len_head := len(data) & 0x7ffffffffffffffc
	zzdeltaDecodeInt64AVX2Core(data)
	var prev int64
	if len_head == 0 {
		prev = 0
	} else {
		prev = data[len_head-1]
	}
	for i := len_head; i < len(data); i++ {
		prev += ZigZagDecode(uint64(data[i]))
		data[i] = prev
	}
}

func zzDeltaDecodeInt64AVX2X(data []int64) {
	zzDecodeInt64AVX2(data)
	deltaDecodeInt64AVX2(data)
}

func zzDeltaDecodeUint64AVX2(data []uint64) {
	len_head := len(data) & 0x7ffffffffffffffc
	zzdeltaDecodeUint64AVX2Core(data)
	var prev uint64
	if len_head == 0 {
		prev = uint64(ZigZagDecode(data[0]))
	} else {
		prev = data[len_head-1]
	}
	for i := len_head; i < len(data); i++ {
		prev += uint64(ZigZagDecode(data[i]))
		data[i] = prev
	}
}

func zzDecodeUint64AVX2(data []uint64) {
	len_head := len(data) & 0x7ffffffffffffffc
	zzDecodeUint64AVX2Core(data)
	for i := len_head; i < len(data); i++ {
		data[i] = uint64(ZigZagDecode(data[i]))
	}
}

func zzDecodeInt64AVX2(data []int64) {
	len_head := len(data) & 0x7ffffffffffffffc
	zzDecodeInt64AVX2Core(data)
	for i := len_head; i < len(data); i++ {
		data[i] = ZigZagDecode(uint64(data[i]))
	}
}

func delta8DecodeUint64AVX2(data []uint64) {
	len_head := len(data) & 0x7ffffffffffffff8
	delta8DecodeUint64AVX2Core(data)
	for i := len_head; i < len(data); i++ {
		data[i] += data[i-8]
	}
}

func deltaDecodeInt64AVX2(data []int64) {
	len_head := len(data) & 0x7ffffffffffffffc
	deltaDecodeInt64AVX2Core(data)
	var prev int64
	if len_head == 0 {
		prev = 0
	} else {
		prev = data[len_head-1]
	}
	for i := len_head; i < len(data); i++ {
		prev += data[i]
		data[i] = prev
	}
}

func delta8EncodeUint64AVX2(data []uint64) uint64 {
	maxdelta := delta8EncodeUint64AVX2Core(data)
	for i := len(data)%8 + 7; i > 7; i-- {
		data[i] = data[i] - data[i-8]
		maxdelta |= data[i]
	}
	return maxdelta
}
