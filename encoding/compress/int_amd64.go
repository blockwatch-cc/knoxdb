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

func zzDeltaDecodeInt64(data []int64) {
	switch {
	case util.UseAVX2:
		zzDeltaDecodeInt64AVX2(data)
	default:
		zzDeltaDecodeInt64Generic(data)
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
	len_head := len(data) & 0x7ffffffffffffffc
	zzdeltaDecodeInt64AVX2Core(data)
	var prev int64
	if len_head == 0 {
		prev = ZigZagDecode(uint64(data[0]))
	} else {
		prev = data[len_head-1]
	}
	for i := len_head; i < len(data); i++ {
		prev += ZigZagDecode(uint64(data[i]))
		data[i] = prev
	}
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

func delta8EncodeUint64AVX2(data []uint64) uint64 {
	maxdelta := delta8EncodeUint64AVX2Core(data)
	for i := len(data)%8 + 7; i > 7; i-- {
		data[i] = data[i] - data[i-8]
		maxdelta |= data[i]
	}
	return maxdelta
}

//go:noescape
func packBytes32BitAVX2Core(src []uint64, dst []byte)

//go:noescape
func unpackBytes32BitAVX2Core(src []byte, dst []uint64)

//go:noescape
func packBytes16BitAVX2Core(src []uint64, dst []byte)

//go:noescape
func unpackBytes16BitAVX2Core(src []byte, dst []uint64)

func packBytes8Bit(src []uint64, buf []byte) {
	switch {
	//case util.UseAVX2:
	//	packBytes8BitAVX2(src, buf)
	default:
		packBytes8BitGeneric(src, buf)
	}
}

func packBytes16Bit(src []uint64, buf []byte) {
	switch {
	case util.UseAVX2:
		packBytes16BitAVX2(src, buf)
	default:
		packBytes16BitGeneric(src, buf)
	}
}

func packBytes24Bit(src []uint64, buf []byte) {
	switch {
	//case util.UseAVX2:
	//	packBytes8BitAVX2(src, buf)
	default:
		packBytes24BitGeneric(src, buf)
	}
}

func packBytes32Bit(src []uint64, buf []byte) {
	switch {
	case util.UseAVX2:
		packBytes32BitAVX2(src, buf)
	default:
		packBytes32BitGeneric(src, buf)
	}
}

func unpackBytes8Bit(src []byte, dst []uint64) {
	switch {
	//case util.UseAVX2:
	//	unpackBytes8BitAVX2(src, dst)
	default:
		unpackBytes8BitGeneric(src, dst)
	}
}

func unpackBytes16Bit(src []byte, dst []uint64) {
	switch {
	case util.UseAVX2:
		unpackBytes16BitAVX2(src, dst)
	default:
		unpackBytes16BitGeneric(src, dst)
	}
}

func unpackBytes24Bit(src []byte, dst []uint64) {
	switch {
	// case util.UseAVX2:
	//	unpackBytes24BitAVX2(src, dst)
	default:
		unpackBytes24BitGeneric(src, dst)
	}
}

func unpackBytes32Bit(src []byte, dst []uint64) {
	switch {
	case util.UseAVX2:
		unpackBytes32BitAVX2(src, dst)
	default:
		unpackBytes32BitGeneric(src, dst)
	}
}

func packBytes16BitAVX2(src []uint64, buf []byte) {
	len_head := len(src) & 0x7ffffffffffffff0
	packBytes16BitAVX2Core(src, buf)

	tmp := buf[len_head*2:]
	for i, v := range src[len_head:] {
		tmp[2*i] = byte((v >> 8) & 0xff)
		tmp[1+2*i] = byte(v & 0xff)
	}
}

func packBytes32BitAVX2(src []uint64, buf []byte) {
	len_head := len(src) & 0x7ffffffffffffff8
	packBytes32BitAVX2Core(src, buf)

	tmp := buf[len_head*4:]
	for i, v := range src[len_head:] {
		tmp[4*i] = byte((v >> 24) & 0xff)
		tmp[1+4*i] = byte((v >> 16) & 0xff)
		tmp[2+4*i] = byte((v >> 8) & 0xff)
		tmp[3+4*i] = byte(v & 0xff)
	}
}

func unpackBytes16BitAVX2(src []byte, res []uint64) {
	rlen := len(src) / 2
	len_head := rlen & 0x7ffffffffffffff0
	unpackBytes16BitAVX2Core(src, res)

	tmp := src[len_head*2:]

	for i, j := len_head, 0; i < rlen; i++ {
		res[i] = uint64(tmp[j])<<8 | uint64(tmp[1+j])
		j += 2
	}
}

func unpackBytes32BitAVX2(src []byte, res []uint64) {
	rlen := len(src) / 4
	len_head := rlen & 0x7ffffffffffffff8
	unpackBytes32BitAVX2Core(src, res)

	tmp := src[len_head*4:]

	for i, j := len_head, 0; i < rlen; i++ {
		res[i] = uint64(tmp[j])<<24 | uint64(tmp[1+j])<<16 | uint64(tmp[2+j])<<8 | uint64(tmp[3+j])
		j += 4
	}
}
