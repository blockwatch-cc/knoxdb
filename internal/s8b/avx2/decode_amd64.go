// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package avx2

import (
	"encoding/binary"
	"errors"

	"blockwatch.cc/knoxdb/internal/s8b/generic"
	"blockwatch.cc/knoxdb/pkg/util"
)

func init() {
	if util.UseAVX2 {
		initUint64AVX2()
		initUint32AVX2()
		initUint16AVX2()
		initUint8AVX2()
	}
}

var (
	packing16  = [16]int{240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1}
	bufOvUint8 = [16]int{0, 0, 0, 2, 0, 1, 4, 6, 0, 1, 0, 0, 0, 0, 0, 0}
)

func DecodeUint8(dst []uint8, src []byte) (int, error) {
	pos := len(src)
	if pos&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}

	var max_pos int = pos
	var num_val int
	pos -= 8
	for pos >= 0 && num_val < 6 {
		v := binary.LittleEndian.Uint64(src[pos:])
		sel := (v >> 60) & 0xf
		bo := bufOvUint8[sel]
		if bo > num_val {
			max_pos = pos
		}
		num_val += packing16[sel]
		pos -= 8
	}
	n1 := decodeUint8AVX2Core(dst, src[:max_pos])
	n2, err := generic.DecodeUint8(dst[n1:], src[max_pos:])
	return n1 + n2, err
}

var bufOvUint16 = [16]int{0, 0, 4, 2, 0, 1, 4, 6, 0, 1, 2, 0, 0, 1, 0, 0}

func DecodeUint16(dst []uint16, src []byte) (int, error) {
	pos := len(src)
	if pos&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}

	var max_pos int = pos
	var num_val int
	pos -= 8
	for pos >= 0 && num_val < 6 {
		v := binary.LittleEndian.Uint64(src[pos:])
		sel := (v >> 60) & 0xf
		bo := bufOvUint16[sel]
		if bo > num_val {
			max_pos = pos
		}
		num_val += packing16[sel]
		pos -= 8
	}
	n1 := decodeUint16AVX2Core(dst, src[:max_pos])
	n2, err := generic.DecodeUint16(dst[n1:], src[max_pos:])
	return n1 + n2, err
}

var bufOvUint32 = [16]int{0, 0, 0, 2, 0, 1, 0, 0, 0, 1, 2, 3, 0, 1, 0, 0}

func DecodeUint32(dst []uint32, src []byte) (int, error) {
	pos := len(src)
	if pos&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}

	var max_pos int = pos
	var num_val int
	pos -= 8
	for pos >= 0 && num_val < 3 {
		v := binary.LittleEndian.Uint64(src[pos:])
		sel := (v >> 60) & 0xf
		bo := bufOvUint32[sel]
		if bo > num_val {
			max_pos = pos
		}
		num_val += packing16[sel]
		pos -= 8
	}
	n1 := decodeUint32AVX2Core(dst, src[:max_pos])
	n2, err := generic.DecodeUint32(dst[n1:], src[max_pos:])
	return n1 + n2, err
}

func DecodeUint64(dst []uint64, src []byte) (int, error) {
	return decodeUint64AVX2(dst, src), nil
}

func CountValues(src []byte) (int, error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}
	return countValuesAVX2Core(src), nil
}

// func CountValuesBigEndian(src []byte) (int, error) {
// 	if len(src)&7 != 0 {
// 		return 0, errors.New("src length is not multiple of 8")
// 	}
// 	return countValuesBigEndianAVX2Core(src), nil
// }
