// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package s8bVec

import (
	"encoding/binary"
	"errors"

	"blockwatch.cc/knoxdb/util"
)

func init() {
	if util.UseAVX512_F {
		initUint64AVX512()
	}
	if util.UseAVX2 {
		initUint64AVX2()
		initUint32AVX2()
		initUint16AVX2()
		initUint8AVX2()
	}
}

func decodeBytesBigEndian(dst []uint64, src []byte) (value int, err error) {
	switch {
	//case util.UseAVX2:
	//	return decodeBytesBigEndianAVX2(dst, src)
	default:
		return decodeBytesBigEndianGeneric(dst, src)
	}
}

func decodeAllUint64(dst []uint64, src []byte) (value int, err error) {
	switch {
	case util.UseAVX2:
		return decodeAllUint64AVX2(dst, src), nil
	default:
		return decodeAllUint64Generic(dst, src)
	}
}

func decodeAllUint32(dst []uint32, src []byte) (value int, err error) {
	switch {
	case util.UseAVX2:
		return decodeAllUint32AVX2(dst, src)
	default:
		return decodeAllUint32Generic(dst, src)
	}
}

func decodeAllUint16(dst []uint16, src []byte) (value int, err error) {
	switch {
	case util.UseAVX2:
		return decodeAllUint16AVX2(dst, src)
	default:
		return decodeAllUint16Generic(dst, src)
	}
}

func decodeAllUint8(dst []uint8, src []byte) (value int, err error) {
	switch {
	case util.UseAVX2:
		return decodeAllUint8AVX2(dst, src)
	default:
		return decodeAllUint8Generic(dst, src)
	}
}

func countValues(b []byte) (int, error) {
	switch {
	case util.UseAVX2:
		return countValuesAVX2(b)
	default:
		return countValuesGeneric(b)
	}
}

func countValuesBigEndian(b []byte) (int, error) {
	switch {
	case util.UseAVX2:
		return countValuesBigEndianAVX2(b)
	default:
		return countValuesBigEndianGeneric(b)
	}
}

var BufOvUint8 = [16]int{0, 0, 0, 2, 0, 1, 4, 6, 0, 1, 0, 0, 0, 0, 0, 0}

func decodeAllUint8AVX2(dst []uint8, src []byte) (int, error) {
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
		bo := BufOvUint8[sel]
		if bo > num_val {
			max_pos = pos
		}
		num_val += selector16[sel].n
		pos -= 8
	}
	n1 := decodeAllUint8AVX2Core(dst, src[:max_pos])
	n2, err := decodeAllUint8Generic(dst[n1:], src[max_pos:])
	return n1 + n2, err
}

var BufOvUint16 = [16]int{0, 0, 4, 2, 0, 1, 4, 6, 0, 1, 2, 0, 0, 1, 0, 0}

func decodeAllUint16AVX2(dst []uint16, src []byte) (int, error) {
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
		bo := BufOvUint16[sel]
		if bo > num_val {
			max_pos = pos
		}
		num_val += selector16[sel].n
		pos -= 8
	}
	n1 := decodeAllUint16AVX2Core(dst, src[:max_pos])
	n2, err := decodeAllUint16Generic(dst[n1:], src[max_pos:])
	return n1 + n2, err
}

var BufOvUint32 = [16]int{0, 0, 0, 2, 0, 1, 0, 0, 0, 1, 2, 3, 0, 1, 0, 0}

func decodeAllUint32AVX2(dst []uint32, src []byte) (int, error) {
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
		bo := BufOvUint32[sel]
		if bo > num_val {
			max_pos = pos
		}
		num_val += selector16[sel].n
		pos -= 8
	}
	n1 := decodeAllUint32AVX2Core(dst, src[:max_pos])
	n2, err := decodeAllUint32Generic(dst[n1:], src[max_pos:])
	return n1 + n2, err
}

func countValuesAVX2(src []byte) (int, error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}
	return countValuesAVX2Core(src), nil
}

func countValuesBigEndianAVX2(src []byte) (int, error) {
	if len(src)&7 != 0 {
		return 0, errors.New("src length is not multiple of 8")
	}
	return countValuesBigEndianAVX2Core(src), nil
}
