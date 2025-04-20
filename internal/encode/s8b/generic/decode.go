// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"encoding/binary"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

//go:nocheckptr
func Decode[T types.Unsigned](dst []T, buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if len(buf)&7 != 0 {
		return 0, ErrInvalidBufferLength
	}

	// pick selector based on input bit width
	unpack := unpackSelector[T]()

	var i, j int
	src := util.FromByteSlice[uint64](buf)
	for range len(src) / 8 {
		v0 := src[i]
		s0 := (v0 >> 60) & 0xf
		n0 := maxNPerSelector[s0]
		unpack[s0](v0, unsafe.Pointer(&dst[j]))

		v1 := src[i+1]
		s1 := (v1 >> 60) & 0xf
		n1 := maxNPerSelector[s1]
		unpack[s1](v1, unsafe.Pointer(&dst[j+n0]))

		v2 := src[i+2]
		s2 := (v2 >> 60) & 0xf
		n2 := maxNPerSelector[s2]
		unpack[s2](v2, unsafe.Pointer(&dst[j+n0+n1]))

		v3 := src[i+3]
		s3 := (v3 >> 60) & 0xf
		n3 := maxNPerSelector[s3]
		unpack[s3](v3, unsafe.Pointer(&dst[j+n0+n1+n2]))
		j += n0 + n1 + n2 + n3

		v4 := src[i+4]
		s4 := (v4 >> 60) & 0xf
		n4 := maxNPerSelector[s4]
		unpack[s4](v4, unsafe.Pointer(&dst[j]))

		v5 := src[i+5]
		s5 := (v5 >> 60) & 0xf
		n5 := maxNPerSelector[s5]
		unpack[s5](v5, unsafe.Pointer(&dst[j+n4]))

		v6 := src[i+6]
		s6 := (v6 >> 60) & 0xf
		n6 := maxNPerSelector[s6]
		unpack[s6](v6, unsafe.Pointer(&dst[j+n4+n5]))

		v7 := src[i+7]
		s7 := (v7 >> 60) & 0xf
		n7 := maxNPerSelector[s7]
		unpack[s7](v7, unsafe.Pointer(&dst[j+n4+n5+n6]))
		j += n4 + n5 + n6 + n7
		i += 8
	}

	for i < len(src) {
		v := src[i]
		sel := (v >> 60) & 0xf
		unpack[sel](v, unsafe.Pointer(&dst[j]))
		j += maxNPerSelector[sel]
		i++
	}
	return j, nil
}

func CountValues(src []byte) int {
	var (
		i int = 7 // little endian encoding
		n int
	)

	for range len(src) / 64 {
		n += maxNPerSelector[src[i]>>4]
		n += maxNPerSelector[src[i+8]>>4]
		n += maxNPerSelector[src[i+16]>>4]
		n += maxNPerSelector[src[i+24]>>4]
		n += maxNPerSelector[src[i+32]>>4]
		n += maxNPerSelector[src[i+40]>>4]
		n += maxNPerSelector[src[i+48]>>4]
		n += maxNPerSelector[src[i+56]>>4]
		i += 64
	}

	for i < len(src) {
		n += maxNPerSelector[src[i]>>4]
		i += 8
	}

	return n
}

func DecodeWord[T types.Unsigned](dst []T, buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if len(buf) != 8 {
		return 0, ErrInvalidBufferLength
	}

	// pick selector based on input bit width
	selector := unpackSelector[T]()

	v := binary.LittleEndian.Uint64(buf)
	sel := (v >> 60)
	selector[sel](v, unsafe.Pointer(&dst[0]))
	return maxNPerSelector[sel], nil
}
