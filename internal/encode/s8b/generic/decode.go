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
func Decode[T types.Integer](dst []T, buf []byte, minv T) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if len(buf)&7 != 0 {
		return 0, ErrInvalidBufferLength
	}

	// pick selector based on input bit width
	unpack := unpackSelector[T](minv)

	var i, j int
	src := util.FromByteSlice[uint64](buf)
	for range len(src) / 8 {
		v0 := src[i]
		s0 := (v0 >> 60) & 0xf
		n0 := maxNPerSelector[s0]
		unpack[s0](v0, unsafe.Pointer(&dst[j]), uint64(minv))

		v1 := src[i+1]
		s1 := (v1 >> 60) & 0xf
		n1 := maxNPerSelector[s1]
		unpack[s1](v1, unsafe.Pointer(&dst[j+n0]), uint64(minv))

		v2 := src[i+2]
		s2 := (v2 >> 60) & 0xf
		n2 := maxNPerSelector[s2]
		unpack[s2](v2, unsafe.Pointer(&dst[j+n0+n1]), uint64(minv))

		v3 := src[i+3]
		s3 := (v3 >> 60) & 0xf
		n3 := maxNPerSelector[s3]
		unpack[s3](v3, unsafe.Pointer(&dst[j+n0+n1+n2]), uint64(minv))
		j += n0 + n1 + n2 + n3

		v4 := src[i+4]
		s4 := (v4 >> 60) & 0xf
		n4 := maxNPerSelector[s4]
		unpack[s4](v4, unsafe.Pointer(&dst[j]), uint64(minv))

		v5 := src[i+5]
		s5 := (v5 >> 60) & 0xf
		n5 := maxNPerSelector[s5]
		unpack[s5](v5, unsafe.Pointer(&dst[j+n4]), uint64(minv))

		v6 := src[i+6]
		s6 := (v6 >> 60) & 0xf
		n6 := maxNPerSelector[s6]
		unpack[s6](v6, unsafe.Pointer(&dst[j+n4+n5]), uint64(minv))

		v7 := src[i+7]
		s7 := (v7 >> 60) & 0xf
		n7 := maxNPerSelector[s7]
		unpack[s7](v7, unsafe.Pointer(&dst[j+n4+n5+n6]), uint64(minv))
		j += n4 + n5 + n6 + n7
		i += 8
	}

	for i < len(src) {
		v := src[i]
		sel := (v >> 60) & 0xf
		unpack[sel](v, unsafe.Pointer(&dst[j]), uint64(minv))
		j += maxNPerSelector[sel]
		i++
	}
	return j, nil
}

func CountValues(src []byte) int {
	var (
		i = 7 // little endian encoding
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

// Seek returns code word position in src and value offset inside code word.
func Seek(src []byte, v int) (int, int) {
	var (
		i = 7 // little endian encoding
		n int
	)

	if v == 0 {
		return 0, 0
	}

	// skip large portions when seeking further ahead
	if v > 256 {
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
			if n >= v {
				break
			}
		}
	}

	switch {
	case v < n:
		// walk back in case large ranges overcounted
		i -= 8
		for i >= -1 {
			n -= maxNPerSelector[src[i]>>4]
			if n <= v {
				return i - 7, v - n
			}
			i -= 8
		}
	case n > 0 && v == n:
		// exact match (seldom)
		return i - 7, 0
	default:
		// walk forward (regular and tail case)
		for i < len(src) {
			s := maxNPerSelector[src[i]>>4]
			if n+s > v {
				return i - 7, v - n
			}
			n += s
			i += 8
		}
	}

	// not found
	return -1, -1
}

type Decoder[T types.Integer] struct {
	unpack *[16]unpackFunc
	minv   T
}

func NewDecoder[T types.Integer](minv T) *Decoder[T] {
	return &Decoder[T]{
		minv:   minv,
		unpack: unpackSelector[T](minv),
	}
}

//go:nocheckptr
func (d *Decoder[T]) DecodeWordPtr(dst unsafe.Pointer, l int, buf []byte) int {
	v := binary.LittleEndian.Uint64(buf)
	sel := v >> 60
	n := maxNPerSelector[sel]
	if l < n {
		// TODO: partial word decode is unsupported and slow
		// tmp := make([]T, n)
		// d.unpack[sel](v, unsafe.Pointer(&tmp[0]), uint64(d.minv))
		// copy(unsafe.Slice((*T)(dst), l), tmp)
		// return l
		return 0
	}
	d.unpack[sel](v, dst, uint64(d.minv))
	return n
}

//go:nocheckptr
func (d *Decoder[T]) DecodeWord(dst []T, buf []byte) int {
	v := binary.LittleEndian.Uint64(buf)
	sel := v >> 60
	n := maxNPerSelector[sel]
	if len(dst) < n {
		return 0
	}
	d.unpack[sel](v, unsafe.Pointer(&dst[0]), uint64(d.minv))
	return n
}
