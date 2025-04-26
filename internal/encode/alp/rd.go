// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package alp

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/pkg/util"
)

type EncoderRD[T Float, U Uint] struct{}

func NewEncoderRD[T Float, U Uint]() *EncoderRD[T, U] {
	return &EncoderRD[T, U]{}
}

type RDResult[U Uint] struct {
	Left  []uint16
	Right []U
}

func NewRDResult[U Uint](sz int) *RDResult[U] {
	return &RDResult[U]{
		Left:  arena.Alloc[uint16](sz)[:sz],
		Right: arena.Alloc[U](sz)[:sz],
	}
}

func (r *RDResult[U]) Close() {
	arena.Free(r.Left)
	arena.Free(r.Right)
	r.Left = nil
	r.Right = nil
}

func (enc *EncoderRD[T, U]) Encode(src []T, split int) *RDResult[U] {
	r := NewRDResult[U](len(src))
	enc.split(src, r.Left, r.Right, split)
	return r
}

func (enc *EncoderRD[T, U]) split(src []T, left []uint16, right []U, shift int) {
	switch util.SizeOf[T]() {
	case 4:
		s32 := util.ReinterpretSlice[T, uint32](src)
		r32 := util.ReinterpretSlice[U, uint32](right)
		split32(s32, left, r32, shift)
	case 8:
		s64 := util.ReinterpretSlice[T, uint64](src)
		r64 := util.ReinterpretSlice[U, uint64](right)
		split64(s64, left, r64, shift)
	}
}

func split64(src []uint64, left []uint16, right []uint64, shift int) {
	if len(src) == 0 {
		return
	}
	var i int
	sp := unsafe.Pointer(&src[0])
	lp := unsafe.Pointer(&left[0])
	rp := unsafe.Pointer(&right[0])
	for range len(src) / 128 {
		s := (*[128]uint64)(unsafe.Add(sp, i*8))
		l := (*[128]uint16)(unsafe.Add(lp, i*2))
		r := (*[128]uint64)(unsafe.Add(rp, i*8))
		splitCore64(s, l, r, shift)
		i += 128
	}

	mask := uint64(1<<shift - 1)
	for i < len(src) {
		val := src[i]
		left[i] = uint16(val >> shift)
		right[i] = val & mask
		i++
	}
}

func splitCore64(src *[128]uint64, left *[128]uint16, right *[128]uint64, shift int) {
	mask := uint64(1<<shift) - 1
	for i := 0; i < len(src); i += 16 {
		v0 := src[i]
		left[i], right[i] = uint16(v0>>shift), v0&mask
		v1 := src[i+1]
		left[i+1], right[i+1] = uint16(v1>>shift), v1&mask
		v2 := src[i+2]
		left[i+2], right[i+2] = uint16(v2>>shift), v2&mask
		v3 := src[i+3]
		left[i+3], right[i+3] = uint16(v3>>shift), v3&mask
		v4 := src[i+4]
		left[i+4], right[i+4] = uint16(v4>>shift), v4&mask
		v5 := src[i+5]
		left[i+5], right[i+5] = uint16(v5>>shift), v5&mask
		v6 := src[i+6]
		left[i+6], right[i+6] = uint16(v6>>shift), v6&mask
		v7 := src[i+7]
		left[i+7], right[i+7] = uint16(v7>>shift), v7&mask
		v8 := src[i+8]
		left[i+8], right[i+8] = uint16(v8>>shift), v8&mask
		v9 := src[i+9]
		left[i+9], right[i+9] = uint16(v9>>shift), v9&mask
		v10 := src[i+10]
		left[i+10], right[i+10] = uint16(v10>>shift), v10&mask
		v11 := src[i+11]
		left[i+11], right[i+11] = uint16(v11>>shift), v11&mask
		v12 := src[i+12]
		left[i+12], right[i+12] = uint16(v12>>shift), v12&mask
		v13 := src[i+13]
		left[i+13], right[i+13] = uint16(v13>>shift), v13&mask
		v14 := src[i+14]
		left[i+14], right[i+14] = uint16(v14>>shift), v14&mask
		v15 := src[i+15]
		left[i+15], right[i+15] = uint16(v15>>shift), v15&mask
	}
}

func split32(src []uint32, left []uint16, right []uint32, shift int) {
	if len(src) == 0 {
		return
	}
	var i int
	sp := unsafe.Pointer(&src[0])
	lp := unsafe.Pointer(&left[0])
	rp := unsafe.Pointer(&right[0])
	for range len(src) / 128 {
		s := (*[128]uint32)(unsafe.Add(sp, i*4))
		l := (*[128]uint16)(unsafe.Add(lp, i*2))
		r := (*[128]uint32)(unsafe.Add(rp, i*4))
		splitCore32(s, l, r, shift)
		i += 128
	}

	mask := uint32(1<<shift) - 1
	for i < len(src) {
		val := src[i]
		left[i] = uint16(val >> shift)
		right[i] = val & mask
		i++
	}
}

func splitCore32(src *[128]uint32, left *[128]uint16, right *[128]uint32, shift int) {
	mask := uint32(1<<shift) - 1
	for i := 0; i < len(src); i += 16 {
		v0 := src[i]
		left[i], right[i] = uint16(v0>>shift), v0&mask
		v1 := src[i+1]
		left[i+1], right[i+1] = uint16(v1>>shift), v1&mask
		v2 := src[i+2]
		left[i+2], right[i+2] = uint16(v2>>shift), v2&mask
		v3 := src[i+3]
		left[i+3], right[i+3] = uint16(v3>>shift), v3&mask
		v4 := src[i+4]
		left[i+4], right[i+4] = uint16(v4>>shift), v4&mask
		v5 := src[i+5]
		left[i+5], right[i+5] = uint16(v5>>shift), v5&mask
		v6 := src[i+6]
		left[i+6], right[i+6] = uint16(v6>>shift), v6&mask
		v7 := src[i+7]
		left[i+7], right[i+7] = uint16(v7>>shift), v7&mask
		v8 := src[i+8]
		left[i+8], right[i+8] = uint16(v8>>shift), v8&mask
		v9 := src[i+9]
		left[i+9], right[i+9] = uint16(v9>>shift), v9&mask
		v10 := src[i+10]
		left[i+10], right[i+10] = uint16(v10>>shift), v10&mask
		v11 := src[i+11]
		left[i+11], right[i+11] = uint16(v11>>shift), v11&mask
		v12 := src[i+12]
		left[i+12], right[i+12] = uint16(v12>>shift), v12&mask
		v13 := src[i+13]
		left[i+13], right[i+13] = uint16(v13>>shift), v13&mask
		v14 := src[i+14]
		left[i+14], right[i+14] = uint16(v14>>shift), v14&mask
		v15 := src[i+15]
		left[i+15], right[i+15] = uint16(v15>>shift), v15&mask
	}
}

type DecoderRD[T Float, U Uint] struct {
	split int
	width int
}

func NewDecoderRD[T Float, U Uint](split int) *DecoderRD[T, U] {
	return &DecoderRD[T, U]{
		split: split,
		width: util.SizeOf[T](),
	}
}

func (d *DecoderRD[T, U]) Close() {
	d.split = 0
	d.width = 0
}

func (d *DecoderRD[T, U]) Decode(dst []T, left []uint16, right []U) []T {
	return d.merge(dst, left, right, d.split)
}

func (d *DecoderRD[T, U]) DecodeValue(left uint16, right U) T {
	if d.width == 8 {
		v := uint64(left)<<d.split | uint64(right)
		return *(*T)(unsafe.Pointer(&v))
	}
	v := uint32(left)<<d.split | uint32(right)
	return *(*T)(unsafe.Pointer(&v))
}

func (d *DecoderRD[T, U]) merge(dst []T, left []uint16, right []U, shift int) []T {
	switch util.SizeOf[T]() {
	case 4:
		d32 := util.ReinterpretSlice[T, uint32](dst)
		r32 := util.ReinterpretSlice[U, uint32](right)
		merge32(d32, left, r32, shift)
	case 8:
		d64 := util.ReinterpretSlice[T, uint64](dst)
		r64 := util.ReinterpretSlice[U, uint64](right)
		merge64(d64, left, r64, shift)
	}
	return dst
}

func merge64(dst []uint64, left []uint16, right []uint64, shift int) {
	if len(dst) == 0 {
		return
	}
	var i int
	dp := unsafe.Pointer(&dst[0])
	lp := unsafe.Pointer(&left[0])
	rp := unsafe.Pointer(&right[0])
	for range len(dst) / 128 {
		d := (*[128]uint64)(unsafe.Add(dp, i*8))
		l := (*[128]uint16)(unsafe.Add(lp, i*2))
		r := (*[128]uint64)(unsafe.Add(rp, i*8))
		mergeCore64(d, l, r, shift)
		i += 128
	}
	for i < len(dst) {
		dst[i] = uint64(left[i])<<shift | right[i]
		i++
	}
}

func mergeCore64(dst *[128]uint64, left *[128]uint16, right *[128]uint64, shift int) {
	for i := 0; i < len(dst); i += 16 {
		dst[i] = uint64(left[i])<<shift | right[i]
		dst[i+1] = uint64(left[i+1])<<shift | right[i+1]
		dst[i+2] = uint64(left[i+2])<<shift | right[i+2]
		dst[i+3] = uint64(left[i+3])<<shift | right[i+3]
		dst[i+4] = uint64(left[i+4])<<shift | right[i+4]
		dst[i+5] = uint64(left[i+5])<<shift | right[i+5]
		dst[i+6] = uint64(left[i+6])<<shift | right[i+6]
		dst[i+7] = uint64(left[i+7])<<shift | right[i+7]
		dst[i+8] = uint64(left[i+8])<<shift | right[i+8]
		dst[i+9] = uint64(left[i+9])<<shift | right[i+9]
		dst[i+10] = uint64(left[i+10])<<shift | right[i+10]
		dst[i+11] = uint64(left[i+11])<<shift | right[i+11]
		dst[i+12] = uint64(left[i+12])<<shift | right[i+12]
		dst[i+13] = uint64(left[i+13])<<shift | right[i+13]
		dst[i+14] = uint64(left[i+14])<<shift | right[i+14]
		dst[i+15] = uint64(left[i+15])<<shift | right[i+15]
	}
}

func merge32(dst []uint32, left []uint16, right []uint32, shift int) {
	if len(dst) == 0 {
		return
	}
	var i int
	dp := unsafe.Pointer(&dst[0])
	lp := unsafe.Pointer(&left[0])
	rp := unsafe.Pointer(&right[0])
	for range len(dst) / 128 {
		d := (*[128]uint32)(unsafe.Add(dp, i*4))
		l := (*[128]uint16)(unsafe.Add(lp, i*2))
		r := (*[128]uint32)(unsafe.Add(rp, i*4))
		mergeCore32(d, l, r, shift)
		i += 128
	}

	for i < len(dst) {
		dst[i] = uint32(left[i])<<shift | right[i]
		i++
	}
}

func mergeCore32(dst *[128]uint32, left *[128]uint16, right *[128]uint32, shift int) {
	for i := 0; i < len(dst); i += 16 {
		dst[i] = uint32(left[i])<<shift | right[i]
		dst[i+1] = uint32(left[i+1])<<shift | right[i+1]
		dst[i+2] = uint32(left[i+2])<<shift | right[i+2]
		dst[i+3] = uint32(left[i+3])<<shift | right[i+3]
		dst[i+4] = uint32(left[i+4])<<shift | right[i+4]
		dst[i+5] = uint32(left[i+5])<<shift | right[i+5]
		dst[i+6] = uint32(left[i+6])<<shift | right[i+6]
		dst[i+7] = uint32(left[i+7])<<shift | right[i+7]
		dst[i+8] = uint32(left[i+8])<<shift | right[i+8]
		dst[i+9] = uint32(left[i+9])<<shift | right[i+9]
		dst[i+10] = uint32(left[i+10])<<shift | right[i+10]
		dst[i+11] = uint32(left[i+11])<<shift | right[i+11]
		dst[i+12] = uint32(left[i+12])<<shift | right[i+12]
		dst[i+13] = uint32(left[i+13])<<shift | right[i+13]
		dst[i+14] = uint32(left[i+14])<<shift | right[i+14]
		dst[i+15] = uint32(left[i+15])<<shift | right[i+15]
	}
}
