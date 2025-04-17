// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64
// +build amd64

package avx2

import (
	"blockwatch.cc/knoxdb/pkg/num"
)

// ASM imports

//go:noescape
func cmp_i256_eq_x2(src num.Int256Stride, val num.Int256, bits []byte) int64

//go:noescape
func cmp_i256_ne_x2(src num.Int256Stride, val num.Int256, bits []byte) int64

//go:noescape
func cmp_i256_lt_x2(src num.Int256Stride, val num.Int256, bits []byte) int64

//go:noescape
func cmp_i256_le_x2(src num.Int256Stride, val num.Int256, bits []byte) int64

//go:noescape
func cmp_i256_gt_x2(src num.Int256Stride, val num.Int256, bits []byte) int64

//go:noescape
func cmp_i256_ge_x2(src num.Int256Stride, val num.Int256, bits []byte) int64

//go:noescape
func cmp_i256_bw_x2(src num.Int256Stride, a, b num.Int256, bits []byte) int64

// Go drivers
func Int256Equal(src num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	res := cmp_i256_eq_x2(src, val, bits)
	len_head := src.Len() & 0x7fffffffffffffe0
	for i := len_head; i < src.Len(); i++ {
		if src.Elem(i).Eq(val) {
			bits[i>>3] |= 1 << (i & 0x7)
			res++
		}
	}
	return res
}

func Int256NotEqual(src num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	res := cmp_i256_ne_x2(src, val, bits)
	len_head := src.Len() & 0x7fffffffffffffe0
	for i := len_head; i < src.Len(); i++ {
		if !src.Elem(i).Eq(val) {
			bits[i>>3] |= 1 << (i & 0x7)
			res++
		}
	}
	return res
}

func Int256Less(src num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	res := cmp_i256_lt_x2(src, val, bits)
	len_head := src.Len() & 0x7fffffffffffffe0
	for i := len_head; i < src.Len(); i++ {
		if src.Elem(i).Lt(val) {
			bits[i>>3] |= 1 << (i & 0x7)
			res++
		}
	}
	return res
}

func Int256LessEqual(src num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	res := cmp_i256_le_x2(src, val, bits)
	len_head := src.Len() & 0x7fffffffffffffe0
	for i := len_head; i < src.Len(); i++ {
		if src.Elem(i).Le(val) {
			bits[i>>3] |= 1 << (i & 0x7)
			res++
		}
	}
	return res
}

func Int256Greater(src num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	res := cmp_i256_gt_x2(src, val, bits)
	len_head := src.Len() & 0x7fffffffffffffe0
	for i := len_head; i < src.Len(); i++ {
		if src.Elem(i).Gt(val) {
			bits[i>>3] |= 1 << (i & 0x7)
			res++
		}
	}
	return res
}

func Int256GreaterEqual(src num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	res := cmp_i256_ge_x2(src, val, bits)
	len_head := src.Len() & 0x7fffffffffffffe0
	for i := len_head; i < src.Len(); i++ {
		if src.Elem(i).Ge(val) {
			bits[i>>3] |= 1 << (i & 0x7)
			res++
		}
	}
	return res
}

func Int256Between(src num.Int256Stride, a, b num.Int256, bits, mask []byte) int64 {
	res := cmp_i256_bw_x2(src, a, b, bits)
	len_head := src.Len() & 0x7fffffffffffffe0
	for i := len_head; i < src.Len(); i++ {
		v := src.Elem(i)
		if a.Le(v) && b.Ge(v) {
			bits[i>>3] |= 1 << (i & 0x7)
			res++
		}
	}
	return res
}
