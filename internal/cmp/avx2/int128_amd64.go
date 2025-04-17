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
func cmp_i128_eq_x2(src num.Int128Stride, val num.Int128, bits []byte) int64

//go:noescape
func cmp_i128_ne_x2(src num.Int128Stride, val num.Int128, bits []byte) int64

//go:noescape
func cmp_i128_lt_x2(src num.Int128Stride, val num.Int128, bits []byte) int64

//go:noescape
func cmp_i128_le_x2(src num.Int128Stride, val num.Int128, bits []byte) int64

//go:noescape
func cmp_i128_gt_x2(src num.Int128Stride, val num.Int128, bits []byte) int64

//go:noescape
func cmp_i128_ge_x2(src num.Int128Stride, val num.Int128, bits []byte) int64

//go:noescape
func cmp_i128_bw_x2(src num.Int128Stride, a, b num.Int128, bits []byte) int64

// Go drivers/exports
func Int128Equal(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	res := cmp_i128_eq_x2(src, val, bits)
	len_head := src.Len() & 0x7fffffffffffffe0
	for i := len_head; i < src.Len(); i++ {
		if src.Elem(i).Eq(val) {
			bits[i>>3] |= 1 << (i & 0x7)
			res++
		}
	}
	return res
}

func Int128NotEqual(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	res := cmp_i128_ne_x2(src, val, bits)
	len_head := src.Len() & 0x7fffffffffffffe0
	for i := len_head; i < src.Len(); i++ {
		if !src.Elem(i).Eq(val) {
			bits[i>>3] |= 1 << (i & 0x7)
			res++
		}
	}
	return res
}

func Int128Less(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	res := cmp_i128_lt_x2(src, val, bits)
	len_head := src.Len() & 0x7fffffffffffffe0
	for i := len_head; i < src.Len(); i++ {
		if src.Elem(i).Lt(val) {
			bits[i>>3] |= 1 << (i & 0x7)
			res++
		}
	}
	return res
}

func Int128LessEqual(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	res := cmp_i128_le_x2(src, val, bits)
	len_head := src.Len() & 0x7fffffffffffffe0
	for i := len_head; i < src.Len(); i++ {
		if src.Elem(i).Le(val) {
			bits[i>>3] |= 1 << (i & 0x7)
			res++
		}
	}
	return res
}

func Int128Greater(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	res := cmp_i128_gt_x2(src, val, bits)
	len_head := src.Len() & 0x7fffffffffffffe0
	for i := len_head; i < src.Len(); i++ {
		if src.Elem(i).Gt(val) {
			bits[i>>3] |= 1 << (i & 0x7)
			res++
		}
	}
	return res
}

func Int128GreaterEqual(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	res := cmp_i128_ge_x2(src, val, bits)
	len_head := src.Len() & 0x7fffffffffffffe0
	for i := len_head; i < src.Len(); i++ {
		if src.Elem(i).Ge(val) {
			bits[i>>3] |= 1 << (i & 0x7)
			res++
		}
	}
	return res
}

func Int128Between(src num.Int128Stride, a, b num.Int128, bits, mask []byte) int64 {
	res := cmp_i128_bw_x2(src, a, b, bits)
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
