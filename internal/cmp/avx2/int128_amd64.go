// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx2

import (
	"blockwatch.cc/knoxdb/internal/cmp/generic"
	"blockwatch.cc/knoxdb/pkg/num"
)

// ASM imports

//go:noescape
func cmp_i128_eq_x2(src num.Int128Stride, val num.Int128, bits, mask []byte) int64

//go:noescape
func cmp_i128_ne_x2(src num.Int128Stride, val num.Int128, bits, mask []byte) int64

//go:noescape
func cmp_i128_lt_x2(src num.Int128Stride, val num.Int128, bits, mask []byte) int64

//go:noescape
func cmp_i128_le_x2(src num.Int128Stride, val num.Int128, bits, mask []byte) int64

//go:noescape
func cmp_i128_gt_x2(src num.Int128Stride, val num.Int128, bits, mask []byte) int64

//go:noescape
func cmp_i128_ge_x2(src num.Int128Stride, val num.Int128, bits, mask []byte) int64

//go:noescape
func cmp_i128_bw_x2(src num.Int128Stride, a, b num.Int128, bits, mask []byte) int64

// Go drivers
func MatchInt128Equal(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := cmp_i128_eq_x2(src, val, bits, mask)
	res += generic.MatchInt128Equal(src.Tail(len_head), val, bits[bitFieldLen(len_head):], mask[bitFieldLen(len_head):])
	return res
}

func MatchInt128NotEqual(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := cmp_i128_ne_x2(src, val, bits, mask)
	res += generic.MatchInt128NotEqual(src.Tail(len_head), val, bits[bitFieldLen(len_head):], mask[bitFieldLen(len_head):])
	return res
}

func MatchInt128Less(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := cmp_i128_lt_x2(src, val, bits, mask)
	res += generic.MatchInt128Less(src.Tail(len_head), val, bits[bitFieldLen(len_head):], mask[bitFieldLen(len_head):])
	return res
}

func MatchInt128LessEqual(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := cmp_i128_le_x2(src, val, bits, mask)
	res += generic.MatchInt128LessEqual(src.Tail(len_head), val, bits[bitFieldLen(len_head):], mask[bitFieldLen(len_head):])
	return res
}

func MatchInt128Greater(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := cmp_i128_gt_x2(src, val, bits, mask)
	res += generic.MatchInt128Greater(src.Tail(len_head), val, bits[bitFieldLen(len_head):], mask[bitFieldLen(len_head):])
	return res
}

func MatchInt128GreaterEqual(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := cmp_i128_ge_x2(src, val, bits, mask)
	res += generic.MatchInt128GreaterEqual(src.Tail(len_head), val, bits[bitFieldLen(len_head):], mask[bitFieldLen(len_head):])
	return res
}

func MatchInt128Between(src num.Int128Stride, a, b num.Int128, bits, mask []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := cmp_i128_bw_x2(src, a, b, bits, mask)
	res += generic.MatchInt128Between(src.Tail(len_head), a, b, bits[bitFieldLen(len_head):], mask[bitFieldLen(len_head):])
	return res
}
