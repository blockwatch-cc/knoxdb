// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package avx2

import (
	"blockwatch.cc/knoxdb/internal/cmp/generic"
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
func MatchInt256Equal(src num.Int256Stride, val num.Int256, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := cmp_i256_eq_x2(src, val, bits)
	res += generic.MatchInt256Equal(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func MatchInt256NotEqual(src num.Int256Stride, val num.Int256, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := cmp_i256_ne_x2(src, val, bits)
	res += generic.MatchInt256NotEqual(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func MatchInt256Less(src num.Int256Stride, val num.Int256, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := cmp_i256_lt_x2(src, val, bits)
	res += generic.MatchInt256Less(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func MatchInt256LessEqual(src num.Int256Stride, val num.Int256, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := cmp_i256_le_x2(src, val, bits)
	res += generic.MatchInt256LessEqual(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func MatchInt256Greater(src num.Int256Stride, val num.Int256, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := cmp_i256_gt_x2(src, val, bits)
	res += generic.MatchInt256Greater(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func MatchInt256GreaterEqual(src num.Int256Stride, val num.Int256, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := cmp_i256_ge_x2(src, val, bits)
	res += generic.MatchInt256GreaterEqual(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func MatchInt256Between(src num.Int256Stride, a, b num.Int256, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := cmp_i256_bw_x2(src, a, b, bits)
	res += generic.MatchInt256Between(src.Tail(len_head), a, b, bits[bitFieldLen(len_head):], nil)
	return res
}
