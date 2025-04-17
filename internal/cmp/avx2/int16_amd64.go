// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64
// +build amd64

package avx2

//go:noescape
func cmp_i16_eq_x2(src []int16, val int16, bits []byte) int64

//go:noescape
func cmp_i16_ne_x2(src []int16, val int16, bits []byte) int64

//go:noescape
func cmp_i16_lt_x2(src []int16, val int16, bits []byte) int64

//go:noescape
func cmp_i16_le_x2(src []int16, val int16, bits []byte) int64

//go:noescape
func cmp_i16_gt_x2(src []int16, val int16, bits []byte) int64

//go:noescape
func cmp_i16_ge_x2(src []int16, val int16, bits []byte) int64

//go:noescape
func cmp_i16_bw_x2(src []int16, a, b int16, bits []byte) int64

// Go exports
var (
	Int16Equal        = cmp_i16_eq_x2
	Int16NotEqual     = cmp_i16_ne_x2
	Int16Less         = cmp_i16_lt_x2
	Int16LessEqual    = cmp_i16_le_x2
	Int16Greater      = cmp_i16_gt_x2
	Int16GreaterEqual = cmp_i16_ge_x2
	Int16Between      = cmp_i16_bw_x2
)
