// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64
// +build amd64

package avx512

// ASM imports

//go:noescape
func cmp_i8_eq_x5(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_ne_x5(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_lt_x5(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_le_x5(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_gt_x5(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_ge_x5(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_bw_x5(src []int8, a, b int8, bits []byte) int64

// Go exports
var (
	Int8Equal        = cmp_i8_eq_x5
	Int8NotEqual     = cmp_i8_ne_x5
	Int8Less         = cmp_i8_lt_x5
	Int8LessEqual    = cmp_i8_le_x5
	Int8Greater      = cmp_i8_gt_x5
	Int8GreaterEqual = cmp_i8_ge_x5
	Int8Between      = cmp_i8_bw_x5
)
