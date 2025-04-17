// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64
// +build amd64

package avx2

//go:noescape
func cmp_i64_eq_x2(src []int64, val int64, bits []byte) int64

//go:noescape
func cmp_i64_ne_x2(src []int64, val int64, bits []byte) int64

//go:noescape
func cmp_i64_lt_x2(src []int64, val int64, bits []byte) int64

//go:noescape
func cmp_i64_le_x2(src []int64, val int64, bits []byte) int64

//go:noescape
func cmp_i64_gt_x2(src []int64, val int64, bits []byte) int64

//go:noescape
func cmp_i64_ge_x2(src []int64, val int64, bits []byte) int64

//go:noescape
func cmp_i64_bw_x2(src []int64, a, b int64, bits []byte) int64

// Go exports
var (
	Int64Equal        = cmp_i64_eq_x2
	Int64NotEqual     = cmp_i64_ne_x2
	Int64Less         = cmp_i64_lt_x2
	Int64LessEqual    = cmp_i64_le_x2
	Int64Greater      = cmp_i64_gt_x2
	Int64GreaterEqual = cmp_i64_ge_x2
	Int64Between      = cmp_i64_bw_x2
)
