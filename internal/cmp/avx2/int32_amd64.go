// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx2

//go:noescape
func cmp_i32_eq_x2(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_ne_x2(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_lt_x2(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_le_x2(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_gt_x2(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_ge_x2(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_bw_x2(src []int32, a, b int32, bits []byte) int64

// Go exports
var (
	MatchInt32Equal        = cmp_i32_eq_x2
	MatchInt32NotEqual     = cmp_i32_ne_x2
	MatchInt32Less         = cmp_i32_lt_x2
	MatchInt32LessEqual    = cmp_i32_le_x2
	MatchInt32Greater      = cmp_i32_gt_x2
	MatchInt32GreaterEqual = cmp_i32_ge_x2
	MatchInt32Between      = cmp_i32_bw_x2
)
