// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package avx512

// ASM imports

//go:noescape
func cmp_i32_eq_x5(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_ne_x5(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_lt_x5(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_le_x5(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_gt_x5(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_ge_x5(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_bw_x5(src []int32, a, b int32, bits []byte) int64

// Go exports
var (
	MatchInt32Equal        = cmp_i32_eq_x5
	MatchInt32NotEqual     = cmp_i32_ne_x5
	MatchInt32Less         = cmp_i32_lt_x5
	MatchInt32LessEqual    = cmp_i32_le_x5
	MatchInt32Greater      = cmp_i32_gt_x5
	MatchInt32GreaterEqual = cmp_i32_ge_x5
	MatchInt32Between      = cmp_i32_bw_x5
)
