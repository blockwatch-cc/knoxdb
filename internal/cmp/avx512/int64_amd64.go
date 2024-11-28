// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx512

// ASM imports

//go:noescape
func cmp_i64_eq_x5(src []int64, val int64, bits []byte) int64

//go:noescape
func cmp_i64_ne_x5(src []int64, val int64, bits []byte) int64

//go:noescape
func cmp_i64_lt_x5(src []int64, val int64, bits []byte) int64

//go:noescape
func cmp_i64_le_x5(src []int64, val int64, bits []byte) int64

//go:noescape
func cmp_i64_gt_x5(src []int64, val int64, bits []byte) int64

//go:noescape
func cmp_i64_ge_x5(src []int64, val int64, bits []byte) int64

//go:noescape
func cmp_i64_bw_x5(src []int64, a, b int64, bits []byte) int64

// Go exports
var (
	MatchInt64Equal        = cmp_i64_eq_x5
	MatchInt64NotEqual     = cmp_i64_ne_x5
	MatchInt64Less         = cmp_i64_lt_x5
	MatchInt64LessEqual    = cmp_i64_le_x5
	MatchInt64Greater      = cmp_i64_gt_x5
	MatchInt64GreaterEqual = cmp_i64_ge_x5
	MatchInt64Between      = cmp_i64_bw_x5
)
