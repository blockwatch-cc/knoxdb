// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx2

//go:noescape
func cmp_i8_eq_x2(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_ne_x2(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_lt_x2(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_le_x2(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_gt_x2(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_ge_x2(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_bw_x2(src []int8, a, b int8, bits []byte) int64

// Go exports
var (
	MatchInt8Equal        = cmp_i8_eq_x2
	MatchInt8NotEqual     = cmp_i8_ne_x2
	MatchInt8Less         = cmp_i8_lt_x2
	MatchInt8LessEqual    = cmp_i8_le_x2
	MatchInt8Greater      = cmp_i8_gt_x2
	MatchInt8GreaterEqual = cmp_i8_ge_x2
	MatchInt8Between      = cmp_i8_bw_x2
)
