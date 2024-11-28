// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

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
	MatchInt16Equal        = cmp_i16_eq_x2
	MatchInt16NotEqual     = cmp_i16_ne_x2
	MatchInt16Less         = cmp_i16_lt_x2
	MatchInt16LessEqual    = cmp_i16_le_x2
	MatchInt16Greater      = cmp_i16_gt_x2
	MatchInt16GreaterEqual = cmp_i16_ge_x2
	MatchInt16Between      = cmp_i16_bw_x2
)
