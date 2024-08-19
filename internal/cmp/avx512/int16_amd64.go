// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package avx512

// ASM imports

//go:noescape
func cmp_i16_eq_x5(src []int16, val int16, bits []byte) int64

//go:noescape
func cmp_i16_ne_x5(src []int16, val int16, bits []byte) int64

//go:noescape
func cmp_i16_lt_x5(src []int16, val int16, bits []byte) int64

//go:noescape
func cmp_i16_le_x5(src []int16, val int16, bits []byte) int64

//go:noescape
func cmp_i16_gt_x5(src []int16, val int16, bits []byte) int64

//go:noescape
func cmp_i16_ge_x5(src []int16, val int16, bits []byte) int64

//go:noescape
func cmp_i16_bw_x5(src []int16, a, b int16, bits []byte) int64

// Go exports
var (
	MatchInt16Equal        = cmp_i16_eq_x5
	MatchInt16NotEqual     = cmp_i16_ne_x5
	MatchInt16Less         = cmp_i16_lt_x5
	MatchInt16LessEqual    = cmp_i16_le_x5
	MatchInt16Greater      = cmp_i16_gt_x5
	MatchInt16GreaterEqual = cmp_i16_ge_x5
	MatchInt16Between      = cmp_i16_bw_x5
)
