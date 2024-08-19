// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package avx2

//go:noescape
func cmp_u8_eq_x2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_ne_x2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_lt_x2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_le_x2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_gt_x2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_ge_x2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_bw_x2(src []uint8, a, b uint8, bits []byte) int64

// Go exports
var (
	MatchUint8Equal        = cmp_u8_eq_x2
	MatchUint8NotEqual     = cmp_u8_ne_x2
	MatchUint8Less         = cmp_u8_lt_x2
	MatchUint8LessEqual    = cmp_u8_le_x2
	MatchUint8Greater      = cmp_u8_gt_x2
	MatchUint8GreaterEqual = cmp_u8_ge_x2
	MatchUint8Between      = cmp_u8_bw_x2
)
