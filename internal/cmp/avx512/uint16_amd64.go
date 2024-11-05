// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx512

// ASM imports

//go:noescape
func cmp_u16_eq_x5(src []uint16, val uint16, bits []byte) int64

//go:noescape
func cmp_u16_ne_x5(src []uint16, val uint16, bits []byte) int64

//go:noescape
func cmp_u16_lt_x5(src []uint16, val uint16, bits []byte) int64

//go:noescape
func cmp_u16_le_x5(src []uint16, val uint16, bits []byte) int64

//go:noescape
func cmp_u16_gt_x5(src []uint16, val uint16, bits []byte) int64

//go:noescape
func cmp_u16_ge_x5(src []uint16, val uint16, bits []byte) int64

//go:noescape
func cmp_u16_bw_x5(src []uint16, a, b uint16, bits []byte) int64

// Go exports
var (
	MatchUint16Equal        = cmp_u16_eq_x5
	MatchUint16NotEqual     = cmp_u16_ne_x5
	MatchUint16Less         = cmp_u16_lt_x5
	MatchUint16LessEqual    = cmp_u16_le_x5
	MatchUint16Greater      = cmp_u16_gt_x5
	MatchUint16GreaterEqual = cmp_u16_ge_x5
	MatchUint16Between      = cmp_u16_bw_x5
)
