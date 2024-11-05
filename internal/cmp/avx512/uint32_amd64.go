// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx512

// ASM imports

//go:noescape
func cmp_u32_eq_x5(src []uint32, val uint32, bits []byte) int64

//go:noescape
func cmp_u32_ne_x5(src []uint32, val uint32, bits []byte) int64

//go:noescape
func cmp_u32_lt_x5(src []uint32, val uint32, bits []byte) int64

//go:noescape
func cmp_u32_le_x5(src []uint32, val uint32, bits []byte) int64

//go:noescape
func cmp_u32_gt_x5(src []uint32, val uint32, bits []byte) int64

//go:noescape
func cmp_u32_ge_x5(src []uint32, val uint32, bits []byte) int64

//go:noescape
func cmp_u32_bw_x5(src []uint32, a, b uint32, bits []byte) int64

// Go exports
var (
	MatchUint32Equal        = cmp_u32_eq_x5
	MatchUint32NotEqual     = cmp_u32_ne_x5
	MatchUint32Less         = cmp_u32_lt_x5
	MatchUint32LessEqual    = cmp_u32_le_x5
	MatchUint32Greater      = cmp_u32_gt_x5
	MatchUint32GreaterEqual = cmp_u32_ge_x5
	MatchUint32Between      = cmp_u32_bw_x5
)
