// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx2

//go:noescape
func cmp_u64_eq_x2(src []uint64, val uint64, bits []byte) int64

//go:noescape
func cmp_u64_ne_x2(src []uint64, val uint64, bits []byte) int64

//go:noescape
func cmp_u64_lt_x2(src []uint64, val uint64, bits []byte) int64

//go:noescape
func cmp_u64_le_x2(src []uint64, val uint64, bits []byte) int64

//go:noescape
func cmp_u64_gt_x2(src []uint64, val uint64, bits []byte) int64

//go:noescape
func cmp_u64_ge_x2(src []uint64, val uint64, bits []byte) int64

//go:noescape
func cmp_u64_bw_x2(src []uint64, a, b uint64, bits []byte) int64

// Go exports
var (
	MatchUint64Equal        = cmp_u64_eq_x2
	MatchUint64NotEqual     = cmp_u64_ne_x2
	MatchUint64Less         = cmp_u64_lt_x2
	MatchUint64LessEqual    = cmp_u64_le_x2
	MatchUint64Greater      = cmp_u64_gt_x2
	MatchUint64GreaterEqual = cmp_u64_ge_x2
	MatchUint64Between      = cmp_u64_bw_x2
)
