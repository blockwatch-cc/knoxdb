// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64
// +build amd64

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
	Uint64Equal        = cmp_u64_eq_x2
	Uint64NotEqual     = cmp_u64_ne_x2
	Uint64Less         = cmp_u64_lt_x2
	Uint64LessEqual    = cmp_u64_le_x2
	Uint64Greater      = cmp_u64_gt_x2
	Uint64GreaterEqual = cmp_u64_ge_x2
	Uint64Between      = cmp_u64_bw_x2
)
