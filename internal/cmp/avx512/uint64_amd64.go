// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64
// +build amd64

package avx512

//go:noescape
func cmp_u64_eq_x5(src []uint64, val uint64, bits []byte) int64

//go:noescape
func cmp_u64_ne_x5(src []uint64, val uint64, bits []byte) int64

//go:noescape
func cmp_u64_lt_x5(src []uint64, val uint64, bits []byte) int64

//go:noescape
func cmp_u64_le_x5(src []uint64, val uint64, bits []byte) int64

//go:noescape
func cmp_u64_gt_x5(src []uint64, val uint64, bits []byte) int64

//go:noescape
func cmp_u64_ge_x5(src []uint64, val uint64, bits []byte) int64

//go:noescape
func cmp_u64_bw_x5(src []uint64, a, b uint64, bits []byte) int64

// Go exports
var (
	Uint64Equal        = cmp_u64_eq_x5
	Uint64NotEqual     = cmp_u64_ne_x5
	Uint64Less         = cmp_u64_lt_x5
	Uint64LessEqual    = cmp_u64_le_x5
	Uint64Greater      = cmp_u64_gt_x5
	Uint64GreaterEqual = cmp_u64_ge_x5
	Uint64Between      = cmp_u64_bw_x5
)
