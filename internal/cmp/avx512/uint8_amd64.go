// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64
// +build amd64

package avx512

// ASM imports

//go:noescape
func cmp_u8_eq_x5(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_ne_x5(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_lt_x5(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_le_x5(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_gt_x5(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_ge_x5(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_bw_x5(src []uint8, a, b uint8, bits []byte) int64

// Go exports
var (
	Uint8Equal        = cmp_u8_eq_x5
	Uint8NotEqual     = cmp_u8_ne_x5
	Uint8Less         = cmp_u8_lt_x5
	Uint8LessEqual    = cmp_u8_le_x5
	Uint8Greater      = cmp_u8_gt_x5
	Uint8GreaterEqual = cmp_u8_ge_x5
	Uint8Between      = cmp_u8_bw_x5
)
