// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64
// +build amd64

package avx2

//go:noescape
func cmp_u16_eq_x2(src []uint16, val uint16, bits []byte) int64

//go:noescape
func cmp_u16_ne_x2(src []uint16, val uint16, bits []byte) int64

//go:noescape
func cmp_u16_lt_x2(src []uint16, val uint16, bits []byte) int64

//go:noescape
func cmp_u16_le_x2(src []uint16, val uint16, bits []byte) int64

//go:noescape
func cmp_u16_gt_x2(src []uint16, val uint16, bits []byte) int64

//go:noescape
func cmp_u16_ge_x2(src []uint16, val uint16, bits []byte) int64

//go:noescape
func cmp_u16_bw_x2(src []uint16, a, b uint16, bits []byte) int64

// Go exports
var (
	Uint16Equal        = cmp_u16_eq_x2
	Uint16NotEqual     = cmp_u16_ne_x2
	Uint16Less         = cmp_u16_lt_x2
	Uint16LessEqual    = cmp_u16_le_x2
	Uint16Greater      = cmp_u16_gt_x2
	Uint16GreaterEqual = cmp_u16_ge_x2
	Uint16Between      = cmp_u16_bw_x2
)
