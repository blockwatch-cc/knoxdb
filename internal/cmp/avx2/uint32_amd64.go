// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64
// +build amd64

package avx2

//go:noescape
func cmp_u32_eq_x2(src []uint32, val uint32, bits []byte) int64

//go:noescape
func cmp_u32_ne_x2(src []uint32, val uint32, bits []byte) int64

//go:noescape
func cmp_u32_lt_x2(src []uint32, val uint32, bits []byte) int64

//go:noescape
func cmp_u32_le_x2(src []uint32, val uint32, bits []byte) int64

//go:noescape
func cmp_u32_gt_x2(src []uint32, val uint32, bits []byte) int64

//go:noescape
func cmp_u32_ge_x2(src []uint32, val uint32, bits []byte) int64

//go:noescape
func cmp_u32_bw_x2(src []uint32, a, b uint32, bits []byte) int64

// Go exports
var (
	Uint32Equal        = cmp_u32_eq_x2
	Uint32NotEqual     = cmp_u32_ne_x2
	Uint32Less         = cmp_u32_lt_x2
	Uint32LessEqual    = cmp_u32_le_x2
	Uint32Greater      = cmp_u32_gt_x2
	Uint32GreaterEqual = cmp_u32_ge_x2
)

func Uint32Between(src []uint32, a, b uint32, bits []byte) int64 {
	// handle full range separate because [b - a + 1 = 0]
	if b-a == 1<<32-1 {
		return fillBits(bits, len(src))
	}
	return cmp_u32_bw_x2(src, a, b, bits)
}
