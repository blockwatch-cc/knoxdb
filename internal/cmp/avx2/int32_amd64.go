// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64
// +build amd64

package avx2

//go:noescape
func cmp_i32_eq_x2(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_ne_x2(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_lt_x2(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_le_x2(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_gt_x2(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_ge_x2(src []int32, val int32, bits []byte) int64

//go:noescape
func cmp_i32_bw_x2(src []int32, a, b int32, bits []byte) int64

// Go exports
var (
	Int32Equal        = cmp_i32_eq_x2
	Int32NotEqual     = cmp_i32_ne_x2
	Int32Less         = cmp_i32_lt_x2
	Int32LessEqual    = cmp_i32_le_x2
	Int32Greater      = cmp_i32_gt_x2
	Int32GreaterEqual = cmp_i32_ge_x2
)

func Int32Between(src []int32, a, b int32, bits []byte) int64 {
	// handle full range separate because [b - a + 1 = 0]
	if uint32(b-a) == 1<<32-1 {
		return fillBits(bits, len(src))
	}
	return cmp_i32_bw_x2(src, a, b, bits)
}
