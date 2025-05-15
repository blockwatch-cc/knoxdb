// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64
// +build amd64

package avx2

//go:noescape
func cmp_i8_eq_x2(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_ne_x2(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_lt_x2(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_le_x2(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_gt_x2(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_ge_x2(src []int8, val int8, bits []byte) int64

//go:noescape
func cmp_i8_bw_x2(src []int8, a, b int8, bits []byte) int64

// Go exports
var (
	Int8Equal        = cmp_i8_eq_x2
	Int8NotEqual     = cmp_i8_ne_x2
	Int8Less         = cmp_i8_lt_x2
	Int8LessEqual    = cmp_i8_le_x2
	Int8Greater      = cmp_i8_gt_x2
	Int8GreaterEqual = cmp_i8_ge_x2
)

func Int8Between(src []int8, a, b int8, bits []byte) int64 {
	// handle full range separate because [b - a + 1 = 0]
	if uint8(b-a) == 255 {
		return fillBits(bits, len(src))
	}
	return cmp_i8_bw_x2(src, a, b, bits)
}
