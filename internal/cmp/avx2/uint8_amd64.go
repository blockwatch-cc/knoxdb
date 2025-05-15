// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64
// +build amd64

package avx2

//go:noescape
func cmp_u8_eq_x2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_ne_x2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_lt_x2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_le_x2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_gt_x2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_ge_x2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func cmp_u8_bw_x2(src []uint8, a, b uint8, bits []byte) int64

// Go exports
var (
	Uint8Equal        = cmp_u8_eq_x2
	Uint8NotEqual     = cmp_u8_ne_x2
	Uint8Less         = cmp_u8_lt_x2
	Uint8LessEqual    = cmp_u8_le_x2
	Uint8Greater      = cmp_u8_gt_x2
	Uint8GreaterEqual = cmp_u8_ge_x2
)

func Uint8Between(src []uint8, a, b uint8, bits []byte) int64 {
	// handle full range separate because [b - a + 1 = 0]
	if b-a == 255 {
		return fillBits(bits, len(src))
	}
	return cmp_u8_bw_x2(src, a, b, bits)
}
