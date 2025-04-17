// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64
// +build amd64

package avx2

// ASM imports

//go:noescape
func cmp_f32_eq_x2(src []float32, val float32, bits []byte) int64

//go:noescape
func cmp_f32_ne_x2(src []float32, val float32, bits []byte) int64

//go:noescape
func cmp_f32_lt_x2(src []float32, val float32, bits []byte) int64

//go:noescape
func cmp_f32_le_x2(src []float32, val float32, bits []byte) int64

//go:noescape
func cmp_f32_gt_x2(src []float32, val float32, bits []byte) int64

//go:noescape
func cmp_f32_ge_x2(src []float32, val float32, bits []byte) int64

//go:noescape
func cmp_f32_bw_x2(src []float32, a, b float32, bits []byte) int64

// Go exports
var (
	Float32Equal        = cmp_f32_eq_x2
	Float32NotEqual     = cmp_f32_ne_x2
	Float32Less         = cmp_f32_lt_x2
	Float32LessEqual    = cmp_f32_le_x2
	Float32Greater      = cmp_f32_gt_x2
	Float32GreaterEqual = cmp_f32_ge_x2
	Float32Between      = cmp_f32_bw_x2
)
