// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64
// +build amd64

package avx512

// ASM imports

//go:noescape
func cmp_f32_eq_x5(src []float32, val float32, bits []byte) int64

//go:noescape
func cmp_f32_ne_x5(src []float32, val float32, bits []byte) int64

//go:noescape
func cmp_f32_lt_x5(src []float32, val float32, bits []byte) int64

//go:noescape
func cmp_f32_le_x5(src []float32, val float32, bits []byte) int64

//go:noescape
func cmp_f32_gt_x5(src []float32, val float32, bits []byte) int64

//go:noescape
func cmp_f32_ge_x5(src []float32, val float32, bits []byte) int64

//go:noescape
func cmp_f32_bw_x5(src []float32, a, b float32, bits []byte) int64

// Go exports
var (
	Float32Equal        = cmp_f32_eq_x5
	Float32NotEqual     = cmp_f32_ne_x5
	Float32Less         = cmp_f32_lt_x5
	Float32LessEqual    = cmp_f32_le_x5
	Float32Greater      = cmp_f32_gt_x5
	Float32GreaterEqual = cmp_f32_ge_x5
	Float32Between      = cmp_f32_bw_x5
)
