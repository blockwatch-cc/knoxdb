// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

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
	MatchFloat32Equal        = cmp_f32_eq_x2
	MatchFloat32NotEqual     = cmp_f32_ne_x2
	MatchFloat32Less         = cmp_f32_lt_x2
	MatchFloat32LessEqual    = cmp_f32_le_x2
	MatchFloat32Greater      = cmp_f32_gt_x2
	MatchFloat32GreaterEqual = cmp_f32_ge_x2
	MatchFloat32Between      = cmp_f32_bw_x2
)
