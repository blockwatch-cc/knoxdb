// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64
// +build amd64

package avx512

// ASM imports

//go:noescape
func cmp_f64_eq_x5(src []float64, val float64, bits []byte) int64

//go:noescape
func cmp_f64_ne_x5(src []float64, val float64, bits []byte) int64

//go:noescape
func cmp_f64_lt_x5(src []float64, val float64, bits []byte) int64

//go:noescape
func cmp_f64_le_x5(src []float64, val float64, bits []byte) int64

//go:noescape
func cmp_f64_gt_x5(src []float64, val float64, bits []byte) int64

//go:noescape
func cmp_f64_ge_x5(src []float64, val float64, bits []byte) int64

//go:noescape
func cmp_f64_bw_x5(src []float64, a, b float64, bits []byte) int64

// Go exports
var (
	Float64Equal        = cmp_f64_eq_x5
	Float64NotEqual     = cmp_f64_ne_x5
	Float64Less         = cmp_f64_lt_x5
	Float64LessEqual    = cmp_f64_le_x5
	Float64Greater      = cmp_f64_gt_x5
	Float64GreaterEqual = cmp_f64_ge_x5
	Float64Between      = cmp_f64_bw_x5
)
