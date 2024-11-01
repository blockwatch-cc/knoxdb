// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

package avx2

// ASM imports

//go:noescape
func cmp_f64_eq_x2(src []float64, val float64, bits []byte) int64

//go:noescape
func cmp_f64_ne_x2(src []float64, val float64, bits []byte) int64

//go:noescape
func cmp_f64_lt_x2(src []float64, val float64, bits []byte) int64

//go:noescape
func cmp_f64_le_x2(src []float64, val float64, bits []byte) int64

//go:noescape
func cmp_f64_gt_x2(src []float64, val float64, bits []byte) int64

//go:noescape
func cmp_f64_ge_x2(src []float64, val float64, bits []byte) int64

//go:noescape
func cmp_f64_bw_x2(src []float64, a, b float64, bits []byte) int64

// Go exports
var (
	MatchFloat64Equal        = cmp_f64_eq_x2
	MatchFloat64NotEqual     = cmp_f64_ne_x2
	MatchFloat64Less         = cmp_f64_lt_x2
	MatchFloat64LessEqual    = cmp_f64_le_x2
	MatchFloat64Greater      = cmp_f64_gt_x2
	MatchFloat64GreaterEqual = cmp_f64_ge_x2
	MatchFloat64Between      = cmp_f64_bw_x2
)
