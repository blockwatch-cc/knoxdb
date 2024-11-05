// Copyright (c) 2023 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64 && !gccgo && !appengine
// +build amd64,!gccgo,!appengine

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
	MatchFloat64Equal        = cmp_f64_eq_x5
	MatchFloat64NotEqual     = cmp_f64_ne_x5
	MatchFloat64Less         = cmp_f64_lt_x5
	MatchFloat64LessEqual    = cmp_f64_le_x5
	MatchFloat64Greater      = cmp_f64_gt_x5
	MatchFloat64GreaterEqual = cmp_f64_ge_x5
	MatchFloat64Between      = cmp_f64_bw_x5
)
