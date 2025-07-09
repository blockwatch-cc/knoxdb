// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build amd64
// +build amd64

package avx2

//go:noescape
func alp_f64_decode(src *int64, dst *float64, len int, fx, ex uint8)

//go:noescape
func alp_f64_decode_safe(src *int64, dst *float64, len int, fx, ex uint8)

//go:noescape
func alp_f32_decode(src *int32, dst *float32, len int, fx, ex uint8)

func Decode64(dst []float64, src []int64, fx, ex byte, isSafe bool) int {
	if isSafe {
		alp_f64_decode_safe(&src[0], &dst[0], len(src), fx, ex)
	} else {
		alp_f64_decode(&src[0], &dst[0], len(src), fx, ex)
	}
	return len(src) &^ 3
}

func Decode32(dst []float32, src []int32, fx, ex byte, isSafe bool) int {
	alp_f32_decode(&src[0], &dst[0], len(src), fx, ex)
	return len(src) &^ 7
}
