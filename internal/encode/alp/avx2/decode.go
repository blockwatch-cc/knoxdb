// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64
// +build !amd64

package avx2

func Decode64(dst []float64, src []int64, fx, ex byte, isSafe bool) int { return 0 }

func Decode32(dst []float32, src []int32, fx, ex byte, isSafe bool) int { return 0 }
