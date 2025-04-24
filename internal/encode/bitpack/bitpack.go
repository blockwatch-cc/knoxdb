// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitpack

// Returns the amount of bytes needed to store bitpacked rounded up to
// the nearest width to accomodate for padding introduced by code words.
// Current format uses 64 bit code words only.
func EstimateSize(log2, n int) int {
	return (log2*n + 63) &^ 63 / 8
}

// Legacy horizontal format used byte boundaries (width = 8)
// func EstimateSizeLegacy(width, log2, n int) int {
// 	return (log2*n + width - 1) &^ (width - 1) / 8
// }
