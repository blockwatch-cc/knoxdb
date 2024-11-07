// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

// Partitions a slice into two parts according to a split function.
// Returns two new slices and a boolean indicating whether a split
// was performed.
// The first part contains all elements where f returned false, the
// second part all elements where f returned true. Does not alter
// the original slice.

func CutFunc[T ~[]E, E any](s T, f func(e E) bool) (T, T, bool) {
	left := make(T, 0, len(s)/2)
	right := make(T, 0, len(s)/2)
	for _, v := range s {
		if f(v) {
			left = append(left, v)
		} else {
			right = append(right, v)
		}
	}
	return left, right, len(right) > 0
}
