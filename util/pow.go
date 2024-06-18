// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"golang.org/x/exp/constraints"
)

// Donald Knuth, The Art of Computer Programming, Volume 2, Section 4.6.3
func Pow[T constraints.Integer](a, b T) (c T) {
	c = 1
	for b > 0 {
		if b&1 != 0 {
			c *= a
		}
		b >>= 1
		a *= a
	}
	return c
}
