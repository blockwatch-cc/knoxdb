// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"cmp"
	"math/bits"
)

func Max[T cmp.Ordered](vals ...T) T {
	var zero T
	switch len(vals) {
	case 0:
		return zero
	case 1:
		return vals[0]
	default:
		n := vals[0]
		for _, v := range vals[1:] {
			if v > n {
				n = v
			}
		}
		return n
	}
}

func Min[T cmp.Ordered](vals ...T) T {
	var zero T
	switch len(vals) {
	case 0:
		return zero
	case 1:
		return vals[0]
	default:
		n := vals[0]
		for _, v := range vals[1:] {
			if v < n {
				n = v
			}
		}
		return n
	}
}

func MinMax[T cmp.Ordered](vals ...T) (T, T) {
	var min, max T
	switch l := len(vals); l {
	case 0:
		// nothing
	case 1:
		min, max = vals[0], vals[0]
	default:
		// If there is more than one element, then initialize min and max
		if vals[0] > vals[1] {
			max = vals[0]
			min = vals[1]
		} else {
			max = vals[1]
			min = vals[0]
		}
		for i := 2; i < l; i++ {
			if vals[i] > max {
				max = vals[i]
			} else if vals[i] < min {
				min = vals[i]
			}
		}
	}
	return min, max
}

func Log2(i int) int {
	return bits.UintSize - bits.LeadingZeros(uint(i)) - 1
}

func Log2ceil(i int) int {
	v := Log2(i)
	if i&(i-1) > 0 {
		v++
	}
	return v
}

// Donald Knuth, The Art of Computer Programming, Volume 2, Section 4.6.3
// func Pow[T constraints.Integer](a, b T) (c T) {
// 	c = 1
// 	for b > 0 {
// 		if b&1 != 0 {
// 			c *= a
// 		}
// 		b >>= 1
// 		a *= a
// 	}
// 	return c
// }
