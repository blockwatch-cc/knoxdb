// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"bytes"
	"time"

	"golang.org/x/exp/constraints"
)

func MinBytes(a, b []byte) []byte {
	if bytes.Compare(a, b) < 0 {
		return a
	}
	return b
}

func MaxBytes(a, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return a
	}
	return b
}

func Max[T constraints.Ordered](vals ...T) T {
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

func Min[T constraints.Ordered](vals ...T) T {
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

func MinMax[T constraints.Ordered](vals ...T) (T, T) {
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

func Clamp[T constraints.Ordered](val, min, max T) T {
	return Min(Max(val, min), max)
}

func NonZero[T constraints.Ordered](x ...T) T {
	var zero T
	for _, v := range x {
		if v != zero {
			return v
		}
	}
	return zero
}

func NonZeroMin[T constraints.Ordered](x ...T) T {
	var min, zero T
	for _, v := range x {
		if v != zero {
			if min == zero {
				min = v
			} else {
				min = Min(min, v)
			}
		}
	}
	return min
}

func Abs[T constraints.Signed](n T) T {
	y := int64(n) >> 63
	return T((int64(n) ^ y) - y)
}

// func MinF64(x, y float64) float64 {
// 	return math.Min(x, y)
// }

// func MaxF64(x, y float64) float64 {
// 	return math.Max(x, y)
// }

// func ClampF64(val, min, max float64) float64 {
// 	return math.Min(math.Max(val, min), max)
// }

func MinMaxTime(s []time.Time) (time.Time, time.Time) {
	var min, max time.Time

	switch l := len(s); l {
	case 0:
		// nothing
	case 1:
		min, max = s[0], s[0]
	default:
		// If there is more than one element, then initialize min and max
		if s[0].After(s[1]) {
			max = s[0]
			min = s[1]
		} else {
			max = s[1]
			min = s[0]
		}

		for i := 2; i < l; i++ {
			if s[i].After(max) {
				max = s[i]
			} else if s[i].Before(min) {
				min = s[i]
			}
		}
	}

	return min, max
}
