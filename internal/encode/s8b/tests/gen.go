// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
)

func ones[T types.Integer](n int) func() []T {
	return func() []T {
		return tests.GenConst[T](n, 1)
	}
}

// bits generates sequence of n numbers with max bits,
// ensuring max bit is set for 50% of the values.
func bits[T types.Integer](n, bits int) func() []T {
	return func() []T {
		out := tests.GenRndBits[T](n, bits)
		for i := range out {
			out[i] |= T((i & 1) << uint8(bits-1))
		}
		return out
	}
}

func combine[T types.Integer](fns ...func() []T) func() []T {
	return func() []T {
		var out []T
		for _, fn := range fns {
			out = append(out, fn()...)
		}
		return out
	}
}
