package tests

import (
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

func ones[T types.Unsigned](n int) func() []T {
	return func() []T {
		in := make([]T, n)
		for i := 0; i < n; i++ {
			in[i] = 1
		}
		return in
	}
}

// bits generates sequence of n numbers with max bits,
// ensuring max bit is set for 50% of the values.
func bits[T types.Unsigned](n, bits int) func() []T {
	return func() []T {
		out := make([]T, n)
		maxVal := T(1<<uint8(bits) - 1)
		for i := range out {
			topBit := T((i & 1) << uint8(bits-1))
			out[i] = T(util.RandInt64n(int64(maxVal))) | topBit
			if out[i] > maxVal {
				panic("max")
			}
		}
		return out
	}
}

func combine[T types.Unsigned](fns ...func() []T) func() []T {
	return func() []T {
		var out []T
		for _, fn := range fns {
			out = append(out, fn()...)
		}
		return out
	}
}
