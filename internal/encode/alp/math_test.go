// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package alp

import (
	"math"
	"testing"
	"testing/quick"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestImpossible(t *testing.T) {
	quick.Check(func(values []float64) bool {
		for _, f := range values {
			x := *(*uint64)(unsafe.Pointer(&f))
			require.True(t, isImpossibleToEncode(f) == isImpossibleToEncodeSlow(f),
				"val=%x isNan=%t/%t isInf=%t/%t isHi=%t/%t isLo=%t/%t isNegZero=%t/%t",
				math.Float64bits(f),
				x == uvnan, math.IsNaN(f),
				x == uvinf || x == uvneginf, math.IsInf(f, 0),
				(x > hi && x < sign), f > ENCODING_UPPER_LIMIT,
				(x < lo && x >= sign), f < ENCODING_LOWER_LIMIT,
				x == sign, f == 0.0 && math.Signbit(f),
			)
		}
		return true
	}, nil)
}

func BenchmarkImpossibleSlow(b *testing.B) {
	for _, n := range tests.BenchmarkSizes {
		src := util.RandFloats[float64](n.N)
		b.Run(n.Name, func(b *testing.B) {
			for i := range b.N {
				isImpossibleToEncodeSlow(src[i%n.N])
			}
		})
	}
}

func BenchmarkImpossibleFast(b *testing.B) {
	for _, n := range tests.BenchmarkSizes {
		src := util.RandFloats[float64](n.N)
		b.Run(n.Name, func(b *testing.B) {
			for i := range b.N {
				isImpossibleToEncode(src[i%n.N])
			}
		})
	}
}
