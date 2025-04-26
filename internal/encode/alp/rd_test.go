// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package alp

import (
	"fmt"
	"math"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestAlpRD(t *testing.T) {
	AlpRDTest[float32, uint32](t)
	AlpRDTest[float64, uint64](t)
}

func AlpRDTest[T Float, U Uint](t *testing.T) {
	w := util.SizeOf[T]()
	for _, c := range MakeTestcases[T]() {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			// estimate shift
			enc := NewEncoderRD[T, U]()
			a := AnalyzeRD[T, U](c.Data)
			require.GreaterOrEqual(t, a.Split, w*8-16)
			require.LessOrEqual(t, a.Split, w*8)

			// split floats
			res := enc.Encode(c.Data, a.Split)

			// merge floats
			dst := make([]T, len(c.Data))
			dec := NewDecoderRD[T, U](a.Split)
			dec.Decode(dst, res.Left, res.Right)

			require.Equal(t, c.Data[0], dst[0], "%x != %x",
				math.Float64bits(float64(c.Data[0])), math.Float64bits(float64(dst[0])))
			for i, v := range c.Data {
				if math.IsNaN(float64(v)) {
					require.Equal(t, math.IsNaN(float64(v)), math.IsNaN(float64(dst[i])), "val %d: %v != %v", i, v, dst[i])
				} else {
					require.Equal(t, v, dst[i], "val %d: %v != %v", i, v, dst[i])
				}
			}
		})
	}
}
