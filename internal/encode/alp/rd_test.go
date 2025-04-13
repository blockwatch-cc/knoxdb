package alp

import (
	"fmt"
	"math"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/require"
)

func TestAlpRD(t *testing.T) {
	// AlpRDTest[float32, uint32](t)
	AlpRDTest[float64, uint64](t)
}

func AlpRDTest[T types.Float, U types.Unsigned](t *testing.T) {
	for _, c := range MakeTestcases[T]() {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			// sample data
			sample := make([]T, MaxSampleLen(len(c.Data)))
			FirstLevelSample(sample, c.Data)

			// estimate shift
			unique := make([]uint16, 1<<16)
			shift := EstimateShift(sample, unique)
			require.GreaterOrEqual(t, shift, 48)
			require.LessOrEqual(t, shift, 64)

			// split floats
			left := make([]uint16, len(c.Data))
			right := make([]uint64, len(c.Data))
			Split(c.Data, left, right, shift)

			// merge floats
			dst := make([]T, len(c.Data))
			Merge(dst, left, right, shift)

			require.Equal(t, c.Data[0], dst[0], "%x != %x",
				math.Float64bits(float64(c.Data[0])), math.Float64bits(float64(dst[0])))
			require.Equal(t, c.Data, dst)
		})
	}
}
