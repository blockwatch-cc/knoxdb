// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"testing"

	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeFloat(t *testing.T) {
	// runs
	x := AnalyzeFloat([]float64{-1.044, -1.044, 5.245, 5.245, 1.50, 1.50}, true, true)
	assert.InDelta(t, 3, x.NumUnique, 1.0, "num_unique")
	assert.Equal(t, 3, x.NumRuns, "num_runs")
	assert.Equal(t, 6, x.NumValues, "num_values")
	assert.Contains(t, x.EligibleSchemes(MAX_CASCADE), TFloatRunEnd, "eligible")
	assert.Contains(t, x.EligibleSchemes(MAX_CASCADE), TFloatRaw, "eligible")
	assert.Contains(t, x.EligibleSchemes(MAX_CASCADE), TFloatDictionary, "eligible")

	// dict-friendly
	x = AnalyzeFloat([]float64{-1.05, 1.05, 5.05, 1.05, -1.05, 1.05}, true, false)
	assert.InDelta(t, 3, x.NumUnique, 1.0, "num_unique")
	assert.Equal(t, 6, x.NumRuns, "num_runs")
	assert.Equal(t, 6, x.NumValues, "num_values")
	assert.NotContains(t, x.EligibleSchemes(MAX_CASCADE), TFloatRunEnd, "not eligible")
	assert.Contains(t, x.EligibleSchemes(MAX_CASCADE), TFloatRaw, "eligible")
	assert.Contains(t, x.EligibleSchemes(MAX_CASCADE), TFloatDictionary, "eligible")
}

func testFloatContainerType[T types.Float](t *testing.T, scheme FloatContainerType) {
	t.Helper()
	for _, c := range etests.MakeShortFloatTests[T](int(scheme)) {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			enc := NewFloat[T](scheme)

			// analyze and encode data into container
			ctx := AnalyzeFloat(c.Data, true, true)
			require.Greater(t, ctx.NumUnique, 0, "%#v", ctx)
			enc.Encode(ctx, c.Data, 1)

			// validate contents
			require.Equal(t, len(c.Data), enc.Len())
			for i, v := range c.Data {
				assert.Equal(t, v, enc.Get(i))
			}

			// serialize to buffer
			buf := make([]byte, 0, enc.Size())
			buf = enc.Store(buf)
			require.NotNil(t, buf)

			// load back into new container
			enc2 := NewFloat[T](scheme)
			buf, err := enc2.Load(buf)
			require.NoError(t, err)
			assert.Len(t, buf, 0)

			// validate contents
			require.Equal(t, len(c.Data), enc2.Len())
			for i, v := range c.Data {
				assert.Equal(t, v, enc2.Get(i))
			}

			// validate append
			all := tests.GenSeq[uint32](len(c.Data))
			dst := make([]T, 0, len(c.Data))
			dst = enc2.AppendTo(all, dst)
			assert.Len(t, dst, len(c.Data))
			assert.Equal(t, c.Data, dst)

			enc2.Close()
			enc.Close()
		})
	}
}

func TestEncodeConstFloat(t *testing.T) {
	testFloatContainerType[float64](t, TFloatConstant)
	testFloatContainerType[float32](t, TFloatConstant)
}

func TestEncodeRawFloat(t *testing.T) {
	testFloatContainerType[float64](t, TFloatRaw)
	testFloatContainerType[float32](t, TFloatRaw)
}

func TestEncodeRunEndFloat(t *testing.T) {
	testFloatContainerType[float64](t, TFloatRunEnd)
	testFloatContainerType[float32](t, TFloatRunEnd)
}

func TestEncodeDictFloat(t *testing.T) {
	testFloatContainerType[float64](t, TFloatDictionary)
	testFloatContainerType[float32](t, TFloatDictionary)
}

func TestEncodeAlpFloat(t *testing.T) {
	testFloatContainerType[float64](t, TFloatAlp)
	testFloatContainerType[float32](t, TFloatAlp)
}

func TestEncodeAlpRdFloat(t *testing.T) {
	testFloatContainerType[float64](t, TFloatAlpRd)
	testFloatContainerType[float32](t, TFloatAlpRd)
}

func testEncodeFloatT[T types.Float](t *testing.T) {
	t.Helper()
	for _, c := range etests.MakeFloatTests[T](16) {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			x := AnalyzeFloat(c.Data, true, true)
			e := EncodeFloat(x, c.Data, MAX_CASCADE)
			require.Equal(t, len(c.Data), e.Len(), "x=%#v", x)
			for i, v := range c.Data {
				require.Equal(t, v, e.Get(i), "i=%d d=%x e=%s", i, c.Data, e.Info())
			}
		})
	}
}

func TestEncodeFloat(t *testing.T) {
	testEncodeFloatT[float64](t)
	testEncodeFloatT[float32](t)
}
