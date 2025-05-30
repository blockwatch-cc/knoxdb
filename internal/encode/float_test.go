// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"testing"

	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeFloat(t *testing.T) {
	// runs
	x := AnalyzeFloat([]float64{-1.044, -1.044, -1.044, -1.044, 5.245, 5.245, 5.245, 5.245, 1.50, 1.50, 1.50, 1.50}, true, true)
	assert.InDelta(t, 3, x.NumUnique, 1.0, "num_unique")
	assert.Equal(t, 3, x.NumRuns, "num_runs")
	assert.Equal(t, 12, x.NumValues, "num_values")
	assert.Contains(t, x.EligibleFloatSchemes(), TFloatRunEnd, "eligible")
	assert.Contains(t, x.EligibleFloatSchemes(), TFloatRaw, "eligible")
	assert.Contains(t, x.EligibleFloatSchemes(), TFloatDictionary, "eligible")

	// dict-friendly
	x = AnalyzeFloat([]float64{-1.05, 1.05, 5.05, 1.05, -1.05, 1.05}, true, false)
	assert.InDelta(t, 3, x.NumUnique, 1.0, "num_unique")
	assert.Equal(t, 6, x.NumRuns, "num_runs")
	assert.Equal(t, 6, x.NumValues, "num_values")
	assert.NotContains(t, x.EligibleFloatSchemes(), TFloatRunEnd, "not eligible")
	assert.Contains(t, x.EligibleFloatSchemes(), TFloatRaw, "eligible")
	assert.Contains(t, x.EligibleFloatSchemes(), TFloatDictionary, "eligible")
}

func TestFloatEncodeConst(t *testing.T) {
	testFloatContainer[float64](t, TFloatConstant)
	testFloatContainer[float32](t, TFloatConstant)
}

func TestFloatEncodeRaw(t *testing.T) {
	testFloatContainer[float64](t, TFloatRaw)
	testFloatContainer[float32](t, TFloatRaw)
}

func TestFloatEncodeRun(t *testing.T) {
	testFloatContainer[float64](t, TFloatRunEnd)
	testFloatContainer[float32](t, TFloatRunEnd)
}

func TestFloatEncodeDict(t *testing.T) {
	testFloatContainer[float64](t, TFloatDictionary)
	testFloatContainer[float32](t, TFloatDictionary)
}

func TestFloatEncodeAlp(t *testing.T) {
	testFloatContainer[float64](t, TFloatAlp)
	testFloatContainer[float32](t, TFloatAlp)
}

func TestFloatEncodeAlpRd(t *testing.T) {
	testFloatContainer[float64](t, TFloatAlpRd)
	testFloatContainer[float32](t, TFloatAlpRd)
}

func TestFloatEncode(t *testing.T) {
	testEncodeFloatT[float64](t)
	testEncodeFloatT[float32](t)
}

func testEncodeFloatT[T types.Float](t *testing.T) {
	for _, c := range etests.MakeFloatTests[T](16) {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			x := AnalyzeFloat(c.Data, true, true)
			e := EncodeFloat(x, c.Data)
			require.Equal(t, len(c.Data), e.Len(), "x=%#v", x)
			for i, v := range c.Data {
				require.Equal(t, v, e.Get(i), "i=%d d=%x e=%s", i, c.Data, e.Info())
			}
		})
	}
}

func testFloatContainer[T types.Float](t *testing.T, scheme ContainerType) {
	// general
	testFloatContainerEncode[T](t, scheme)
	if t.Failed() {
		t.FailNow()
	}

	// iterator
	testFloatContainerIterator[T](t, scheme)
	if t.Failed() {
		t.FailNow()
	}

	// compare
	testFloatContainerCompare[T](t, scheme)
	if t.Failed() {
		t.FailNow()
	}
}

func testFloatContainerEncode[T types.Float](t *testing.T, scheme ContainerType) {
	for _, c := range etests.MakeShortFloatTests[T](int(scheme)) {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			enc := NewFloat[T](scheme)

			// analyze and encode data into container
			ctx := AnalyzeFloat(c.Data, true, true).WithLevel(1)
			require.Greater(t, ctx.NumUnique, 0, "%#v", ctx)
			enc.Encode(ctx, c.Data)
			t.Logf("Info: %s", enc.Info())

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
			dst := make([]T, 0, len(c.Data))
			dst = enc2.AppendTo(dst, nil)
			assert.Len(t, dst, len(c.Data))
			assert.Equal(t, c.Data, dst)

			// validate append selector
			sel := util.RandUintsn[uint32](len(c.Data)/2, uint32(len(c.Data)))
			clear(dst)
			dst = dst[:0]
			dst = enc2.AppendTo(dst, sel)
			assert.Len(t, dst, len(sel))
			for i, v := range sel {
				assert.Equal(t, c.Data[v], dst[i], "sel[%d]", v)
			}

			enc2.Close()
			enc.Close()
		})
	}
}

func testFloatContainerCompare[T types.Float](t *testing.T, scheme ContainerType) {
	// validate matchers
	for _, sz := range etests.CompareSizes {
		t.Run(fmt.Sprintf("%T/cmp/%d", T(0), sz), func(t *testing.T) {
			src := etests.GenForFloatScheme[T](int(scheme), sz)
			enc := NewFloat[T](scheme)
			ctx := AnalyzeFloat(src, true, true).WithLevel(1)
			enc.Encode(ctx, src)
			t.Logf("Info: %s", enc.Info())

			// equal
			t.Run("EQ", func(t *testing.T) {
				testCompareFunc[T](t, enc.MatchEqual, src, types.FilterModeEqual)
			})

			// not equal
			t.Run("NE", func(t *testing.T) {
				testCompareFunc[T](t, enc.MatchNotEqual, src, types.FilterModeNotEqual)
			})

			// less
			t.Run("LT", func(t *testing.T) {
				testCompareFunc[T](t, enc.MatchLess, src, types.FilterModeLt)
			})

			// less equal
			t.Run("LE", func(t *testing.T) {
				testCompareFunc[T](t, enc.MatchLessEqual, src, types.FilterModeLe)
			})

			// greater
			t.Run("GT", func(t *testing.T) {
				testCompareFunc[T](t, enc.MatchGreater, src, types.FilterModeGt)
			})

			// greater equal
			t.Run("GE", func(t *testing.T) {
				testCompareFunc[T](t, enc.MatchGreaterEqual, src, types.FilterModeGe)
			})

			// between
			t.Run("RG", func(t *testing.T) {
				testCompareFunc2[T](t, enc.MatchBetween, src, types.FilterModeRange)
			})
		})
	}
}

func testFloatContainerIterator[T types.Float](t *testing.T, scheme ContainerType) {
	for _, sz := range etests.ItSizes {
		t.Run(fmt.Sprintf("%T/it-next/%d", T(0), sz), func(t *testing.T) {
			// setup
			src := etests.GenForFloatScheme[T](int(scheme), sz)
			enc := NewFloat[T](scheme)
			ctx := AnalyzeFloat(src, true, true).WithLevel(1)
			enc.Encode(ctx, src)
			t.Logf("Info: %s", enc.Info())

			// --------------------------
			// test next
			//
			for i, v := range enc.Iterator() {
				require.Equal(t, src[i], v, "invalid val at pos=%d", i)
			}

			// --------------------
			// test chunk
			//
			it := enc.Chunks()
			if it == nil {
				t.Skip()
			}
			var seen int
			for {
				dst, n := it.NextChunk()
				if n == 0 {
					break
				}
				require.GreaterOrEqual(t, n, 0, "next chunk returned negative n")
				require.LessOrEqual(t, seen+n, len(src), "next chunk returned too large n")
				for i, v := range dst[:n] {
					require.Equal(t, src[seen+i], v, "invalid val=%v pos=%d src=%v", v, seen+i, src[seen+i])
				}
				seen += n
			}
			require.Equal(t, len(src), seen, "next chunk did not return all values")
			it.Close()

			// --------------------------
			// test skip
			it = enc.Chunks()
			seen = it.SkipChunk()
			seen += it.SkipChunk()
			for {
				dst, n := it.NextChunk()
				if n == 0 {
					break
				}
				require.GreaterOrEqual(t, n, 0, "next chunk returned negative n")
				require.LessOrEqual(t, seen+n, len(src), "next chunk returned too large n")
				for i, v := range dst[:n] {
					require.Equal(t, src[seen+i], v, "invalid val=%v pos=%d src=%v after skip", v, seen+i, src[seen+i])
				}
				seen += n
			}
			require.Equal(t, len(src), seen, "skip&next chunk did not return all values")
			it.Close()

			// --------------------------
			// test seek
			//
			it = enc.Chunks()
			for range len(src) {
				i := util.RandIntn(len(src))
				ok := it.Seek(i)
				require.True(t, ok, "seek to existing pos %d/%d failed", i, len(src))
				vals, n := it.NextChunk()
				require.Greater(t, n, 0, "next after seek to existing pos %d/%d failed", i, len(src))
				require.Equal(t, src[i], vals[i%CHUNK_SIZE], "invalid val at pos=%d after seek, vals=%v ", i, vals[:n])
			}

			// seek to invalid values
			require.False(t, it.Seek(-1), "seek to negative")
			_, n := it.NextChunk()
			require.Equal(t, 0, n, "next after bad seek")

			require.False(t, it.Seek(len(src)), "seek to end")
			_, n = it.NextChunk()
			require.Equal(t, 0, n, "next after bad seek to end")

			require.False(t, it.Seek(len(src)+1), "seek beyond end")
			_, n = it.NextChunk()
			require.Equal(t, 0, n, "next after bad seek beyond end")

			it.Close()
		})
	}
}
