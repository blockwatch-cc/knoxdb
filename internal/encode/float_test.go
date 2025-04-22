// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"testing"

	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
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

func TestEncodeFloatConst(t *testing.T) {
	testFloatContainer[float64](t, TFloatConstant)
	testFloatContainer[float32](t, TFloatConstant)
}

func TestEncodeFloatRaw(t *testing.T) {
	testFloatContainer[float64](t, TFloatRaw)
	testFloatContainer[float32](t, TFloatRaw)
}

func TestEncodeFloatRun(t *testing.T) {
	testFloatContainer[float64](t, TFloatRunEnd)
	testFloatContainer[float32](t, TFloatRunEnd)
}

func TestEncodeFloatDict(t *testing.T) {
	testFloatContainer[float64](t, TFloatDictionary)
	testFloatContainer[float32](t, TFloatDictionary)
}

func TestEncodeFloatAlp(t *testing.T) {
	testFloatContainer[float64](t, TFloatAlp)
	testFloatContainer[float32](t, TFloatAlp)
}

func TestEncodeFloatAlpRd(t *testing.T) {
	testFloatContainer[float64](t, TFloatAlpRd)
	testFloatContainer[float32](t, TFloatAlpRd)
}

func TestEncodeFloat(t *testing.T) {
	testEncodeFloatT[float64](t)
	testEncodeFloatT[float32](t)
}

func testEncodeFloatT[T types.Float](t *testing.T) {
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

func testFloatContainer[T types.Float](t *testing.T, scheme FloatContainerType) {
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

func testFloatContainerEncode[T types.Float](t *testing.T, scheme FloatContainerType) {
	for _, c := range etests.MakeShortFloatTests[T](int(scheme)) {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			enc := NewFloat[T](scheme)

			// analyze and encode data into container
			ctx := AnalyzeFloat(c.Data, true, true)
			require.Greater(t, ctx.NumUnique, 0, "%#v", ctx)
			enc.Encode(ctx, c.Data, 1)
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
			all := tests.GenSeq[uint32](len(c.Data), 1)
			dst := make([]T, 0, len(c.Data))
			dst = enc2.AppendTo(all, dst)
			assert.Len(t, dst, len(c.Data))
			assert.Equal(t, c.Data, dst)

			enc2.Close()
			enc.Close()
		})
	}
}

func testFloatContainerCompare[T types.Float](t *testing.T, scheme FloatContainerType) {
	// validate matchers
	for _, sz := range etests.CompareSizes {
		t.Run(fmt.Sprintf("%T/cmp/%d", T(0), sz), func(t *testing.T) {
			src := etests.GenForFloatScheme[T](int(scheme), sz)
			enc := NewFloat[T](scheme)
			ctx := AnalyzeFloat(src, true, true)
			enc.Encode(ctx, src, 1)
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

func testFloatContainerIterator[T types.Float](t *testing.T, scheme FloatContainerType) {
	for _, sz := range etests.ItSizes {
		t.Run(fmt.Sprintf("%T/it-next/%d", T(0), sz), func(t *testing.T) {
			// setup
			src := etests.GenForFloatScheme[T](int(scheme), sz)
			enc := NewFloat[T](scheme)
			ctx := AnalyzeFloat(src, true, true)
			enc.Encode(ctx, src, 1)
			it := enc.Iterator()
			if it == nil {
				t.Skip()
			}
			t.Logf("Info: %s", enc.Info())

			// --------------------------
			// test next
			//
			for i, v := range src {
				val, ok := it.Next()
				require.True(t, ok, "short iterator at pos %d", i)
				require.Equal(t, v, val, "invalid val=%v pos=%d src=%v", val, i, src[i])
			}

			// --------------------------
			// test reset
			//
			it.Reset()
			require.Equal(t, len(src), it.Len(), "bad it len post reset")
			for i, v := range src {
				val, ok := it.Next()
				require.True(t, ok, "short iterator at pos %d post reset", i)
				require.Equal(t, v, val, "invalid val=%v pos=%d post reset", val, i)
			}

			// --------------------
			// test chunk
			//
			it.Reset()
			var seen int
			for {
				dst, n := it.NextChunk()
				if n == 0 {
					break
				}
				require.LessOrEqual(t, seen+n, len(src), "next chunk returned too large n")
				for i, v := range dst[:n] {
					require.Equal(t, src[seen+i], v, "invalid val=%v pos=%d src=%v", v, seen+i, src[seen+i])
				}
				seen += n
			}
			require.Equal(t, len(src), seen, "next chunk did not return all values")

			// --------------------------
			// test skip
			it.Reset()
			seen = it.SkipChunk()
			seen += it.SkipChunk()
			for {
				dst, n := it.NextChunk()
				if n == 0 {
					break
				}
				require.LessOrEqual(t, seen+n, len(src), "next chunk returned too large n")
				for i, v := range dst[:n] {
					require.Equal(t, src[seen+i], v, "invalid val=%v pos=%d src=%v after skip", v, seen+i, src[seen+i])
				}
				seen += n
			}
			require.Equal(t, len(src), seen, "skip&next chunk did not return all values")

			// --------------------------
			// test seek
			//
			it.Reset()
			for range len(src) {
				i := util.RandIntn(len(src))
				ok := it.Seek(i)
				require.True(t, ok, "seek to existing pos %d/%d failed", i, len(src))
				val, ok := it.Next()
				require.True(t, ok, "next after seek to existing pos %d/%d failed", i, len(src))
				require.Equal(t, src[i], val, "invalid val=%v pos=%d after seek", val, i)
			}

			// seek to invalid values
			require.False(t, it.Seek(-1), "seek to negative")
			_, ok := it.Next()
			require.False(t, ok, "next after bad seek")

			require.False(t, it.Seek(len(src)), "seek to end")
			_, ok = it.Next()
			require.False(t, it.Seek(len(src)), "seek to end")

			require.False(t, it.Seek(len(src)+1), "seek beyond end")
			_, ok = it.Next()
			require.False(t, it.Seek(len(src)), "seek to end")

			it.Close()
		})
	}
}
