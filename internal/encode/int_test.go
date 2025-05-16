// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"slices"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeInt(t *testing.T) {
	// delta, no dups
	x := AnalyzeInt([]int64{-1, 0, 1, 2}, true)
	assert.Equal(t, int64(-1), x.Min, "min")
	assert.Equal(t, int64(2), x.Max, "max")
	assert.Equal(t, int64(1), x.Delta, "delta")
	assert.Equal(t, 64, x.PhyBits, "phybits")
	assert.Equal(t, 2, x.UseBits, "usebits")
	assert.InDelta(t, 4, x.NumUnique, 1.0, "num_unique")
	assert.Equal(t, 4, x.NumRuns, "num_runs")
	assert.Equal(t, 4, x.NumValues, "num_values")
	assert.Len(t, x.EligibleSchemes(), 1, "eligible list")
	assert.Contains(t, x.EligibleSchemes(), TIntegerDelta, "delta only")

	// runs
	x = AnalyzeInt([]int64{-1, -1, 5, 5, 1, 1}, true)
	assert.Equal(t, int64(-1), x.Min, "min")
	assert.Equal(t, int64(5), x.Max, "max")
	assert.Equal(t, int64(0), x.Delta, "delta")
	assert.Equal(t, 64, x.PhyBits, "phybits")
	assert.Equal(t, 3, x.UseBits, "usebits")
	assert.InDelta(t, 3, x.NumUnique, 1.0, "num_unique")
	assert.Equal(t, 3, x.NumUnique, "num_unique")
	assert.Equal(t, 3, x.NumRuns, "num_runs")
	assert.Equal(t, 6, x.NumValues, "num_values")
	// assert.Contains(t, x.EligibleSchemes(), TIntegerRunEnd, "missing eligible scheme")
	assert.Contains(t, x.EligibleSchemes(), TIntegerBitpacked, "missing eligible scheme")
	assert.Contains(t, x.EligibleSchemes(), TIntegerRaw, "missing eligible scheme")
	// assert.Contains(t, x.EligibleSchemes(), TIntegerSimple8, "missing eligible scheme")

	// dict-friendly
	x = AnalyzeInt([]int64{
		0, 42, 100, 42, 100, 42, 100, 42, 100, 42, 100, 42, 100, 42, 100, 42, 100,
		42, 100, 42, 100, 42, 100, 42, 100, 42, 100, 42, 100, 42}, true)
	assert.Equal(t, int64(0), x.Min, "min")
	assert.Equal(t, int64(100), x.Max, "max")
	assert.Equal(t, int64(0), x.Delta, "delta")
	assert.Equal(t, 64, x.PhyBits, "phybits")
	assert.Equal(t, 7, x.UseBits, "usebits")
	assert.InDelta(t, 3, x.NumUnique, 1.0, "num_unique")
	assert.Equal(t, 30, x.NumRuns, "num_runs")
	assert.Equal(t, 30, x.NumValues, "num_values")
	assert.NotContains(t, x.EligibleSchemes(), TIntegerRunEnd, "not eligible")
	assert.Contains(t, x.EligibleSchemes(), TIntegerBitpacked, "missing eligible scheme")
	assert.Contains(t, x.EligibleSchemes(), TIntegerRaw, "missing eligible scheme")
	assert.Contains(t, x.EligibleSchemes(), TIntegerDictionary, "missing eligible scheme")
	// assert.Contains(t, x.EligibleSchemes(), TIntegerSimple8, "missing eligible scheme")
}

func TestIntEncodeConst(t *testing.T) {
	testIntContainer[int64](t, TIntegerConstant)
	testIntContainer[int32](t, TIntegerConstant)
	testIntContainer[int16](t, TIntegerConstant)
	testIntContainer[int8](t, TIntegerConstant)

	testIntContainer[uint64](t, TIntegerConstant)
	testIntContainer[uint32](t, TIntegerConstant)
	testIntContainer[uint16](t, TIntegerConstant)
	testIntContainer[uint8](t, TIntegerConstant)
}

func TestIntEncodeDelta(t *testing.T) {
	testIntContainer[int64](t, TIntegerDelta)
	testIntContainer[int32](t, TIntegerDelta)
	testIntContainer[int16](t, TIntegerDelta)
	testIntContainer[int8](t, TIntegerDelta)

	testIntContainer[uint64](t, TIntegerDelta)
	testIntContainer[uint32](t, TIntegerDelta)
	testIntContainer[uint16](t, TIntegerDelta)
	testIntContainer[uint8](t, TIntegerDelta)
}

func TestIntEncodeRaw(t *testing.T) {
	testIntContainer[int64](t, TIntegerRaw)
	testIntContainer[int32](t, TIntegerRaw)
	testIntContainer[int16](t, TIntegerRaw)
	testIntContainer[int8](t, TIntegerRaw)

	testIntContainer[uint64](t, TIntegerRaw)
	testIntContainer[uint32](t, TIntegerRaw)
	testIntContainer[uint16](t, TIntegerRaw)
	testIntContainer[uint8](t, TIntegerRaw)
}

func TestIntEncodeBitpack(t *testing.T) {
	testIntContainer[int64](t, TIntegerBitpacked)
	testIntContainer[int32](t, TIntegerBitpacked)
	testIntContainer[int16](t, TIntegerBitpacked)
	testIntContainer[int8](t, TIntegerBitpacked)

	testIntContainer[uint64](t, TIntegerBitpacked)
	testIntContainer[uint32](t, TIntegerBitpacked)
	testIntContainer[uint16](t, TIntegerBitpacked)
	testIntContainer[uint8](t, TIntegerBitpacked)
}

func TestIntEncodeDict(t *testing.T) {
	testIntContainer[int64](t, TIntegerDictionary)
	testIntContainer[int32](t, TIntegerDictionary)
	testIntContainer[int16](t, TIntegerDictionary)
	testIntContainer[int8](t, TIntegerDictionary)

	testIntContainer[uint64](t, TIntegerDictionary)
	testIntContainer[uint32](t, TIntegerDictionary)
	testIntContainer[uint16](t, TIntegerDictionary)
	testIntContainer[uint8](t, TIntegerDictionary)
}

func TestIntEncodeRun(t *testing.T) {
	testIntContainer[int64](t, TIntegerRunEnd)
	testIntContainer[int32](t, TIntegerRunEnd)
	testIntContainer[int16](t, TIntegerRunEnd)
	testIntContainer[int8](t, TIntegerRunEnd)

	testIntContainer[uint64](t, TIntegerRunEnd)
	testIntContainer[uint32](t, TIntegerRunEnd)
	testIntContainer[uint16](t, TIntegerRunEnd)
	testIntContainer[uint8](t, TIntegerRunEnd)
}

func TestIntEncodeSimple8(t *testing.T) {
	testIntContainer[int64](t, TIntegerSimple8)
	testIntContainer[int32](t, TIntegerSimple8)
	testIntContainer[int16](t, TIntegerSimple8)
	testIntContainer[int8](t, TIntegerSimple8)

	testIntContainer[uint64](t, TIntegerSimple8)
	testIntContainer[uint32](t, TIntegerSimple8)
	testIntContainer[uint16](t, TIntegerSimple8)
	testIntContainer[uint8](t, TIntegerSimple8)
}

func TestIntEncode(t *testing.T) {
	testIntEncodeT[int64](t)
	testIntEncodeT[int32](t)
	testIntEncodeT[int16](t)
	testIntEncodeT[int8](t)

	testIntEncodeT[uint64](t)
	testIntEncodeT[uint32](t)
	testIntEncodeT[uint16](t)
	testIntEncodeT[uint8](t)
}

func testIntEncodeT[T types.Integer](t *testing.T) {
	for _, c := range etests.MakeIntTests[T](1024) {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			x := AnalyzeInt(c.Data, true)
			e := EncodeInt(x, c.Data, MAX_CASCADE)
			require.Equal(t, len(c.Data), e.Len(), "T=%s x=%#v", e, x)
			dst := make([]T, len(c.Data))
			e.AppendTo(nil, dst)
			for i, v := range c.Data {
				require.Equal(t, v, e.Get(i), "T=%s i=%d minv=%d\nsrc=%x\ndec=%x",
					e.Info(), i, x.Min, c.Data, dst)
			}
		})
	}
}

func testIntContainer[T types.Integer](t *testing.T, scheme IntegerContainerType) {
	// general
	testIntContainerEncode[T](t, scheme)
	if t.Failed() {
		t.FailNow()
	}

	// iterator
	testIntContainerIterator[T](t, scheme)
	if t.Failed() {
		t.FailNow()
	}

	// skip cmp tests for bitpack until implemented
	if scheme == TIntegerBitpacked {
		t.Logf("WARN: skipping bitpack compare tests")
		t.Skip()
	}

	// compare
	testIntContainerCompare[T](t, scheme)
	if t.Failed() {
		t.FailNow()
	}
}

func testIntContainerEncode[T types.Integer](t *testing.T, scheme IntegerContainerType) {
	for _, c := range etests.MakeShortIntTests[T](int(scheme)) {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			enc := NewInt[T](scheme)

			// analyze and encode data into container
			ctx := AnalyzeInt(c.Data, true)
			enc.Encode(ctx, c.Data, 1)
			t.Logf("Info: %s", enc.Info())

			// validate contents
			require.Equal(t, len(c.Data), enc.Len())
			for i, v := range c.Data {
				require.Equal(t, v, enc.Get(i))
			}

			// serialize to buffer
			buf := make([]byte, 0, enc.Size())
			buf = enc.Store(buf)
			require.NotNil(t, buf)

			// load back into new container
			enc2 := NewInt[T](scheme)
			buf, err := enc2.Load(buf)
			require.NoError(t, err)
			require.Len(t, buf, 0)

			// validate contents
			require.Equal(t, len(c.Data), enc2.Len())
			for i, v := range c.Data {
				require.Equal(t, v, enc2.Get(i))
			}

			// validate append
			dst := make([]T, 0, len(c.Data))
			dst = enc2.AppendTo(nil, dst)
			require.Len(t, dst, len(c.Data))
			require.Equal(t, c.Data, dst)

			// validate append selector
			sel := util.RandUintsn[uint32](max(1, len(c.Data)/2), uint32(len(c.Data)))
			clear(dst)
			dst = dst[:0]
			dst = enc2.AppendTo(sel, dst)
			require.Len(t, dst, len(sel))
			for i, v := range sel {
				require.Equal(t, c.Data[v], dst[i], "sel[%d]", v)
			}

			enc2.Close()
			enc.Close()
		})
		if t.Failed() {
			t.FailNow()
		}
	}
}

func isCompatibleTest[T types.Integer](scheme IntegerContainerType, ctx *IntegerContext[T]) bool {
	maxv := uint64(1<<(util.SizeOf[T]()*8) - 1)
	if types.IsSigned[T]() {
		maxv >>= 1
	}
	if scheme == TIntegerDelta {
		if uint64(ctx.NumValues) > maxv || ctx.Delta == 0 {
			return false
		}
	}
	return true
}

func testIntContainerCompare[T types.Integer](t *testing.T, scheme IntegerContainerType) {
	// validate matchers
	for _, sz := range etests.CompareSizes {
		t.Run(fmt.Sprintf("%T/cmp/%d", T(0), sz), func(t *testing.T) {
			src := etests.GenForIntScheme[T](int(scheme), sz)
			enc := NewInt[T](scheme)
			ctx := AnalyzeInt(src, true)

			if !isCompatibleTest[T](scheme, ctx) {
				t.Logf("Skipping cmp test sz=%d for %s/%T", sz, scheme, T(0))
				t.Skip()
			}

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

			// in set
			t.Run("IN", func(t *testing.T) {
				testCompareFunc3[T](t, enc.MatchInSet, src, types.FilterModeIn)
			})

			// not in set
			t.Run("NI", func(t *testing.T) {
				testCompareFunc3[T](t, enc.MatchNotInSet, src, types.FilterModeNotIn)
			})
		})
		if t.Failed() {
			t.FailNow()
		}
	}
}

type CompareFunc[T types.Number] func(T, *Bitset, *Bitset)
type CompareFunc2[T types.Number] func(T, T, *Bitset, *Bitset)
type CompareFunc3[T types.Number] func(any, *Bitset, *Bitset)

func testCompareFunc[T types.Number](t *testing.T, cmp CompareFunc[T], src []T, mode types.FilterMode) {
	bits := bitset.NewBitset(len(src))
	minv, maxv := slices.Min(src), slices.Max(src)

	// single value
	val := src[len(src)/2]
	cmp(val, bits, nil)
	etests.EnsureBits(t, src, val, val, bits, nil, mode)
	bits.Zero()
	require.Equal(t, 0, bits.Count(), "cleared")

	// value over bounds
	if maxv < types.MaxVal[T]() {
		over := maxv + 1
		cmp(over, bits, nil)
		etests.EnsureBits(t, src, over, over, bits, nil, mode)
		bits.Zero()
		require.Equal(t, 0, bits.Count(), "cleared")
	}

	// value under bounds
	if minv > types.MinVal[T]() {
		under := minv - 1
		cmp(under, bits, nil)
		etests.EnsureBits(t, src, under, under, bits, nil, mode)
		bits.Zero()
		require.Equal(t, 0, bits.Count(), "cleared")
	}
}

func testCompareFunc2[T types.Number](t *testing.T, cmp CompareFunc2[T], src []T, mode types.FilterMode) {
	bits := bitset.NewBitset(len(src))
	minv, maxv := slices.Min(src), slices.Max(src)

	// single value
	val := src[len(src)/2]
	cmp(val, val, bits, nil)
	etests.EnsureBits(t, src, val, val, bits, nil, mode)
	bits.Zero()
	require.Equal(t, 0, bits.Count(), "cleared")

	// full range
	cmp(minv, maxv, bits, nil)
	etests.EnsureBits(t, src, minv, maxv, bits, nil, mode)
	bits.Zero()

	// partial range
	from, to := max(val/2, minv+1), min(val*2, maxv-1)
	if from > to {
		from, to = to, from
	}
	// skip test if values would wrap around
	if from > minv && to < maxv {
		cmp(from, to, bits, nil)
		etests.EnsureBits(t, src, from, to, bits, nil, mode)
		bits.Zero()
	}

	// out of bounds (over)
	if maxv < types.MaxVal[T]()-1 {
		cmp(maxv+1, maxv+1, bits, nil)
		etests.EnsureBits(t, src, maxv+1, maxv+1, bits, nil, mode)
		bits.Zero()
	}

	// out of bounds (under)
	if minv > types.MinVal[T]()+2 {
		cmp(minv-1, minv-1, bits, nil)
		etests.EnsureBits(t, src, minv-1, minv-1, bits, nil, mode)
		bits.Zero()
	}
}

func testCompareFunc3[T types.Number](t *testing.T, cmp CompareFunc3[T], src []T, mode types.FilterMode) {
	bits := bitset.NewBitset(len(src))

	// construct set
	set := xroar.NewBitmap()
	for range 10 {
		set.Set(uint64(src[util.RandIntn(len(src))]))
	}

	// run cmp
	cmp(set, bits, nil)
	etests.EnsureBits(t, src, 0, 0, bits, set, mode)
}

func testIntContainerIterator[T types.Integer](t *testing.T, scheme IntegerContainerType) {
	for _, sz := range etests.ItSizes {
		t.Run(fmt.Sprintf("%T/it/%d", T(0), sz), func(t *testing.T) {
			// setup
			src := etests.GenForIntScheme[T](int(scheme), sz)
			enc := NewInt[T](scheme)
			ctx := AnalyzeInt(src, true)
			enc.Encode(ctx, src, 1)
			it := enc.Iterator()
			if it == nil {
				t.Skip()
			}

			// --------------------------
			// test next
			//
			for i, v := range src {
				val, ok := it.Next()
				require.True(t, ok, "short iterator at pos %d", i)
				require.Equal(t, v, val, "invalid val=%d pos=%d src=%d min=%d", val, i, src[i], ctx.Min)
			}

			// --------------------------
			// test reset
			//
			it.Reset()
			require.Equal(t, len(src), it.Len(), "bad it len post reset")
			for i, v := range src {
				val, ok := it.Next()
				require.True(t, ok, "short iterator at pos %d post reset", i)
				require.Equal(t, v, val, "invalid val=%d pos=%d post reset", val, i)
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
				require.GreaterOrEqual(t, n, 0, "next chunk returned negative n")
				require.LessOrEqual(t, seen+n, len(src), "next chunk returned too large n")
				for i, v := range dst[:n] {
					require.Equal(t, src[seen+i], v, "invalid val=%d pos=%d src=%d", v, seen+i, src[seen+i])
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
				require.GreaterOrEqual(t, n, 0, "next chunk returned negative n")
				require.LessOrEqual(t, seen+n, len(src), "next chunk returned too large n")
				for i, v := range dst[:n] {
					require.Equal(t, src[seen+i], v, "invalid val=%d pos=%d src=%d after skip", v, seen+i, src[seen+i])
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
				require.Equal(t, src[i], val, "invalid val=%d pos=%d after seek", val, i)
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
		if t.Failed() {
			t.FailNow()
		}
	}
}
