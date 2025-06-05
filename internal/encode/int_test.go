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
	assert.Len(t, x.EligibleIntSchemes(), 1, "eligible list")
	assert.Contains(t, x.EligibleIntSchemes(), TIntDelta, "delta only")

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
	// assert.Contains(t, x.EligibleIntSchemes(), TIntRunEnd, "missing eligible scheme")
	assert.Contains(t, x.EligibleIntSchemes(), TIntBitpacked, "missing eligible scheme")
	assert.Contains(t, x.EligibleIntSchemes(), TIntRaw, "missing eligible scheme")
	// assert.Contains(t, x.EligibleIntSchemes(), TIntSimple8, "missing eligible scheme")

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
	assert.NotContains(t, x.EligibleIntSchemes(), TIntRunEnd, "not eligible")
	assert.Contains(t, x.EligibleIntSchemes(), TIntBitpacked, "missing eligible scheme")
	assert.Contains(t, x.EligibleIntSchemes(), TIntRaw, "missing eligible scheme")
	assert.Contains(t, x.EligibleIntSchemes(), TIntDictionary, "missing eligible scheme")
	// assert.Contains(t, x.EligibleIntSchemes(), TIntSimple8, "missing eligible scheme")
}

func TestIntEncodeConst(t *testing.T) {
	testIntContainer[int64](t, TIntConstant)
	testIntContainer[int32](t, TIntConstant)
	testIntContainer[int16](t, TIntConstant)
	testIntContainer[int8](t, TIntConstant)

	testIntContainer[uint64](t, TIntConstant)
	testIntContainer[uint32](t, TIntConstant)
	testIntContainer[uint16](t, TIntConstant)
	testIntContainer[uint8](t, TIntConstant)
}

func TestIntEncodeDelta(t *testing.T) {
	testIntContainer[int64](t, TIntDelta)
	testIntContainer[int32](t, TIntDelta)
	testIntContainer[int16](t, TIntDelta)
	testIntContainer[int8](t, TIntDelta)

	testIntContainer[uint64](t, TIntDelta)
	testIntContainer[uint32](t, TIntDelta)
	testIntContainer[uint16](t, TIntDelta)
	testIntContainer[uint8](t, TIntDelta)
}

func TestIntEncodeRaw(t *testing.T) {
	testIntContainer[int64](t, TIntRaw)
	testIntContainer[int32](t, TIntRaw)
	testIntContainer[int16](t, TIntRaw)
	testIntContainer[int8](t, TIntRaw)

	testIntContainer[uint64](t, TIntRaw)
	testIntContainer[uint32](t, TIntRaw)
	testIntContainer[uint16](t, TIntRaw)
	testIntContainer[uint8](t, TIntRaw)
}

func TestIntEncodeBitpack(t *testing.T) {
	testIntContainer[int64](t, TIntBitpacked)
	testIntContainer[int32](t, TIntBitpacked)
	testIntContainer[int16](t, TIntBitpacked)
	testIntContainer[int8](t, TIntBitpacked)

	testIntContainer[uint64](t, TIntBitpacked)
	testIntContainer[uint32](t, TIntBitpacked)
	testIntContainer[uint16](t, TIntBitpacked)
	testIntContainer[uint8](t, TIntBitpacked)
}

func TestIntEncodeDict(t *testing.T) {
	testIntContainer[int64](t, TIntDictionary)
	testIntContainer[int32](t, TIntDictionary)
	testIntContainer[int16](t, TIntDictionary)
	testIntContainer[int8](t, TIntDictionary)

	testIntContainer[uint64](t, TIntDictionary)
	testIntContainer[uint32](t, TIntDictionary)
	testIntContainer[uint16](t, TIntDictionary)
	testIntContainer[uint8](t, TIntDictionary)
}

func TestIntEncodeRun(t *testing.T) {
	testIntContainer[int64](t, TIntRunEnd)
	testIntContainer[int32](t, TIntRunEnd)
	testIntContainer[int16](t, TIntRunEnd)
	testIntContainer[int8](t, TIntRunEnd)

	testIntContainer[uint64](t, TIntRunEnd)
	testIntContainer[uint32](t, TIntRunEnd)
	testIntContainer[uint16](t, TIntRunEnd)
	testIntContainer[uint8](t, TIntRunEnd)
}

func TestIntEncodeSimple8(t *testing.T) {
	testIntContainer[int64](t, TIntSimple8)
	testIntContainer[int32](t, TIntSimple8)
	testIntContainer[int16](t, TIntSimple8)
	testIntContainer[int8](t, TIntSimple8)

	testIntContainer[uint64](t, TIntSimple8)
	testIntContainer[uint32](t, TIntSimple8)
	testIntContainer[uint16](t, TIntSimple8)
	testIntContainer[uint8](t, TIntSimple8)
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
			e := EncodeInt(x, c.Data)
			require.Equal(t, len(c.Data), e.Len(), "T=%s x=%#v", e, x)
			dst := make([]T, len(c.Data))
			e.AppendTo(dst, nil)
			for i, v := range c.Data {
				require.Equal(t, v, e.Get(i), "T=%s i=%d minv=%d\nsrc=%x\ndec=%x",
					e.Info(), i, x.Min, c.Data, dst)
			}
		})
	}
}

func testIntContainer[T types.Integer](t *testing.T, scheme ContainerType) {
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

	// compare
	testIntContainerCompare[T](t, scheme)
	if t.Failed() {
		t.FailNow()
	}
}

func testIntContainerEncode[T types.Integer](t *testing.T, scheme ContainerType) {
	for _, c := range etests.MakeShortIntTests[T](int(scheme)) {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			enc := NewInt[T](scheme)

			// analyze and encode data into container
			ctx := AnalyzeInt(c.Data, true).WithLevel(1)
			enc.Encode(ctx, c.Data)
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
			dst = enc2.AppendTo(dst, nil)
			require.Len(t, dst, len(c.Data))
			require.Equal(t, c.Data, dst)

			// validate append selector
			sel := util.RandUintsn[uint32](max(1, len(c.Data)/2), uint32(len(c.Data)))
			clear(dst)
			dst = dst[:0]
			dst = enc2.AppendTo(dst, sel)
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

func isCompatibleTest[T types.Integer](scheme ContainerType, ctx *Context[T]) bool {
	maxv := uint64(1<<(util.SizeOf[T]()*8) - 1)
	if types.IsSigned[T]() {
		maxv >>= 1
	}
	if scheme == TIntDelta {
		if uint64(ctx.NumValues) > maxv || ctx.Delta == 0 {
			return false
		}
	}
	return true
}

func testIntContainerCompare[T types.Integer](t *testing.T, scheme ContainerType) {
	// validate matchers
	for _, sz := range etests.CompareSizes {
		t.Run(fmt.Sprintf("%T/cmp/%d", T(0), sz), func(t *testing.T) {
			src := etests.GenForIntScheme[T](int(scheme), sz)
			enc := NewInt[T](scheme)
			ctx := AnalyzeInt(src, true).WithLevel(1)

			if !isCompatibleTest[T](scheme, ctx) {
				t.Logf("Skipping cmp test sz=%d for %s/%T", sz, scheme, T(0))
				t.Skip()
			}

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
	bits := bitset.New(len(src))
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
	bits := bitset.New(len(src))
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
	bits := bitset.New(len(src))

	// construct set
	set := xroar.New()
	for range 10 {
		set.Set(uint64(src[util.RandIntn(len(src))]))
	}

	// run cmp
	cmp(set, bits, nil)
	etests.EnsureBits(t, src, 0, 0, bits, set, mode)
}

func testIntContainerIterator[T types.Integer](t *testing.T, scheme ContainerType) {
	for _, sz := range etests.ItSizes {
		t.Run(fmt.Sprintf("%T/it/%d", T(0), sz), func(t *testing.T) {
			// setup
			src := etests.GenForIntScheme[T](int(scheme), sz)
			enc := NewInt[T](scheme)
			ctx := AnalyzeInt(src, true).WithLevel(1)
			enc.Encode(ctx, src)

			// --------------------------
			// test next
			//
			for i, v := range enc.Iterator() {
				require.Equal(t, src[i], v, "invalid val at pos=%d", i)
			}

			// FIXME: ignore s8b iterator which seeks to encoder word boundaries only
			if scheme == TIntSimple8 {
				t.Skipf("FIXME: Skip s8b iterator chunk tests, reimplement to align with chunk size reads")
				return
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
					require.Equal(t, src[seen+i], v, "invalid val=%d pos=%d src=%d", v, seen+i, src[seen+i])
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
					require.Equal(t, src[seen+i], v, "invalid val=%d pos=%d src=%d after skip", v, seen+i, src[seen+i])
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
			require.Equal(t, 0, n, "next after bad seek to end")

			it.Close()
		})
		if t.Failed() {
			t.FailNow()
		}
	}
}
