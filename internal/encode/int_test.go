// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"slices"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/tests"
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
	assert.Contains(t, x.EligibleSchemes(), TIntegerSimple8, "missing eligible scheme")

	// dict-friendly
	x = AnalyzeInt([]int64{
		0, 42, 100, 42, 100, 42, 100, 42, 100, 42, 100, 42, 100, 42, 100, 42, 100,
		42, 100, 42, 100, 42, 100, 42, 100, 42, 100, 42, 100, 42, 100, 42, 100}, true)
	assert.Equal(t, int64(0), x.Min, "min")
	assert.Equal(t, int64(100), x.Max, "max")
	assert.Equal(t, int64(0), x.Delta, "delta")
	assert.Equal(t, 64, x.PhyBits, "phybits")
	assert.Equal(t, 7, x.UseBits, "usebits")
	assert.InDelta(t, 3, x.NumUnique, 1.0, "num_unique")
	assert.Equal(t, 33, x.NumRuns, "num_runs")
	assert.Equal(t, 33, x.NumValues, "num_values")
	assert.NotContains(t, x.EligibleSchemes(), TIntegerRunEnd, "not eligible")
	assert.Contains(t, x.EligibleSchemes(), TIntegerBitpacked, "missing eligible scheme")
	assert.Contains(t, x.EligibleSchemes(), TIntegerRaw, "missing eligible scheme")
	assert.Contains(t, x.EligibleSchemes(), TIntegerDictionary, "missing eligible scheme")
	assert.Contains(t, x.EligibleSchemes(), TIntegerSimple8, "missing eligible scheme")
}

func testIntContainerType[T types.Integer](t *testing.T, scheme IntegerContainerType) {
	for _, c := range etests.MakeShortIntTests[T](int(scheme)) {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			enc := NewInt[T](scheme)

			// analyze and encode data into container
			ctx := AnalyzeInt(c.Data, true)
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
			enc2 := NewInt[T](scheme)
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

	if scheme == TIntegerBitpacked {
		return
	}

	// validate matchers
	for _, sz := range etests.CompareSizes {
		t.Run(fmt.Sprintf("%T/cmp_%d", T(0), sz), func(t *testing.T) {
			src := etests.GenForIntScheme[T](int(scheme), sz)
			enc := NewInt[T](scheme)
			ctx := AnalyzeInt(src, true)
			enc.Encode(ctx, src, 1)

			// equal
			t.Run("EQ", func(t *testing.T) {
				testIntCompareFunc[T](t, enc.MatchEqual, src, types.FilterModeEqual)
			})

			// not equal
			t.Run("NE", func(t *testing.T) {
				testIntCompareFunc[T](t, enc.MatchNotEqual, src, types.FilterModeNotEqual)
			})

			// less
			t.Run("LT", func(t *testing.T) {
				testIntCompareFunc[T](t, enc.MatchLess, src, types.FilterModeLt)
			})

			// less equal
			t.Run("LE", func(t *testing.T) {
				testIntCompareFunc[T](t, enc.MatchLessEqual, src, types.FilterModeLe)
			})

			// greater
			t.Run("GT", func(t *testing.T) {
				testIntCompareFunc[T](t, enc.MatchGreater, src, types.FilterModeGt)
			})

			// greater equal
			t.Run("GE", func(t *testing.T) {
				testIntCompareFunc[T](t, enc.MatchGreaterEqual, src, types.FilterModeGe)
			})

			// between
			t.Run("RG", func(t *testing.T) {
				testIntCompareFunc2[T](t, enc.MatchBetween, src, types.FilterModeRange)
			})

			// in set
			t.Run("IN", func(t *testing.T) {
				testIntCompareFunc3[T](t, enc.MatchInSet, src, types.FilterModeIn)
			})

			// not in set
			t.Run("NI", func(t *testing.T) {
				testIntCompareFunc3[T](t, enc.MatchNotInSet, src, types.FilterModeNotIn)
			})
		})
	}
}

type IntCompareFunc[T types.Integer] func(T, *Bitset, *Bitset)
type IntCompareFunc2[T types.Integer] func(T, T, *Bitset, *Bitset)
type IntCompareFunc3[T types.Integer] func(any, *Bitset, *Bitset)

func testIntCompareFunc[T types.Integer](t *testing.T, cmp IntCompareFunc[T], src []T, mode types.FilterMode) {
	bits := bitset.NewBitset(len(src))
	minv, maxv := slices.Min(src), slices.Max(src)

	// single value
	val := src[len(src)/2]
	cmp(val, bits, nil)
	ensureBits(t, src, val, val, bits, nil, mode)
	bits.Zero()
	require.Equal(t, 0, bits.Count(), "cleared")

	// value over bounds
	over := maxv + 1
	cmp(over, bits, nil)
	ensureBits(t, src, over, over, bits, nil, mode)
	bits.Zero()
	require.Equal(t, 0, bits.Count(), "cleared")

	// value under bounds
	under := minv
	if under > 0 {
		under--
	}
	cmp(under, bits, nil)
	ensureBits(t, src, under, under, bits, nil, mode)
	bits.Zero()
	require.Equal(t, 0, bits.Count(), "cleared")
}

func testIntCompareFunc2[T types.Integer](t *testing.T, cmp IntCompareFunc2[T], src []T, mode types.FilterMode) {
	bits := bitset.NewBitset(len(src))
	minv, maxv := slices.Min(src), slices.Max(src)

	// single value
	val := src[len(src)/2]
	cmp(val, val, bits, nil)
	ensureBits(t, src, val, val, bits, nil, mode)
	bits.Zero()
	require.Equal(t, 0, bits.Count(), "cleared")

	// full range
	cmp(minv, maxv, bits, nil)
	ensureBits(t, src, minv, maxv, bits, nil, mode)
	bits.Zero()

	// partial range
	from, to := max(val/2, minv+1), min(val*2, maxv-1)
	if from > to {
		from, to = to, from
	}
	cmp(from, to, bits, nil)
	ensureBits(t, src, from, to, bits, nil, mode)
	bits.Zero()

	// out of bounds (over)
	cmp(maxv+1, maxv+1, bits, nil)
	ensureBits(t, src, maxv+1, maxv+1, bits, nil, mode)
	bits.Zero()

	// out of bounds (under)
	if minv > 2 {
		cmp(minv-1, minv-1, bits, nil)
		ensureBits(t, src, minv-1, minv-1, bits, nil, mode)
		bits.Zero()
	}
}

func testIntCompareFunc3[T types.Integer](t *testing.T, cmp IntCompareFunc3[T], src []T, mode types.FilterMode) {
	bits := bitset.NewBitset(len(src))

	// construct set
	set := xroar.NewBitmap()
	for range 10 {
		set.Set(uint64(src[util.RandIntn(len(src))]))
	}

	// run cmp
	cmp(set, bits, nil)
	ensureBits(t, src, 0, 0, bits, set, mode)
}

func ensureBits[T types.Integer](t *testing.T, vals []T, val, val2 T, bits *Bitset, set *xroar.Bitmap, mode types.FilterMode) {
	if !testing.Short() {
		for i, v := range vals {
			t.Logf("Val %d: %d", i, v)
		}
		t.Logf("Bitset %x", bits.Bytes())
	}
	minv, maxv := slices.Min(vals), slices.Max(vals)
	switch mode {
	case types.FilterModeEqual:
		for i, v := range vals {
			require.Equal(t, v == val, bits.IsSet(i), "bit=%d val=%d %s %d min=%d max=%d",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeNotEqual:
		for i, v := range vals {
			require.Equal(t, v != val, bits.IsSet(i), "bit=%d val=%d %s %d min=%d max=%d",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeLt:
		for i, v := range vals {
			require.Equal(t, v < val, bits.IsSet(i), "bit=%d val=%d %s %d min=%d max=%d",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeLe:
		for i, v := range vals {
			require.Equal(t, v <= val, bits.IsSet(i), "bit=%d val=%d %s %d min=%d max=%d",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeGt:
		for i, v := range vals {
			require.Equal(t, v > val, bits.IsSet(i), "bit=%d val=%d %s %d min=%d max=%d",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeGe:
		for i, v := range vals {
			require.Equal(t, v >= val, bits.IsSet(i), "bit=%d val=%d %s %d min=%d max=%d",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeRange:
		for i, v := range vals {
			require.Equal(t, v >= val && v <= val2, bits.IsSet(i), "bit=%d val=%d %s [%d,%d] min=%d max=%d",
				i, v, mode, val, val2, minv, maxv)
		}

	case types.FilterModeIn:
		for i, v := range vals {
			require.Equal(t, set.Contains(uint64(v)), bits.IsSet(i), "bit=%d min=%d max=%d val=%d %s %v",
				i, minv, maxv, v, mode, set.ToArray())
		}

	case types.FilterModeNotIn:
		for i, v := range vals {
			require.Equal(t, !set.Contains(uint64(v)), bits.IsSet(i), "bit=%d min=%d max=%d val=%d %s %v",
				i, minv, maxv, v, mode, set.ToArray())
		}
	}
}

func TestEncodeConstInt(t *testing.T) {
	testIntContainerType[int64](t, TIntegerConstant)
	testIntContainerType[int32](t, TIntegerConstant)
	testIntContainerType[int16](t, TIntegerConstant)
	testIntContainerType[int8](t, TIntegerConstant)

	testIntContainerType[uint64](t, TIntegerConstant)
	testIntContainerType[uint32](t, TIntegerConstant)
	testIntContainerType[uint16](t, TIntegerConstant)
	testIntContainerType[uint8](t, TIntegerConstant)
}

// func TestEncodeDelta(t *testing.T) {
// 	testIntContainerType[int64](t, TIntegerDelta)
// 	testIntContainerType[int32](t, TIntegerDelta)
// 	testIntContainerType[int16](t, TIntegerDelta)
// 	testIntContainerType[int8](t, TIntegerDelta)

// 	testIntContainerType[uint64](t, TIntegerDelta)
// 	testIntContainerType[uint32](t, TIntegerDelta)
// 	testIntContainerType[uint16](t, TIntegerDelta)
// 	testIntContainerType[uint8](t, TIntegerDelta)
// }

func TestEncodeRawInt(t *testing.T) {
	testIntContainerType[int64](t, TIntegerRaw)
	testIntContainerType[int32](t, TIntegerRaw)
	testIntContainerType[int16](t, TIntegerRaw)
	testIntContainerType[int8](t, TIntegerRaw)

	testIntContainerType[uint64](t, TIntegerRaw)
	testIntContainerType[uint32](t, TIntegerRaw)
	testIntContainerType[uint16](t, TIntegerRaw)
	testIntContainerType[uint8](t, TIntegerRaw)
}

func TestEncodeBitpack(t *testing.T) {
	testIntContainerType[int64](t, TIntegerBitpacked)
	testIntContainerType[int32](t, TIntegerBitpacked)
	testIntContainerType[int16](t, TIntegerBitpacked)
	testIntContainerType[int8](t, TIntegerBitpacked)

	testIntContainerType[uint64](t, TIntegerBitpacked)
	testIntContainerType[uint32](t, TIntegerBitpacked)
	testIntContainerType[uint16](t, TIntegerBitpacked)
	testIntContainerType[uint8](t, TIntegerBitpacked)
}

func TestEncodeDict(t *testing.T) {
	testIntContainerType[int64](t, TIntegerDictionary)
	testIntContainerType[int32](t, TIntegerDictionary)
	testIntContainerType[int16](t, TIntegerDictionary)
	testIntContainerType[int8](t, TIntegerDictionary)

	testIntContainerType[uint64](t, TIntegerDictionary)
	testIntContainerType[uint32](t, TIntegerDictionary)
	testIntContainerType[uint16](t, TIntegerDictionary)
	testIntContainerType[uint8](t, TIntegerDictionary)
}

func TestEncodeRun(t *testing.T) {
	testIntContainerType[int64](t, TIntegerRunEnd)
	testIntContainerType[int32](t, TIntegerRunEnd)
	testIntContainerType[int16](t, TIntegerRunEnd)
	testIntContainerType[int8](t, TIntegerRunEnd)

	testIntContainerType[uint64](t, TIntegerRunEnd)
	testIntContainerType[uint32](t, TIntegerRunEnd)
	testIntContainerType[uint16](t, TIntegerRunEnd)
	testIntContainerType[uint8](t, TIntegerRunEnd)
}

func TestEncodeSimple8(t *testing.T) {
	testIntContainerType[int64](t, TIntegerSimple8)
	testIntContainerType[int32](t, TIntegerSimple8)
	testIntContainerType[int16](t, TIntegerSimple8)
	testIntContainerType[int8](t, TIntegerSimple8)

	testIntContainerType[uint64](t, TIntegerSimple8)
	testIntContainerType[uint32](t, TIntegerSimple8)
	testIntContainerType[uint16](t, TIntegerSimple8)
	testIntContainerType[uint8](t, TIntegerSimple8)
}

func TestEncodeInt(t *testing.T) {
	testEncodeIntT[int64](t)
	testEncodeIntT[int32](t)
	testEncodeIntT[int16](t)
	testEncodeIntT[int8](t)

	testEncodeIntT[uint64](t)
	testEncodeIntT[uint32](t)
	testEncodeIntT[uint16](t)
	testEncodeIntT[uint8](t)
}

func testEncodeIntT[T types.Integer](t *testing.T) {
	for _, c := range etests.MakeIntTests[T](1024) {
		t.Run(fmt.Sprintf("%T/%s", T(0), c.Name), func(t *testing.T) {
			x := AnalyzeInt(c.Data, true)
			e := EncodeInt(x, c.Data, MAX_CASCADE)
			require.Equal(t, len(c.Data), e.Len(), "T=%v x=%#v", e, x)
			for i, v := range c.Data {
				require.Equal(t, v, e.Get(i), "T=%v i=%d d=%x", e, i, c.Data)
			}
		})
	}
}

func TestUniqueArray(t *testing.T) {
	for _, c := range tests.BenchmarkSizes {
		data := util.RandInts[int16](c.N)
		minx := slices.Min(data)
		maxx := slices.Max(data)

		// map
		u := make(map[int16]struct{}, c.N)
		for _, v := range data {
			u[v] = struct{}{}
		}

		// array
		var card int
		a := make([]uint16, int(maxx)-int(minx)+1)
		for _, v := range data {
			a[int(v)-int(minx)] = 1
		}
		for _, v := range a {
			if v > 0 {
				card++
			}
		}
		require.Equal(t, card, len(u))
	}
}
