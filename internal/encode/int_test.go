// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/filter/llb"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/internal/zip"
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
			buf := make([]byte, 0, enc.MaxSize())
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
				testIntCompareFunc3[T](t, enc.MatchSet, src, types.FilterModeIn)
			})

			// not in set
			t.Run("NI", func(t *testing.T) {
				testIntCompareFunc3[T](t, enc.MatchNotSet, src, types.FilterModeNotIn)
			})
		})
	}
}

type IntCompareFunc[T types.Integer] func(T, *Bitset, *Bitset) *Bitset
type IntCompareFunc2[T types.Integer] func(T, T, *Bitset, *Bitset) *Bitset
type IntCompareFunc3[T types.Integer] func(any, *Bitset, *Bitset) *Bitset

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
			t.Logf("Val %d: %d ", i, v)
		}
		t.Logf("Bitset %x", bits.Bytes())
	}
	switch mode {
	case types.FilterModeEqual:
		for i, v := range vals {
			require.Equal(t, v == val, bits.IsSet(i), "bit=%d val=%d %s %d", i, v, mode, val)
		}

	case types.FilterModeNotEqual:
		for i, v := range vals {
			require.Equal(t, v != val, bits.IsSet(i), "bit=%d val=%d %s %d", i, v, mode, val)
		}

	case types.FilterModeLt:
		for i, v := range vals {
			require.Equal(t, v < val, bits.IsSet(i), "bit=%d val=%d %s %d", i, v, mode, val)
		}

	case types.FilterModeLe:
		for i, v := range vals {
			require.Equal(t, v <= val, bits.IsSet(i), "bit=%d val=%d %s %d", i, v, mode, val)
		}

	case types.FilterModeGt:
		for i, v := range vals {
			require.Equal(t, v > val, bits.IsSet(i), "bit=%d val=%d %s %d", i, v, mode, val)
		}

	case types.FilterModeGe:
		for i, v := range vals {
			require.Equal(t, v >= val, bits.IsSet(i), "bit=%d val=%d %s %d", i, v, mode, val)
		}

	case types.FilterModeRange:
		for i, v := range vals {
			require.Equal(t, v >= val && v <= val2, bits.IsSet(i), "bit=%d val=%d %s [%d,%d]", i, v, mode, val, val2)
		}

	case types.FilterModeIn:
		for i, v := range vals {
			require.Equal(t, set.Contains(uint64(v)), bits.IsSet(i), "bit=%d val=%d %s %v", i, v, mode, set.ToArray())
		}

	case types.FilterModeNotIn:
		for i, v := range vals {
			require.Equal(t, !set.Contains(uint64(v)), bits.IsSet(i), "bit=%d val=%d %s %v", i, v, mode, set.ToArray())
		}
	}
}

func TestEncodeConstInt(t *testing.T) {
	testIntContainerType[int64](t, TIntegerConstant)
	testIntContainerType[uint64](t, TIntegerConstant)
	testIntContainerType[int32](t, TIntegerConstant)
	testIntContainerType[uint32](t, TIntegerConstant)
	testIntContainerType[int16](t, TIntegerConstant)
	testIntContainerType[uint16](t, TIntegerConstant)
	testIntContainerType[int8](t, TIntegerConstant)
	testIntContainerType[uint8](t, TIntegerConstant)
}

func TestEncodeDelta(t *testing.T) {
	testIntContainerType[int64](t, TIntegerDelta)
	testIntContainerType[uint64](t, TIntegerDelta)
	testIntContainerType[int32](t, TIntegerDelta)
	testIntContainerType[uint32](t, TIntegerDelta)
	testIntContainerType[int16](t, TIntegerDelta)
	testIntContainerType[uint16](t, TIntegerDelta)
	testIntContainerType[int8](t, TIntegerDelta)
	testIntContainerType[uint8](t, TIntegerDelta)
}

func TestEncodeRawInt(t *testing.T) {
	testIntContainerType[int64](t, TIntegerRaw)
	testIntContainerType[uint64](t, TIntegerRaw)
	testIntContainerType[int32](t, TIntegerRaw)
	testIntContainerType[uint32](t, TIntegerRaw)
	testIntContainerType[int16](t, TIntegerRaw)
	testIntContainerType[uint16](t, TIntegerRaw)
	testIntContainerType[int8](t, TIntegerRaw)
	testIntContainerType[uint8](t, TIntegerRaw)
}

func TestEncodeBitpack(t *testing.T) {
	testIntContainerType[int64](t, TIntegerBitpacked)
	testIntContainerType[uint64](t, TIntegerBitpacked)
	testIntContainerType[int32](t, TIntegerBitpacked)
	testIntContainerType[uint32](t, TIntegerBitpacked)
	testIntContainerType[int16](t, TIntegerBitpacked)
	testIntContainerType[uint16](t, TIntegerBitpacked)
	testIntContainerType[int8](t, TIntegerBitpacked)
	testIntContainerType[uint8](t, TIntegerBitpacked)
}

func TestEncodeDict(t *testing.T) {
	testIntContainerType[int64](t, TIntegerDictionary)
	testIntContainerType[uint64](t, TIntegerDictionary)
	testIntContainerType[int32](t, TIntegerDictionary)
	testIntContainerType[uint32](t, TIntegerDictionary)
	testIntContainerType[int16](t, TIntegerDictionary)
	testIntContainerType[uint16](t, TIntegerDictionary)
	testIntContainerType[int8](t, TIntegerDictionary)
	testIntContainerType[uint8](t, TIntegerDictionary)
}

func TestEncodeRun(t *testing.T) {
	testIntContainerType[int64](t, TIntegerRunEnd)
	testIntContainerType[uint64](t, TIntegerRunEnd)
	testIntContainerType[int32](t, TIntegerRunEnd)
	testIntContainerType[uint32](t, TIntegerRunEnd)
	testIntContainerType[int16](t, TIntegerRunEnd)
	testIntContainerType[uint16](t, TIntegerRunEnd)
	testIntContainerType[int8](t, TIntegerRunEnd)
	testIntContainerType[uint8](t, TIntegerRunEnd)
}

func TestEncodeSimple8(t *testing.T) {
	testIntContainerType[int64](t, TIntegerSimple8)
	testIntContainerType[uint64](t, TIntegerSimple8)
	testIntContainerType[int32](t, TIntegerSimple8)
	testIntContainerType[uint32](t, TIntegerSimple8)
	testIntContainerType[int16](t, TIntegerSimple8)
	testIntContainerType[uint16](t, TIntegerSimple8)
	testIntContainerType[int8](t, TIntegerSimple8)
	testIntContainerType[uint8](t, TIntegerSimple8)
}

func TestEncodeInt(t *testing.T) {
	testEncodeIntT[int64](t)
	testEncodeIntT[uint64](t)
	testEncodeIntT[int32](t)
	testEncodeIntT[uint32](t)
	testEncodeIntT[int16](t)
	testEncodeIntT[uint16](t)
	testEncodeIntT[int8](t)
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

// ---------------------------------------------
// Benchmarks
//

func BenchmarkAnalyzeInt(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[uint64]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			for range b.N {
				ctx := AnalyzeInt(c.Data, true)
				ctx.Close()
			}
		})
	}
}

func BenchmarkEstimateInt(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[uint64]() {
		ctx := AnalyzeInt(c.Data, true)
		for _, scheme := range []IntegerContainerType{
			TIntegerConstant,
			TIntegerDelta,
			TIntegerRunEnd,
			TIntegerBitpacked,
			TIntegerDictionary,
			TIntegerSimple8,
			TIntegerRaw,
		} {
			b.Run(c.Name+"_"+scheme.String(), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(c.Data) * 8))
				for range b.N {
					_ = EstimateInt(scheme, ctx, c.Data, MAX_CASCADE)
				}
			})
		}
	}
}

func BenchmarkEncodeInt(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []IntegerContainerType{
			TIntegerConstant,
			TIntegerDelta,
			TIntegerRunEnd,
			TIntegerBitpacked,
			TIntegerDictionary,
			TIntegerSimple8,
			TIntegerRaw,
		} {
			data := etests.GenForIntScheme[int64](int(scheme), c.N)
			ctx := AnalyzeInt(data, scheme == TIntegerDictionary)
			once := etests.ShowInfo
			b.Run(c.Name+"_"+scheme.String(), func(b *testing.B) {
				if once  {
					enc := NewInt[int64](scheme).Encode(ctx, data, MAX_CASCADE)
					b.Log(enc.Info())
					enc.Close()
					once = false
				}
				b.ResetTimer()
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for range b.N {
					enc := NewInt[int64](scheme).Encode(ctx, data, MAX_CASCADE)
					enc.Close()
				}
			})
			ctx.Close()
		}
	}
}

func BenchmarkEncodeAndStoreInt(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []IntegerContainerType{
			TIntegerConstant,
			TIntegerDelta,
			TIntegerRunEnd,
			TIntegerBitpacked,
			TIntegerDictionary,
			TIntegerSimple8,
			TIntegerRaw,
		} {
			data := etests.GenForIntScheme[int16](int(scheme), c.N)
			b.Run(c.Name+"_"+scheme.String(), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for range b.N {
					ctx := AnalyzeInt(data, scheme == TIntegerDictionary)
					enc := NewInt[int16](scheme).Encode(ctx, data, MAX_CASCADE)
					sz := enc.MaxSize()
					buf := enc.Store(make([]byte, 0, enc.MaxSize()))
					require.LessOrEqual(b, len(buf), sz)
					enc.Close()
					ctx.Close()
				}
			})
		}
	}
}

func BenchmarkEncodeBestInt(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[uint64]() {
		once := etests.ShowInfo
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			var sz int
			for range b.N {
				enc := EncodeInt(nil, c.Data, MAX_CASCADE)
				sz += enc.MaxSize()
				if once {
					b.Log(enc.Info())
					once = false
				}
				enc.Close()
			}
			b.ReportMetric(float64(sz/b.N), "c(B)")
			b.ReportMetric(100*float64(sz)/float64(b.N*c.N*8), "c(%)")
		})
	}
}

func BenchmarkEncodeLegacyInt(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[uint64]() {
		buf := bytes.NewBuffer(make([]byte, zip.Int64EncodedSize(len(c.Data))))
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			var sz int
			for range b.N {
				n, _ := zip.EncodeUint64(c.Data, buf)
				sz += n
				buf.Reset()
			}
			b.ReportMetric(float64(sz/b.N), "c(B)")
			b.ReportMetric(100*float64(sz)/float64(b.N*c.N*8), "c(%)")
		})
	}
}

func BenchmarkAppendToInt(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []IntegerContainerType{
			TIntegerConstant,
			TIntegerDelta,
			TIntegerRunEnd,
			TIntegerBitpacked,
			TIntegerDictionary,
			TIntegerSimple8,
			TIntegerRaw,
		} {
			data := etests.GenForIntScheme[int64](int(scheme), c.N)
			ctx := AnalyzeInt(data, true)
			enc := NewInt[int64](scheme).Encode(ctx, data, MAX_CASCADE)
			buf := enc.Store(make([]byte, 0, enc.MaxSize()))
			dst := make([]int64, 0, c.N)
			all := tests.GenSeq[uint32](c.N)

			b.Run(c.Name+"_"+scheme.String(), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for range b.N {
					enc2 := NewInt[int64](scheme)
					_, err := enc2.Load(buf)
					require.NoError(b, err)
					dst = enc2.AppendTo(all, dst)
					dst = dst[:0]
					enc2.Close()
				}
			})
		}
	}
}

// -----------------------------------------------
// Microbenchmarks
//

func BenchmarkUniqueMap(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := tests.GenRnd[int16](c.N)
		var card int
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 2))
			for range b.N {
				u := make(map[int16]struct{}, c.N)
				for _, v := range data {
					u[v] = struct{}{}
				}
				card = len(u)
			}
			_ = card
		})
	}
}

func BenchmarkUniqueArray(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := tests.GenRnd[int16](c.N)
		minx := slices.Min(data)
		maxx := slices.Max(data)
		var card int
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 2))
			for range b.N {
				u := make([]uint16, int(maxx)-int(minx)+1)
				for _, v := range data {
					u[int(v)-int(minx)] = 1
				}
				for _, v := range u {
					if v > 0 {
						card++
					}
				}
			}
		})
	}
}

func BenchmarkUniqueBitset(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := tests.GenRnd[int16](c.N)
		minx := slices.Min(data)
		maxx := slices.Max(data)
		var card int
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 2))
			for range b.N {
				u := bitset.NewBitset(int(maxx) - int(minx) + 1)
				for _, v := range data {
					u.Set(int(v) - int(minx))
				}
				card = u.Count()
			}
		})
		_ = card
	}
}

func BenchmarkUniqueRoaring(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := tests.GenRnd[int16](c.N)
		minx := slices.Min(data)
		var card int
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 2))
			for range b.N {
				u := xroar.NewBitmap()
				for _, v := range data {
					u.Set(uint64(v) - uint64(minx))
				}
				card = u.GetCardinality()
			}
		})
		_ = card
	}
}

func BenchmarkUniqueLLB(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := tests.GenRnd[uint32](c.N)
		var card int
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 2))
			for range b.N {
				flt := llb.NewFilterWithPrecision(8)
				flt.AddMultiUint32(data)
				card = int(flt.Cardinality())
			}
		})
		_ = card
	}
}

func BenchmarkDictArray(b *testing.B) {
	DictArrayBenchmark[uint16](b)
	DictArrayBenchmark[uint8](b)
}

func DictArrayBenchmark[T types.Integer](b *testing.B) {
	for _, p := range tests.BenchmarkPatterns {
		for _, c := range tests.BenchmarkSizes {
			data := tests.GenDups[T](c.N, min(c.N, p.Size), 30)
			ctx := AnalyzeInt(data, true)
			var card int
			b.Run(fmt.Sprintf("%T/%s/%s", T(0), c.Name, p.Name), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * int(unsafe.Sizeof(T(0)))))
				for range b.N {
					dict, codes := dictEncodeArray(ctx, data)
					card = len(dict)
					arena.FreeT(dict)
					arena.FreeT(codes)
				}
				_ = card
			})
		}
	}
}
