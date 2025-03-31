// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"
	"slices"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/filter/loglogbeta"
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
	assert.Contains(t, x.EligibleSchemes(), TIntegerRunEnd, "eligible")
	assert.Contains(t, x.EligibleSchemes(), TIntegerBitpacked, "eligible")
	assert.Contains(t, x.EligibleSchemes(), TIntegerRaw, "eligible")
	assert.Contains(t, x.EligibleSchemes(), TIntegerDictionary, "eligible")
	assert.Contains(t, x.EligibleSchemes(), TIntegerSimple8, "eligible")

	// dict-friendly
	x = AnalyzeInt([]int64{-1, 1, 5, 1, -1, 1}, true)
	assert.Equal(t, int64(-1), x.Min, "min")
	assert.Equal(t, int64(5), x.Max, "max")
	assert.Equal(t, int64(0), x.Delta, "delta")
	assert.Equal(t, 64, x.PhyBits, "phybits")
	assert.Equal(t, 3, x.UseBits, "usebits")
	assert.InDelta(t, 3, x.NumUnique, 1.0, "num_unique")
	assert.Equal(t, 6, x.NumRuns, "num_runs")
	assert.Equal(t, 6, x.NumValues, "num_values")
	assert.NotContains(t, x.EligibleSchemes(), TIntegerRunEnd, "not eligible")
	assert.Contains(t, x.EligibleSchemes(), TIntegerBitpacked, "eligible")
	assert.Contains(t, x.EligibleSchemes(), TIntegerRaw, "eligible")
	assert.Contains(t, x.EligibleSchemes(), TIntegerDictionary, "eligible")
	assert.Contains(t, x.EligibleSchemes(), TIntegerSimple8, "eligible")
}

func testIntContainerType[T types.Integer](t *testing.T, scheme IntegerContainerType) {
	for _, c := range tests.MakeShortIntTests[T](int(scheme)) {
		t.Run(c.Name, func(t *testing.T) {
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
			assert.Equal(t, dst, c.Data)

			enc2.Close()
			enc.Close()
		})
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
	for _, c := range tests.MakeIntTests[T](1024) {
		t.Run(c.Name, func(t *testing.T) {
			x := AnalyzeInt(c.Data, true)
			e := EncodeInt(x, c.Data, MAX_CASCADE)
			require.Equal(t, len(c.Data), e.Len(), "x=%#v", x)
			for i, v := range c.Data {
				require.Equal(t, v, e.Get(i), "i=%d d=%x", i, c.Data)
			}
		})
	}
}

func BenchmarkAnalyzeInt(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[uint64]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			for i := 0; i < b.N; i++ {
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
				for i := 0; i < b.N; i++ {
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
			data := tests.GenForScheme[int64](int(scheme), c.N)
			b.Run(c.Name+"_"+scheme.String(), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for i := 0; i < b.N; i++ {
					ctx := AnalyzeInt(data, scheme == TIntegerDictionary)
					enc := NewInt[int64](scheme).Encode(ctx, data, MAX_CASCADE)
					sz := enc.MaxSize()
					buf := enc.Store(make([]byte, 0, enc.MaxSize()))
					require.Less(b, len(buf), sz)
					enc.Close()
					ctx.Close()
				}
			})
		}
	}
}

func BenchmarkEncodeBestInt(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[uint64]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			for i := 0; i < b.N; i++ {
				enc := EncodeInt(nil, c.Data, MAX_CASCADE)
				enc.Close()
			}
		})
	}
}

func BenchmarkEncodeLegacyInt(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[uint64]() {
		buf := bytes.NewBuffer(make([]byte, zip.Int64EncodedSize(len(c.Data))))
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			for i := 0; i < b.N; i++ {
				_, _ = zip.EncodeUint64(c.Data, buf)
				buf.Reset()
			}
		})
	}
}

func BenchmarkAppendTo(b *testing.B) {
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
			data := tests.GenForScheme[int64](int(scheme), c.N)
			ctx := AnalyzeInt(data, true)
			enc := NewInt[int64](scheme).Encode(ctx, data, MAX_CASCADE)
			buf := enc.Store(make([]byte, 0, enc.MaxSize()))
			dst := make([]int64, 0, c.N)
			all := tests.GenSeq[uint32](c.N)

			b.Run(c.Name+"_"+scheme.String(), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for i := 0; i < b.N; i++ {
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

func BenchmarkUniqueMap(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := util.RandInts[int16](c.N)
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
		data := util.RandInts[int16](c.N)
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
		data := util.RandInts[int16](c.N)
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
		data := util.RandInts[int16](c.N)
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
		data := util.RandInts[int16](c.N)
		var card int
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 2))
			for range b.N {
				flt := loglogbeta.NewFilterWithPrecision(8)
				flt.AddManyInt16(data)
				card = int(flt.Cardinality())
			}
		})
		_ = card
	}
}
