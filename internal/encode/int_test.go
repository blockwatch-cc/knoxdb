// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeInt(t *testing.T) {
	// delta, no dups
	x := AnalyzeInt([]int64{-1, 0, 1, 2})
	assert.Equal(t, int64(-1), x.Min, "min")
	assert.Equal(t, int64(2), x.Max, "max")
	assert.Equal(t, int64(1), x.Delta, "delta")
	assert.Equal(t, 64, x.PhyBits, "phybits")
	assert.Equal(t, 2, x.UseBits, "usebits")
	// assert.Equal(t, 4, x.NumUnique, "num_unique")
	assert.InDelta(t, 4, x.NumUnique, 1.0, "num_unique")
	assert.Equal(t, 4, x.NumRuns, "num_runs")
	assert.Equal(t, 4, x.NumValues, "num_values")
	assert.Len(t, x.EligibleSchemes(), 1, "eligible list")
	assert.Contains(t, x.EligibleSchemes(), TIntegerDelta, "delta only")

	// runs
	x = AnalyzeInt([]int64{-1, -1, 5, 5, 1, 1})
	assert.Equal(t, int64(-1), x.Min, "min")
	assert.Equal(t, int64(5), x.Max, "max")
	assert.Equal(t, int64(0), x.Delta, "delta")
	assert.Equal(t, 64, x.PhyBits, "phybits")
	assert.Equal(t, 3, x.UseBits, "usebits")
	assert.InDelta(t, 3, x.NumUnique, 1.0, "num_unique")
	// assert.Equal(t, 3, x.NumUnique, "num_unique")
	assert.Equal(t, 3, x.NumRuns, "num_runs")
	assert.Equal(t, 6, x.NumValues, "num_values")
	assert.Contains(t, x.EligibleSchemes(), TIntegerRunEnd, "eligible")
	assert.Contains(t, x.EligibleSchemes(), TIntegerBitpacked, "eligible")
	assert.Contains(t, x.EligibleSchemes(), TIntegerRaw, "eligible")
	assert.Contains(t, x.EligibleSchemes(), TIntegerDictionary, "eligible")
	assert.Contains(t, x.EligibleSchemes(), TIntegerSimple8, "eligible")

	// dict-friendly
	x = AnalyzeInt([]int64{-1, 1, 5, 1, -1, 1})
	assert.Equal(t, int64(-1), x.Min, "min")
	assert.Equal(t, int64(5), x.Max, "max")
	assert.Equal(t, int64(0), x.Delta, "delta")
	assert.Equal(t, 64, x.PhyBits, "phybits")
	assert.Equal(t, 3, x.UseBits, "usebits")
	// assert.Equal(t, 3, x.NumUnique, "num_unique")
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
	t.Helper()
	for _, c := range tests.MakeShortIntTests[T](int(scheme)) {
		t.Run(c.Name, func(t *testing.T) {
			enc := NewInt[T](scheme)

			// analyze and encode data into container
			ctx := AnalyzeInt(c.Data)
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
	t.Helper()
	for _, c := range tests.MakeIntTests[T](1024) {
		t.Run(c.Name, func(t *testing.T) {
			x := AnalyzeInt(c.Data)
			e := EncodeInt(x, c.Data, MAX_CASCADE)
			require.Equal(t, len(c.Data), e.Len(), "x=%#v", x)
			for i, v := range c.Data {
				require.Equal(t, v, e.Get(i), "i=%d d=%x", i, c.Data)
			}
		})
	}
}

func BenchmarkAnalyzeInt(b *testing.B) {
	for _, c := range tests.Benchmarks {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			for i := 0; i < b.N; i++ {
				_ = AnalyzeInt(c.Data)
			}
		})
	}
}

func BenchmarkEstimateInt(b *testing.B) {
	for _, c := range tests.Benchmarks {
		ctx := AnalyzeInt(c.Data)
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
	for _, c := range tests.Benchmarks {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			for i := 0; i < b.N; i++ {
				_ = EncodeInt(nil, c.Data, MAX_CASCADE)
			}
		})
	}
}
