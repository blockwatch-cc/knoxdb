// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"
	"fmt"
	"testing"

	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/zip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeFloat(t *testing.T) {
	// runs
	x := AnalyzeFloat([]float64{-1.044, -1.044, 5.245, 5.245, 1.50, 1.50}, true, true)
	assert.Equal(t, float64(-1.044), x.Min, "min")
	assert.Equal(t, float64(5.245), x.Max, "max")
	assert.InDelta(t, 3, x.NumUnique, 1.0, "num_unique")
	assert.Equal(t, 3, x.NumRuns, "num_runs")
	assert.Equal(t, 6, x.NumValues, "num_values")
	assert.Contains(t, x.EligibleSchemes(MAX_CASCADE), TFloatRunEnd, "eligible")
	assert.Contains(t, x.EligibleSchemes(MAX_CASCADE), TFloatRaw, "eligible")
	assert.Contains(t, x.EligibleSchemes(MAX_CASCADE), TFloatDictionary, "eligible")

	// dict-friendly
	x = AnalyzeFloat([]float64{-1.05, 1.05, 5.05, 1.05, -1.05, 1.05}, true, false)
	assert.Equal(t, float64(-1.05), x.Min, "min")
	assert.Equal(t, float64(5.05), x.Max, "max")
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
				require.Equal(t, v, e.Get(i), "i=%d d=%x", i, c.Data)
			}
		})
	}
}

func TestEncodeFloat(t *testing.T) {
	testEncodeFloatT[float64](t)
	testEncodeFloatT[float32](t)
}

func BenchmarkAnalyzeFloat(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[float64]() {
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			for range b.N {
				ctx := AnalyzeFloat(c.Data, true, true)
				ctx.Close()
			}
		})
	}
}

func BenchmarkEstimateFloat(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[float64]() {
		ctx := AnalyzeFloat(c.Data, true, true)
		for _, scheme := range []FloatContainerType{
			TFloatConstant,
			TFloatRunEnd,
			TFloatDictionary,
			TFloatAlp,
			TFloatAlpRd,
			TFloatRaw,
		} {
			b.Run(c.Name+"_"+scheme.String(), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(c.Data) * 8))
				for range b.N {
					_ = EstimateFloat(scheme, ctx, c.Data, MAX_CASCADE)
				}
			})
		}
	}
}

func BenchmarkEncodeFloat(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []FloatContainerType{
			TFloatConstant,
			TFloatRunEnd,
			TFloatDictionary,
			TFloatAlp,
			TFloatAlpRd,
			TFloatRaw,
		} {
			data := etests.GenForFloatScheme[float64](int(scheme), c.N)
			ctx := AnalyzeFloat(data, scheme == TFloatDictionary, scheme == TFloatAlp)
			once := etests.ShowInfo
			b.Run(c.Name+"_"+scheme.String(), func(b *testing.B) {
				if once && etests.ShowInfo {
					enc := NewFloat[float64](scheme).Encode(ctx, data, MAX_CASCADE)
					b.Log(enc.Info())
					enc.Close()
					once = false
				}
				b.ResetTimer()
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for range b.N {
					enc := NewFloat[float64](scheme).Encode(ctx, data, MAX_CASCADE)
					enc.Close()
				}
			})
			ctx.Close()
		}
	}
}

func BenchmarkEncodeAndStoreFloat(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []FloatContainerType{
			TFloatConstant,
			TFloatRunEnd,
			TFloatDictionary,
			TFloatAlp,
			TFloatAlpRd,
			TFloatRaw,
		} {
			data := etests.GenForFloatScheme[float64](int(scheme), c.N)
			b.Run(c.Name+"_"+scheme.String(), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for range b.N {
					ctx := AnalyzeFloat(data, scheme == TFloatDictionary, scheme == TFloatAlp)
					enc := NewFloat[float64](scheme).Encode(ctx, data, MAX_CASCADE)
					_ = enc.Store(make([]byte, 0, enc.Size()))
					enc.Close()
					ctx.Close()
				}
			})
		}
	}
}

func BenchmarkEncodeBestFloat(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[float64]() {
		once := etests.ShowInfo
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			var sz int
			for range b.N {
				enc := EncodeFloat(nil, c.Data, MAX_CASCADE)
				sz += enc.Size()
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

func BenchmarkEncodeLegacyFloat(b *testing.B) {
	for _, c := range tests.MakeBenchmarks[float64]() {
		buf := bytes.NewBuffer(make([]byte, zip.Int64EncodedSize(len(c.Data))))
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(c.Data) * 8))
			var sz int
			for range b.N {
				n, _ := zip.EncodeFloat64(c.Data, buf)
				sz += n
				buf.Reset()
			}
			b.ReportMetric(float64(sz/b.N), "c(B)")
			b.ReportMetric(100*float64(sz)/float64(b.N*c.N*8), "c(%)")
		})
	}
}

func BenchmarkAppendToFloat(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		for _, scheme := range []FloatContainerType{
			TFloatConstant,
			TFloatRunEnd,
			TFloatDictionary,
			TFloatAlp,
			TFloatAlpRd,
			TFloatRaw,
		} {
			data := etests.GenForFloatScheme[float64](int(scheme), c.N)
			ctx := AnalyzeFloat(data, scheme == TFloatDictionary, scheme == TFloatAlp)
			enc := NewFloat[float64](scheme).Encode(ctx, data, MAX_CASCADE)
			buf := enc.Store(make([]byte, 0, enc.Size()))
			dst := make([]float64, 0, c.N)
			all := tests.GenSeq[uint32](c.N)

			b.Run(c.Name+"_"+scheme.String(), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(c.N * 8))
				for range b.N {
					enc2 := NewFloat[float64](scheme)
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
