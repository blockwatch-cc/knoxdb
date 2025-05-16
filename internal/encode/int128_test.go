// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestInt128Encode(t *testing.T) {
	for _, c := range MakeInt128Tests(16) {
		t.Run(c.Name, func(t *testing.T) {
			// analyze and encode data into container
			enc := EncodeInt128(c.Data)
			t.Log(enc.Info())

			// validate contents
			require.Equal(t, c.N, enc.Len(), "T=%s", enc)
			for i, v := range c.Data.Iterator() {
				require.Equal(t, v, enc.Get(i))
			}

			// serialize to buffer
			buf := make([]byte, 0, enc.Size())
			buf = enc.Store(buf)
			require.NotNil(t, buf)

			// load back into new container
			enc2 := NewInt128()
			buf, err := enc2.Load(buf)
			require.NoError(t, err)
			require.Len(t, buf, 0)

			// validate contents
			require.Equal(t, c.N, enc2.Len(), "T=%s", enc)
			for i, v := range c.Data.Iterator() {
				require.Equal(t, v, enc2.Get(i))
			}

			// validate append
			dst := num.MakeInt128Stride(c.N)
			dst = enc.AppendTo(nil, dst)
			require.Equal(t, c.N, dst.Len())
			require.Equal(t, c.Data, dst)

			// validate append selector
			sel := util.RandUintsn[uint32](max(1, c.N/2), uint32(c.N))
			clear(dst.X0)
			clear(dst.X1)
			dst = enc2.AppendTo(sel, dst)
			require.Equal(t, len(sel), dst.Len())
			for i, v := range sel {
				require.Equal(t, c.Data.Elem(int(v)), dst.Elem(i), "sel[%d]", v)
			}

			enc2.Close()
			enc.Close()
		})
	}
}

func TestInt128Iterator(t *testing.T) {
	for _, sz := range etests.ItSizes {
		for _, c := range MakeInt128Tests(sz) {
			t.Run(fmt.Sprintf("%s/%d", c.Name, sz), func(t *testing.T) {
				// setup
				src := c.Data
				enc := NewInt128()
				enc.Encode(src)
				t.Logf("Enc %s", enc.Info())
				it := enc.Iterator()
				if it == nil {
					t.Skip()
				}

				// --------------------------
				// test next
				//
				for i, v := range src.Iterator() {
					val, ok := it.Next()
					require.True(t, ok, "short iterator at pos %d", i)
					require.Equal(t, v, val, "invalid val=%d pos=%d src=%d", val, i, src.Elem(i))
				}

				// --------------------------
				// test reset
				//
				it.Reset()
				require.Equal(t, c.N, it.Len(), "bad it len post reset")
				for i, v := range src.Iterator() {
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
					require.LessOrEqual(t, seen+n, c.N, "next chunk returned too large n")
					for i, v := range dst.Subslice(0, n).Iterator() {
						require.Equal(t, src.Elem(seen+i), v, "invalid val=%d pos=%d src=%d", v, seen+i, src.Elem(seen+i))
					}
					seen += n
				}
				require.Equal(t, c.N, seen, "next chunk did not return all values")

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
					require.LessOrEqual(t, seen+n, c.N, "next chunk returned too large n")
					for i, v := range dst.Subslice(0, n).Iterator() {
						require.Equal(t, src.Elem(seen+i), v, "invalid val=%d pos=%d src=%d after skip", v, seen+i, src.Elem(seen+i))
					}
					seen += n
				}
				require.Equal(t, c.N, seen, "skip&next chunk did not return all values")

				// --------------------------
				// test seek
				//
				it.Reset()
				for range c.N {
					i := util.RandIntn(c.N)
					ok := it.Seek(i)
					require.True(t, ok, "seek to existing pos %d/%d failed", i, c.N)
					val, ok := it.Next()
					require.True(t, ok, "next after seek to existing pos %d/%d failed", i, c.N)
					require.Equal(t, src.Elem(i), val, "invalid val=%d pos=%d after seek", val, i)
				}

				// seek to invalid values
				require.False(t, it.Seek(-1), "seek to negative")
				_, ok := it.Next()
				require.False(t, ok, "next after bad seek")

				require.False(t, it.Seek(c.N), "seek to end")
				_, ok = it.Next()
				require.False(t, it.Seek(c.N), "seek to end")

				require.False(t, it.Seek(c.N+1), "seek beyond end")
				_, ok = it.Next()
				require.False(t, it.Seek(c.N), "seek to end")

				it.Close()
			})
			if t.Failed() {
				t.FailNow()
			}
		}
	}
}

func TestInt128Compare(t *testing.T) {
	// validate matchers
	for _, sz := range etests.CompareSizes {
		for _, c := range MakeInt128Tests(sz) {
			t.Run(fmt.Sprintf("%s/%d", c.Name, sz), func(t *testing.T) {
				src := c.Data
				enc := NewInt128()
				enc.Encode(src)
				t.Logf("Info: %s", enc.Info())

				// equal
				t.Run("EQ", func(t *testing.T) {
					i128TestCompare(t, enc.MatchEqual, src, types.FilterModeEqual)
				})

				// not equal
				t.Run("NE", func(t *testing.T) {
					i128TestCompare(t, enc.MatchNotEqual, src, types.FilterModeNotEqual)
				})

				// less
				t.Run("LT", func(t *testing.T) {
					i128TestCompare(t, enc.MatchLess, src, types.FilterModeLt)
				})

				// less equal
				t.Run("LE", func(t *testing.T) {
					i128TestCompare(t, enc.MatchLessEqual, src, types.FilterModeLe)
				})

				// greater
				t.Run("GT", func(t *testing.T) {
					i128TestCompare(t, enc.MatchGreater, src, types.FilterModeGt)
				})

				// greater equal
				t.Run("GE", func(t *testing.T) {
					i128TestCompare(t, enc.MatchGreaterEqual, src, types.FilterModeGe)
				})

				// between
				t.Run("RG", func(t *testing.T) {
					i128TestCompare2(t, enc.MatchBetween, src, types.FilterModeRange)
				})

			})
			if t.Failed() {
				t.FailNow()
			}
		}
	}
}

type TestCaseInt128 struct {
	Name string
	N    int
	Data num.Int128Stride
}

func MakeInt128Tests(n int) []TestCaseInt128 {
	return []TestCaseInt128{
		{"const", n, num.Int128Stride{
			X0: tests.GenConst[int64](n, 0),
			X1: tests.GenConst[uint64](n, 42),
		}},
		{"delta-", n, num.Int128Stride{
			X0: tests.GenConst[int64](n, 0),
			X1: tests.GenSeq[uint64](n, 1),
		}},
		{"delta+", n, num.Int128Stride{
			X0: tests.GenConst[int64](n, -1),
			X1: tests.GenSeq[uint64](n, -1),
		}},
		{"dups", n, num.Int128Stride{
			X0: tests.GenConst[int64](n, 1),
			X1: tests.GenDups[uint64](n, n/10, -1),
		}},
		{"runs", n, num.Int128Stride{
			X0: tests.GenConst[int64](n, 1),
			X1: tests.GenRuns[uint64](n, min(n, 5), -1),
		}},
		{"rand", n, num.Int128Stride{
			X0: tests.GenRndBits[int64](n, 5),
			X1: tests.GenRnd[uint64](n),
		}},
	}
}

func i128EnsureBits(t *testing.T, vals num.Int128Stride, val, val2 num.Int128, bits *bitset.Bitset, mode types.FilterMode) {
	if etests.ShowValues {
		for i, v := range vals.Iterator() {
			t.Logf("Val %d: %v", i, v)
		}
		t.Logf("Bitset %x", bits.Bytes())
	}
	minv, maxv := vals.MinMax()
	switch mode {
	case types.FilterModeEqual:
		for i, v := range vals.Iterator() {
			require.Equal(t, v == val, bits.IsSet(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeNotEqual:
		for i, v := range vals.Iterator() {
			require.Equal(t, v != val, bits.IsSet(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeLt:
		for i, v := range vals.Iterator() {
			require.Equal(t, v.Lt(val), bits.IsSet(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeLe:
		for i, v := range vals.Iterator() {
			require.Equal(t, v.Le(val), bits.IsSet(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeGt:
		for i, v := range vals.Iterator() {
			require.Equal(t, v.Gt(val), bits.IsSet(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeGe:
		for i, v := range vals.Iterator() {
			require.Equal(t, v.Ge(val), bits.IsSet(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeRange:
		for i, v := range vals.Iterator() {
			require.Equal(t, v.Ge(val) && v.Le(val2), bits.IsSet(i), "bit=%d val=%v %s [%v,%v] min=%v max=%v",
				i, v, mode, val, val2, minv, maxv)
		}
	}
}

type i128CompareFunc func(num.Int128, *Bitset, *Bitset)
type i128CompareFunc2 func(num.Int128, num.Int128, *Bitset, *Bitset)

func i128TestCompare(t *testing.T, cmp i128CompareFunc, src num.Int128Stride, mode types.FilterMode) {
	bits := bitset.NewBitset(src.Len())
	minv, maxv := src.MinMax()

	// single value
	val := src.Elem(src.Len() / 2)
	cmp(val, bits, nil)
	i128EnsureBits(t, src, val, val, bits, mode)
	bits.Zero()
	require.Equal(t, 0, bits.Count(), "cleared")

	// value over bounds
	if maxv.Lt(num.MaxInt128) {
		over := maxv.Add64(1)
		cmp(over, bits, nil)
		i128EnsureBits(t, src, over, over, bits, mode)
		bits.Zero()
		require.Equal(t, 0, bits.Count(), "cleared")
	}

	// value under bounds
	if minv.Gt(num.MinInt128) {
		under := minv.Sub64(1)
		cmp(under, bits, nil)
		i128EnsureBits(t, src, under, under, bits, mode)
		bits.Zero()
		require.Equal(t, 0, bits.Count(), "cleared")
	}
}

func i128TestCompare2(t *testing.T, cmp i128CompareFunc2, src num.Int128Stride, mode types.FilterMode) {
	bits := bitset.NewBitset(src.Len())
	minv, maxv := src.MinMax()

	// single value
	val := src.Elem(src.Len() / 2)
	cmp(val, val, bits, nil)
	i128EnsureBits(t, src, val, val, bits, mode)
	bits.Zero()
	require.Equal(t, 0, bits.Count(), "cleared")

	// full range
	cmp(minv, maxv, bits, nil)
	i128EnsureBits(t, src, minv, maxv, bits, mode)
	bits.Zero()

	// partial range
	from, to := num.Max128(val.Div64(2), minv.Add64(1)), num.Min128(val.Mul64(2), maxv.Sub64(1))
	if from.Gt(to) {
		from, to = to, from
	}
	// skip test if values would wrap around
	if from.Gt(minv) && to.Lt(maxv) {
		cmp(from, to, bits, nil)
		i128EnsureBits(t, src, from, to, bits, mode)
		bits.Zero()
	}

	// out of bounds (over)
	if maxv.Lt(num.MaxInt128) {
		val := maxv.Add64(1)
		cmp(val, val, bits, nil)
		i128EnsureBits(t, src, val, val, bits, mode)
		bits.Zero()
	}

	// out of bounds (under)
	if minv.Gt(num.MinInt128.Add64(2)) {
		val := minv.Sub64(1)
		cmp(val, val, bits, nil)
		i128EnsureBits(t, src, val, val, bits, mode)
		bits.Zero()
	}
}

// ---------------------------------------------
// Benchmarks
//

func GenInt128Data(n int) num.Int128Stride {
	return num.Int128Stride{
		X0: tests.GenRndBits[int64](n, 5),
		X1: tests.GenRnd[uint64](n),
	}
}

func BenchmarkInt128Encode(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := GenInt128Data(c.N)
		once := etests.ShowInfo
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 16))
			var sz int
			for b.Loop() {
				enc := NewInt128().Encode(data)
				if once {
					b.Log(enc.Info())
					once = false
				}
				sz += enc.Size()
				enc.Close()
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			b.ReportMetric(float64(sz*8/b.N/c.N), "bits/val")
			b.ReportMetric(100*float64(sz)/float64(b.N*c.N*16), "c(%)")
		})
	}
}

func BenchmarkInt128EncodeAndStore(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := GenInt128Data(c.N)
		once := etests.ShowInfo
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 16))
			var sz int
			for b.Loop() {
				enc := NewInt128().Encode(data)
				buf := enc.Store(make([]byte, 0, enc.Size()))
				require.LessOrEqual(b, len(buf), enc.Size())
				if once {
					b.Log(enc.Info())
					once = false
				}
				sz += enc.Size()
				enc.Close()
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
			b.ReportMetric(float64(sz*8/b.N/c.N), "bits/val")
			b.ReportMetric(100*float64(sz)/float64(b.N*c.N*16), "c(%)")
		})
	}
}

func BenchmarkInt128Decode(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := GenInt128Data(c.N)
		enc := NewInt128().Encode(data)
		buf := enc.Store(make([]byte, 0, enc.Size()))
		dst := num.MakeInt128Stride(c.N)
		once := etests.ShowInfo
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 16))
			for b.Loop() {
				enc2, err := LoadInt128(buf)
				require.NoError(b, err)
				dst = enc2.AppendTo(nil, dst)
				if once {
					b.Log(enc2.Info())
					once = false
				}
				enc2.Close()
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkInt128Append(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := GenInt128Data(c.N)
		enc := NewInt128().Encode(data)
		buf := enc.Store(make([]byte, 0, enc.Size()))
		dst := num.MakeInt128Stride(c.N)
		once := etests.ShowInfo
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 16))
			for b.Loop() {
				enc2, err := LoadInt128(buf)
				require.NoError(b, err)
				dst = enc2.AppendTo(nil, dst)
				if once {
					b.Log(enc2.Info())
					once = false
				}
				enc2.Close()
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkInt128Cmp(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := GenInt128Data(c.N)
		enc := NewInt128().Encode(data)
		bits := bitset.NewBitset(c.N)
		b.Log(enc.Info())
		b.Run(c.Name, func(b *testing.B) {
			b.SetBytes(int64(c.N * 16))
			for b.Loop() {
				enc.MatchEqual(data.Elem(0), bits, nil)
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}

func BenchmarkInt128Iterator(b *testing.B) {
	for _, c := range tests.BenchmarkSizes {
		data := GenInt128Data(c.N)
		enc := NewInt128().Encode(data)
		buf := enc.Store(make([]byte, 0, enc.Size()))
		once := etests.ShowInfo
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(c.N * 16))
			for b.Loop() {
				enc2, err := LoadInt128(buf)
				require.NoError(b, err)
				if once {
					b.Log(enc2.Info())
					once = false
				}
				it := enc2.Iterator()
				for {
					_, n := it.NextChunk()
					if n == 0 {
						break
					}
				}
				it.Close()
				enc2.Close()
			}
			b.ReportMetric(float64(c.N*b.N)/float64(b.Elapsed().Nanoseconds()), "vals/ns")
		})
	}
}
