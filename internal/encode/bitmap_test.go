// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestBitmapEncode(t *testing.T) {
	for _, c := range MakeBitmapTests(129) {
		t.Run(c.Name, func(t *testing.T) {
			// analyze and encode data into container
			ctx := AnalyzeBitmap(c.Data)
			enc := EncodeBitmap(ctx, c.Data)
			t.Log(enc.Info())

			// validate contents
			require.Equal(t, c.N, enc.Len(), "T=%s", enc.Info())
			for i := range c.N {
				require.Equal(t, c.Data.Contains(i), enc.Get(i), "bit %d", i)
			}

			// validate iterator
			for i := range enc.Iterator() {
				require.True(t, enc.Get(i))
			}

			// serialize to buffer
			buf := make([]byte, 0, enc.Size())
			buf = enc.Store(buf)
			require.NotNil(t, buf)

			// load back into new container
			enc2 := NewBitmap()
			buf, err := enc2.Load(buf)
			require.NoError(t, err)
			require.Len(t, buf, 0)

			// validate contents
			require.Equal(t, c.N, enc2.Len(), "T=%s", enc2)
			for i := range c.N {
				require.Equal(t, c.Data.Contains(i), enc2.Get(i), "bit %d", i)
			}

			// validate iterator
			for i := range enc2.Iterator() {
				require.True(t, enc2.Get(i))
			}

			// validate append
			dst := bitset.New(c.N).Resize(0)
			enc.AppendTo(dst, nil)
			require.Equal(t, c.N, dst.Len())
			require.Equal(t, c.Data.Bytes(), dst.Bytes())

			// validate append selector
			sel := util.RandUintsn[uint32](max(1, c.N/2), uint32(c.N))
			enc2.AppendTo(dst.Resize(0), sel)
			require.Equal(t, len(sel), dst.Len())
			for i, v := range sel {
				require.Equal(t, c.Data.Contains(int(v)), dst.Contains(i), "sel[%d]", v)
			}

			enc2.Close()
			enc.Close()
			ctx.Close()
		})
	}
}

func TestBitmapCompare(t *testing.T) {
	// validate matchers
	for _, sz := range etests.CompareSizes {
		for _, c := range MakeBitmapTests(sz) {
			t.Run(fmt.Sprintf("%s/%d", c.Name, sz), func(t *testing.T) {
				src := c.Data
				enc := NewBitmap()
				enc.Encode(nil, src)
				t.Logf("Info: %s", enc.Info())

				// equal
				t.Run("EQ", func(t *testing.T) {
					bitmapTestCompare(t, enc.MatchEqual, src, types.FilterModeEqual)
				})

				// not equal
				t.Run("NE", func(t *testing.T) {
					bitmapTestCompare(t, enc.MatchNotEqual, src, types.FilterModeNotEqual)
				})

				// less
				t.Run("LT", func(t *testing.T) {
					bitmapTestCompare(t, enc.MatchLess, src, types.FilterModeLt)
				})

				// less equal
				t.Run("LE", func(t *testing.T) {
					bitmapTestCompare(t, enc.MatchLessEqual, src, types.FilterModeLe)
				})

				// greater
				t.Run("GT", func(t *testing.T) {
					bitmapTestCompare(t, enc.MatchGreater, src, types.FilterModeGt)
				})

				// greater equal
				t.Run("GE", func(t *testing.T) {
					bitmapTestCompare(t, enc.MatchGreaterEqual, src, types.FilterModeGe)
				})

				// between
				t.Run("RG", func(t *testing.T) {
					bitmapTestCompare2(t, enc.MatchBetween, src, types.FilterModeRange)
				})

			})
			if t.Failed() {
				t.FailNow()
			}
		}
	}
}

type TestCaseBitmap struct {
	Name string
	N    int
	Data *bitset.Bitset
}

func MakeBitmapTests(n int) []TestCaseBitmap {
	return []TestCaseBitmap{
		{"zero", n, bitset.New(n)},
		{"one", n, bitset.New(n).One()},
		{"dense", n, bitset.New(n).SetIndexes(seq(n/2, 2))},
		{"sparse", n, bitset.New(n).SetIndexes(seq(n/32, 32))},
		{"rand", n, bitset.New(n).SetIndexes(slicex.Unique(util.RandIntsn(n, n)))},
	}
}

func seq(n, step int) []int {
	res := make([]int, n)
	for i := range n {
		res[i] = i * step
	}
	return res
}

func bitmapEnsureBits(t *testing.T, vals *bitset.Bitset, val, val2 bool, bits *bitset.Bitset, mode types.FilterMode) {
	if etests.ShowValues {
		t.Logf("Vals: %x", vals.Bytes())
		t.Logf("Bits: %x", bits.Bytes())
	}
	minv, maxv := vals.MinMax()
	switch mode {
	case types.FilterModeEqual:
		for i := range vals.Len() {
			v := vals.Contains(i)
			require.Equal(t, v == val, bits.Contains(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeNotEqual:
		for i := range vals.Len() {
			v := vals.Contains(i)
			require.Equal(t, v != val, bits.Contains(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeLt:
		a := util.Bool2byte(val)
		for i := range vals.Len() {
			v := vals.Contains(i)
			require.Equal(t, util.Bool2byte(v) < a, bits.Contains(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeLe:
		a := util.Bool2byte(val)
		for i := range vals.Len() {
			v := vals.Contains(i)
			require.Equal(t, util.Bool2byte(v) <= a, bits.Contains(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeGt:
		a := util.Bool2byte(val)
		for i := range vals.Len() {
			v := vals.Contains(i)
			require.Equal(t, util.Bool2byte(v) > a, bits.Contains(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeGe:
		a := util.Bool2byte(val)
		for i := range vals.Len() {
			v := vals.Contains(i)
			require.Equal(t, util.Bool2byte(v) >= a, bits.Contains(i), "bit=%d val=%v %s %v min=%v max=%v",
				i, v, mode, val, minv, maxv)
		}

	case types.FilterModeRange:
		a, b := util.Bool2byte(val), util.Bool2byte(val2)
		for i := range vals.Len() {
			v := util.Bool2byte(vals.Contains(i))
			require.Equal(t, v >= a && v <= b, bits.Contains(i), "bit=%d val=%v %s [%v,%v] min=%v max=%v",
				i, vals.Contains(i), mode, val, val2, minv, maxv)
		}
	}
}

type bitmapCompareFunc func(bool, *Bitset, *Bitset)
type bitmapCompareFunc2 func(bool, bool, *Bitset, *Bitset)

func bitmapTestCompare(t *testing.T, cmp bitmapCompareFunc, src *bitset.Bitset, mode types.FilterMode) {
	bits := bitset.New(src.Len())

	// single value
	val := true
	cmp(val, bits, nil)
	bitmapEnsureBits(t, src, val, val, bits, mode)
	bits.Zero()
	require.Equal(t, 0, bits.Count(), "cleared")

	// single value
	val = false
	cmp(val, bits, nil)
	bitmapEnsureBits(t, src, val, val, bits, mode)
	bits.Zero()
	require.Equal(t, 0, bits.Count(), "cleared")
}

func bitmapTestCompare2(t *testing.T, cmp bitmapCompareFunc2, src *bitset.Bitset, mode types.FilterMode) {
	bits := bitset.New(src.Len())

	// single value
	val := true
	cmp(val, val, bits, nil)
	bitmapEnsureBits(t, src, val, val, bits, mode)
	bits.Zero()
	require.Equal(t, 0, bits.Count(), "cleared")

	// single value
	val = false
	cmp(val, val, bits, nil)
	bitmapEnsureBits(t, src, val, val, bits, mode)
	bits.Zero()
	require.Equal(t, 0, bits.Count(), "cleared")

	// full range
	cmp(false, true, bits, nil)
	bitmapEnsureBits(t, src, false, true, bits, mode)
	bits.Zero()
}

func TestBitmapIterator(t *testing.T) {
	for _, sz := range etests.ItSizes {
		for _, c := range MakeBitmapTests(sz) {
			t.Run(fmt.Sprintf("%s/%d", c.Name, sz), func(t *testing.T) {
				src := c.Data
				enc := NewBitmap()
				enc.Encode(nil, src)
				t.Logf("Info: %s", enc.Info())

				// --------------------------
				// test next
				//
				for i := range enc.Iterator() {
					require.True(t, src.Get(i), "invalid val at pos=%d", i)
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
					dst, ok := it.Next()
					if !ok {
						break
					}
					n := len(dst)
					require.GreaterOrEqual(t, n, 1, "next chunk returned empty dst")
					require.LessOrEqual(t, seen+n, src.Count(), "next chunk returned too large n")
					for i, v := range dst[:n] {
						require.True(t, src.Get(v), "invalid at pos=%d", seen+i)
					}
					seen += n
				}
				require.Equal(t, src.Count(), seen, "next chunk did not return all values")
				it.Close()
			})
			if t.Failed() {
				t.FailNow()
			}
		}
	}
}
