// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	etests "blockwatch.cc/knoxdb/internal/encode/tests"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeString(t *testing.T) {
	// fixed
	p := util.NewStringPool(3)
	p.AppendManyStrings("a", "b", "c")
	x := AnalyzeString(p)
	assert.Equal(t, []byte("a"), x.Min, "min")
	assert.Equal(t, []byte("c"), x.Max, "max")
	assert.Equal(t, 1, x.MinLen, "min_len")
	assert.Equal(t, 1, x.MaxLen, "max_len")
	assert.Equal(t, 3, x.NumUnique, "num_unique")
	assert.Equal(t, 3, x.NumValues, "num_values")
	assert.Len(t, x.Unique, 3, "unique map len")
	assert.Len(t, x.Dups, 3, "dups list len")
	assert.Equal(t, TStringFixed, x.UseScheme(), "selected scheme")
	x.Close()
	p.Close()

	// no dups, not fixed -> compact
	p = util.NewStringPool(4)
	p.AppendManyStrings("a", "b", "c", "dd")
	x = AnalyzeString(p)
	assert.Equal(t, []byte("a"), x.Min, "min")
	assert.Equal(t, []byte("dd"), x.Max, "max")
	assert.Equal(t, 1, x.MinLen, "min_len")
	assert.Equal(t, 2, x.MaxLen, "max_len")
	assert.Equal(t, 4, x.NumUnique, "num_unique")
	assert.Equal(t, 4, x.NumValues, "num_values")
	assert.Len(t, x.Unique, 4, "unique map len")
	assert.Len(t, x.Dups, 4, "dups list len")
	assert.Equal(t, TStringCompact, x.UseScheme(), "selected scheme")
	x.Close()
	p.Close()

	// const
	p = util.NewStringPool(4)
	p.AppendManyStrings("a", "a", "a", "a")
	x = AnalyzeString(p)
	assert.Equal(t, []byte("a"), x.Min, "min")
	assert.Equal(t, []byte("a"), x.Max, "max")
	assert.Equal(t, 1, x.MinLen, "min_len")
	assert.Equal(t, 1, x.MaxLen, "max_len")
	assert.Equal(t, 1, x.NumUnique, "num_unique")
	assert.Equal(t, 4, x.NumValues, "num_values")
	assert.Len(t, x.Unique, 1, "unique map len")
	assert.Len(t, x.Dups, 4, "dups list len")
	assert.Equal(t, TStringConstant, x.UseScheme(), "selected scheme")
	x.Close()
	p.Close()

	// dict
	p = util.NewStringPool(6)
	p.AppendManyStrings("ax", "bxx", "ax", "cx", "ax", "cx", "ax", "cx")
	x = AnalyzeString(p)
	assert.Equal(t, []byte("ax"), x.Min, "min")
	assert.Equal(t, []byte("cx"), x.Max, "max")
	assert.Equal(t, 2, x.MinLen, "min_len")
	assert.Equal(t, 3, x.MaxLen, "max_len")
	assert.Equal(t, 3, x.NumUnique, "num_unique")
	assert.Equal(t, 8, x.NumValues, "num_values")
	assert.Len(t, x.Unique, 3, "unique map len")
	assert.Len(t, x.Dups, 8, "dups list len")
	assert.Equal(t, TStringDictionary, x.UseScheme(), "selected scheme")
	x.Close()
	p.Close()
}

func TestStringEncode(t *testing.T) {
	testStringEncode(t, TStringConstant)
	testStringEncode(t, TStringFixed)
	testStringEncode(t, TStringCompact)
	testStringEncode(t, TStringDictionary)
}

func testStringEncode(t *testing.T, scheme ContainerType) {
	for _, c := range MakeShortStringTests(scheme) {
		t.Run(fmt.Sprintf("%s", c.Name), func(t *testing.T) {
			enc := NewString(scheme)

			// analyze and encode data into container
			ctx := AnalyzeString(c.Data)
			enc.Encode(ctx, c.Data)
			t.Logf("Info: %s", enc.Info())

			// validate contents
			require.Equal(t, c.N, enc.Len())
			for i, v := range c.Data.Iterator2() {
				require.Equal(t, v, enc.Get(i))
			}

			// serialize to buffer
			buf := make([]byte, 0, enc.Size())
			buf = enc.Store(buf)
			require.NotNil(t, buf)

			// load back into new container
			enc2 := NewString(scheme)
			buf, err := enc2.Load(buf)
			require.NoError(t, err)
			require.Len(t, buf, 0)

			// validate contents
			require.Equal(t, c.N, enc2.Len())
			for i, v := range c.Data.Iterator2() {
				require.Equal(t, v, enc2.Get(i))
			}

			// validate append
			dst := util.NewStringPool(c.N)
			enc2.AppendTo(dst, nil)
			require.Equal(t, c.N, dst.Len())
			for i, v := range dst.Iterator2() {
				require.Equal(t, v, c.Data.Get(i), "i=%d", i)
			}
			for i, v := range c.Data.Iterator2() {
				require.Equal(t, v, dst.Get(i), "i=%d", i)
			}

			// validate append selector
			sel := util.RandUintsn[uint32](max(1, c.N/2), uint32(c.N))
			dst.Clear()
			enc2.AppendTo(dst, sel)
			require.Equal(t, len(sel), dst.Len())
			for i, v := range sel {
				require.Equal(t, c.Data.Get(int(v)), dst.Get(i), "sel[%d]", v)
			}

			enc2.Close()
			enc.Close()
		})
		if t.Failed() {
			t.FailNow()
		}
	}
}

func testStringContainerCompare(t *testing.T, scheme ContainerType) {
	// validate matchers
	for _, sz := range etests.CompareSizes {
		t.Run(fmt.Sprintf("cmp/%d", sz), func(t *testing.T) {
			src := etests.GenForStringScheme(int(scheme), sz)
			enc := NewString(scheme)
			ctx := AnalyzeString(src)

			enc.Encode(ctx, src)
			t.Logf("Info: %s", enc.Info())

			// equal
			t.Run("EQ", func(t *testing.T) {
				testStringCompareFunc(t, enc.MatchEqual, src, types.FilterModeEqual)
			})

			// not equal
			t.Run("NE", func(t *testing.T) {
				testStringCompareFunc(t, enc.MatchNotEqual, src, types.FilterModeNotEqual)
			})

			// less
			t.Run("LT", func(t *testing.T) {
				testStringCompareFunc(t, enc.MatchLess, src, types.FilterModeLt)
			})

			// less equal
			t.Run("LE", func(t *testing.T) {
				testStringCompareFunc(t, enc.MatchLessEqual, src, types.FilterModeLe)
			})

			// greater
			t.Run("GT", func(t *testing.T) {
				testStringCompareFunc(t, enc.MatchGreater, src, types.FilterModeGt)
			})

			// greater equal
			t.Run("GE", func(t *testing.T) {
				testStringCompareFunc(t, enc.MatchGreaterEqual, src, types.FilterModeGe)
			})

			// between
			t.Run("RG", func(t *testing.T) {
				testStringCompareFunc2(t, enc.MatchBetween, src, types.FilterModeRange)
			})
		})
		if t.Failed() {
			t.FailNow()
		}
	}
}

type StringCompareFunc func([]byte, *Bitset, *Bitset)
type StringCompareFunc2 func([]byte, []byte, *Bitset, *Bitset)

func testStringCompareFunc(t *testing.T, cmp StringCompareFunc, src *util.StringPool, mode types.FilterMode) {
	bits := bitset.New(src.Len())
	minv, maxv, _, _ := src.MinMax()

	// single value
	val := src.Get(src.Len() / 2)
	cmp(val, bits, nil)
	EnsureStringBits(t, src, val, val, bits, mode)
	bits.Zero()
	require.Equal(t, 0, bits.Count(), "cleared")

	// value over bounds
	over := append([]byte{'z'}, maxv...)
	cmp(over, bits, nil)
	EnsureStringBits(t, src, over, over, bits, mode)
	bits.Zero()
	require.Equal(t, 0, bits.Count(), "cleared")

	// value under bounds
	if len(minv) > 0 {
		under := slices.Clone(minv)
		under[0] = 'a'
		cmp(under, bits, nil)
		EnsureStringBits(t, src, under, under, bits, mode)
		bits.Zero()
		require.Equal(t, 0, bits.Count(), "cleared")
	}
}

func testStringCompareFunc2(t *testing.T, cmp StringCompareFunc2, src *util.StringPool, mode types.FilterMode) {
	bits := bitset.New(src.Len())
	minv, maxv, _, _ := src.MinMax()

	// single value
	val := src.Get(src.Len() / 2)
	cmp(val, val, bits, nil)
	EnsureStringBits(t, src, val, val, bits, mode)
	bits.Zero()
	require.Equal(t, 0, bits.Count(), "cleared")

	// full range
	cmp(minv, maxv, bits, nil)
	EnsureStringBits(t, src, minv, maxv, bits, mode)
	bits.Zero()

	// partial range
	from, to := src.Get(2), src.Get(3)
	if bytes.Compare(from, to) > 0 {
		from, to = to, from
	}

	// out of bounds (over)
	over := append([]byte{'z'}, maxv...)
	cmp(over, over, bits, nil)
	EnsureStringBits(t, src, over, over, bits, mode)
	bits.Zero()

	// out of bounds (under)
	if len(minv) > 0 {
		under := slices.Clone(minv)
		under[0] = 'a'
		cmp(under, under, bits, nil)
		EnsureStringBits(t, src, under, under, bits, mode)
		bits.Zero()
	}
}

type TestCaseString struct {
	Name string
	N    int
	Data *util.StringPool
}

func MakeStringTests(n int) []TestCaseString {
	return []TestCaseString{
		{"const", n, tests.GenStringConst(n, []byte("42"))},
		{"fixed", n, tests.GenStringRnd(n, 8)},                    // fixed length 8, random cardinality
		{"compact", n, tests.GenStringDups(n, min(1, n*3/4), -1)}, // random length at cardinality 3/4n
		{"dict", n, tests.GenStringDups(n, n/5, -1)},              // random length at cardinality n/5
		{"rand", n, tests.GenStringRnd(n, -1)},                    // random length, random cardinality
	}
}

func MakeShortStringTests(scheme ContainerType) []TestCaseString {
	n := 6
	switch scheme {
	case TStringConstant:
		return []TestCaseString{{"const", n, tests.GenStringConst(n, []byte("42"))}}
	case TStringFixed:
		return []TestCaseString{{"fixed", n, tests.GenStringRnd(n, 8)}}
	case TStringCompact:
		return []TestCaseString{{"compact", n, tests.GenStringDups(n, min(1, n*3/4), -1)}}
	case TStringDictionary:
		return []TestCaseString{{"dict", n, tests.GenStringDups(n, n/5, -1)}}
	default:
		return nil
	}
}

func EnsureStringBits(t *testing.T, vals *util.StringPool, val, val2 []byte, bits *bitset.Bitset, mode types.FilterMode) {
	if etests.ShowValues {
		for i, v := range vals.Iterator2() {
			t.Logf("Val %d: %v", i, v)
			i++
		}
		t.Logf("Bitset %x", bits.Bytes())
	}
	switch mode {
	case types.FilterModeEqual:
		for i, v := range vals.Iterator2() {
			require.Equal(t, bytes.Equal(v, val), bits.Contains(i), "bit=%d val=%x %s %x",
				i, v, mode, val)
		}

	case types.FilterModeNotEqual:
		for i, v := range vals.Iterator2() {
			require.Equal(t, !bytes.Equal(v, val), bits.Contains(i), "bit=%d val=%v %s %v",
				i, v, mode, val)
		}

	case types.FilterModeLt:
		for i, v := range vals.Iterator2() {
			require.Equal(t, bytes.Compare(v, val) < 0, bits.Contains(i), "bit=%d val=%v %s %v",
				i, v, mode, val)
		}

	case types.FilterModeLe:
		for i, v := range vals.Iterator2() {
			require.Equal(t, bytes.Compare(v, val) <= 0, bits.Contains(i), "bit=%d val=%v %s %v",
				i, v, mode, val)
		}

	case types.FilterModeGt:
		for i, v := range vals.Iterator2() {
			require.Equal(t, bytes.Compare(v, val) > 0, bits.Contains(i), "bit=%d val=%v %s %v",
				i, v, mode, val)
		}

	case types.FilterModeGe:
		for i, v := range vals.Iterator2() {
			require.Equal(t, bytes.Compare(v, val) >= 0, bits.Contains(i), "bit=%d val=%v %s %v",
				i, v, mode, val)
		}

	case types.FilterModeRange:
		for i, v := range vals.Iterator2() {
			require.Equal(t, bytes.Compare(v, val) >= 0 && bytes.Compare(v, val2) <= 0, bits.Contains(i), "bit=%d val=%v %s [%v,%v]",
				i, v, mode, val, val2)
		}
	}
}
