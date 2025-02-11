// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"regexp"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/cmp"
	tests "blockwatch.cc/knoxdb/internal/tests/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testMatchBlockTypes = []BlockType{
		BlockTime,
		BlockInt64,
		BlockInt32,
		BlockInt16,
		BlockInt8,
		BlockUint64,
		BlockUint32,
		BlockUint16,
		BlockUint8,
		BlockFloat64,
		BlockFloat32,
		BlockBytes,
		BlockBool,
		BlockInt128,
		BlockInt256,
	}
	testMatchSingleValueModes = []FilterMode{
		FilterModeEqual,
		FilterModeNotEqual,
		FilterModeGt,
		FilterModeGe,
		FilterModeLt,
		FilterModeLe,
	}
)

func makeRandomValue(typ BlockType) any {
	switch typ {
	case BlockTime, BlockInt64:
		return util.RandInt64()
	case BlockInt32:
		return util.RandInt32()
	case BlockInt16:
		return int16(util.RandInt32())
	case BlockInt8:
		return int8(util.RandInt32())
	case BlockUint64:
		return util.RandUint64()
	case BlockUint32:
		return util.RandUint32()
	case BlockUint16:
		return uint16(util.RandUint32())
	case BlockUint8:
		return uint8(util.RandUint32())
	case BlockFloat64:
		return util.RandFloat64()
	case BlockFloat32:
		return util.RandFloat32()
	case BlockBool:
		return util.RandIntn(2) == 1
	case BlockBytes:
		return util.RandBytes(8)
	case BlockInt128:
		return num.Int128From2Int64(util.RandInt64(), util.RandInt64())
	case BlockInt256:
		return num.Int256From4Int64(util.RandInt64(), util.RandInt64(), util.RandInt64(), util.RandInt64())
	default:
		return nil
	}
}

func makeRandomBlock(typ BlockType, sz int) *block.Block {
	b := block.New(typ, sz)
	for i := 0; i < sz; i++ {
		switch typ {
		case BlockTime, BlockInt64:
			b.Int64().Append(util.RandInt64())
		case BlockInt32:
			b.Int32().Append(util.RandInt32())
		case BlockInt16:
			b.Int16().Append(int16(util.RandInt32()))
		case BlockInt8:
			b.Int8().Append(int8(util.RandInt32()))
		case BlockUint64:
			b.Uint64().Append(util.RandUint64())
		case BlockUint32:
			b.Uint32().Append(util.RandUint32())
		case BlockUint16:
			b.Uint16().Append(uint16(util.RandUint32()))
		case BlockUint8:
			b.Uint8().Append(uint8(util.RandUint32()))
		case BlockFloat64:
			b.Float64().Append(util.RandFloat64())
		case BlockFloat32:
			b.Float32().Append(util.RandFloat32())
		case BlockBool:
			b.Bool().Append(util.RandIntn(2) == 1)
		case BlockBytes:
			b.Bytes().Append(util.RandBytes(8))
		case BlockInt128:
			b.Int128().Append(num.Int128From2Int64(util.RandInt64(), util.RandInt64()))
		case BlockInt256:
			b.Int256().Append(num.Int256From4Int64(util.RandInt64(), util.RandInt64(), util.RandInt64(), util.RandInt64()))
		}
	}
	return b
}

func TestMatchValue(t *testing.T) {
	for _, typ := range testMatchBlockTypes {
		for _, mode := range testMatchSingleValueModes {
			t.Logf("%s_%s", typ, mode)
			for i := 0; i < 10; i++ {
				m := newFactory(typ).New(mode)
				v1, v2 := makeRandomValue(typ), makeRandomValue(typ)
				m.WithValue(v1)
				require.Equal(t, v1, m.Value(), "set/get")
				require.Equal(t, 1, m.Len(), "len")
				require.Equal(t,
					cmp.Match(mode, typ, v1, v1),
					m.MatchValue(v1),
					"match %v %s %v = %t", v1, mode, v1, cmp.Match(mode, typ, v1, v1),
				)
				require.Equal(t,
					cmp.Match(mode, typ, v2, v1),
					m.MatchValue(v2),
					"match %v %s %v = %t", v2, mode, v1, cmp.Match(mode, typ, v2, v1),
				)
			}
		}
	}
}

const matchBlockSize = 16

func TestMatchVector(t *testing.T) {
	for _, typ := range testMatchBlockTypes {
		for _, mode := range testMatchSingleValueModes {
			t.Logf("%s_%s", typ, mode)
			m := newFactory(typ).New(mode)
			v := makeRandomValue(typ)
			b := makeRandomBlock(typ, matchBlockSize)
			m.WithValue(v)
			set := bitset.NewBitset(matchBlockSize)
			set2 := m.MatchVector(b, set, nil)
			require.NotNil(t, set2)
			require.Equal(t, matchBlockSize, set2.Len())
		}
	}
}

var rangeTestResults = map[FilterMode][5]bool{
	FilterModeEqual:    {false, true, true, true, false},
	FilterModeNotEqual: {true, false, false, false, true},
	FilterModeGt:       {true, true, true, false, false},
	FilterModeGe:       {true, true, true, true, false},
	FilterModeLt:       {false, false, true, true, true},
	FilterModeLe:       {false, true, true, true, true},
}

func runRangeTest(t *testing.T, typ BlockType, mode FilterMode, a, b, c, d, e any) {
	m := newFactory(typ).New(mode)
	l, r := b, d
	res := rangeTestResults[mode]
	m.WithValue(a)
	assert.Equal(t, m.MatchRange(l, r), res[0], "before")
	m.WithValue(b)
	assert.Equal(t, m.MatchRange(l, r), res[1], "left")
	m.WithValue(c)
	assert.Equal(t, m.MatchRange(l, r), res[2], "in")
	m.WithValue(d)
	assert.Equal(t, m.MatchRange(l, r), res[3], "right")
	m.WithValue(e)
	assert.Equal(t, m.MatchRange(l, r), res[4], "after")
}

func TestMatchRange(t *testing.T) {
	for _, gen := range tests.Generators {
		// skip bool tests since range cannot be outside value domain
		if gen.Type() == BlockBool {
			continue
		}
		a := gen.MakeValue(9)
		b := gen.MakeValue(10)
		c := gen.MakeValue(11)
		d := gen.MakeValue(23)
		e := gen.MakeValue(24)
		for _, mode := range testMatchSingleValueModes {
			t.Run(gen.Name()+"_"+mode.String(), func(t *testing.T) {
				runRangeTest(t, gen.Type(), mode, a, b, c, d, e)
			})
		}
	}
}

func TestMatchSet(t *testing.T) {
	for _, gen := range tests.Generators {
		t.Run(gen.Name()+"_in", func(t *testing.T) {
			// skip bool tests here
			if gen.Type() == BlockBool {
				return
			}
			slice := gen.MakeSlice(0, 1, 2, 3, 4, 5)
			m := newFactory(gen.Type()).New(FilterModeIn)
			m.WithSlice(slice)
			assert.Equal(t, slice, m.Value())
			assert.Equal(t, reflectSliceLen(slice), m.Len(), "len")

			// value matching
			assert.True(t, m.MatchValue(reflectSliceIndex(slice, 0)), "match-in: %v in %v", reflectSliceIndex(slice, 0), slice)
			assert.False(t, m.MatchValue(gen.MakeValue(10)), "match-notin: %v nin %v", gen.MakeValue(10), slice)

			// range matching
			assert.True(t, m.MatchRange(reflectSliceIndex(slice, 0), reflectSliceIndex(slice, 1)), "range-in")
			assert.False(t, m.MatchRange(gen.MakeValue(10), gen.MakeValue(12)), "range-out")

			// bitmap matching only supported in int types
			switch gen.Type() {
			case BlockFloat32, BlockFloat64, BlockBytes, BlockInt128, BlockInt256:
				return
			}
			set := xroar.NewBitmap()
			assert.False(t, m.MatchFilter(set), "set-empty")
			set.Set(1)
			assert.True(t, m.MatchFilter(set), "set-in: %v in %v", gen.MakeValue(1), slice)
			set.Remove(1)
			set.Set(10)
			assert.False(t, m.MatchFilter(set), "set-notin")
		})
	}
}

func TestMatchRegexp(t *testing.T) {
	// prepare data
	hello := []byte("Hello")
	world := []byte("World")
	restr := "Hel.*"
	re := regexp.MustCompile(restr)
	assert.True(t, re.Match(hello))
	assert.False(t, re.Match(world))

	// string regexp value
	m := newFactory(BlockBytes).New(FilterModeRegexp)
	m.WithValue(restr)
	assert.Equal(t, m.Value(), restr)
	assert.True(t, m.MatchValue(hello), "match")
	assert.False(t, m.MatchValue(world), "nomatch")
	assert.True(t, m.MatchRange(hello, world), "range-always-true")

	// block match
	b := block.New(BlockBytes, 2)
	b.Bytes().Append(hello)
	b.Bytes().Append(world)
	set := bitset.NewBitset(2)
	set2 := m.MatchVector(b, set, nil)
	require.NotNil(t, set2)
	require.Equal(t, 1, set2.Count())
	require.True(t, set2.IsSet(0))

	// using compiled regexp value
	m.WithValue(re)
	assert.True(t, m.MatchValue(hello), "match")
	assert.False(t, m.MatchValue(world), "nomatch")
	assert.True(t, m.MatchRange(hello, world), "range-always-true")
	set.Zero()
	set2 = m.MatchVector(b, set, nil)
	require.NotNil(t, set2)
	require.Equal(t, 1, set2.Count())
	require.True(t, set2.IsSet(0))
}

func TestMatchBool(t *testing.T) {
	eq := newFactory(BlockBool).New(FilterModeEqual)
	eq.WithValue(false)
	assert.Equal(t, false, eq.Value())
	assert.True(t, eq.MatchValue(false), "match")
	assert.False(t, eq.MatchValue(true), "nomatch")
	assert.True(t, eq.MatchRange(false, false), "range-match")
	assert.True(t, eq.MatchRange(false, true), "range-match")
	assert.False(t, eq.MatchRange(true, true), "range-nomatch")

	eq.WithValue(true)
	assert.Equal(t, true, eq.Value())
	assert.True(t, eq.MatchValue(true), "match")
	assert.False(t, eq.MatchValue(false), "nomatch")
	assert.False(t, eq.MatchRange(false, false), "range-nomatch")
	assert.True(t, eq.MatchRange(false, true), "range-match")
	assert.True(t, eq.MatchRange(true, true), "range-match")

	ne := newFactory(BlockBool).New(FilterModeNotEqual)
	ne.WithValue(false)
	assert.Equal(t, false, ne.Value())
	assert.True(t, ne.MatchValue(true), "match")
	assert.False(t, ne.MatchValue(false), "nomatch")
	assert.False(t, ne.MatchRange(false, false), "range-nomatch")
	assert.True(t, ne.MatchRange(false, true), "range-match")
	assert.True(t, ne.MatchRange(true, true), "range-match")

	ne.WithValue(true)
	assert.Equal(t, true, ne.Value())
	assert.True(t, ne.MatchValue(false), "match")
	assert.False(t, ne.MatchValue(true), "nomatch")
	assert.True(t, ne.MatchRange(false, false), "range-match")
	assert.True(t, ne.MatchRange(false, true), "range-match")
	assert.False(t, ne.MatchRange(true, true), "range-nomatch")

	lt := newFactory(BlockBool).New(FilterModeLt)
	lt.WithValue(false)
	assert.Equal(t, false, lt.Value())
	assert.False(t, lt.MatchValue(true), "nomatch")
	assert.False(t, lt.MatchValue(false), "nomatch")
	assert.False(t, lt.MatchRange(false, false), "range-nomatch")
	assert.False(t, lt.MatchRange(false, true), "range-nomatch")
	assert.False(t, lt.MatchRange(true, true), "range-nomatch")

	lt.WithValue(true)
	assert.Equal(t, true, lt.Value())
	assert.True(t, lt.MatchValue(false), "match")
	assert.False(t, lt.MatchValue(true), "nomatch")
	assert.True(t, lt.MatchRange(false, false), "range-match")
	assert.True(t, lt.MatchRange(false, true), "range-match")
	assert.False(t, lt.MatchRange(true, true), "range-nomatch")

	le := newFactory(BlockBool).New(FilterModeLe)
	le.WithValue(false)
	assert.Equal(t, false, le.Value())
	assert.True(t, le.MatchValue(false), "match")
	assert.False(t, le.MatchValue(true), "nomatch")
	assert.True(t, le.MatchRange(false, false), "range-match")
	assert.True(t, le.MatchRange(false, true), "range-match")
	assert.True(t, le.MatchRange(true, true), "range-match")

	le.WithValue(true)
	assert.Equal(t, true, le.Value())
	assert.True(t, le.MatchValue(true), "match")
	assert.True(t, le.MatchValue(false), "match")
	assert.True(t, le.MatchRange(false, false), "range-match")
	assert.True(t, le.MatchRange(false, true), "range-match")
	assert.True(t, le.MatchRange(true, true), "range-match")

	gt := newFactory(BlockBool).New(FilterModeGt)
	gt.WithValue(false)
	assert.Equal(t, false, gt.Value())
	assert.True(t, gt.MatchValue(true), "match")
	assert.False(t, gt.MatchValue(false), "nomatch")
	assert.False(t, gt.MatchRange(false, false), "range-nomatch")
	assert.True(t, gt.MatchRange(false, true), "range-match")
	assert.True(t, gt.MatchRange(true, true), "range-match")

	gt.WithValue(true)
	assert.Equal(t, true, gt.Value())
	assert.False(t, gt.MatchValue(false), "nomatch")
	assert.False(t, gt.MatchValue(true), "nomatch")
	assert.False(t, gt.MatchRange(false, false), "range-nomatch")
	assert.False(t, gt.MatchRange(false, true), "range-nomatch")
	assert.False(t, gt.MatchRange(true, true), "range-nomatch")

	ge := newFactory(BlockBool).New(FilterModeGe)
	ge.WithValue(false)
	assert.Equal(t, false, ge.Value())
	assert.True(t, ge.MatchValue(true), "match")
	assert.True(t, ge.MatchValue(false), "match")
	assert.True(t, ge.MatchRange(false, false), "range-match")
	assert.True(t, ge.MatchRange(false, true), "range-match")
	assert.True(t, ge.MatchRange(true, true), "range-match")

	ge.WithValue(true)
	assert.Equal(t, true, ge.Value())
	assert.False(t, ge.MatchValue(false), "nomatch")
	assert.True(t, ge.MatchValue(true), "match")
	assert.False(t, ge.MatchRange(false, false), "range-nomatch")
	assert.True(t, ge.MatchRange(false, true), "range-match")
	assert.True(t, ge.MatchRange(true, true), "range-match")

	rg := newFactory(BlockBool).New(FilterModeRange)
	rg.WithValue(RangeValue{false, false})
	assert.Equal(t, RangeValue{false, false}, rg.Value())
	assert.True(t, rg.MatchValue(false), "match")
	assert.False(t, rg.MatchValue(true), "nomatch")
	assert.True(t, rg.MatchRange(false, false), "range-match")
	assert.True(t, rg.MatchRange(false, true), "range-match")
	assert.False(t, rg.MatchRange(true, true), "range-nomatch")

	rg.WithValue(RangeValue{false, true})
	assert.Equal(t, RangeValue{false, true}, rg.Value())
	assert.True(t, rg.MatchValue(false), "match")
	assert.True(t, rg.MatchValue(true), "match")
	assert.True(t, rg.MatchRange(false, false), "range-match")
	assert.True(t, rg.MatchRange(false, true), "range-match")
	assert.True(t, rg.MatchRange(true, true), "range-match")

	rg.WithValue(RangeValue{true, true})
	assert.Equal(t, RangeValue{true, true}, rg.Value())
	assert.False(t, rg.MatchValue(false), "nomatch")
	assert.True(t, rg.MatchValue(true), "match")
	assert.False(t, rg.MatchRange(false, false), "range-nomatch")
	assert.True(t, rg.MatchRange(false, true), "range-match")
	assert.True(t, rg.MatchRange(true, true), "range-match")
}
