// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
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
		BlockString,
		BlockBytes,
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
		return rand.Int63()
	case BlockInt32:
		return rand.Int31()
	case BlockInt16:
		return int16(rand.Int31())
	case BlockInt8:
		return int8(rand.Int31())
	case BlockUint64:
		return rand.Uint64()
	case BlockUint32:
		return rand.Uint32()
	case BlockUint16:
		return uint16(rand.Uint32())
	case BlockUint8:
		return uint8(rand.Uint32())
	case BlockFloat64:
		return rand.Float64()
	case BlockFloat32:
		return rand.Float32()
	case BlockBool:
		return rand.Intn(2) == 1
	case BlockString, BlockBytes:
		k := make([]byte, 8)
		rand.Read(k)
		return k
	case BlockInt128:
		return num.Int128From2Int64(rand.Int63(), rand.Int63())
	case BlockInt256:
		return num.Int256From4Int64(rand.Int63(), rand.Int63(), rand.Int63(), rand.Int63())
	default:
		return nil
	}
}

func makeRandomBlock(typ BlockType, sz int) *block.Block {
	b := block.New(typ, sz)
	for i := 0; i < sz; i++ {
		switch typ {
		case BlockTime, BlockInt64:
			b.Int64().Append(rand.Int63())
		case BlockInt32:
			b.Int32().Append(rand.Int31())
		case BlockInt16:
			b.Int16().Append(int16(rand.Int31()))
		case BlockInt8:
			b.Int8().Append(int8(rand.Int31()))
		case BlockUint64:
			b.Uint64().Append(rand.Uint64())
		case BlockUint32:
			b.Uint32().Append(rand.Uint32())
		case BlockUint16:
			b.Uint16().Append(uint16(rand.Uint32()))
		case BlockUint8:
			b.Uint8().Append(uint8(rand.Uint32()))
		case BlockFloat64:
			b.Float64().Append(rand.Float64())
		case BlockFloat32:
			b.Float32().Append(rand.Float32())
		case BlockBool:
			if rand.Intn(2) == 1 {
				b.Bool().Set(i)
			}
		case BlockString, BlockBytes:
			k := make([]byte, 8)
			rand.Read(k)
			b.Bytes().Append(k)
		case BlockInt128:
			b.Int128().Append(num.Int128From2Int64(rand.Int63(), rand.Int63()))
		case BlockInt256:
			b.Int256().Append(num.Int256From4Int64(rand.Int63(), rand.Int63(), rand.Int63(), rand.Int63()))
		}
	}
	return b
}

func makeRandomSlice(typ BlockType, sz int) (slice any, in any, notin any) {
	if sz == 0 {
		sz = 1
	}
	v1 := makeRandomValue(typ)
	rslice := reflect.MakeSlice(reflect.TypeOf(v1), sz, sz)
	for rslice.Len() < sz {
		v1 = makeRandomValue(typ)
		rv1 := reflect.ValueOf(v1)
		if rv1.IsZero() {
			continue
		}
		rslice = reflect.AppendSlice(rslice, rv1)
	}
	rv2 := reflect.Indirect(reflect.New(reflect.TypeOf(v1)))
	return rslice.Interface(), rslice.Index(0).Interface(), rv2.Interface()
}

func TestMatchValue(t *testing.T) {
	for _, typ := range testMatchBlockTypes {
		for _, mode := range testMatchSingleValueModes {
			t.Run(fmt.Sprintf("%s_%s", typ, mode), func(t *testing.T) {
				m := newFactory(typ).New(mode)
				v1 := makeRandomValue(typ)
				v2 := makeRandomValue(typ)
				m.WithValue(v1)
				require.Equal(t, v1, m.Value(), "set/get")
				require.Equal(t, 1, m.Len(), "len")
				require.Equal(t, cmp.Match(mode, typ, v1, v1), m.MatchValue(v1), "match-1")
				t.Log("v2=", v2, mode, "v1=", v1, "cmp=", cmp.Match(mode, typ, v2, v1))
				require.Equal(t, cmp.Match(mode, typ, v2, v1), m.MatchValue(v2), "match-2")
			})
		}
	}
}

const matchBlockSize = 16

func TestMatchBlock(t *testing.T) {
	for _, typ := range testMatchBlockTypes {
		for _, mode := range testMatchSingleValueModes {
			t.Run(fmt.Sprintf("%s_%s", typ, mode), func(t *testing.T) {
				m := newFactory(typ).New(mode)
				v := makeRandomValue(typ)
				b := makeRandomBlock(typ, matchBlockSize)
				m.WithValue(v)
				set := bitset.NewBitset(matchBlockSize)
				set2 := m.MatchBlock(b, set, nil)
				require.NotNil(t, set2)
				require.Equal(t, matchBlockSize, set2.Len())
			})
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

func TestMatchRangeInt64(t *testing.T) {
	a := int64(9)
	b := int64(10)
	c := int64(11)
	d := int64(23)
	e := int64(24)
	for _, mode := range testMatchSingleValueModes {
		t.Run(mode.String(), func(t *testing.T) {
			runRangeTest(t, BlockInt64, mode, a, b, c, d, e)
		})
	}
}

func TestMatchRangeBytes(t *testing.T) {
	a := []byte{9}
	b := []byte{10}
	c := []byte{11}
	d := []byte{23}
	e := []byte{24}
	for _, mode := range testMatchSingleValueModes {
		t.Run(mode.String(), func(t *testing.T) {
			runRangeTest(t, BlockBytes, mode, a, b, c, d, e)
		})
	}
}

func TestMatchSetInt64(t *testing.T) {
	slice := []uint64{0, 1, 2, 3, 4, 5}
	m := newFactory(BlockUint64).New(FilterModeIn)
	m.WithSlice(slice)
	assert.Equal(t, m.Value(), slice)
	assert.Equal(t, len(slice), m.Len(), "len")

	// value matching
	assert.True(t, m.MatchValue(slice[0]), "match-in")
	assert.False(t, m.MatchValue(uint64(10)), "match-notin")

	// range matching
	assert.True(t, m.MatchRange(slice[0], slice[1]), "range-in")
	assert.False(t, m.MatchRange(uint64(10), uint64(12)), "range-out")

	// bitmap matching
	set := xroar.NewBitmap()
	assert.False(t, m.MatchBitmap(set), "set-empty")
	set.Set(1)
	assert.True(t, m.MatchBitmap(set), "set-in")
	set.Remove(1)
	set.Set(10)
	assert.False(t, m.MatchBitmap(set), "set-notin")
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
	set2 := m.MatchBlock(b, set, nil)
	require.NotNil(t, set2)
	require.Equal(t, 1, set2.Count())
	require.True(t, set2.IsSet(0))

	// using compiled regexp value
	m.WithValue(re)
	assert.True(t, m.MatchValue(hello), "match")
	assert.False(t, m.MatchValue(world), "nomatch")
	assert.True(t, m.MatchRange(hello, world), "range-always-true")
	set.Zero()
	set2 = m.MatchBlock(b, set, nil)
	require.NotNil(t, set2)
	require.Equal(t, 1, set2.Count())
	require.True(t, set2.IsSet(0))
}

func TestMatchBool(t *testing.T) {
	eq := newFactory(BlockBool).New(FilterModeEqual)
	eq.WithValue(false)
	assert.Equal(t, eq.Value(), false)
	assert.True(t, eq.MatchValue(false), "match")
	assert.False(t, eq.MatchValue(true), "nomatch")
	assert.True(t, eq.MatchRange(false, false), "range-match")
	assert.True(t, eq.MatchRange(false, true), "range-match")
	assert.False(t, eq.MatchRange(true, true), "range-nomatch")

	eq.WithValue(true)
	assert.Equal(t, eq.Value(), true)
	assert.True(t, eq.MatchValue(true), "match")
	assert.False(t, eq.MatchValue(false), "nomatch")
	assert.False(t, eq.MatchRange(false, false), "range-nomatch")
	assert.True(t, eq.MatchRange(false, true), "range-match")
	assert.True(t, eq.MatchRange(true, true), "range-match")

	ne := newFactory(BlockBool).New(FilterModeNotEqual)
	ne.WithValue(false)
	assert.Equal(t, ne.Value(), false)
	assert.True(t, ne.MatchValue(true), "match")
	assert.False(t, ne.MatchValue(false), "nomatch")
	assert.False(t, ne.MatchRange(false, false), "range-nomatch")
	assert.True(t, ne.MatchRange(false, true), "range-match")
	assert.True(t, ne.MatchRange(true, true), "range-match")

	ne.WithValue(true)
	assert.Equal(t, ne.Value(), true)
	assert.True(t, ne.MatchValue(false), "match")
	assert.False(t, ne.MatchValue(true), "nomatch")
	assert.True(t, ne.MatchRange(false, false), "range-match")
	assert.True(t, ne.MatchRange(false, true), "range-match")
	assert.False(t, ne.MatchRange(true, true), "range-nomatch")
}
