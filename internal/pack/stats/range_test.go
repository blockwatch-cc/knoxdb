// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func newBlock[T constraints.Integer](vals ...uint) *block.Block {
	var (
		t T
		b *block.Block
	)
	switch any(t).(type) {
	case int64:
		b = block.New(block.BlockInt64, len(vals))
	case int32:
		b = block.New(block.BlockInt32, len(vals))
	case int16:
		b = block.New(block.BlockInt16, len(vals))
	case int8:
		b = block.New(block.BlockInt8, len(vals))
	case uint64:
		b = block.New(block.BlockUint64, len(vals))
	case uint32:
		b = block.New(block.BlockUint32, len(vals))
	case uint16:
		b = block.New(block.BlockUint16, len(vals))
	case uint8:
		b = block.New(block.BlockUint8, len(vals))
	default:
		return nil
	}

	for _, v := range vals {
		b.Append(T(v))
	}
	return b
}

func newFilter(b *block.Block, m string, val ...any) *query.Filter {
	var v any
	if len(val) == 2 {
		v = query.RangeValue{val[0], val[1]}
	} else {
		v = val[0]
	}
	return &query.Filter{
		Type:  b.Type(),
		Value: v,
		Mode:  types.ParseFilterMode(m),
	}
}

func TestRangeIndexBuild(t *testing.T) {
	block := newBlock[int64](1, 2, 3, 5, 1)
	minVal, maxVal := block.MinMax()
	idx, err := BuildRangeIndex(block, minVal, maxVal)
	require.NoError(t, err)
	require.Equal(t, 5, idx.NumSlots(), "num slots")
	require.Equal(t, 4, idx.NumUsedSlots(), "num used slots")
	require.Equal(t, 1, idx.NumGroups(), "num groups")
	require.Equal(t, 5*8, idx.Size(), "byte size")
	require.Equal(t, types.Range{0, 4}, idx.Range(1, 1), "range 1")
	require.Equal(t, types.Range{1, 1}, idx.Range(2, 1), "range 2")
	require.Equal(t, types.Range{2, 2}, idx.Range(3, 1), "range 3")
	require.Equal(t, types.Range{3, 3}, idx.Range(5, 1), "range 5")
	require.Equal(t, types.InvalidRange, idx.Range(7, 1), "invalid 7")
	require.Equal(t, types.InvalidRange, idx.Range(4, 1), "invalid 4")

	// export/import
	idx2 := RangeIndexFromBytes(idx.Bytes())
	require.Equal(t, 5, idx2.NumSlots(), "num slots")
	require.Equal(t, 4, idx2.NumUsedSlots(), "num used slots")
	require.Equal(t, 1, idx2.NumGroups(), "num groups")
	require.Equal(t, 5*8, idx2.Size(), "byte size")
	require.Equal(t, types.Range{0, 4}, idx2.Range(1, 1), "range 1")
	require.Equal(t, types.Range{1, 1}, idx2.Range(2, 1), "range 2")
	require.Equal(t, types.Range{2, 2}, idx2.Range(3, 1), "range 3")
	require.Equal(t, types.Range{3, 3}, idx2.Range(5, 1), "range 5")
	require.Equal(t, types.InvalidRange, idx2.Range(7, 1), "invalid 7")
	require.Equal(t, types.InvalidRange, idx2.Range(4, 1), "invalid 4")
}

func TestRangeIndexQuery(t *testing.T) {
	block := newBlock[int64](1, 2, 3, 5, 1)
	minVal, maxVal := block.MinMax()
	idx, err := BuildRangeIndex(block, minVal, maxVal)
	require.NoError(t, err)

	// t.Log("lower", idx.lower)
	// t.Log("upper", idx.upper)

	//
	// values exist
	//
	f := newFilter(block, "eq", int64(1))
	require.Equal(t, types.Range{0, 4}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "eq", int64(2))
	require.Equal(t, types.Range{1, 1}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "eq", int64(3))
	require.Equal(t, types.Range{2, 2}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "eq", int64(5))
	require.Equal(t, types.Range{3, 3}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "lt", int64(2))
	require.Equal(t, types.Range{0, 4}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "le", int64(2))
	require.Equal(t, types.Range{0, 4}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "gt", int64(2))
	require.Equal(t, types.Range{2, 3}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "ge", int64(2))
	require.Equal(t, types.Range{1, 3}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "rg", int64(2), int64(3))
	require.Equal(t, types.Range{1, 2}, idx.Query(f, minVal, block.Len()), f)

	//
	// values do not exist directly
	//
	f = newFilter(block, "eq", int64(4))
	require.Equal(t, types.InvalidRange, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "lt", int64(4))
	require.Equal(t, types.Range{0, 4}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "le", int64(4))
	require.Equal(t, types.Range{0, 4}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "gt", int64(4))
	require.Equal(t, types.Range{3, 3}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "ge", int64(4))
	require.Equal(t, types.Range{3, 3}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "rg", int64(4), int64(4))
	require.Equal(t, types.InvalidRange, idx.Query(f, minVal, block.Len()), f)

	//
	// values out of bounds
	//
	f = newFilter(block, "eq", int64(0))
	require.Equal(t, types.InvalidRange, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "lt", int64(0))
	require.Equal(t, types.InvalidRange, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "le", int64(0))
	require.Equal(t, types.InvalidRange, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "gt", int64(0))
	require.Equal(t, types.Range{0, 4}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "ge", int64(0))
	require.Equal(t, types.Range{0, 4}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "rg", int64(-1), int64(0))
	require.Equal(t, types.InvalidRange, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "eq", int64(7))
	require.Equal(t, types.InvalidRange, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "lt", int64(7))
	require.Equal(t, types.Range{0, 4}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "le", int64(7))
	require.Equal(t, types.Range{0, 4}, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "gt", int64(7))
	require.Equal(t, types.InvalidRange, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "ge", int64(7))
	require.Equal(t, types.InvalidRange, idx.Query(f, minVal, block.Len()), f)

	f = newFilter(block, "rg", int64(7), int64(10))
	require.Equal(t, types.InvalidRange, idx.Query(f, minVal, block.Len()), f)
}

func TestRangeIndexBuildI32(t *testing.T) {
	block := newBlock[int64](util.RandUintsn(65536, uint(1<<32-1))...)
	minVal, maxVal := block.MinMax()
	idx, err := BuildRangeIndex(block, minVal, maxVal)
	require.NoError(t, err)
	require.Equal(t, 4*256, idx.NumSlots(), "num slots")
	require.Equal(t, 4, idx.NumGroups(), "num groups")
	require.Equal(t, 4*256*8, idx.Size(), "byte size")
}

func TestRangeIndexBuildU64(t *testing.T) {
	block := newBlock[uint64](util.RandUintsn(65536, uint(1<<64-1))...)
	minVal, maxVal := block.MinMax()
	idx, err := BuildRangeIndex(block, minVal, maxVal)
	require.NoError(t, err)
	require.Equal(t, 8*256, idx.NumSlots(), "num slots")
	require.Equal(t, 8, idx.NumGroups(), "num groups")
	require.Equal(t, 8*256*8, idx.Size(), "byte size")
}

func BenchmarkRangeIndexBuild(b *testing.B) {
	block := newBlock[int64](util.RandUintsn(65536, uint(1<<32-1))...)
	minVal, maxVal := block.MinMax()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = BuildRangeIndex(block, minVal, maxVal)
	}
}

func BenchmarkRangeIndexLookup(b *testing.B) {
	block := newBlock[int64](util.RandUintsn(65536, uint(1<<32-1))...)
	minVal, maxVal := block.MinMax()
	idx, err := BuildRangeIndex(block, minVal, maxVal)
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Range(util.RandIntn(65536), int(minVal.(int64)))
	}
}

func BenchmarkRangeIndexQuery(b *testing.B) {
	for _, m := range []types.FilterMode{
		types.FilterModeGt,
		types.FilterModeGe,
		types.FilterModeLt,
		types.FilterModeLe,
	} {
		block := newBlock[int64](util.RandUintsn(65536, uint(1<<32-1))...)
		minVal, maxVal := block.MinMax()
		idx, err := BuildRangeIndex(block, minVal, maxVal)
		b.Run(m.String(), func(b *testing.B) {
			flt := &query.Filter{
				Type:  block.Type(),
				Value: util.RandInt64n(1<<32 - 1),
				Mode:  m,
			}
			require.NoError(b, err)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx.Query(flt, minVal, 65536)
			}
		})
	}
}
