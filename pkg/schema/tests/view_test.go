// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema_tests

import (
	"iter"
	"testing"

	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/encode"
	"blockwatch.cc/knoxdb/pkg/schema/reflect"
	"github.com/stretchr/testify/require"
)

func TestViewFixed(t *testing.T) {
	base := NewArrayTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := reflect.MustSchemaFor[ArrayTypes]()
	baseEnc := encode.NewEncoder(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	view := schema.NewView(baseSchema).Reset(buf)
	require.True(t, view.IsValid())
	require.True(t, view.IsFixed())
	require.Equal(t, baseSchema.MinWireSize, view.Len())
	require.Equal(t, view.Buffer(), buf)
	val := view.Get(0)
	require.Equal(t, base.Id, val)
	require.Equal(t, base.Id, view.GetPk())
}

func TestViewDynamic(t *testing.T) {
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := reflect.MustSchemaFor[AllTypes]()
	baseEnc := encode.NewEncoder(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	view := schema.NewView(baseSchema).Reset(buf)
	require.True(t, view.IsValid())
	require.False(t, view.IsFixed())
	require.Equal(t, baseSchema.MinWireSize+8+8+16+5, view.Len()) // big(8), bytes(8), string(16), union(5)
	require.Equal(t, view.Buffer(), buf)
}

func testViewGetVal(t *testing.T, view *schema.View, pos int, cmp any) {
	t.Helper()
	val := view.Get(pos)
	require.Equal(t, cmp, val)
}

func testViewGetFail(t *testing.T, view *schema.View, pos int) {
	t.Helper()
	require.Nil(t, view.Get(pos))
}

func TestViewGet(t *testing.T) {
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := reflect.MustSchemaFor[AllTypes]()
	baseEnc := encode.NewEncoder(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	view := schema.NewView(baseSchema).Reset(buf)

	require.Equal(t, base.Id, view.GetPk())
	testViewGetVal(t, view, 0, base.Id)
	testViewGetVal(t, view, 1, base.Int64)
	testViewGetVal(t, view, 2, base.Int32)
	testViewGetVal(t, view, 3, base.Int16)
	testViewGetVal(t, view, 4, base.Int8)
	testViewGetVal(t, view, 5, base.Uint64)
	testViewGetVal(t, view, 6, base.Uint32)
	testViewGetVal(t, view, 7, base.Uint16)
	testViewGetVal(t, view, 8, base.Uint8)
	testViewGetVal(t, view, 9, base.Float64)
	testViewGetVal(t, view, 10, base.Float32)
	testViewGetVal(t, view, 11, base.D32)
	testViewGetVal(t, view, 12, base.D64)
	testViewGetVal(t, view, 13, base.D128)
	testViewGetVal(t, view, 14, base.D256)
	testViewGetVal(t, view, 15, base.I128)
	testViewGetVal(t, view, 16, base.I256)
	testViewGetVal(t, view, 17, base.Bool)
	testViewGetVal(t, view, 18, base.Time)
	testViewGetVal(t, view, 19, base.Hash)
	testViewGetVal(t, view, 20, base.Array[:]) // return type is []byte
	testViewGetVal(t, view, 21, base.String)
	testViewGetVal(t, view, 22, uint16(0))
	testViewGetVal(t, view, 23, base.Big)
	testViewGetVal(t, view, 24, base.Duration)
	testViewGetVal(t, view, 25, base.Union)
}

func TestViewGetWithVisibility(t *testing.T) {
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := reflect.MustSchemaFor[AllTypes]()
	visSchema, err := baseSchema.DeleteId(2)
	require.NoError(t, err)
	visSchema, err = visSchema.DeleteId(4)
	require.NoError(t, err)
	visSchema, err = visSchema.DeleteId(5)
	require.NoError(t, err)
	visEnc := encode.NewEncoder(visSchema)
	buf, err := visEnc.Encode(&base, nil)
	require.NoError(t, err)
	view := schema.NewView(visSchema).Reset(buf)

	require.Equal(t, base.Id, view.GetPk())
	testViewGetVal(t, view, 0, base.Id)
	testViewGetFail(t, view, 1)
	testViewGetVal(t, view, 2, base.Int32)
	testViewGetFail(t, view, 3)
	testViewGetFail(t, view, 4)
	testViewGetVal(t, view, 5, base.Uint64)
	testViewGetVal(t, view, 6, base.Uint32)
	testViewGetVal(t, view, 7, base.Uint16)
	testViewGetVal(t, view, 8, base.Uint8)
	testViewGetVal(t, view, 9, base.Float64)
	testViewGetVal(t, view, 10, base.Float32)
	testViewGetVal(t, view, 11, base.D32)
	testViewGetVal(t, view, 12, base.D64)
	testViewGetVal(t, view, 13, base.D128)
	testViewGetVal(t, view, 14, base.D256)
	testViewGetVal(t, view, 15, base.I128)
	testViewGetVal(t, view, 16, base.I256)
	testViewGetVal(t, view, 17, base.Bool)
	testViewGetVal(t, view, 18, base.Time)
	testViewGetVal(t, view, 19, base.Hash)
	testViewGetVal(t, view, 20, base.Array[:]) // return type is []byte
	testViewGetVal(t, view, 21, base.String)
	testViewGetVal(t, view, 22, uint16(0))
	testViewGetVal(t, view, 23, base.Big)
	testViewGetVal(t, view, 24, base.Duration)
	testViewGetVal(t, view, 25, base.Union)
}

// TestViewSet tests the Set method of the View struct
func TestViewSet(t *testing.T) {
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := reflect.MustSchemaFor[AllTypes]()
	baseEnc := encode.NewEncoder(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	view := schema.NewView(baseSchema).Reset(buf)

	// Test setting uint64 field
	newId := uint64(12345)
	safeSet(t, view, 0, newId)
	require.Equal(t, newId, view.Get(0), "Uint64 field should have been updated")

	// Read the original string value
	originalString := view.Get(21)
	// require.True(t, ok)

	// Test setting a shorter string
	shortString := "Hello"
	safeSet(t, view, 21, shortString)
	require.Equal(t, originalString, view.Get(21), "String value should not have changed when setting a shorter string")

	// Test setting a string of the same length as the original
	sameLength := "0123456789abcdef"
	safeSet(t, view, 21, sameLength)
	require.Equal(t, originalString, view.Get(21), "String value should not have changed when setting a same-length string")

	// Test setting a longer string
	longString := sameLength + "extra"
	safeSet(t, view, 21, longString)
	require.Equal(t, originalString, view.Get(21), "String value should not have changed when setting a longer string")

	// Test setting invalid index
	panicSet(t, view, -1, 42)
	panicSet(t, view, len(baseSchema.Fields), 42)

	// Test setting incompatible type
	originalId := view.Get(0)
	// require.True(t, ok)
	safeSet(t, view, 0, "not a uint64")
	require.Equal(t, originalId, view.Get(0), "Value should not have changed when setting incompatible type")
}

// safeSet is a helper function to safely call Set and log any panics
func safeSet(t *testing.T, view *schema.View, index int, value any) {
	t.Helper()
	require.NotPanics(t, func() {
		view.Set(index, value)
	}, "setting index %d with value %v", index, value)
}

// panicSet is a helper function to check if calls to Set panic
func panicSet(t *testing.T, view *schema.View, index int, value any) {
	t.Helper()
	require.Panics(t, func() {
		view.Set(index, value)
	}, "setting index %d with value %v", index, value)
}

func TestViewNestingL1(t *testing.T) {
	// type ListFields struct {
	// 	Int64a      int64
	// 	U64List     []uint64        `knox:"u64_list"`
	// 	TimeList    []time.Time     `knox:"time_list,element=date"`
	// 	PairList    []Pair          `knox:"pair_list"`
	// 	ByteList    [][]byte        `knox:"byte_list,notnull"`
	// 	ArrList     [][2]byte       `knox:"arr_list,notnull"`
	// 	DecimalList []num.Decimal32 `knox:"dec_list,notnull,element=scale=4"`
	// 	Int64b      int64
	// }

	// one level nested
	base := NewListFields()
	baseSchema := reflect.MustSchemaFor[ListFields]()
	baseEnc := encode.NewEncoder(baseSchema)
	buf, err := baseEnc.Encode([]ListFields{base, base}, nil)
	require.NoError(t, err)
	require.LessOrEqual(t, 2*baseSchema.MinWireSize, len(buf))

	// test view methods for 2 records with nested fields each
	v := schema.NewView(baseSchema)
	require.Equal(t, 2, v.Count(buf))

	v.Reset(buf)
	require.LessOrEqual(t, baseSchema.MinWireSize, v.Len())
	require.Equal(t, len(buf)/2, v.Len())

	// list iterators produce correct number of list elements
	require.Equal(t, 2, countIter(v.List(1)))
	require.Equal(t, 2, countIter(v.List(2)))
	require.Equal(t, 2, countIter(v.List(3)))
	require.Equal(t, 2, countIter(v.List(4)))
	require.Equal(t, 2, countIter(v.List(5)))
	require.Equal(t, 3, countIter(v.List(6)))

	// individual values
	require.Equal(t, base.Int64a, v.Int64(0))
	// []uint64
	for i, vv := range v.List(1) {
		require.Equal(t, base.U64List[i], vv.Uint64(0), "U64List[%d]", i)
	}
	// []Pair
	for i, vv := range v.List(3) {
		require.Equal(t, base.PairList[i].Key, vv.Int64(0), "PairList[%d].Key", i)
		require.Equal(t, base.PairList[i].Val, vv.Int64(1), "PairList[%d].Val", i)
	}
	// [][]byte
	for i, vv := range v.List(4) {
		require.Equal(t, base.ByteList[i], vv.Bytes(0), "ByteList[%d]", i)
	}
}

func TestViewNestingL2(t *testing.T) {
	// type ListInListFields struct {
	// 	Int64a      int64
	// 	NestedUints [][]uint64
	// 	NestedPairs [][]Pair
	// 	Int64b      int64
	// }
	// one level nested
	base := NewListInListFields()
	baseSchema := reflect.MustSchemaFor[ListInListFields]()
	baseEnc := encode.NewEncoder(baseSchema)
	buf, err := baseEnc.Encode([]ListInListFields{base, base}, nil)
	require.NoError(t, err)
	require.LessOrEqual(t, 2*baseSchema.MinWireSize, len(buf))

	// test view methods for 2 records with nested fields each
	v := schema.NewView(baseSchema)
	require.Equal(t, 2, v.Count(buf))

	v.Reset(buf)
	require.LessOrEqual(t, baseSchema.MinWireSize, v.Len())
	require.Equal(t, len(buf)/2, v.Len())

	// list iterators produce correct number of list elements
	require.Equal(t, 2, countIter(v.List(1)))
	require.Equal(t, 2, countIter(v.List(2)))

	// individual values
	// [][]uint64
	for i, vv := range v.List(1) {
		require.Equal(t, 2, countIter(vv.List(0)))
		for j, vvv := range vv.List(0) {
			require.Equal(t, base.NestedUints[i][j], vvv.Uint64(0), "U64List[%d][%d]", i, j)
		}
	}
	// [][]Pair
	for i, vv := range v.List(2) {
		require.Equal(t, 2, countIter(vv.List(0)))
		for j, vvv := range vv.List(0) {
			require.Equal(t, base.NestedPairs[i][j].Key, vvv.Int64(0), "Pairs[%d][%d].Key", i, j)
			require.Equal(t, base.NestedPairs[i][j].Val, vvv.Int64(1), "Pairs[%d][%d].Val", i, j)
		}
	}
}

func TestViewNestingL3(t *testing.T) {
	// type ListInStructInListFields struct {
	// 	Int64a int64
	// 	Pairs1 []OuterPairStruct
	// 	Int64b int64
	// }
	// one level nested
	base := NewListInStructInListFields()
	baseSchema := reflect.MustSchemaFor[ListInStructInListFields]()
	baseEnc := encode.NewEncoder(baseSchema)
	buf, err := baseEnc.Encode([]ListInStructInListFields{base, base}, nil)
	require.NoError(t, err)
	require.LessOrEqual(t, 2*baseSchema.MinWireSize, len(buf))

	// test view methods for 2 records with nested fields each
	v := schema.NewView(baseSchema)
	require.Equal(t, 2, v.Count(buf))

	v.Reset(buf)
	require.LessOrEqual(t, baseSchema.MinWireSize, v.Len())
	require.Equal(t, len(buf)/2, v.Len())

	// list iterators produce correct number of list elements
	require.Equal(t, 2, countIter(v.List(1)))

	// individual values
	// []OuterPairStruct
	for i, vv := range v.List(1) {
		require.Equal(t, 2, countIter(vv.List(1)))
		for j, vvv := range vv.List(1) {
			require.Equal(t, base.Pairs1[i].Pairs2[j].Key, vvv.Int64(0), "Pairs1[%d].Pairs2[%d].Key", i, j)
			require.Equal(t, base.Pairs1[i].Pairs2[j].Val, vvv.Int64(1), "Pairs1[%d].Pairs2[%d].Val", i, j)
		}
	}
}

func countIter(it iter.Seq2[int, *schema.View]) int {
	var n int
	for range it {
		n++
	}
	return n
}
