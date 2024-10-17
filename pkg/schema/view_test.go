// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestViewFixed(t *testing.T) {
	base := NewFixedTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := MustSchemaOf(FixedTypes{})
	baseEnc := NewEncoder(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	view := NewView(baseSchema).Reset(buf)
	require.True(t, view.IsValid())
	require.True(t, view.IsFixed())
	require.Equal(t, baseSchema.WireSize(), view.Len())
	require.Equal(t, view.Bytes(), buf)
	val, ok := view.Get(0)
	require.True(t, ok)
	require.Equal(t, base.Id, val)
	require.Equal(t, base.Id, view.GetPk())
}

func TestViewDynamic(t *testing.T) {
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := MustSchemaOf(AllTypes{})
	baseEnc := NewEncoder(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	view := NewView(baseSchema).Reset(buf)
	require.True(t, view.IsValid())
	require.False(t, view.IsFixed())
	require.Equal(t, view.Len(), baseSchema.WireSize()+8+16)
	require.Equal(t, view.Bytes(), buf)
}

func testViewGetVal(t *testing.T, view *View, pos int, cmp any) {
	val, ok := view.Get(pos)
	require.True(t, ok)
	require.Equal(t, cmp, val)
}

func TestViewGet(t *testing.T) {
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := MustSchemaOf(AllTypes{})
	baseEnc := NewEncoder(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	view := NewView(baseSchema).Reset(buf)

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
}

// TODO
// func TestViewSet(t *testing.T) {}
// func TestViewAppend(t *testing.T) {}

func BenchmarkViewSetPk(b *testing.B) {
	baseSchema := MustSchemaOf(AllTypes{})
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseEnc := NewEncoder(baseSchema)
	view := NewView(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(b, err)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		view.Reset(buf)
		view.SetPk(1)
	}
}

func BenchmarkViewCut(b *testing.B) {
	baseSchema := MustSchemaOf(AllTypes{})
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseEnc := NewEncoder(baseSchema)
	buf := bytes.NewBuffer(nil)
	_, err := baseEnc.Encode(&base, buf)
	require.NoError(b, err)
	_, err = baseEnc.Encode(&base, buf)
	require.NoError(b, err)
	view := NewView(baseSchema)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		view.Cut(buf.Bytes())
	}
}

// TestViewSet tests the Set method of the View struct, verifying correct behavior
// when setting values of various types and lengths, including edge cases and error conditions.
func TestViewSet(t *testing.T) {
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := MustSchemaOf(AllTypes{})
	baseEnc := NewEncoder(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	view := NewView(baseSchema).Reset(buf)

	// Check the original string value
	originalString, ok := view.Get(21)
	require.True(t, ok)
	// t.Logf("Original string: %v", originalString)

	// Test setting a shorter string
	shortString := "Hello"
	err = view.Set(21, shortString)
	require.NoError(t, err)
	val, ok := view.Get(21)
	require.True(t, ok)
	require.Equal(t, shortString, val, "Short string should have been set correctly")

	// Test setting a string of the same length as the original
	sameLength := "0123456789abcdef"
	err = view.Set(21, sameLength)
	require.NoError(t, err)
	val, ok = view.Get(21)
	require.True(t, ok)
	require.Equal(t, sameLength, val, "Same length string should have been set correctly")

	// Test setting the original string value back
	err = view.Set(21, originalString.(string))
	require.NoError(t, err)
	val, ok = view.Get(21)
	require.True(t, ok)
	require.Equal(t, originalString, val, "Original string should have been set correctly")

	// Test setting a longer string (this should not change the value)
	longString := sameLength + "extra"
	err = view.Set(21, longString)
	require.Error(t, err, "Setting a longer string should return an error")
	val, ok = view.Get(21)
	require.True(t, ok)
	require.NotEqual(t, longString, val, "Long string should not have been set")
	require.Equal(t, originalString, val, "Value should remain unchanged for too long strings")

	// Test setting invalid index
	err = view.Set(-1, 42)
	require.Error(t, err, "Setting an invalid index should return an error")
	err = view.Set(len(baseSchema.fields), 42)
	require.Error(t, err, "Setting an out-of-bounds index should return an error")

	// Test setting incompatible type
	originalId := base.Id
	err = view.Set(0, "not a uint64")
	require.Error(t, err, "Setting an incompatible type should return an error")
	val, ok = view.Get(0)
	require.True(t, ok)
	require.Equal(t, originalId, val, "Value should not have changed when setting incompatible type")

	// Test setting uint64 field
	newId := uint64(12345)
	err = view.Set(0, newId)
	require.NoError(t, err)
	val, ok = view.Get(0)
	require.True(t, ok)
	require.Equal(t, newId, val, "Uint64 field should have been updated")
}
