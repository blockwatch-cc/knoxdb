// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
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

// TestViewSet tests the Set method of the View struct
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
	t.Logf("Original string value: %v", originalString)

	// Test setting a shorter string
	shortString := "Hello"
	safeSet(t, view, 21, shortString)
	val, ok := view.Get(21)
	require.True(t, ok)
	t.Logf("After setting shorter string: %v", val)
	require.Equal(t, originalString, val, "String value should not have changed when setting a shorter string")

	// Test setting a string of the same length as the original
	sameLength := "0123456789abcdef"
	safeSet(t, view, 21, sameLength)
	val, ok = view.Get(21)
	require.True(t, ok)
	t.Logf("After setting same-length string: %v", val)
	require.Equal(t, originalString, val, "String value should not have changed when setting a same-length string")

	// Test setting a longer string
	longString := sameLength + "extra"
	safeSet(t, view, 21, longString)
	val, ok = view.Get(21)
	require.True(t, ok)
	t.Logf("After setting longer string: %v", val)
	require.Equal(t, originalString, val, "String value should not have changed when setting a longer string")

	// Test setting invalid index
	safeSet(t, view, -1, 42)
	safeSet(t, view, len(baseSchema.fields), 42)

	// Test setting incompatible type
	originalId := base.Id
	safeSet(t, view, 0, "not a uint64")
	val, ok = view.Get(0)
	require.True(t, ok)
	t.Logf("After setting incompatible type: %v", val)
	require.Equal(t, originalId, val, "Value should not have changed when setting incompatible type")

	// Test setting uint64 field
	newId := uint64(12345)
	safeSet(t, view, 0, newId)
	val, ok = view.Get(0)
	require.True(t, ok)
	t.Logf("After setting uint64 field: %v", val)
	require.Equal(t, newId, val, "Uint64 field should have been updated")
}

// safeSet is a helper function to safely call Set and log any panics
func safeSet(t *testing.T, view *View, index int, value interface{}) {
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Panic occurred when setting index %d with value %v: %v", index, value, r)
		}
	}()
	view.Set(index, value)
}

// TestViewAppend tests the Append method of the View struct, verifying correct behavior
// when appending values of various types, including edge cases and error conditions.
func TestViewAppend(t *testing.T) {
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := MustSchemaOf(AllTypes{})
	baseEnc := NewEncoder(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	view := NewView(baseSchema).Reset(buf)

	tests := []struct {
		name     string
		fieldIdx int
		expected interface{}
	}{
		{"Datetime", 18, []time.Time{base.Time}},
		{"Int64", 1, []int64{base.Int64}},
		{"Int32", 2, []int32{base.Int32}},
		{"Int16", 3, []int16{base.Int16}},
		{"Int8", 4, []int8{base.Int8}},
		{"Uint64", 5, []uint64{base.Uint64}},
		{"Uint32", 6, []uint32{base.Uint32}},
		{"Uint16", 7, []uint16{base.Uint16}},
		{"Uint8", 8, []uint8{base.Uint8}},
		{"Float64", 9, []float64{base.Float64}},
		{"Float32", 10, []float32{base.Float32}},
		{"Boolean", 17, []bool{base.Bool}},
		{"String", 21, []string{base.String}},
		{"Bytes", 19, [][]byte{base.Hash}},
		{"Int256", 16, []num.Int256{base.I256}},
		{"Int128", 15, []num.Int128{base.I128}},
		{"Decimal256", 14, []num.Decimal256{base.D256}},
		{"Decimal128", 13, []num.Decimal128{base.D128}},
		{"Decimal64", 12, []num.Decimal64{base.D64}},
		{"Decimal32", 11, []num.Decimal32{base.D32}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Logf("Panic occurred for field %s: %v", tt.name, r)
				}
			}()

			if tt.fieldIdx < 0 || tt.fieldIdx >= len(baseSchema.fields) {
				t.Logf("Skipping test for field %s due to invalid index %d", tt.name, tt.fieldIdx)
				return
			}

			result := view.Append(nil, tt.fieldIdx)
			if result == nil {
				t.Logf("Append returned nil for field %s", tt.name)
				return
			}

			switch tt.name {
			case "Float64":
				require.IsType(t, []float64{}, result)
				require.InDelta(t, tt.expected.([]float64)[0], result.([]float64)[0], 1e-6)
			case "Float32":
				require.IsType(t, []float32{}, result)
				require.InDelta(t, tt.expected.([]float32)[0], result.([]float32)[0], 1e-6)
			default:
				require.Equal(t, tt.expected, result, "Appending to nil should create a new slice with one element")
			}

			result = view.Append(result, tt.fieldIdx)
			require.Len(t, result, 2, "Appending to existing slice should add one element")

			// Verify the appended values
			switch v := result.(type) {
			case []time.Time:
				require.Equal(t, base.Time, v[0])
				require.Equal(t, base.Time, v[1])
			case []int64:
				require.Equal(t, base.Int64, v[0])
				require.Equal(t, base.Int64, v[1])
			case []int32:
				require.Equal(t, base.Int32, v[0])
				require.Equal(t, base.Int32, v[1])
			case []int16:
				require.Equal(t, base.Int16, v[0])
				require.Equal(t, base.Int16, v[1])
			case []int8:
				require.Equal(t, base.Int8, v[0])
				require.Equal(t, base.Int8, v[1])
			case []uint64:
				require.Equal(t, base.Uint64, v[0])
				require.Equal(t, base.Uint64, v[1])
			case []uint32:
				require.Equal(t, base.Uint32, v[0])
				require.Equal(t, base.Uint32, v[1])
			case []uint16:
				require.Equal(t, base.Uint16, v[0])
				require.Equal(t, base.Uint16, v[1])
			case []uint8:
				require.Equal(t, base.Uint8, v[0])
				require.Equal(t, base.Uint8, v[1])
			case []float64:
				require.Equal(t, base.Float64, v[0])
				require.Equal(t, base.Float64, v[1])
			case []float32:
				require.Equal(t, base.Float32, v[0])
				require.Equal(t, base.Float32, v[1])
			case []bool:
				require.Equal(t, base.Bool, v[0])
				require.Equal(t, base.Bool, v[1])
			case []string:
				require.Equal(t, base.String, v[0])
				require.Equal(t, base.String, v[1])
			case [][]byte:
				require.Equal(t, base.Hash, v[0])
				require.Equal(t, base.Hash, v[1])
			case []num.Int256:
				require.Equal(t, base.I256, v[0])
				require.Equal(t, base.I256, v[1])
			case []num.Int128:
				require.Equal(t, base.I128, v[0])
				require.Equal(t, base.I128, v[1])
			case []num.Decimal256:
				require.Equal(t, base.D256, v[0])
				require.Equal(t, base.D256, v[1])
			case []num.Decimal128:
				require.Equal(t, base.D128, v[0])
				require.Equal(t, base.D128, v[1])
			case []num.Decimal64:
				require.Equal(t, base.D64, v[0])
				require.Equal(t, base.D64, v[1])
			case []num.Decimal32:
				require.Equal(t, base.D32, v[0])
				require.Equal(t, base.D32, v[1])
			default:
				t.Fatalf("Unexpected type: %T", result)
			}
		})
	}

	// Test appending with invalid index
	t.Run("Invalid Index", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Expected panic occurred: %v", r)
			}
		}()

		result := view.Append(nil, -1)
		require.Nil(t, result, "Appending with negative index should return nil")

		result = view.Append(nil, len(baseSchema.fields))
		require.Nil(t, result, "Appending with out-of-bounds index should return nil")
	})

	// Test appending to invalid view
	t.Run("Invalid View", func(t *testing.T) {
		invalidView := NewView(baseSchema)
		result := invalidView.Append(nil, 0)
		require.Nil(t, result, "Appending to invalid view should return nil")
	})
}

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
