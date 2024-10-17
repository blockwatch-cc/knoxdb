// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package schema

import (
	"math"
	"testing"
	"github.com/stretchr/testify/assert"
	"blockwatch.cc/knoxdb/internal/types"
)

// TestCastNewCaster tests the NewCaster function to ensure it returns the correct
// caster type for each FieldType.
func TestCastNewCaster(t *testing.T) {
	tests := []struct {
		name      string
		fieldType types.FieldType
		expected  interface{}
	}{
		{"Datetime", types.FieldTypeDatetime, TimeCaster{}},
		{"Boolean", types.FieldTypeBoolean, BoolCaster{}},
		{"String", types.FieldTypeString, StringCaster{}},
		{"Bytes", types.FieldTypeBytes, BytesCaster{}},
		{"Int8", types.FieldTypeInt8, IntCaster[int8]{}},
		{"Int16", types.FieldTypeInt16, IntCaster[int16]{}},
		{"Int32", types.FieldTypeInt32, IntCaster[int32]{}},
		{"Int64", types.FieldTypeInt64, IntCaster[int64]{}},
		{"Uint8", types.FieldTypeUint8, UintCaster[uint8]{}},
		{"Uint16", types.FieldTypeUint16, UintCaster[uint16]{}},
		{"Uint32", types.FieldTypeUint32, UintCaster[uint32]{}},
		{"Uint64", types.FieldTypeUint64, UintCaster[uint64]{}},
		{"Float32", types.FieldTypeFloat32, FloatCaster[float32]{}},
		{"Float64", types.FieldTypeFloat64, FloatCaster[float64]{}},
		{"Int128", types.FieldTypeInt128, I128Caster{}},
		{"Int256", types.FieldTypeInt256, I256Caster{}},
		{"Decimal32", types.FieldTypeDecimal32, IntCaster[int32]{}},
		{"Decimal64", types.FieldTypeDecimal64, IntCaster[int64]{}},
		{"Decimal128", types.FieldTypeDecimal128, I128Caster{}},
		{"Decimal256", types.FieldTypeDecimal256, I256Caster{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caster := NewCaster(tt.fieldType)
			assert.IsType(t, tt.expected, caster)
		})
	}
}

// TestCastIntCaster tests the IntCaster's ability to cast various input types
// to int32, including edge cases and error scenarios.
func TestCastIntCaster(t *testing.T) {
	caster := IntCaster[int32]{}

	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
		hasError bool
	}{
		{"Int", 42, int32(42), false},
		{"Int64", int64(42), int32(42), false},
		{"Float64", 42.5, int32(42), false},
		{"String", "42", nil, true},
		{"MaxInt32", int64(math.MaxInt32), int32(math.MaxInt32), false},
		{"MinInt32", int64(math.MinInt32), int32(math.MinInt32), false},
		{"Overflow", uint64(math.MaxInt32) + 1, nil, true},
		{"Underflow", int64(math.MinInt32) - 1, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := caster.CastValue(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}

	// Test slice casting
	t.Run("CastSlice", func(t *testing.T) {
		input := []int64{1, 2, 3}
		result, err := caster.CastSlice(input)
		assert.NoError(t, err)
		assert.IsType(t, []int32{}, result)
		assert.Equal(t, []int32{1, 2, 3}, result)

		_, err = caster.CastSlice([]string{"not", "ints"})
		assert.Error(t, err)
	})
}

// TestCastUintCaster tests the UintCaster's ability to cast various input types
// to uint32, including edge cases and error scenarios.
func TestCastUintCaster(t *testing.T) {
	caster := UintCaster[uint32]{}

	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
		hasError bool
	}{
		{"Uint", uint(42), uint32(42), false},
		{"Int", 42, uint32(42), false},
		{"Float64", 42.5, uint32(42), false},
		{"String", "42", nil, true},
		{"MaxUint32", uint64(math.MaxUint32), uint32(math.MaxUint32), false},
		{"Overflow", uint64(math.MaxUint32) + 1, nil, true},
		{"Negative", -1, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := caster.CastValue(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}

	// Test slice casting
	t.Run("CastSlice", func(t *testing.T) {
		input := []uint64{1, 2, 3}
		result, err := caster.CastSlice(input)
		assert.NoError(t, err)
		assert.IsType(t, []uint32{}, result)
		assert.Equal(t, []uint32{1, 2, 3}, result)

		_, err = caster.CastSlice([]string{"not", "uints"})
		assert.Error(t, err)
	})
}

// TestCastFloatCaster tests the FloatCaster's ability to cast various input types
// to float32, including edge cases and error scenarios.
func TestCastFloatCaster(t *testing.T) {
	caster := FloatCaster[float32]{}

	tests := []struct {
		name     string
		input    interface{}
		expected float32
		hasError bool
	}{
		{"Float32", float32(3.14), 3.14, false},
		{"Float64", 3.14, 3.14, false},
		{"Int", 42, 42.0, false},
		{"String", "3.14", 0, true},
		{"MaxFloat32", float64(math.MaxFloat32), math.MaxFloat32, false},
		{"Overflow", float64(math.MaxFloat32) * 2, float32(math.Inf(1)), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := caster.CastValue(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}

	// Test slice casting
	t.Run("CastSlice", func(t *testing.T) {
		input := []float64{1.1, 2.2, 3.3}
		result, err := caster.CastSlice(input)
		assert.NoError(t, err)
		assert.Equal(t, []float32{1.1, 2.2, 3.3}, result)

		_, err = caster.CastSlice([]string{"not", "floats"})
		assert.Error(t, err)
	})
}

type CustomBinaryMarshaler struct {
	data []byte
}

func (c CustomBinaryMarshaler) MarshalBinary() ([]byte, error) {
	return c.data, nil
}

// TestCastBytesCaster tests the BytesCaster's ability to cast various input types
// to []byte, including custom types implementing MarshalBinary.
func TestCastBytesCaster(t *testing.T) {
	caster := BytesCaster{}

	tests := []struct {
		name     string
		input    interface{}
		expected []byte
		hasError bool
	}{
		{"String", "hello", []byte("hello"), false},
		{"Bytes", []byte{1, 2, 3}, []byte{1, 2, 3}, false},
		{"Int", int32(42), []byte{0, 0, 0, 42}, false},
		{"CustomBinaryMarshaler", CustomBinaryMarshaler{[]byte{4, 5, 6}}, []byte{4, 5, 6}, false},
		{"InvalidType", struct{}{}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := caster.CastValue(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}

	// Test slice casting
	t.Run("CastSlice", func(t *testing.T) {
		input := [][]byte{{1, 2}, {3, 4}}
		result, err := caster.CastSlice(input)
		assert.NoError(t, err)
		assert.IsType(t, [][]byte{}, result)
		assert.Equal(t, input, result)

		_, err = caster.CastSlice([]string{"not", "bytes"})
		assert.Error(t, err)
	})
}

// Add more test functions for other casters (TimeCaster, BoolCaster, StringCaster, I128Caster, I256Caster)
