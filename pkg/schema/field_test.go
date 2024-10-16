// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package schema

import (
	"reflect"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewField(t *testing.T) {
	tests := []struct {
		name     string
		typ      types.FieldType
		expected Field
	}{
		{
			name: "Int32",
			typ:  types.FieldTypeInt32,
			expected: Field{
				typ:      types.FieldTypeInt32,
				dataSize: 4,
				wireSize: 4,
			},
		},
		{
			name: "String",
			typ:  types.FieldTypeString,
			expected: Field{
				typ:      types.FieldTypeString,
				dataSize: 16, // assuming 64-bit system
				wireSize: 16, // assuming 64-bit system
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewField(tt.typ)
			assert.Equal(t, tt.expected.typ, f.typ)
			assert.Equal(t, tt.expected.dataSize, f.dataSize)
			assert.Equal(t, tt.expected.wireSize, f.wireSize)
		})
	}
}

func TestFieldMethods(t *testing.T) {
	base := NewField(types.FieldTypeInt32)

	t.Run("WithName", func(t *testing.T) {
		f := base.WithName("test_field")
		assert.Equal(t, "test_field", f.Name())
	})

	t.Run("WithFlags", func(t *testing.T) {
		f := base.WithFlags(types.FieldFlagIndexed | types.FieldFlagInternal)
		assert.True(t, f.Is(types.FieldFlagIndexed))
		assert.True(t, f.Is(types.FieldFlagInternal))
	})

	t.Run("WithCompression", func(t *testing.T) {
		f := base.WithCompression(types.FieldCompression(1))
		assert.Equal(t, types.FieldCompression(1), f.Compress())
	})

	t.Run("WithFixed", func(t *testing.T) {
		f := NewField(types.FieldTypeString).WithFixed(10)
		assert.Equal(t, uint16(10), f.Fixed())
	})

	t.Run("WithScale", func(t *testing.T) {
		f := NewField(types.FieldTypeDecimal64).WithScale(2)
		assert.Equal(t, uint8(2), f.Scale())
	})

	t.Run("WithIndex", func(t *testing.T) {
		f := base.WithIndex(types.IndexTypeInt)
		assert.Equal(t, types.IndexTypeInt, f.Index())
		assert.True(t, f.Is(types.FieldFlagIndexed))
	})
}

func TestFieldGoType(t *testing.T) {
	type TestStruct struct {
		IntField    int32
		StringField string
	}

	typ := reflect.TypeOf(TestStruct{})

	t.Run("Int32Field", func(t *testing.T) {
		field, _ := typ.FieldByName("IntField")
		f := NewField(types.FieldTypeInt32).WithGoType(field.Type, field.Index, field.Offset)
		assert.Equal(t, []int{0}, f.Path())
		assert.Equal(t, field.Offset, f.Offset())
		assert.Equal(t, uint16(4), f.dataSize)
		assert.Equal(t, uint16(4), f.wireSize)
	})

	t.Run("StringField", func(t *testing.T) {
		field, _ := typ.FieldByName("StringField")
		f := NewField(types.FieldTypeString).WithGoType(field.Type, field.Index, field.Offset)
		assert.Equal(t, []int{1}, f.Path())
		assert.Equal(t, field.Offset, f.Offset())
		assert.Equal(t, uint16(16), f.dataSize)  // assuming 64-bit system
		assert.Equal(t, uint16(16), f.wireSize) // assuming 64-bit system
	})
}

func TestFieldValidate(t *testing.T) {
	tests := []struct {
		name      string
		field     Field
		expectErr bool
	}{
		{
			name:      "Valid int32 field",
			field:     NewField(types.FieldTypeInt32).WithName("test_field"),
			expectErr: false,
		},
		{
			name:      "Invalid scale on non-decimal field",
			field:     NewField(types.FieldTypeInt32).WithName("test_field").WithScale(2),
			expectErr: true,
		},
		{
			name:      "Valid decimal field with scale",
			field:     NewField(types.FieldTypeDecimal64).WithName("test_field").WithScale(2),
			expectErr: false,
		},
		{
			name:      "Invalid fixed on non-string/bytes field",
			field:     NewField(types.FieldTypeInt32).WithName("test_field").WithFixed(10),
			expectErr: true,
		},
		{
			name:      "Valid string field with fixed",
			field:     NewField(types.FieldTypeString).WithName("test_field").WithFixed(10),
			expectErr: false,
		},
		{
			name:      "Invalid index kind",
			field:     NewField(types.FieldTypeInt32).WithName("test_field").WithIndex(types.IndexType(100)),
			expectErr: true,
		},
		{
			name:      "Valid int field with int index",
			field:     NewField(types.FieldTypeInt32).WithName("test_field").WithIndex(types.IndexTypeInt),
			expectErr: false,
		},
		{
			name:      "Invalid int index on non-int field",
			field:     NewField(types.FieldTypeString).WithName("test_field").WithIndex(types.IndexTypeInt),
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.field.Validate()
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFieldCodec(t *testing.T) {
	tests := []struct {
		name     string
		field    Field
		expected OpCode
	}{
		{"Datetime", NewField(types.FieldTypeDatetime), OpCodeDateTime},
		{"Int64", NewField(types.FieldTypeInt64), OpCodeInt64},
		{"Int32", NewField(types.FieldTypeInt32), OpCodeInt32},
		{"Int16", NewField(types.FieldTypeInt16), OpCodeInt16},
		{"Int8", NewField(types.FieldTypeInt8), OpCodeInt8},
		{"Uint64", NewField(types.FieldTypeUint64), OpCodeUint64},
		{"Uint32", NewField(types.FieldTypeUint32), OpCodeUint32},
		{"Uint16", NewField(types.FieldTypeUint16), OpCodeUint16},
		{"Uint8", NewField(types.FieldTypeUint8), OpCodeUint8},
		{"Float64", NewField(types.FieldTypeFloat64), OpCodeFloat64},
		{"Float32", NewField(types.FieldTypeFloat32), OpCodeFloat32},
		{"Boolean", NewField(types.FieldTypeBoolean), OpCodeBool},
		{"String", NewField(types.FieldTypeString), OpCodeString},
		{"Bytes", NewField(types.FieldTypeBytes), OpCodeBytes},
		{"Int256", NewField(types.FieldTypeInt256), OpCodeInt256},
		{"Int128", NewField(types.FieldTypeInt128), OpCodeInt128},
		{"Decimal256", NewField(types.FieldTypeDecimal256), OpCodeDecimal256},
		{"Decimal128", NewField(types.FieldTypeDecimal128), OpCodeDecimal128},
		{"Decimal64", NewField(types.FieldTypeDecimal64), OpCodeDecimal64},
		{"Decimal32", NewField(types.FieldTypeDecimal32), OpCodeDecimal32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.field.Codec())
		})
	}
}

func TestFieldGenericCodec(t *testing.T) {
	type TestStruct struct {
		IntField    int32   `knox:"int_field"`
		StringField string  `knox:"string_field"`
		FloatField  float64 `knox:"float_field"`
		TimeField   time.Time `knox:"time_field"`
		DecimalField num.Decimal64 `knox:"decimal_field,scale=2"`
	}

	enc := NewGenericEncoder[TestStruct]()
	dec := NewGenericDecoder[TestStruct]()

	testData := TestStruct{
		IntField:    42,
		StringField: "test",
		FloatField:  3.14,
		TimeField:   time.Now().UTC(),
		DecimalField: num.NewDecimal64(314, 2),
	}

	buf, err := enc.Encode(testData, nil)
	require.NoError(t, err)

	decoded, err := dec.Decode(buf, nil)
	require.NoError(t, err)

	assert.Equal(t, testData, *decoded)
}

func TestFieldStructValue(t *testing.T) {
	type TestStruct struct {
		IntField    int32
		StringField string
		PtrField    *int32
	}

	intValue := int32(42)
	value := TestStruct{
		IntField:    42,
		StringField: "test",
		PtrField:    &intValue,
	}

	rval := reflect.ValueOf(value)

	tests := []struct {
		name     string
		field    Field
		expected interface{}
	}{
		{
			name:     "IntField",
			field:    NewField(types.FieldTypeInt32).WithGoType(reflect.TypeOf(value.IntField), []int{0}, 0),
			expected: int32(42),
		},
		{
			name:     "StringField",
			field:    NewField(types.FieldTypeString).WithGoType(reflect.TypeOf(value.StringField), []int{1}, 0),
			expected: "test",
		},
		{
			name:     "PtrField",
			field:    NewField(types.FieldTypeInt32).WithGoType(reflect.TypeOf(value.PtrField), []int{2}, 0),
			expected: int32(42),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.field.StructValue(rval)
			if result.Kind() == reflect.Ptr {
				result = result.Elem()
			}
			assert.Equal(t, tt.expected, result.Interface())
		})
	}
}

func TestExportedField(t *testing.T) {
	type TestStruct struct {
		IntField    int32
		StringField string
		PtrField    *int32
	}

	value := TestStruct{
		IntField:    42,
		StringField: "test",
		PtrField:    nil,
	}

	rval := reflect.ValueOf(value)

	tests := []struct {
		name     string
		field    ExportedField
		expected interface{}
	}{
		{
			name:     "IntField",
			field:    ExportedField{Name: "IntField", Type: types.FieldTypeInt32, path: []int{0}},
			expected: int32(42),
		},
		{
			name:     "StringField",
			field:    ExportedField{Name: "StringField", Type: types.FieldTypeString, path: []int{1}},
			expected: "test",
		},
		{
			name:     "PtrField",
			field:    ExportedField{Name: "PtrField", Type: types.FieldTypeInt32, path: []int{2}},
			expected: int32(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.field.StructValue(rval)
			assert.Equal(t, tt.expected, result.Interface())
		})
	}
}
