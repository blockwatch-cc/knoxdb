// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package schema

import (
	"bytes"
	"encoding"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewField verifies that new Field instances are created correctly with the expected properties.
func TestNewField(t *testing.T) {
	testCases := []struct {
		name      string
		fieldType types.FieldType
		expected  Field
	}{
		{"Int32", types.FieldTypeInt32, Field{typ: types.FieldTypeInt32, wireSize: 4}},
		{"String", types.FieldTypeString, Field{typ: types.FieldTypeString, wireSize: 4}},
		{"DateTime", types.FieldTypeDatetime, Field{typ: types.FieldTypeDatetime, wireSize: 8}},
		{"Boolean", types.FieldTypeBoolean, Field{typ: types.FieldTypeBoolean, wireSize: 1}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			field := NewField(tc.fieldType)
			assert.Equal(t, tc.expected, field)
		})
	}
}

// TestFieldWithMethods ensures that Field methods correctly set and return field properties.
func TestFieldWithMethods(t *testing.T) {
	baseField := NewField(types.FieldTypeInt32).WithName("test_field")

	t.Run("WithName", func(t *testing.T) {
		field := baseField.WithName("new_name")
		assert.Equal(t, "new_name", field.Name())
	})

	t.Run("WithFlags", func(t *testing.T) {
		field := baseField.WithFlags(types.FieldFlagIndexed)
		assert.True(t, field.Is(types.FieldFlagIndexed))
	})

	t.Run("WithFixed", func(t *testing.T) {
		field := NewField(types.FieldTypeString).WithFixed(10)
		assert.Equal(t, uint16(10), field.Fixed())
	})

	t.Run("WithScale", func(t *testing.T) {
		field := NewField(types.FieldTypeDecimal64).WithScale(2)
		assert.Equal(t, uint8(2), field.Scale())
	})

	t.Run("WithIndex", func(t *testing.T) {
		field := baseField.WithIndex(types.IndexTypeInt)
		assert.Equal(t, types.IndexTypeInt, field.Index())
		assert.True(t, field.Is(types.FieldFlagIndexed))
	})
}

// TestFieldWithGoType verifies that Field correctly handles Go types and sets appropriate properties.
func TestFieldWithGoType(t *testing.T) {
	type TestStruct struct {
		IntField    int32
		StringField string
	}

	testStruct := TestStruct{}
	structType := reflect.TypeOf(testStruct)

	t.Run("Int32Field", func(t *testing.T) {
		field := NewField(types.FieldTypeInt32).WithGoType(structType.Field(0).Type, []int{0}, structType.Field(0).Offset)
		assert.Equal(t, uint16(4), field.wireSize)
		assert.Equal(t, []int{0}, field.Path())
		assert.Equal(t, uintptr(0), field.Offset())
	})

	t.Run("StringField", func(t *testing.T) {
		field := NewField(types.FieldTypeString).WithGoType(structType.Field(1).Type, []int{1}, structType.Field(1).Offset)
		assert.Equal(t, uint16(16), field.wireSize)
		assert.Equal(t, []int{1}, field.Path())
	})
}

// TestFieldValidation checks if Field properly validates its configuration for various field types and settings.
func TestFieldValidation(t *testing.T) {
	testCases := []struct {
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

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.field.Validate()
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestFieldCodecMapping verifies that Field correctly maps to appropriate OpCode values for different field types.
func TestFieldCodecMapping(t *testing.T) {
	testCases := []struct {
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

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.field.Codec())
		})
	}
}

// TestFieldGenericCodecRoundTrip tests the round-trip encoding and decoding of various field types using GenericEncoder and GenericDecoder.
func TestFieldGenericCodecRoundTrip(t *testing.T) {
	type TestStruct struct {
		IntField     int32         `knox:"int_field"`
		StringField  string        `knox:"string_field"`
		FloatField   float64       `knox:"float_field"`
		TimeField    time.Time     `knox:"time_field"`
		DecimalField num.Decimal64 `knox:"decimal_field,scale=2"`
	}

	enc := NewGenericEncoder[TestStruct]()
	dec := NewGenericDecoder[TestStruct]()

	testData := TestStruct{
		IntField:     42,
		StringField:  "test",
		FloatField:   3.14,
		TimeField:    time.Now().UTC(),
		DecimalField: num.NewDecimal64(314, 2),
	}

	buf, err := enc.Encode(testData, nil)
	require.NoError(t, err)

	decoded, err := dec.Decode(buf, nil)
	require.NoError(t, err)

	assert.Equal(t, testData, *decoded)
}

// TestFieldStructValueRetrieval ensures Field can correctly retrieve values from structs, including pointer fields.
func TestFieldStructValueRetrieval(t *testing.T) {
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

	testCases := []struct {
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

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.field.StructValue(rval)
			if result.Kind() == reflect.Ptr {
				result = result.Elem()
			}
			assert.Equal(t, tc.expected, result.Interface())
		})
	}
}

// Helper function for encoding and decoding
func encodeDecodeField(t *testing.T, field Field, value interface{}) interface{} {
	var buf bytes.Buffer
	err := field.Encode(&buf, value)
	require.NoError(t, err, "Encoding failed")

	decoded, err := field.Decode(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err, "Decoding failed")

	return decoded
}

// TestFieldEncodingDecoding verifies the encoding and decoding of various field types, including integer, float, and other basic types.
func TestFieldEncodingDecoding(t *testing.T) {
	testCases := []struct {
		name     string
		field    Field
		value    interface{}
		expected interface{}
	}{
		{"Int8_Zero", NewField(types.FieldTypeInt8), int8(0), int8(0)},
		{"Int8_Max", NewField(types.FieldTypeInt8), int8(math.MaxInt8), int8(math.MaxInt8)},
		{"Int16_Zero", NewField(types.FieldTypeInt16), int16(0), int16(0)},
		{"Int16_Max", NewField(types.FieldTypeInt16), int16(math.MaxInt16), int16(math.MaxInt16)},
		{"Int32_Zero", NewField(types.FieldTypeInt32), int32(0), int32(0)},
		{"Int32_Max", NewField(types.FieldTypeInt32), int32(math.MaxInt32), int32(math.MaxInt32)},
		{"Int64_Zero", NewField(types.FieldTypeInt64), int64(0), int64(0)},
		{"Int64_Max", NewField(types.FieldTypeInt64), int64(math.MaxInt64), int64(math.MaxInt64)},
		{"Uint8_Zero", NewField(types.FieldTypeUint8), uint8(0), uint8(0)},
		{"Uint8_Max", NewField(types.FieldTypeUint8), uint8(math.MaxUint8), uint8(math.MaxUint8)},
		{"Uint16_Zero", NewField(types.FieldTypeUint16), uint16(0), uint16(0)},
		{"Uint16_Max", NewField(types.FieldTypeUint16), uint16(math.MaxUint16), uint16(math.MaxUint16)},
		{"Uint32_Zero", NewField(types.FieldTypeUint32), uint32(0), uint32(0)},
		{"Uint32_Max", NewField(types.FieldTypeUint32), uint32(math.MaxUint32), uint32(math.MaxUint32)},
		{"Uint64_Zero", NewField(types.FieldTypeUint64), uint64(0), uint64(0)},
		{"Uint64_Max", NewField(types.FieldTypeUint64), uint64(math.MaxUint64), uint64(math.MaxUint64)},
		{"Float32_Zero", NewField(types.FieldTypeFloat32), float32(0), float32(0)},
		{"Float32_Max", NewField(types.FieldTypeFloat32), float32(math.MaxFloat32), float32(math.MaxFloat32)},
		{"Float64_Zero", NewField(types.FieldTypeFloat64), float64(0), float64(0)},
		{"Float64_Max", NewField(types.FieldTypeFloat64), float64(math.MaxFloat64), float64(math.MaxFloat64)},
		{"Boolean_True", NewField(types.FieldTypeBoolean), true, true},
		{"Boolean_False", NewField(types.FieldTypeBoolean), false, false},
		{"String_Empty", NewField(types.FieldTypeString), "", ""},
		{"String_Hello", NewField(types.FieldTypeString), "Hello, World!", "Hello, World!"},
		{"Bytes_Empty", NewField(types.FieldTypeBytes), []byte{}, []byte{}},
		{"Bytes_Data", NewField(types.FieldTypeBytes), []byte{1, 2, 3, 4}, []byte{1, 2, 3, 4}},
		{"DateTime_Now", NewField(types.FieldTypeDatetime), time.Now().UTC(), time.Now().UTC()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			decoded := encodeDecodeField(t, tc.field, tc.value)
			if tc.field.Type() == types.FieldTypeDatetime {
				assert.WithinDuration(t, tc.expected.(time.Time), decoded.(time.Time), time.Millisecond)
			} else {
				assert.Equal(t, tc.expected, decoded)
			}
		})
	}
}

// TestFieldSerializationRoundTrip verifies that Field can be serialized and deserialized correctly, preserving all properties.
func TestFieldSerializationRoundTrip(t *testing.T) {
	original := NewField(types.FieldTypeString).
		WithName("test_field").
		WithFlags(types.FieldFlagIndexed).
		WithIndex(types.IndexTypeHash).
		WithFixed(10)

	var buf bytes.Buffer
	err := original.WriteTo(&buf)
	require.NoError(t, err)

	var readField Field
	err = readField.ReadFrom(&buf)
	require.NoError(t, err)

	assert.Equal(t, original.name, readField.name)
	assert.Equal(t, original.id, readField.id)
	assert.Equal(t, original.typ, readField.typ)
	assert.Equal(t, original.flags, readField.flags)
	assert.Equal(t, original.compress, readField.compress)
	assert.Equal(t, original.index, readField.index)
	assert.Equal(t, original.fixed, readField.fixed)
	assert.Equal(t, original.scale, readField.scale)
}

// TestFieldRangeAndOverflow combines range testing and overflow handling for all integer types and their slices.
func TestFieldRangeAndOverflow(t *testing.T) {
	intTypes := []struct {
		fieldType  types.FieldType
		goType     reflect.Type
		zero       interface{}
		min        interface{}
		max        interface{}
		isUnsigned bool
	}{
		{types.FieldTypeInt8, reflect.TypeOf(int8(0)), int8(0), int8(math.MinInt8), int8(math.MaxInt8), false},
		{types.FieldTypeInt16, reflect.TypeOf(int16(0)), int16(0), int16(math.MinInt16), int16(math.MaxInt16), false},
		{types.FieldTypeInt32, reflect.TypeOf(int32(0)), int32(0), int32(math.MinInt32), int32(math.MaxInt32), false},
		{types.FieldTypeInt64, reflect.TypeOf(int64(0)), int64(0), int64(math.MinInt64), int64(math.MaxInt64), false},
		{types.FieldTypeUint8, reflect.TypeOf(uint8(0)), uint8(0), uint8(0), uint8(math.MaxUint8), true},
		{types.FieldTypeUint16, reflect.TypeOf(uint16(0)), uint16(0), uint16(0), uint16(math.MaxUint16), true},
		{types.FieldTypeUint32, reflect.TypeOf(uint32(0)), uint32(0), uint32(0), uint32(math.MaxUint32), true},
		{types.FieldTypeUint64, reflect.TypeOf(uint64(0)), uint64(0), uint64(0), uint64(math.MaxUint64), true},
	}

	for _, targetType := range intTypes {
		field := NewField(targetType.fieldType)

		t.Run(fmt.Sprintf("%v_Range", targetType.fieldType), func(t *testing.T) {
			testValue := func(v interface{}) {
				decoded := encodeDecodeField(t, field, v)
				assert.Equal(t, v, decoded)
			}

			testValue(targetType.zero)
			testValue(targetType.min)
			testValue(targetType.max)
		})

		for _, inputType := range intTypes {
			t.Run(fmt.Sprintf("%v_to_%v", inputType.fieldType, targetType.fieldType), func(t *testing.T) {
				testConversion := func(v interface{}) {
					var buf bytes.Buffer
					err := field.Encode(&buf, v)
					isErrorExpected := false
					inputValue := reflect.ValueOf(v)
					targetMax := reflect.ValueOf(targetType.max)
					targetMin := reflect.ValueOf(targetType.min)

					if inputType.isUnsigned {
						inputUint := inputValue.Uint()
						if targetType.isUnsigned {
							isErrorExpected = inputUint > targetMax.Uint()
						} else {
							isErrorExpected = inputUint > uint64(targetMax.Int())
						}
					} else {
						inputInt := inputValue.Int()
						if targetType.isUnsigned {
							isErrorExpected = inputInt < 0 || uint64(inputInt) > targetMax.Uint()
						} else {
							isErrorExpected = inputInt < targetMin.Int() || inputInt > targetMax.Int()
						}
					}

					if isErrorExpected {
						assert.Error(t, err, "Expected overflow error for %v to %v with value %v", inputType.fieldType, targetType.fieldType, v)
					} else {
						assert.NoError(t, err, "Unexpected error for %v to %v with value %v: %v", inputType.fieldType, targetType.fieldType, v, err)
						decoded, err := field.Decode(bytes.NewReader(buf.Bytes()))
						assert.NoError(t, err, "Decoding failed for %v to %v with value %v", inputType.fieldType, targetType.fieldType, v)

						decodedValue := reflect.ValueOf(decoded).Convert(targetType.goType)
						expectedValue := reflect.ValueOf(v).Convert(targetType.goType)
						assert.Equal(t, expectedValue.Interface(), decodedValue.Interface(), "Decoded value does not match expected for %v to %v with value %v", inputType.fieldType, targetType.fieldType, v)
					}
				}

				testConversion(inputType.zero)
				testConversion(inputType.min)
				testConversion(inputType.max)

				if !inputType.isUnsigned {
					testConversion(int64(-1)) // Test negative values for unsigned target types
				}
				if inputType.isUnsigned && !targetType.isUnsigned {
					testConversion(uint64(reflect.ValueOf(targetType.max).Int()) + 1) // Test overflow for signed target types
				}
			})
		}
	}

	t.Run("TimeCaster", func(t *testing.T) {
		field := NewField(types.FieldTypeDatetime)
		now := time.Now().UTC()
		decoded := encodeDecodeField(t, field, now)
		assert.Equal(t, now.UTC(), decoded.(time.Time).UTC())
	})
}

// TestFieldUtilityMethods verifies the correctness of various utility methods on the Field struct.
func TestFieldUtilityMethods(t *testing.T) {
	tests := []struct {
		name            string
		field           Field
		expectedValid   bool
		expectedVisible bool
		expectedFixed   bool
		expectedIface   bool
		expectedArray   bool
	}{
		{
			name:            "Valid and visible field",
			field:           NewField(types.FieldTypeInt32).WithName("test"),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
			expectedIface:   false,
			expectedArray:   false,
		},
		{
			name:            "Invalid field (no name)",
			field:           NewField(types.FieldTypeInt32),
			expectedValid:   false,
			expectedVisible: true,
			expectedFixed:   true,
			expectedIface:   false,
			expectedArray:   false,
		},
		{
			name:            "Invisible field",
			field:           NewField(types.FieldTypeInt32).WithName("test").WithFlags(types.FieldFlagDeleted),
			expectedValid:   true,
			expectedVisible: false,
			expectedFixed:   true,
			expectedIface:   false,
			expectedArray:   false,
		},
		{
			name:            "Variable-size field",
			field:           NewField(types.FieldTypeString).WithName("test"),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   false,
			expectedIface:   false,
			expectedArray:   false,
		},
		{
			name:            "Interface field",
			field:           NewField(types.FieldTypeBytes).WithName("test").WithGoType(reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem(), nil, 0),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   false,
			expectedIface:   true,
			expectedArray:   false,
		},
		{
			name:            "Array field",
			field:           NewField(types.FieldTypeBytes).WithName("test").WithGoType(reflect.TypeOf([10]byte{}), nil, 0),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
			expectedIface:   false,
			expectedArray:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectedValid, tt.field.IsValid())
			assert.Equal(t, tt.expectedVisible, tt.field.IsVisible())
			assert.Equal(t, tt.expectedFixed, tt.field.IsFixedSize())
			assert.Equal(t, tt.expectedIface, tt.field.IsInterface())
			assert.Equal(t, tt.expectedArray, tt.field.IsArray())
		})
	}

	t.Run("Fixed-size byte array", func(t *testing.T) {
		f := NewField(types.FieldTypeBytes).WithFixed(10)
		assert.True(t, f.IsArray(), "Fixed-size byte array should be considered an array")
	})

	t.Run("Go array and slice types", func(t *testing.T) {
		f := Field{}.WithGoType(reflect.TypeOf([5]int{}), nil, 0)
		assert.True(t, f.IsArray(), "Go array type should be considered an array")

		f = Field{}.WithGoType(reflect.TypeOf([]int{}), nil, 0)
		assert.True(t, f.IsArray(), "Go slice type should be considered an array")
	})
}

// TestFieldCodecSpecialCases verifies that Field correctly handles special codec cases.
func TestFieldCodecSpecialCases(t *testing.T) {
	tests := []struct {
		name     string
		field    Field
		expected OpCode
	}{
		{"FixedString", NewField(types.FieldTypeString).WithFixed(10), OpCodeFixedString},
		{"Enum", NewField(types.FieldTypeUint16).WithFlags(types.FieldFlagEnum), OpCodeEnum},
		{"TextMarshaler", NewField(types.FieldTypeString).WithGoType(reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem(), nil, 0), OpCodeMarshalText},
		{"Stringer", NewField(types.FieldTypeString).WithGoType(reflect.TypeOf((*fmt.Stringer)(nil)).Elem(), nil, 0), OpCodeStringer},
		{"BinaryMarshaler", NewField(types.FieldTypeBytes).WithGoType(reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem(), nil, 0), OpCodeMarshalBinary},
		{"FixedArray", NewField(types.FieldTypeBytes).WithGoType(reflect.TypeOf([10]byte{}), nil, 0), OpCodeFixedArray},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.field.Codec())
		})
	}
}

// TestFieldStructValueComplexCases verifies that Field can correctly retrieve values from complex struct types.
func TestFieldStructValueComplexCases(t *testing.T) {
	type NestedStruct struct {
		NestedInt int
	}
	type TestStruct struct {
		IntField       int
		StringField    string
		PtrField       *int
		NestedField    NestedStruct
		NestedPtrField *NestedStruct
	}

	intValue := 42
	value := TestStruct{
		IntField:       42,
		StringField:    "test",
		PtrField:       &intValue,
		NestedField:    NestedStruct{NestedInt: 10},
		NestedPtrField: &NestedStruct{NestedInt: 20},
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
			expected: 42,
		},
		{
			name:     "StringField",
			field:    NewField(types.FieldTypeString).WithGoType(reflect.TypeOf(value.StringField), []int{1}, 0),
			expected: "test",
		},
		{
			name:     "PtrField",
			field:    NewField(types.FieldTypeInt32).WithGoType(reflect.TypeOf(value.PtrField), []int{2}, 0),
			expected: 42,
		},
		{
			name:     "NestedField",
			field:    NewField(types.FieldTypeInt32).WithGoType(reflect.TypeOf(value.NestedField.NestedInt), []int{3, 0}, 0),
			expected: 10,
		},
		{
			name:     "NestedPtrField",
			field:    NewField(types.FieldTypeInt32).WithGoType(reflect.TypeOf(value.NestedPtrField.NestedInt), []int{4, 0}, 0),
			expected: 20,
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

// TestExportedField verifies that ExportedField correctly represents and handles Field properties.
func TestExportedField(t *testing.T) {
	originalField := NewField(types.FieldTypeUint16).
		WithName("test_field").
		WithFlags(types.FieldFlagIndexed | types.FieldFlagEnum).
		WithIndex(types.IndexTypeHash).
		WithEnum(NewEnumDictionary("test_enum"))

	exported := ExportedField{
		Name:      originalField.Name(),
		Id:        originalField.Id(),
		Type:      originalField.Type(),
		Flags:     originalField.Flags(),
		Compress:  originalField.Compress(),
		Index:     originalField.Index(),
		IsVisible: originalField.IsVisible(),
		IsArray:   originalField.IsArray(),
		Iface:     originalField.iface,
		Scale:     originalField.Scale(),
		Fixed:     originalField.Fixed(),
		Offset:    originalField.Offset(),
		path:      originalField.Path(),
	}

	t.Run("ExportedField properties", func(t *testing.T) {
		assert.Equal(t, originalField.Name(), exported.Name)
		assert.Equal(t, originalField.Id(), exported.Id)
		assert.Equal(t, originalField.Type(), exported.Type)
		assert.Equal(t, originalField.Flags(), exported.Flags)
		assert.Equal(t, originalField.Compress(), exported.Compress)
		assert.Equal(t, originalField.Index(), exported.Index)
		assert.Equal(t, originalField.IsVisible(), exported.IsVisible)
		assert.Equal(t, originalField.IsArray(), exported.IsArray)
		assert.Equal(t, originalField.iface, exported.Iface)
		assert.Equal(t, originalField.Scale(), exported.Scale)
		assert.Equal(t, originalField.Fixed(), exported.Fixed)
		assert.Equal(t, originalField.Offset(), exported.Offset)
		assert.Equal(t, originalField.Path(), exported.path)
	})

	t.Run("ExportedField StructValue", func(t *testing.T) {
		type TestStruct struct {
			Test int32
		}
		testStruct := TestStruct{Test: 42}
		rval := reflect.ValueOf(testStruct)

		result := exported.StructValue(rval)
		assert.Equal(t, reflect.Int32, result.Kind())
		assert.Equal(t, int32(42), result.Interface().(int32))
	})
}

// Helper function for encoding and decoding round-trip tests
func testEncodeDecodeRoundTrip(t *testing.T, field Field, value interface{}) {
	var buf bytes.Buffer
	err := field.Encode(&buf, value)
	require.NoError(t, err, "Encoding failed")

	decoded, err := field.Decode(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err, "Decoding failed")

	if reflect.TypeOf(value).Kind() == reflect.Slice {
		assert.Equal(t, reflect.ValueOf(value).Interface(), reflect.ValueOf(decoded).Interface(),
			"Decoded slice does not match original")
	} else if _, ok := value.(time.Time); ok {
		assert.Equal(t, value.(time.Time).UTC(), decoded.(time.Time).UTC(),
			"Decoded time does not match original")
	} else {
		assert.Equal(t, value, decoded, "Decoded value does not match original")
	}
}