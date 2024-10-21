// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package schema

import (
	"bytes"
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

// TestNewField tests the creation of new Field instances with different types,
// verifying that the resulting fields have the correct properties.
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

// TestFieldWithMethods verifies that Field methods correctly set and return field properties.
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

// TestFieldWithGoType ensures that Field correctly handles Go types and sets appropriate properties.
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
			var buf bytes.Buffer
			err := tc.field.Encode(&buf, tc.value)
			assert.NoError(t, err, "Encoding failed for %v", tc.name)

			decoded, err := tc.field.Decode(bytes.NewReader(buf.Bytes()))
			assert.NoError(t, err, "Decoding failed for %v", tc.name)

			if tc.field.Type() == types.FieldTypeDatetime {
				assert.WithinDuration(t, tc.expected.(time.Time), decoded.(time.Time), time.Millisecond)
			} else {
				assert.Equal(t, tc.expected, decoded, "Decoded value does not match expected for %v", tc.name)
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
		fieldType types.FieldType
		goType    reflect.Type
		zero      interface{}
		min       interface{}
		max       interface{}
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

		// Test range for the target type
		t.Run(fmt.Sprintf("%v_Range", targetType.fieldType), func(t *testing.T) {
			testValue := func(v interface{}) {
				var buf bytes.Buffer
				err := field.Encode(&buf, v)
				require.NoError(t, err, "Encoding failed for value %v", v)

				decoded, err := field.Decode(bytes.NewReader(buf.Bytes()))
				require.NoError(t, err, "Decoding failed for value %v", v)

				assert.Equal(t, v, decoded, "Decoded value does not match original for %v", v)
			}

			testValue(targetType.zero)
			testValue(targetType.min)
			testValue(targetType.max)
		})

		// Test conversions and potential overflows
		for _, inputType := range intTypes {
			t.Run(fmt.Sprintf("%v_to_%v", inputType.fieldType, targetType.fieldType), func(t *testing.T) {
				testConversion := func(v interface{}) {
					var buf bytes.Buffer
					err := field.Encode(&buf, v)
					
					isErrorExpected := reflect.TypeOf(v).Size() > targetType.goType.Size() ||
						(inputType.isUnsigned && !targetType.isUnsigned)

					if isErrorExpected {
						assert.Error(t, err, "Expected overflow error for %v to %v", inputType.fieldType, targetType.fieldType)
					} else {
						assert.NoError(t, err, "Unexpected error for %v to %v", inputType.fieldType, targetType.fieldType)
						if err == nil {
							decoded, decodeErr := field.Decode(bytes.NewReader(buf.Bytes()))
							assert.NoError(t, decodeErr, "Decoding failed for %v to %v", inputType.fieldType, targetType.fieldType)
							assert.Equal(t, reflect.ValueOf(v).Convert(targetType.goType).Interface(), decoded, 
								"Decoded value does not match expected for %v to %v", inputType.fieldType, targetType.fieldType)
						}
					}
				}

				testConversion(inputType.zero)
				testConversion(inputType.min)
				testConversion(inputType.max)
			})
		}

		// Test array handling
		t.Run(fmt.Sprintf("%v_Array", targetType.fieldType), func(t *testing.T) {
			arrayField := field
			arrayField.isArray = true
			slice := reflect.MakeSlice(reflect.SliceOf(targetType.goType), 1, 1)
			slice.Index(0).Set(reflect.ValueOf(targetType.max))
			testEncodeDecodeRoundTrip(t, arrayField, slice.Type(), slice.Interface())
		})
	}

	// Time caster test
	t.Run("TimeCaster", func(t *testing.T) {
		field := NewField(types.FieldTypeDatetime)
		now := time.Now()
		testEncodeDecodeRoundTrip(t, field, reflect.TypeOf(now), now)
	})
}

func testEncodeDecodeRoundTrip(t *testing.T, field Field, inputType reflect.Type, value interface{}) {
	var buf bytes.Buffer
	err := field.Encode(&buf, value)

	testName := fmt.Sprintf("%v_to_%v_%v", inputType, field.Type(), reflect.ValueOf(value).Kind())
	if err != nil {
		t.Logf("Encoding error for %v: %v", testName, err)
		return
	}

	decoded, decodeErr := field.Decode(bytes.NewReader(buf.Bytes()))
	assert.NoError(t, decodeErr, "Decoding failed for %v", testName)

	if reflect.ValueOf(value).Kind() == reflect.Slice {
		assert.Equal(t, reflect.ValueOf(value).Index(0).Interface(), reflect.ValueOf(decoded).Index(0).Interface(), 
			"Decoded value does not match original for %v", testName)
	} else {
		assert.Equal(t, value, decoded, "Decoded value does not match original for %v", testName)
	}
}
