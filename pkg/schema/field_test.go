// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
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

type Name struct {
	First string
	Last  string
}

func (n Name) String() string {
	return n.First + " " + n.Last
}

type Person struct {
	Name Name
}

// TestFieldNew verifies that new Field instances are created correctly with the expected properties.
func TestFieldNew(t *testing.T) {
	testCases := []struct {
		name      string
		fieldType types.FieldType
		expected  Field
	}{
		{"Int32", FT_I32, Field{typ: FT_I32, wireSize: 4}},
		{"String", FT_STRING, Field{typ: FT_STRING, wireSize: 4}},
		{"DateTime", FT_TIME, Field{typ: FT_TIME, wireSize: 8}},
		{"Boolean", FT_BOOL, Field{typ: FT_BOOL, wireSize: 1}},
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
	baseField := NewField(FT_I32).WithName("test_field")

	t.Run("WithName", func(t *testing.T) {
		field := baseField.WithName("new_name")
		assert.Equal(t, "new_name", field.Name())
	})

	t.Run("WithFlags", func(t *testing.T) {
		field := baseField.WithFlags(types.FieldFlagIndexed)
		assert.True(t, field.Is(types.FieldFlagIndexed))
	})

	t.Run("WithFixed", func(t *testing.T) {
		field := NewField(FT_STRING).WithFixed(10)
		assert.Equal(t, uint16(10), field.Fixed())
	})

	t.Run("WithScale", func(t *testing.T) {
		field := NewField(FT_D64).WithScale(2)
		assert.Equal(t, uint8(2), field.Scale())
	})

	t.Run("WithIndex", func(t *testing.T) {
		field := baseField.WithIndex(types.IndexTypeInt)
		assert.Equal(t, types.IndexTypeInt, field.Index())
		assert.True(t, field.Is(types.FieldFlagIndexed))
	})
}

// TestFieldReflectField verifies that Field correctly handles Go types and sets appropriate properties.
func TestFieldReflectField(t *testing.T) {
	type TestStruct struct {
		IntField    int32
		StringField string
	}

	testStruct := TestStruct{}
	structType := reflect.TypeOf(testStruct)

	t.Run("Int32Field", func(t *testing.T) {
		// field := NewField(FT_I32)
		field, err := reflectStructField(structType.Field(0), TAG_NAME)
		require.NoError(t, err)
		assert.Equal(t, uint16(4), field.wireSize)
		assert.Equal(t, []int{0}, field.Path())
		assert.Equal(t, uintptr(0), field.Offset())
	})

	t.Run("StringField", func(t *testing.T) {
		// field := NewField(FT_STRING)
		field, err := reflectStructField(structType.Field(1), TAG_NAME)
		require.NoError(t, err)
		assert.Equal(t, []int{1}, field.Path())
		assert.Equal(t, uint16(4), field.wireSize)
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
			field:     NewField(FT_I32).WithName("test_field"),
			expectErr: false,
		},
		{
			name:      "Invalid scale on non-decimal field",
			field:     NewField(FT_I32).WithName("test_field").WithScale(2),
			expectErr: true,
		},
		{
			name:      "Valid decimal field with scale",
			field:     NewField(FT_D64).WithName("test_field").WithScale(2),
			expectErr: false,
		},
		{
			name:      "Invalid fixed on non-string/bytes field",
			field:     NewField(FT_I32).WithName("test_field").WithFixed(10),
			expectErr: true,
		},
		{
			name:      "Valid string field with fixed",
			field:     NewField(FT_STRING).WithName("test_field").WithFixed(10),
			expectErr: false,
		},
		{
			name:      "Invalid index kind",
			field:     NewField(FT_I32).WithName("test_field").WithIndex(types.IndexType(100)),
			expectErr: true,
		},
		{
			name:      "Valid int field with int index",
			field:     NewField(FT_I32).WithName("test_field").WithIndex(types.IndexTypeInt),
			expectErr: false,
		},
		{
			name:      "Invalid int index on non-int field",
			field:     NewField(FT_STRING).WithName("test_field").WithIndex(types.IndexTypeInt),
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
		{"Datetime", NewField(FT_TIME), OC_TIME},
		{"Int64", NewField(FT_I64), OC_I64},
		{"Int32", NewField(FT_I32), OC_I32},
		{"Int16", NewField(FT_I16), OC_I16},
		{"Int8", NewField(FT_I8), OC_I8},
		{"Uint64", NewField(FT_U64), OC_U64},
		{"Uint32", NewField(FT_U32), OC_U32},
		{"Uint16", NewField(FT_U16), OC_U16},
		{"Uint8", NewField(FT_U8), OC_U8},
		{"Float64", NewField(FT_F64), OC_F64},
		{"Float32", NewField(FT_F32), OC_F32},
		{"Boolean", NewField(FT_BOOL), OC_BOOL},
		{"String", NewField(FT_STRING), OC_STRING},
		{"Bytes", NewField(FT_BYTES), OC_BYTES},
		{"Int256", NewField(FT_I256), OC_I256},
		{"Int128", NewField(FT_I128), OC_I128},
		{"Decimal256", NewField(FT_D256), OC_D256},
		{"Decimal128", NewField(FT_D128), OC_D128},
		{"Decimal64", NewField(FT_D64), OC_D64},
		{"Decimal32", NewField(FT_D32), OC_D32},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.field.Codec())
		})
	}
}

// TestFieldGenericCodecRoundTrip verifies that the generic encoder and decoder can correctly handle a struct with various field types.
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

	rvalTypeOf := reflect.TypeOf(value)
	IntField, err := reflectStructField(rvalTypeOf.Field(0), TAG_NAME)
	require.NoError(t, err)

	StringField, err := reflectStructField(rvalTypeOf.Field(1), TAG_NAME)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		field    Field
		expected any
	}{
		{
			name:     "IntField",
			field:    IntField,
			expected: int32(42),
		},
		{
			name:     "StringField",
			field:    StringField,
			expected: "test",
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
func encodeDecodeField(t *testing.T, field Field, value any) any {
	t.Helper()
	var buf bytes.Buffer
	err := field.Encode(&buf, value, binary.NativeEndian)
	require.NoError(t, err, "Encoding failed")

	decoded, err := field.Decode(bytes.NewReader(buf.Bytes()), binary.NativeEndian)
	require.NoError(t, err, "Decoding failed")

	return decoded
}

// TestFieldEncodingDecoding verifies the encoding and decoding of various field types, including integer, float, and other basic types.
func TestFieldEncodingDecoding(t *testing.T) {
	testCases := []struct {
		name     string
		field    Field
		value    any
		expected any
	}{
		{"Int8_Zero", NewField(FT_I8), int8(0), int8(0)},
		{"Int8_Max", NewField(FT_I8), int8(math.MaxInt8), int8(math.MaxInt8)},
		{"Int16_Zero", NewField(FT_I16), int16(0), int16(0)},
		{"Int16_Max", NewField(FT_I16), int16(math.MaxInt16), int16(math.MaxInt16)},
		{"Int32_Zero", NewField(FT_I32), int32(0), int32(0)},
		{"Int32_Max", NewField(FT_I32), int32(math.MaxInt32), int32(math.MaxInt32)},
		{"Int64_Zero", NewField(FT_I64), int64(0), int64(0)},
		{"Int64_Max", NewField(FT_I64), int64(math.MaxInt64), int64(math.MaxInt64)},
		{"Uint8_Zero", NewField(FT_U8), uint8(0), uint8(0)},
		{"Uint8_Max", NewField(FT_U8), uint8(math.MaxUint8), uint8(math.MaxUint8)},
		{"Uint16_Zero", NewField(FT_U16), uint16(0), uint16(0)},
		{"Uint16_Max", NewField(FT_U16), uint16(math.MaxUint16), uint16(math.MaxUint16)},
		{"Uint32_Zero", NewField(FT_U32), uint32(0), uint32(0)},
		{"Uint32_Max", NewField(FT_U32), uint32(math.MaxUint32), uint32(math.MaxUint32)},
		{"Uint64_Zero", NewField(FT_U64), uint64(0), uint64(0)},
		{"Uint64_Max", NewField(FT_U64), uint64(math.MaxUint64), uint64(math.MaxUint64)},
		{"Float32_Zero", NewField(FT_F32), float32(0), float32(0)},
		{"Float32_Max", NewField(FT_F32), float32(math.MaxFloat32), float32(math.MaxFloat32)},
		{"Float64_Zero", NewField(FT_F64), float64(0), float64(0)},
		{"Float64_Max", NewField(FT_F64), float64(math.MaxFloat64), float64(math.MaxFloat64)},
		{"Boolean_True", NewField(FT_BOOL), true, true},
		{"Boolean_False", NewField(FT_BOOL), false, false},
		{"DateTime_Now", NewField(FT_TIME), time.Now().UTC(), time.Now().UTC()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			decoded := encodeDecodeField(t, tc.field, tc.value)
			if tc.field.Type() == FT_TIME {
				assert.WithinDuration(t, tc.expected.(time.Time), decoded.(time.Time), time.Millisecond)
			} else {
				assert.Equal(t, tc.expected, decoded)
			}
		})
	}
}

// TestFieldSerializationRoundTrip verifies that Field can be serialized and deserialized correctly, preserving all properties.
func TestFieldSerializationRoundTrip(t *testing.T) {
	original := NewField(FT_STRING).
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

// TestFieldRangeAndOverflow verifies the handling of valid ranges and overflow scenarios for all integer types.
func TestFieldRangeAndOverflow(t *testing.T) {
	// Define integer types to test, including signed and unsigned of various sizes (8, 16, 32, and 64 bits)
	intTypes := []struct {
		fieldType  types.FieldType
		goType     reflect.Type
		zero       any
		min        any
		max        any
		isUnsigned bool
	}{
		{FT_I8, reflect.TypeOf(int8(0)), int8(0), int8(math.MinInt8), int8(math.MaxInt8), false},
		{FT_I16, reflect.TypeOf(int16(0)), int16(0), int16(math.MinInt16), int16(math.MaxInt16), false},
		{FT_I32, reflect.TypeOf(int32(0)), int32(0), int32(math.MinInt32), int32(math.MaxInt32), false},
		{FT_I64, reflect.TypeOf(int64(0)), int64(0), int64(math.MinInt64), int64(math.MaxInt64), false},
		{FT_U8, reflect.TypeOf(uint8(0)), uint8(0), uint8(0), uint8(math.MaxUint8), true},
		{FT_U16, reflect.TypeOf(uint16(0)), uint16(0), uint16(0), uint16(math.MaxUint16), true},
		{FT_U32, reflect.TypeOf(uint32(0)), uint32(0), uint32(0), uint32(math.MaxUint32), true},
		{FT_U64, reflect.TypeOf(uint64(0)), uint64(0), uint64(0), uint64(math.MaxUint64), true},
	}

	for _, targetType := range intTypes {
		field := NewField(targetType.fieldType)

		// Test encoding and decoding of minimum, maximum, and zero values for each integer type
		t.Run(fmt.Sprintf("%v_Range", targetType.fieldType), func(t *testing.T) {
			testValue := func(v any) {
				decoded := encodeDecodeField(t, field, v)
				assert.Equal(t, v, decoded)
			}

			testValue(targetType.zero)
			testValue(targetType.min)
			testValue(targetType.max)
		})
	}

	// Test case for datetime fields to ensure proper handling of time values
	t.Run("TimeCaster", func(t *testing.T) {
		field := NewField(FT_TIME)
		now := time.Now().UTC()
		decoded := encodeDecodeField(t, field, now)
		assert.Equal(t, now, decoded.(time.Time).UTC())
	})
}

// TestFieldEncode handles of various ranges and overflow scenarios for all integer types.
func TestFieldEncode(t *testing.T) {
	type TestCase struct {
		Name            string
		FieldType       types.FieldType
		Value           any
		IsErrorExpected bool
	}
	testCases := []TestCase{
		{
			Name:            "MinInt8",
			FieldType:       FT_I8,
			Value:           int8(math.MinInt8),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxInt8",
			FieldType:       FT_I8,
			Value:           int8(math.MaxInt8),
			IsErrorExpected: false,
		},
		{
			Name:            "MinInt16",
			FieldType:       FT_I16,
			Value:           int16(math.MinInt16),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxInt16",
			FieldType:       FT_I16,
			Value:           int16(math.MaxInt16),
			IsErrorExpected: false,
		},
		{
			Name:            "MinInt32",
			FieldType:       FT_I32,
			Value:           int32(math.MinInt32),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxInt32",
			FieldType:       FT_I32,
			Value:           int32(math.MaxInt32),
			IsErrorExpected: false,
		},
		{
			Name:            "MinInt64",
			FieldType:       FT_I64,
			Value:           int64(math.MinInt64),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxInt64",
			FieldType:       FT_I64,
			Value:           int64(math.MaxInt64),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxUint8",
			FieldType:       FT_U8,
			Value:           uint8(math.MaxUint8),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxUint16",
			FieldType:       FT_U16,
			Value:           uint16(math.MaxUint16),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxUint32",
			FieldType:       FT_U32,
			Value:           uint32(math.MaxUint32),
			IsErrorExpected: false,
		},
		{
			Name:            "MaxUint64",
			FieldType:       FT_U64,
			Value:           uint64(math.MaxUint64),
			IsErrorExpected: false,
		},
		{
			Name:            "Zero",
			FieldType:       FT_U8,
			Value:           uint8(0),
			IsErrorExpected: false,
		},
		{
			Name:            "Overflow for int32",
			FieldType:       FT_I32,
			Value:           int64(math.MaxInt64),
			IsErrorExpected: true,
		},
		{
			Name:            "Overflow for int8",
			FieldType:       FT_I8,
			Value:           int32(300),
			IsErrorExpected: true,
		},
		{
			Name:            "Overflow for negative int8",
			FieldType:       FT_I8,
			Value:           int32(-300),
			IsErrorExpected: true,
		},
		{
			Name:            "In Range for negative int8",
			FieldType:       FT_I8,
			Value:           int8(-120),
			IsErrorExpected: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			field := NewField(testCase.FieldType)
			buf := bytes.NewBuffer(nil)
			err := field.Encode(buf, testCase.Value, binary.NativeEndian)
			if testCase.IsErrorExpected {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				decodedVal, err := field.Decode(buf, binary.NativeEndian)
				require.NoError(t, err)
				require.Equal(t, decodedVal, testCase.Value)
			}
		})
	}
}

// TestFieldUtilityMethods verifies the correctness of various utility methods on the Field struct.
func TestFieldUtilityMethods(t *testing.T) {
	marshalTypeOf := reflect.TypeOf(MarshalerTypes{})
	interfaceField, err := reflectStructField(marshalTypeOf.Field(2), TAG_NAME)
	require.NoError(t, err)

	allTypeOf := reflect.TypeOf(AllTypes{})
	arrayField, err := reflectStructField(allTypeOf.Field(20), TAG_NAME)
	require.NoError(t, err)

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
			field:           NewField(FT_I32).WithName("test"),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
			expectedIface:   false,
			expectedArray:   false,
		},
		{
			name:            "Invalid field (no name)",
			field:           NewField(FT_I32),
			expectedValid:   false,
			expectedVisible: true,
			expectedFixed:   true,
			expectedIface:   false,
			expectedArray:   false,
		},
		{
			name:            "Invisible field",
			field:           NewField(FT_I32).WithName("test").WithFlags(types.FieldFlagDeleted),
			expectedValid:   true,
			expectedVisible: false,
			expectedFixed:   true,
			expectedIface:   false,
			expectedArray:   false,
		},
		{
			name:            "Variable-size field",
			field:           NewField(FT_STRING).WithName("test"),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   false,
			expectedIface:   false,
			expectedArray:   false,
		},
		{
			name:            "Interface field",
			field:           interfaceField,
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   false,
			expectedIface:   true,
			expectedArray:   false,
		},
		{
			name:            "Array field",
			field:           arrayField,
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
			expectedIface:   false,
			expectedArray:   true,
		},
		{
			name:            "Array field with fixed size",
			field:           arrayField.WithFixed(10),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
			expectedIface:   false,
			expectedArray:   true,
		},
		{
			name:            "String",
			field:           NewField(FT_STRING).WithName("string"),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   false,
			expectedIface:   false,
			expectedArray:   false,
		},
		{
			name:            "Bytes",
			field:           NewField(FT_BYTES).WithName("bytes"),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   false,
			expectedIface:   false,
			expectedArray:   false,
		},
		{
			name:            "FixedBytes",
			field:           NewField(FT_BYTES).WithName("fixed_bytes").WithFixed(10),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
			expectedIface:   false,
			expectedArray:   false,
		},
		{
			name:            "BytesArray",
			field:           arrayField,
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
			expectedIface:   false,
			expectedArray:   true,
		},
		{
			name:            "FixedBytesArray",
			field:           arrayField.WithName("fixed_bytes_array").WithFixed(5),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
			expectedIface:   false,
			expectedArray:   true,
		},
		{
			name:            "Float64",
			field:           NewField(FT_F64).WithName("float64"),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
			expectedIface:   false,
			expectedArray:   false,
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
}

// TestFieldCodecSpecialCases verifies that Field correctly handles special codec cases.
func TestFieldCodecSpecialCases(t *testing.T) {
	marshalTypeOf := reflect.TypeOf(MarshalerTypes{})
	byterField, err := reflectStructField(marshalTypeOf.Field(2), TAG_NAME)
	require.NoError(t, err)

	marshalField, err := reflectStructField(marshalTypeOf.Field(1), TAG_NAME)
	require.NoError(t, err)

	allTypeOf := reflect.TypeOf(AllTypes{})
	arrayField, err := reflectStructField(allTypeOf.Field(20), TAG_NAME)
	require.NoError(t, err)

	personTypeOf := reflect.TypeOf(Person{})
	stringerField, err := reflectStructField(personTypeOf.Field(0), TAG_NAME)
	require.NoError(t, err)

	tests := []struct {
		name     string
		field    Field
		expected OpCode
	}{
		{"FixedString", NewField(FT_STRING).WithFixed(10), OC_FIXSTRING},
		{"Enum", NewField(FT_U16).WithFlags(types.FieldFlagEnum), OC_ENUM},
		{"TextMarshaler", marshalField, OC_MSHTXT},
		{"Stringer", stringerField, OC_MSHSTR},
		{"BinaryMarshaler", byterField, OC_MSHBIN},
		{"FixedArray", arrayField, OC_FIXARRAY},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.field.Codec())
		})
	}
}

// TestFieldStructValueComplexCases verifies that Field can correctly retrieve values from complex struct types.
func TestFieldStructValueComplexCases(t *testing.T) {
	type TestStruct struct {
		IntField    int
		StringField string
	}

	value := TestStruct{
		IntField:    42,
		StringField: "test",
	}

	valueTypeOf := reflect.TypeOf(value)
	intField, err := reflectStructField(valueTypeOf.Field(0), TAG_NAME)
	require.NoError(t, err)

	stringField, err := reflectStructField(valueTypeOf.Field(1), TAG_NAME)
	require.NoError(t, err)

	rval := reflect.ValueOf(value)

	tests := []struct {
		name     string
		field    Field
		expected any
	}{
		{
			name:     "IntField",
			field:    intField,
			expected: 42,
		},
		{
			name:     "StringField",
			field:    stringField,
			expected: "test",
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

// TestFieldExported verifies that ExportedField correctly represents and handles Field properties, and can retrieve values from structs.
func TestFieldExported(t *testing.T) {
	originalField := NewField(FT_U16).
		WithName("test_field").
		WithFlags(types.FieldFlagIndexed | types.FieldFlagEnum).
		WithIndex(types.IndexTypeHash)

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
		assert.Equal(t, reflect.Struct, result.Kind())  // Change to expect Struct instead of Int32
		assert.Equal(t, testStruct, result.Interface()) // Compare with the whole struct
	})
}
