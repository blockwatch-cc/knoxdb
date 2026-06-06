// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package schema_tests

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
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

func TestFieldNew(t *testing.T) {
	testCases := []struct {
		name      string
		fieldType schema.FieldType
		expected  *schema.Field
	}{
		{"Int32", schema.Int32, &schema.Field{Type: schema.Int32}},
		{"String", schema.String, &schema.Field{Type: schema.String}},
		{"DateTime", schema.Timestamp, &schema.Field{Type: schema.Timestamp}},
		{"Boolean", schema.Boolean, &schema.Field{Type: schema.Boolean}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			field := schema.FieldOf(tc.fieldType)
			assert.Equal(t, tc.expected, field)
		})
	}
}

func TestFieldCreate(t *testing.T) {
	tests := []struct {
		name            string
		field           *schema.Field
		expectedValid   bool
		expectedVisible bool
		expectedFixed   bool
	}{
		{
			name:            "Valid and visible field",
			field:           schema.FieldOf(schema.Int32, schema.WithName("test")),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
		},
		{
			name:            "Float64",
			field:           schema.FieldOf(schema.Float64, schema.WithName("float64")),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
		},
		{
			name:            "Invalid field (no name)",
			field:           schema.FieldOf(schema.Int32),
			expectedValid:   false,
			expectedVisible: true,
			expectedFixed:   true,
		},
		{
			name: "Invisible field",
			field: schema.FieldOf(schema.Int32,
				schema.WithName("test"),
				schema.WithFlags(schema.FlagDeleted),
			),
			expectedValid:   true,
			expectedVisible: false,
			expectedFixed:   true,
		},
		{
			name:            "Variable string",
			field:           schema.FieldOf(schema.String, schema.WithName("test")),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   false,
		},
		{
			name:            "Variable text",
			field:           schema.FieldOf(schema.Text, schema.WithName("text")),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   false,
		},
		{
			name:            "Variable Bytes",
			field:           schema.FieldOf(schema.Bytes, schema.WithName("bytes")),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   false,
		},
		{
			name:            "Variable Blob",
			field:           schema.FieldOf(schema.Binary, schema.WithName("blob")),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   false,
		},
		{
			name: "String Array",
			field: schema.FieldOf(schema.String,
				schema.WithName("string_array"),
				schema.WithArray(10),
			),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
		},
		{
			name: "Bytes Array",
			field: schema.FieldOf(schema.Bytes,
				schema.WithName("bytes_array"),
				schema.WithArray(10),
			),
			expectedValid:   true,
			expectedVisible: true,
			expectedFixed:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectedValid, tt.field.IsValid())
			assert.Equal(t, tt.expectedVisible, tt.field.IsVisible())
			assert.Equal(t, tt.expectedFixed, tt.field.IsFixedSize())
		})
	}
}

func TestFieldWithMethods(t *testing.T) {
	t.Run("WithName", func(t *testing.T) {
		field := schema.FieldOf(schema.Int32, schema.WithName("new_name"))
		assert.Equal(t, "new_name", field.Name)
	})

	t.Run("schema.WithFlags", func(t *testing.T) {
		field := schema.FieldOf(schema.Int32, schema.WithFlags(schema.FlagTimebase))
		assert.True(t, field.Is(schema.FlagTimebase))
	})

	t.Run("schema.WithArray", func(t *testing.T) {
		field := schema.FieldOf(schema.String, schema.WithArray(10))
		assert.Equal(t, uint8(10), field.Scale)
		assert.Equal(t, schema.FlagArray, field.Flags)
	})

	t.Run("WithScale", func(t *testing.T) {
		field := schema.FieldOf(schema.Decimal64, schema.WithScale(2))
		assert.Equal(t, uint8(2), field.Scale)
	})

	t.Run("WithFilter", func(t *testing.T) {
		field := schema.FieldOf(schema.Int64, schema.WithFilter(schema.BitsFilter))
		assert.Equal(t, schema.BitsFilter, field.Filter)
	})

	t.Run("WithCompress", func(t *testing.T) {
		field := schema.FieldOf(schema.Int64, schema.WithCompression(schema.LZ4))
		assert.Equal(t, schema.LZ4, field.Compress)
	})
}

func TestFieldValidation(t *testing.T) {
	testCases := []struct {
		name      string
		field     *schema.Field
		expectErr bool
	}{
		{
			name:      "Valid int32 field",
			field:     schema.FieldOf(schema.Int32, schema.WithName("test_field")),
			expectErr: false,
		},
		{
			name: "Invalid scale on non-decimal field",
			field: schema.FieldOf(schema.Int32,
				schema.WithName("test_field"),
				schema.WithScale(2),
			),
			expectErr: true,
		},
		{
			name: "Valid decimal field with scale",
			field: schema.FieldOf(schema.Decimal64,
				schema.WithName("test_field"),
				schema.WithScale(2),
			),
			expectErr: false,
		},
		{
			name: "Invalid array on non-string/bytes field",
			field: schema.FieldOf(schema.Int32,
				schema.WithName("test_field"),
				schema.WithArray(10),
			),
			expectErr: true,
		},
		{
			name: "Valid string array",
			field: schema.FieldOf(schema.String,
				schema.WithName("test_field"),
				schema.WithArray(10),
			),
			expectErr: false,
		},
		{
			name: "Valid timebase flag",
			field: schema.FieldOf(schema.Timestamp,
				schema.WithName("test_field"),
				schema.WithFlags(schema.FlagTimebase),
			),
			expectErr: false,
		},
		{
			name: "Valid enum flag for u16 type",
			field: schema.FieldOf(schema.Uint16,
				schema.WithName("test_field"),
				schema.WithFlags(schema.FlagEnum),
				schema.WithEnum(enum.NewEnumDictionary("test_field")),
			),
			expectErr: false,
		},
		{
			name: "Invalid enum flag for string type (must be U16 interally)",
			field: schema.FieldOf(schema.String,
				schema.WithName("test_field"),
				schema.WithFlags(schema.FlagEnum),
			),
			expectErr: true,
		},
		{
			name: "Invalid filter kind",
			field: schema.FieldOf(schema.Int32,
				schema.WithName("test_field"),
				schema.WithFilter(schema.FilterType(100)),
			),
			expectErr: true,
		},
		{
			name: "Valid int field with filter",
			field: schema.FieldOf(schema.Int32,
				schema.WithName("test_field"),
				schema.WithFilter(schema.BitsFilter),
			),
			expectErr: false,
		},
		{
			name: "Valid string field with filter",
			field: schema.FieldOf(schema.String,
				schema.WithName("test_field"),
				schema.WithFilter(schema.BloomFilter2b),
			),
			expectErr: false,
		},
		{
			name: "Invalid timebase flag",
			field: schema.FieldOf(schema.String,
				schema.WithName("test_field"),
				schema.WithFlags(schema.FlagTimebase),
			),
			expectErr: true,
		},
		{
			name: "Invalid enum flag",
			field: schema.FieldOf(schema.Date,
				schema.WithName("test_field"),
				schema.WithFlags(schema.FlagEnum),
			),
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

func TestFieldSerializationRoundTrip(t *testing.T) {
	original := schema.FieldOf(schema.String,
		schema.WithName("test_field"),
		schema.WithFilter(schema.BloomFilter2b),
		schema.WithArray(10),
	)

	var buf bytes.Buffer
	err := original.WriteTo(&buf)
	require.NoError(t, err)

	var readField schema.Field
	err = readField.ReadFrom(&buf)
	require.NoError(t, err)

	assert.Equal(t, original.Name, readField.Name)
	assert.Equal(t, original.Id, readField.Id)
	assert.Equal(t, original.Type, readField.Type)
	assert.Equal(t, original.Flags, readField.Flags)
	assert.Equal(t, original.Compress, readField.Compress)
	assert.Equal(t, original.Filter, readField.Filter)
	assert.Equal(t, original.Scale, readField.Scale)
}

// Helper function for encoding and decoding
func encodeDecodeField(t *testing.T, field *schema.Field, value any) any {
	t.Helper()
	var buf bytes.Buffer
	err := field.WriteValue(&buf, value, binary.NativeEndian)
	require.NoError(t, err, "Encoding failed")

	decoded, err := field.ReadValue(bytes.NewReader(buf.Bytes()), binary.NativeEndian)
	require.NoError(t, err, "Decoding failed")

	return decoded
}

func TestFieldEncodeRoundtrip(t *testing.T) {
	testCases := []struct {
		name  string
		field *schema.Field
		value any
	}{
		{"Int8_Zero", schema.FieldOf(schema.Int8), int8(1)},
		{"Int8_Max", schema.FieldOf(schema.Int8), int8(math.MaxInt8)},
		{"Int16_Zero", schema.FieldOf(schema.Int16), int16(2)},
		{"Int16_Max", schema.FieldOf(schema.Int16), int16(math.MaxInt16)},
		{"Int32_Zero", schema.FieldOf(schema.Int32), int32(3)},
		{"Int32_Max", schema.FieldOf(schema.Int32), int32(math.MaxInt32)},
		{"Int64_Zero", schema.FieldOf(schema.Int64), int64(4)},
		{"Int64_Max", schema.FieldOf(schema.Int64), int64(math.MaxInt64)},
		{"Uint8_Zero", schema.FieldOf(schema.Uint8), uint8(5)},
		{"Uint8_Max", schema.FieldOf(schema.Uint8), uint8(math.MaxUint8)},
		{"Uint16_Zero", schema.FieldOf(schema.Uint16), uint16(6)},
		{"Uint16_Max", schema.FieldOf(schema.Uint16), uint16(math.MaxUint16)},
		{"Uint32_Zero", schema.FieldOf(schema.Uint32), uint32(7)},
		{"Uint32_Max", schema.FieldOf(schema.Uint32), uint32(math.MaxUint32)},
		{"Uint64_Zero", schema.FieldOf(schema.Uint64), uint64(8)},
		{"Uint64_Max", schema.FieldOf(schema.Uint64), uint64(math.MaxUint64)},
		{"Float32_Zero", schema.FieldOf(schema.Float32), float32(9)},
		{"Float32_Max", schema.FieldOf(schema.Float32), float32(math.MaxFloat32)},
		{"Float64_Zero", schema.FieldOf(schema.Float64), float64(10)},
		{"Float64_Max", schema.FieldOf(schema.Float64), float64(math.MaxFloat64)},
		{"Boolean_True", schema.FieldOf(schema.Boolean), true},
		{"Boolean_False", schema.FieldOf(schema.Boolean), false},
		{"DateTime_Now", schema.FieldOf(schema.Timestamp), time.Now().UTC()},
		{"Int128", schema.FieldOf(schema.Int128), num.OneInt128},
		{"Int128", schema.FieldOf(schema.Int256), num.OneInt256},
		{"String", schema.FieldOf(schema.String), "hello"},
		{"Bytes", schema.FieldOf(schema.Bytes), []byte("world")},
		{"StringArray", schema.FieldOf(schema.String, schema.WithArray(2)), "xx"},
		{"BytesArray", schema.FieldOf(schema.Bytes, schema.WithArray(2)), []byte{1, 2}},
		{"Text", schema.FieldOf(schema.Text), "hello"},
		{"Blob", schema.FieldOf(schema.Binary), []byte("world")},
		{"BigInt", schema.FieldOf(schema.Bigint), num.NewBig(11)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			decoded := encodeDecodeField(t, tc.field, tc.value)
			assert.Equal(t, tc.value, decoded)
		})
	}
}
