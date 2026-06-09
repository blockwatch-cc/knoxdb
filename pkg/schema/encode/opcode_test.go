// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFieldCodecMapping verifies that Field correctly maps to appropriate OpCode values for different field types.
func TestFieldCodecMapping(t *testing.T) {
	testCases := []struct {
		name     string
		field    *schema.Field
		expected OpCode
	}{
		{"Datetime", schema.FieldOf(schema.Timestamp), OC_TIMESTAMP},
		{"Date", schema.FieldOf(schema.Date), OC_DATE},
		{"Time", schema.FieldOf(schema.Time), OC_TIME},
		{"Int64", schema.FieldOf(schema.Int64), OC_I64},
		{"Int32", schema.FieldOf(schema.Int32), OC_I32},
		{"Int16", schema.FieldOf(schema.Int16), OC_I16},
		{"Int8", schema.FieldOf(schema.Int8), OC_I8},
		{"Uint64", schema.FieldOf(schema.Uint64), OC_U64},
		{"Uint32", schema.FieldOf(schema.Uint32), OC_U32},
		{"Uint16", schema.FieldOf(schema.Uint16), OC_U16},
		{"Uint8", schema.FieldOf(schema.Uint8), OC_U8},
		{"Float64", schema.FieldOf(schema.Float64), OC_F64},
		{"Float32", schema.FieldOf(schema.Float32), OC_F32},
		{"Boolean", schema.FieldOf(schema.Boolean), OC_BOOL},
		{"String", schema.FieldOf(schema.String), OC_STRING},
		{"ArrayString", schema.FieldOf(schema.String, schema.WithArray(2)), OC_FIXSTRING},
		{"Bytes", schema.FieldOf(schema.Bytes), OC_BYTES},
		{"ArrayBytes", schema.FieldOf(schema.Bytes, schema.WithArray(2)), OC_FIXBYTES},
		{"Int256", schema.FieldOf(schema.Int256), OC_I256},
		{"Int128", schema.FieldOf(schema.Int128), OC_I128},
		{"Decimal256", schema.FieldOf(schema.Decimal256), OC_D256},
		{"Decimal128", schema.FieldOf(schema.Decimal128), OC_D128},
		{"Decimal64", schema.FieldOf(schema.Decimal64), OC_D64},
		{"Decimal32", schema.FieldOf(schema.Decimal32), OC_D32},
		{"Bigint", schema.FieldOf(schema.Bigint), OC_BIGINT},
		{"Text", schema.FieldOf(schema.Text), OC_TEXT},
		{"Binary", schema.FieldOf(schema.Binary), OC_BLOB},
		{"Duration", schema.FieldOf(schema.Duration), OC_DURATION},
		{"List", schema.FieldOf(schema.List), OC_LIST},
		{"Map", schema.FieldOf(schema.Map), OC_MAP},
		{"Union", schema.FieldOf(schema.Union), OC_UNION},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, CodecFor(tc.field))
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

	enc := NewEncoderFor[TestStruct]()
	dec := NewDecoderFor[TestStruct]()

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
