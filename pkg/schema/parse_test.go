// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package schema

import (
	"math"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSingleValueParsing tests parsing of single values for various field types.
func TestSingleValueParsing(t *testing.T) {
	tests := []struct {
		name     string
		parser   ValueParser
		input    string
		expected any
	}{
		{"Int8", NewParser(types.FieldTypeInt8, 0, nil), "127", int8(127)},
		{"Int16", NewParser(types.FieldTypeInt16, 0, nil), "32767", int16(32767)},
		{"Int32", NewParser(types.FieldTypeInt32, 0, nil), "2147483647", int32(2147483647)},
		{"Int64", NewParser(types.FieldTypeInt64, 0, nil), "9223372036854775807", int64(9223372036854775807)},
		{"Uint8", NewParser(types.FieldTypeUint8, 0, nil), "255", uint8(255)},
		{"Uint16", NewParser(types.FieldTypeUint16, 0, nil), "65535", uint16(65535)},
		{"Uint32", NewParser(types.FieldTypeUint32, 0, nil), "4294967295", uint32(4294967295)},
		{"Uint64", NewParser(types.FieldTypeUint64, 0, nil), "18446744073709551615", uint64(18446744073709551615)},
		{"Float32", NewParser(types.FieldTypeFloat32, 0, nil), "3.4028235e+38", float32(3.4028235e+38)},
		{"Float64", NewParser(types.FieldTypeFloat64, 0, nil), "1.7976931348623157e+308", float64(1.7976931348623157e+308)},
		{"Bool", NewParser(types.FieldTypeBoolean, 0, nil), "true", true},
		{"String", NewParser(types.FieldTypeString, 0, nil), "hello world", []byte("hello world")},
		{"Bytes", NewParser(types.FieldTypeBytes, 0, nil), "0x68656c6c6f", []byte("hello")},
		{"Time", NewParser(types.FieldTypeDatetime, 0, nil), "2023-05-17T12:34:56Z", time.Date(2023, 5, 17, 12, 34, 56, 0, time.UTC).UnixNano()},
		{"Int128", NewParser(types.FieldTypeInt128, 0, nil), "170141183460469231731687303715884105727", func() num.Int128 { i, _ := num.ParseInt128("170141183460469231731687303715884105727"); return i }()},
		{"Int256", NewParser(types.FieldTypeInt256, 0, nil), "57896044618658097711785492504343953926634992332820282019728792003956564819967", func() num.Int256 {
			i, _ := num.ParseInt256("57896044618658097711785492504343953926634992332820282019728792003956564819967")
			return i
		}()},
		{"Decimal32", NewParser(types.FieldTypeDecimal32, 2, nil), "123.45", int32(12345)},
		{"Decimal64", NewParser(types.FieldTypeDecimal64, 2, nil), "123456789.12", int64(12345678912)},
		{"Decimal128", NewParser(types.FieldTypeDecimal128, 2, nil), "340282366920938463463374607431.76", func() num.Int128 {
			d, _ := num.ParseDecimal128("340282366920938463463374607431.76")
			return d.Quantize(2).Int128()
		}()},
		{"Decimal256", NewParser(types.FieldTypeDecimal256, 2, nil), "115792089237316195423570985008687907853269984665640564039457.58", func() num.Int256 {
			d, _ := num.ParseDecimal256("115792089237316195423570985008687907853269984665640564039457.58")
			return d.Quantize(2).Int256()
		}()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.parser.ParseValue(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestSliceParsing tests parsing of slice values for various field types.
func TestSliceParsing(t *testing.T) {
	tests := []struct {
		name     string
		parser   ValueParser
		input    string
		expected any
	}{
		{"Int64Slice", NewParser(types.FieldTypeInt64, 0, nil), "-9223372036854775808,0,9223372036854775807", []int64{math.MinInt64, 0, math.MaxInt64}},
		{"Float64Slice", NewParser(types.FieldTypeFloat64, 0, nil), "-1.7976931348623157e+308,0,1.7976931348623157e+308", []float64{-math.MaxFloat64, 0, math.MaxFloat64}},
		{"BoolSlice", NewParser(types.FieldTypeBoolean, 0, nil), "true,false,true", []bool{true, false, true}},
		{"StringSlice", NewParser(types.FieldTypeString, 0, nil), "a,b,c", [][]byte{[]byte("a"), []byte("b"), []byte("c")}},
		{"BytesSlice", NewParser(types.FieldTypeBytes, 0, nil), "0x68,0x65,0x6c", [][]byte{{0x68}, {0x65}, {0x6c}}},
		{"TimeSlice", NewParser(types.FieldTypeDatetime, 0, nil), "2023-05-17T12:34:56Z,2023-05-18T12:34:56Z", []int64{
			time.Date(2023, 5, 17, 12, 34, 56, 0, time.UTC).UnixNano(),
			time.Date(2023, 5, 18, 12, 34, 56, 0, time.UTC).UnixNano(),
		}},
		{"Decimal64Slice", NewParser(types.FieldTypeDecimal64, 2, nil), "-123.45,0,678.90", []int64{-12345, 0, 67890}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.parser.ParseSlice(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestErrorHandling tests error handling for invalid input values.
func TestErrorHandling(t *testing.T) {
	tests := []struct {
		name   string
		parser ValueParser
		input  string
		errMsg string
	}{
		{"InvalidInt", NewParser(types.FieldTypeInt32, 0, nil), "not_a_number", "strconv.ParseInt: parsing \"not_a_number\": invalid syntax"},
		{"InvalidFloat", NewParser(types.FieldTypeFloat64, 0, nil), "not_a_float", "strconv.ParseFloat: parsing \"not_a_float\": invalid syntax"},
		{"InvalidBool", NewParser(types.FieldTypeBoolean, 0, nil), "not_a_bool", "strconv.ParseBool: parsing \"not_a_bool\": invalid syntax"},
		{"InvalidTime", NewParser(types.FieldTypeDatetime, 0, nil), "not_a_time", "time: parsing"},
		{"InvalidHex", NewParser(types.FieldTypeBytes, 0, nil), "0xnothex", "encoding/hex: invalid byte: U+006E 'n'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.parser.ParseValue(tt.input)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

// TestEdgeCases tests parsing of edge cases like empty strings, unicode strings,
// and extreme numerical values.
func TestEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		parser   ValueParser
		input    string
		expected any
	}{
		{"EmptyString", NewParser(types.FieldTypeString, 0, nil), "", []byte("")},
		{"UnicodeString", NewParser(types.FieldTypeString, 0, nil), "ビットコイン", []byte("ビットコイン")},
		{"MinInt64", NewParser(types.FieldTypeInt64, 0, nil), "-9223372036854775808", int64(-9223372036854775808)},
		{"MaxUint64", NewParser(types.FieldTypeUint64, 0, nil), "18446744073709551615", uint64(18446744073709551615)},
		{"SmallFloat", NewParser(types.FieldTypeFloat64, 0, nil), "1.1754943508222875e-38", 1.1754943508222875e-38},
		{"LargeFloat", NewParser(types.FieldTypeFloat64, 0, nil), "1.7976931348623157e+308", 1.7976931348623157e+308},
		{"BinaryInt", NewParser(types.FieldTypeInt32, 0, nil), "0b1010", int32(10)},
		{"OctalInt", NewParser(types.FieldTypeInt32, 0, nil), "0o12", int32(10)},
		{"HexInt", NewParser(types.FieldTypeInt32, 0, nil), "0xA", int32(10)},
		{"ScientificNotation", NewParser(types.FieldTypeFloat64, 0, nil), "1.23e-5", 1.23e-5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.parser.ParseValue(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}

	// t.Run("EmptySlice", func(t *testing.T) {
	// 	parser := NewParser(types.FieldTypeInt32, 0, nil)
	// 	result, err := parser.ParseSlice("")
	// 	require.NoError(t, err)
	// 	assert.Equal(t, []int32{}, result)
	// })

	t.Run("SingleElementSlice", func(t *testing.T) {
		parser := NewParser(types.FieldTypeInt32, 0, nil)
		result, err := parser.ParseSlice("42")
		require.NoError(t, err)
		assert.Equal(t, []int32{42}, result)
	})
}

// TestIntegerParsing tests parsing of integer types, including empty and single-element slices.
func TestIntegerParsing(t *testing.T) {
	t.Run("ParseValue", func(t *testing.T) {
		integerTests := []struct {
			name      string
			fieldType types.FieldType
			input     string
			expected  any
		}{
			{"Int8", types.FieldTypeInt8, "127", int8(127)},
			{"Int16", types.FieldTypeInt16, "32767", int16(32767)},
			{"Int32", types.FieldTypeInt32, "2147483647", int32(2147483647)},
			{"Int64", types.FieldTypeInt64, "9223372036854775807", int64(9223372036854775807)},
			{"Uint8", types.FieldTypeUint8, "255", uint8(255)},
			{"Uint16", types.FieldTypeUint16, "65535", uint16(65535)},
			{"Uint32", types.FieldTypeUint32, "4294967295", uint32(4294967295)},
			{"Uint64", types.FieldTypeUint64, "18446744073709551615", uint64(18446744073709551615)},
		}

		for _, tt := range integerTests {
			t.Run(tt.name, func(t *testing.T) {
				parser := NewParser(tt.fieldType, 0, nil)
				result, err := parser.ParseValue(tt.input)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	// Test empty slice
	t.Run("EmptySlice", func(t *testing.T) {
		parser := NewParser(types.FieldTypeInt64, 0, nil)
		_, err := parser.ParseSlice("")
		require.Error(t, err)
	})

	// Test single element slice
	t.Run("SingleSliceItem", func(t *testing.T) {
		parser := NewParser(types.FieldTypeInt64, 0, nil)
		singleSlice, err := parser.ParseSlice("127")
		require.NoError(t, err)
		require.Equal(t, []int64{127}, singleSlice)
	})

	t.Run("ParseSlice", func(t *testing.T) {
		integerTests := []struct {
			name      string
			fieldType types.FieldType
			input     string
			expected  any
		}{
			{"[]Int8", types.FieldTypeInt8, "127", []int8{127}},
			{"[]Int16", types.FieldTypeInt16, "32767", []int16{32767}},
			{"[]Int32", types.FieldTypeInt32, "2147483647", []int32{2147483647}},
			{"[]Int64", types.FieldTypeInt64, "9223372036854775807", []int64{9223372036854775807}},
			{"[]Uint8", types.FieldTypeUint8, "255", []uint8{255}},
			{"[]Uint16", types.FieldTypeUint16, "65535", []uint16{65535}},
			{"[]Uint32", types.FieldTypeUint32, "4294967295", []uint32{4294967295}},
			{"[]Uint64", types.FieldTypeUint64, "18446744073709551615", []uint64{18446744073709551615}},
		}

		for _, tt := range integerTests {
			t.Run(tt.name, func(t *testing.T) {
				parser := NewParser(tt.fieldType, 0, nil)
				result, err := parser.ParseSlice(tt.input)
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			})
		}
	})
}
