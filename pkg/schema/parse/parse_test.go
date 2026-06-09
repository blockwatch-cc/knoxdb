// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package parse

import (
	"math"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSingleValueParsing tests parsing of single values for various field schema.
func TestSingleValueParsing(t *testing.T) {
	tests := []struct {
		name     string
		parser   ValueParser
		input    string
		expected any
	}{
		{"Int8", NewParser(schema.Int8, 0, nil), "127", int8(127)},
		{"Int16", NewParser(schema.Int16, 0, nil), "32767", int16(32767)},
		{"Int32", NewParser(schema.Int32, 0, nil), "2147483647", int32(2147483647)},
		{"Int64", NewParser(schema.Int64, 0, nil), "9223372036854775807", int64(9223372036854775807)},
		{"Uint8", NewParser(schema.Uint8, 0, nil), "255", uint8(255)},
		{"Uint16", NewParser(schema.Uint16, 0, nil), "65535", uint16(65535)},
		{"Uint32", NewParser(schema.Uint32, 0, nil), "4294967295", uint32(4294967295)},
		{"Uint64", NewParser(schema.Uint64, 0, nil), "18446744073709551615", uint64(18446744073709551615)},
		{"Float32", NewParser(schema.Float32, 0, nil), "3.4028235e+38", float32(3.4028235e+38)},
		{"Float64", NewParser(schema.Float64, 0, nil), "1.7976931348623157e+308", float64(1.7976931348623157e+308)},
		{"Bool", NewParser(schema.Boolean, 0, nil), "true", true},
		{"String", NewParser(schema.String, 0, nil), "hello world", []byte("hello world")},
		{"Bytes", NewParser(schema.Bytes, 0, nil), "0x68656c6c6f", []byte("hello")},
		{"Timestamp_s", NewParser(schema.Timestamp, 3, nil), "2023-05-17 12:34:56 UTC", time.Date(2023, 5, 17, 12, 34, 56, 0, time.UTC).Unix()},
		{"Timestamp_ms", NewParser(schema.Timestamp, 2, nil), "2023-05-17 12:34:56.001 UTC", time.Date(2023, 5, 17, 12, 34, 56, 1000000, time.UTC).UnixMilli()},
		{"Timestamp_us", NewParser(schema.Timestamp, 1, nil), "2023-05-17 12:34:56.000001 UTC", time.Date(2023, 5, 17, 12, 34, 56, 1000, time.UTC).UnixMicro()},
		{"Timestamp_ns", NewParser(schema.Timestamp, 0, nil), "2023-05-17 12:34:56.000000001 UTC", time.Date(2023, 5, 17, 12, 34, 56, 1, time.UTC).UnixNano()},
		{"Time_s", NewParser(schema.Time, 3, nil), "12:34:56", time.Date(1970, 1, 1, 12, 34, 56, 0, time.UTC).Unix()},
		{"Time_ms", NewParser(schema.Time, 2, nil), "12:34:56.001", time.Date(1970, 1, 1, 12, 34, 56, 1000000, time.UTC).UnixMilli()},
		{"Time_us", NewParser(schema.Time, 1, nil), "12:34:56.000001", time.Date(1970, 1, 1, 12, 34, 56, 1000, time.UTC).UnixMicro()},
		{"Time_ns", NewParser(schema.Time, 0, nil), "12:34:56.000000001", time.Date(1970, 1, 1, 12, 34, 56, 1, time.UTC).UnixNano()},
		{"Duration_s", NewParser(schema.Duration, 3, nil), "1s", int64(1)},
		{"Duration_ms", NewParser(schema.Duration, 2, nil), "1s1ms", int64(1001)},
		{"Duration_us", NewParser(schema.Duration, 1, nil), "1s1us", int64(1000001)},
		{"Duration_ns", NewParser(schema.Duration, 0, nil), "1s1ns", int64(1000000001)},
		{"Date", NewParser(schema.Date, 4, nil), "2023-05-17", schema.UnixDays(time.Date(2023, 5, 17, 0, 0, 0, 0, time.UTC))},
		{"Int128", NewParser(schema.Int128, 0, nil), "170141183460469231731687303715884105727", func() num.Int128 { i, _ := num.ParseInt128("170141183460469231731687303715884105727"); return i }()},
		{"Int256", NewParser(schema.Int256, 0, nil), "57896044618658097711785492504343953926634992332820282019728792003956564819967", func() num.Int256 {
			i, _ := num.ParseInt256("57896044618658097711785492504343953926634992332820282019728792003956564819967")
			return i
		}()},
		{"Decimal32", NewParser(schema.Decimal32, 2, nil), "123.45", int32(12345)},
		{"Decimal64", NewParser(schema.Decimal64, 2, nil), "123456789.12", int64(12345678912)},
		{"Decimal128", NewParser(schema.Decimal128, 2, nil), "340282366920938463463374607431.76", func() num.Int128 {
			d, _ := num.ParseDecimal128("340282366920938463463374607431.76")
			return d.Quantize(2).Int128()
		}()},
		{"Decimal256", NewParser(schema.Decimal256, 2, nil), "115792089237316195423570985008687907853269984665640564039457.58", func() num.Int256 {
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

// TestSliceParsing tests parsing of slice values for various field schema.
func TestSliceParsing(t *testing.T) {
	tests := []struct {
		name     string
		parser   ValueParser
		input    string
		expected any
	}{
		{"Int64Slice", NewParser(schema.Int64, 0, nil), "-9223372036854775808,0,9223372036854775807", []int64{math.MinInt64, 0, math.MaxInt64}},
		{"Float64Slice", NewParser(schema.Float64, 0, nil), "-1.7976931348623157e+308,0,1.7976931348623157e+308", []float64{-math.MaxFloat64, 0, math.MaxFloat64}},
		{"BoolSlice", NewParser(schema.Boolean, 0, nil), "true,false,true", []bool{true, false, true}},
		{"StringSlice", NewParser(schema.String, 0, nil), "a,b,c", [][]byte{[]byte("a"), []byte("b"), []byte("c")}},
		{"BytesSlice", NewParser(schema.Bytes, 0, nil), "0x68,0x65,0x6c", [][]byte{{0x68}, {0x65}, {0x6c}}},
		{"TimeSlice_sec", NewParser(schema.Timestamp, 3, nil), "2023-05-17 12:34:56 UTC,2023-05-18 12:34:56 UTC", []int64{
			time.Date(2023, 5, 17, 12, 34, 56, 0, time.UTC).Unix(),
			time.Date(2023, 5, 18, 12, 34, 56, 0, time.UTC).Unix(),
		}},
		{"TimeSlice_ms", NewParser(schema.Timestamp, 2, nil), "2023-05-17 12:34:56.001 UTC,2023-05-18 12:34:56.002 UTC", []int64{
			time.Date(2023, 5, 17, 12, 34, 56, 1000000, time.UTC).UnixMilli(),
			time.Date(2023, 5, 18, 12, 34, 56, 2000000, time.UTC).UnixMilli(),
		}},
		{"TimeSlice_us", NewParser(schema.Timestamp, 1, nil), "2023-05-17 12:34:56.000001 UTC,2023-05-18 12:34:56.000002 UTC", []int64{
			time.Date(2023, 5, 17, 12, 34, 56, 1000, time.UTC).UnixMicro(),
			time.Date(2023, 5, 18, 12, 34, 56, 2000, time.UTC).UnixMicro(),
		}},
		{"TimeSlice_ns", NewParser(schema.Timestamp, 0, nil), "2023-05-17 12:34:56.000000001 UTC,2023-05-18 12:34:56.000000002 UTC", []int64{
			time.Date(2023, 5, 17, 12, 34, 56, 1, time.UTC).UnixNano(),
			time.Date(2023, 5, 18, 12, 34, 56, 2, time.UTC).UnixNano(),
		}},
		{"DateSlice", NewParser(schema.Date, 4, nil), "2023-05-17,2023-05-18", []int64{
			schema.UnixDays(time.Date(2023, 5, 17, 0, 0, 0, 0, time.UTC)),
			schema.UnixDays(time.Date(2023, 5, 18, 0, 0, 0, 0, time.UTC)),
		}},
		{"Decimal64Slice", NewParser(schema.Decimal64, 2, nil), "-123.45,0,678.90", []int64{-12345, 0, 67890}},
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
		{"InvalidInt", NewParser(schema.Int32, 0, nil), "not_a_number", "strconv.ParseInt: parsing \"not_a_number\": invalid syntax"},
		{"InvalidFloat", NewParser(schema.Float64, 0, nil), "not_a_float", "strconv.ParseFloat: parsing \"not_a_float\": invalid syntax"},
		{"InvalidBool", NewParser(schema.Boolean, 0, nil), "not_a_bool", "strconv.ParseBool: parsing \"not_a_bool\": invalid syntax"},
		{"InvalidTime", NewParser(schema.Timestamp, 0, nil), "not_a_time", "parsing time"},
		{"InvalidHex", NewParser(schema.Bytes, 0, nil), "0xnothex", "encoding/hex: invalid byte: U+006E 'n'"},
		{"InvalidOver", NewParser(schema.Bytes, 2, nil), "0x123456", "bytes len 3 > max 2"},
		{"InvalidUnder", NewParser(schema.Bytes, 2, nil), "0x12", "bytes len 1 < min 2"},
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
		{"EmptyString", NewParser(schema.String, 0, nil), "", []byte("")},
		{"UnicodeString", NewParser(schema.String, 0, nil), "ビットコイン", []byte("ビットコイン")},
		{"MinInt64", NewParser(schema.Int64, 0, nil), "-9223372036854775808", int64(-9223372036854775808)},
		{"MaxUint64", NewParser(schema.Uint64, 0, nil), "18446744073709551615", uint64(18446744073709551615)},
		{"SmallFloat", NewParser(schema.Float64, 0, nil), "1.1754943508222875e-38", 1.1754943508222875e-38},
		{"LargeFloat", NewParser(schema.Float64, 0, nil), "1.7976931348623157e+308", 1.7976931348623157e+308},
		{"BinaryInt", NewParser(schema.Int32, 0, nil), "0b1010", int32(10)},
		{"OctalInt", NewParser(schema.Int32, 0, nil), "0o12", int32(10)},
		{"HexInt", NewParser(schema.Int32, 0, nil), "0xA", int32(10)},
		{"ScientificNotation", NewParser(schema.Float64, 0, nil), "1.23e-5", 1.23e-5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.parser.ParseValue(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}

	// t.Run("EmptySlice", func(t *testing.T) {
	// 	parser := NewParser(schema.Int32, 0, nil)
	// 	result, err := parser.ParseSlice("")
	// 	require.NoError(t, err)
	// 	assert.Equal(t, []int32{}, result)
	// })

	t.Run("SingleElementSlice", func(t *testing.T) {
		parser := NewParser(schema.Int32, 0, nil)
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
			fieldType schema.FieldType
			input     string
			expected  any
		}{
			{"Int8", schema.Int8, "127", int8(127)},
			{"Int16", schema.Int16, "32767", int16(32767)},
			{"Int32", schema.Int32, "2147483647", int32(2147483647)},
			{"Int64", schema.Int64, "9223372036854775807", int64(9223372036854775807)},
			{"Uint8", schema.Uint8, "255", uint8(255)},
			{"Uint16", schema.Uint16, "65535", uint16(65535)},
			{"Uint32", schema.Uint32, "4294967295", uint32(4294967295)},
			{"Uint64", schema.Uint64, "18446744073709551615", uint64(18446744073709551615)},
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
		parser := NewParser(schema.Int64, 0, nil)
		_, err := parser.ParseSlice("")
		require.Error(t, err)
	})

	// Test single element slice
	t.Run("SingleSliceItem", func(t *testing.T) {
		parser := NewParser(schema.Int64, 0, nil)
		singleSlice, err := parser.ParseSlice("127")
		require.NoError(t, err)
		require.Equal(t, []int64{127}, singleSlice)
	})

	t.Run("ParseSlice", func(t *testing.T) {
		integerTests := []struct {
			name      string
			fieldType schema.FieldType
			input     string
			expected  any
		}{
			{"[]Int8", schema.Int8, "127", []int8{127}},
			{"[]Int16", schema.Int16, "32767", []int16{32767}},
			{"[]Int32", schema.Int32, "2147483647", []int32{2147483647}},
			{"[]Int64", schema.Int64, "9223372036854775807", []int64{9223372036854775807}},
			{"[]Uint8", schema.Uint8, "255", []uint8{255}},
			{"[]Uint16", schema.Uint16, "65535", []uint16{65535}},
			{"[]Uint32", schema.Uint32, "4294967295", []uint32{4294967295}},
			{"[]Uint64", schema.Uint64, "18446744073709551615", []uint64{18446744073709551615}},
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
