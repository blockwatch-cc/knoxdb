// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package schema

import (
	"math"
	"reflect"
	"testing"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	intEnum8  int8
	uintEnum8 uint8
)

var (
	intZeroCases = []any{
		int(0),
		int64(0),
		int32(0),
		int16(0),
		int8(0),
		uint(0),
		uint64(0),
		uint32(0),
		uint16(0),
		uint8(0),
		float32(0),
		float64(0),
		num.Decimal32Zero,
		num.Decimal64Zero,
		num.Decimal128Zero,
		num.Decimal256Zero,
		num.ZeroInt128,
		num.ZeroInt256,
		intEnum8(0),
		uintEnum8(0),
	}

	intMaxCases = []any{
		int(^uint(0) >> 1),
		int64(^uint64(0) >> 1),
		int32(^uint32(0) >> 1),
		int16(^uint16(0) >> 1),
		int8(^uint8(0) >> 1),
		^uint(0),
		^uint64(0),
		^uint32(0),
		^uint16(0),
		^uint8(0),
		// math.MaxFloat32,
		// math.MaxFloat64,
		// num.NewDecimal32(int(^uint(0)>>1), 0),
		// num.NewDecimal64(int64(^uint64(0)>>1), 0),
		// num.NewDecimal128(num.MaxInt128, 0),
		// num.NewDecimal256(num.MaxInt256, 0),
		// num.MaxInt128,
		// num.MaxInt256,
		intEnum8(0x7f),
		uintEnum8(0xff),
	}

	intZeroSliceCases = []any{
		[]int{0},
		[]int64{0},
		[]int32{0},
		[]int16{0},
		[]int8{0},
		[]uint{0},
		[]uint64{0},
		[]uint32{0},
		[]uint16{0},
		[]uint8{0},
		[]float32{0},
		[]float64{0},
		[]num.Decimal32{num.Decimal32Zero},
		[]num.Decimal64{num.Decimal64Zero},
		[]num.Decimal128{num.Decimal128Zero},
		[]num.Decimal256{num.Decimal256Zero},
		[]num.Int128{num.ZeroInt128},
		[]num.Int256{num.ZeroInt256},
		[]intEnum8{intEnum8(0)},
		[]uintEnum8{uintEnum8(0)},
	}

	intMaxSliceCases = []any{
		[]int{int(^uint(0) >> 1)},
		[]int64{int64(^uint64(0) >> 1)},
		[]int32{int32(^uint32(0) >> 1)},
		[]int16{int16(^uint16(0) >> 1)},
		[]int8{int8(^uint8(0) >> 1)},
		[]uint{^uint(0)},
		[]uint64{^uint64(0)},
		[]uint32{^uint32(0)},
		[]uint16{^uint16(0)},
		[]uint8{^uint8(0)},
		// []float32{math.MaxFloat32},
		// math.MaxFloat64,
		// num.NewDecimal32(int(^uint(0)>>1), 0),
		// num.NewDecimal64(int64(^uint64(0)>>1), 0),
		// num.NewDecimal128(num.MaxInt128, 0),
		// num.NewDecimal256(num.MaxInt256, 0),
		// num.MaxInt128,
		// num.MaxInt256,
		[]intEnum8{intEnum8(0x7f)},
		[]uintEnum8{uintEnum8(0xff)},
	}
)

func IsInt(v any) bool {
	switch reflect.Indirect(reflect.ValueOf(v)).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	default:
		return false
	}
}

func IsUint(v any) bool {
	switch reflect.Indirect(reflect.ValueOf(v)).Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	default:
		return false
	}
}

func IsFloat(v any) bool {
	switch reflect.Indirect(reflect.ValueOf(v)).Kind() {
	case reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func IsUintOverFlowIntCaster(v any, width uintptr) bool {
	vv := reflect.Indirect(reflect.ValueOf(v))
	switch vv.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return vv.Uint()>>(width-1) != 0
	default:
		return false
	}
}

func IsUintOverFlowUintCaster(v any, width uintptr) bool {
	vv := reflect.Indirect(reflect.ValueOf(v))
	switch vv.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return vv.Uint()>>width != 0
	default:
		return false
	}
}

func ValueSize(v any) uintptr {
	switch v.(type) {
	case int8:
		return 8
	case int16:
		return 16
	case int32:
		return 32
	case int64:
		return 64
	case int:
		return 32 << (^uint(0) >> 63) // 32 or 64
	case uint8:
		return 8
	case uint16:
		return 16
	case uint32:
		return 32
	case uint64:
		return 64
	case uint:
		return 32 << (^uint(0) >> 63) // 32 or 64
	case float32:
		return unsafe.Sizeof(math.MaxFloat32) * 8
	case float64:
		return unsafe.Sizeof(math.MaxFloat64) * 8
	default:
		return 0
	}
}

func IsInt128(v any) bool {
	_, ok := v.(num.Int128)
	return ok
}

// TestCastNewCaster tests the NewCaster function to ensure it returns the correct
// caster for each field type.
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

// TestCastIntCaster tests the IntCaster to ensure it correctly casts various
// input types to int32 and handles edge cases and errors appropriately.
func TestCastIntCaster(t *testing.T) {
	tests := []struct {
		name   string
		caster ValueCaster
		size   uintptr
	}{
		{"Int", IntCaster[int]{}, unsafe.Sizeof(int(0)) * 8},
		{"Int8", IntCaster[int8]{}, unsafe.Sizeof(int8(0)) * 8},
		{"Int16", IntCaster[int16]{}, unsafe.Sizeof(int16(0)) * 8},
		{"Int32", IntCaster[int32]{}, unsafe.Sizeof(int32(0)) * 8},
		{"Int64", IntCaster[int64]{}, unsafe.Sizeof(int64(0)) * 8},
	}

	// zero cases
	t.Run("ZeroCases", func(t *testing.T) {
		for _, testCase := range tests {
			t.Run(testCase.name, func(t *testing.T) {
				for _, z := range intZeroCases {
					_, err := testCase.caster.CastValue(z)
					require.NoError(t, err)
				}
			})
		}
	})

	// max cases
	t.Run("MaxCases", func(t *testing.T) {
		for _, testCase := range tests {
			t.Run(testCase.name, func(t *testing.T) {
				for _, v := range intMaxCases {
					isErrorExpected := testCase.size < ValueSize(v) || (IsUint(v) && IsUintOverFlowIntCaster(v, testCase.size))
					_, err := testCase.caster.CastValue(v)
					if isErrorExpected {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
				}
			})
		}
	})

	// zero slice cases
	t.Run("ZeroSliceCases", func(t *testing.T) {
		for _, testCase := range tests {
			t.Run(testCase.name, func(t *testing.T) {
				for _, v := range intZeroSliceCases {
					_, err := testCase.caster.CastSlice(v)
					require.NoError(t, err)
				}
			})
		}
	})

	// max slice cases
	t.Run("MaxSliceCases", func(t *testing.T) {
		for _, testCase := range tests {
			t.Run(testCase.name, func(t *testing.T) {
				for _, v := range intMaxSliceCases {
					val := reflect.ValueOf(v)
					switch val.Kind() {
					case reflect.Array, reflect.Slice:
						idxZeroVal := val.Index(0).Interface()
						isErrorExpected := testCase.size < ValueSize(idxZeroVal) || (IsUint(idxZeroVal) && IsUintOverFlowIntCaster(idxZeroVal, testCase.size))
						_, err := testCase.caster.CastSlice(v)
						if isErrorExpected {
							require.Error(t, err)
						} else {
							require.NoError(t, err)
						}
					default:
						t.Errorf("value is not supported: %v", v)
					}
				}
			})
		}
	})

	t.Run("Int32", func(t *testing.T) {
		caster := IntCaster[int32]{}
		tests := []struct {
			name     string
			input    interface{}
			expected interface{}
			hasError bool
		}{
			{"Int", 42, int32(42), false},
			{"Int64", int64(42), int32(42), false},
			{"String", "42", nil, true},
			{"MaxInt32", int32(math.MaxInt32), int32(math.MaxInt32), false},
			{"MinInt32", int(math.MinInt32), int32(math.MinInt32), false},
			{"Overflow", int(math.MaxInt32) << 2, nil, true},
			{"Underflow", int64(math.MinInt32) << 2, nil, true},
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

		t.Run("CastSlice", func(t *testing.T) {
			input := []int64{1, 2, 3}
			result, err := caster.CastSlice(input)
			assert.NoError(t, err)
			assert.IsType(t, []int32{}, result)
			assert.Equal(t, []int32{1, 2, 3}, result)

			_, err = caster.CastSlice([]string{"not", "ints"})
			assert.Error(t, err)
		})

	})
}

// TestCastUintCaster tests the UintCaster to ensure it correctly casts various
// input types to uint32 and handles edge cases and errors appropriately.
func TestCastUintCaster(t *testing.T) {
	tests := []struct {
		name   string
		caster ValueCaster
		size   uintptr
	}{
		{"Uint", UintCaster[uint]{}, unsafe.Sizeof(uint(0)) * 8},
		{"Uint8", UintCaster[uint8]{}, unsafe.Sizeof(uint8(0)) * 8},
		{"Uint16", UintCaster[uint16]{}, unsafe.Sizeof(uint16(0)) * 8},
		{"Uint32", UintCaster[uint32]{}, unsafe.Sizeof(uint32(0)) * 8},
		{"Uint64", UintCaster[uint64]{}, unsafe.Sizeof(uint64(0)) * 8},
	}

	// zero cases
	t.Run("ZeroCases", func(t *testing.T) {
		for _, testCase := range tests {
			t.Run(testCase.name, func(t *testing.T) {
				for _, z := range intZeroCases {
					_, err := testCase.caster.CastValue(z)
					require.NoError(t, err)
				}
			})
		}
	})

	// max cases
	t.Run("MaxCases", func(t *testing.T) {
		for _, testCase := range tests {
			t.Run(testCase.name, func(t *testing.T) {
				for _, v := range intMaxCases {
					isErrorExpected := testCase.size < ValueSize(v) || (IsUint(v) && IsUintOverFlowUintCaster(v, testCase.size))
					_, err := testCase.caster.CastValue(v)
					if isErrorExpected {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
				}
			})
		}
	})

	// zero slice cases
	t.Run("ZeroSliceCases", func(t *testing.T) {
		for _, testCase := range tests {
			t.Run(testCase.name, func(t *testing.T) {
				for _, v := range intZeroSliceCases {
					_, err := testCase.caster.CastSlice(v)
					require.NoError(t, err)
				}
			})
		}
	})

	// max slice cases
	t.Run("MaxSliceCases", func(t *testing.T) {
		for _, testCase := range tests {
			t.Run(testCase.name, func(t *testing.T) {
				for _, v := range intMaxSliceCases {
					val := reflect.ValueOf(v)
					switch val.Kind() {
					case reflect.Array, reflect.Slice:
						idxZeroVal := val.Index(0).Interface()
						isErrorExpected := testCase.size < ValueSize(idxZeroVal) || (IsUint(idxZeroVal) && IsUintOverFlowUintCaster(idxZeroVal, testCase.size))
						_, err := testCase.caster.CastSlice(v)
						if isErrorExpected {
							require.Error(t, err)
						} else {
							require.NoError(t, err)
						}
					default:
						t.Errorf("value is not supported: %v", v)
					}
				}
			})
		}
	})

	t.Run("Uint32", func(t *testing.T) {
		caster := UintCaster[uint32]{}
		tests := []struct {
			name     string
			input    interface{}
			expected interface{}
			hasError bool
		}{
			{"Uint", uint(42), uint32(42), false},
			{"Int", 42, uint32(42), false},
			{"String", "42", nil, true},
			{"MaxUint32", uint32(math.MaxUint32), uint32(math.MaxUint32), false},
			{"Overflow", math.MaxUint32 << 2, nil, true},
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

		t.Run("CastSlice", func(t *testing.T) {
			input := []uint64{1, 2, 3}
			result, err := caster.CastSlice(input)
			assert.NoError(t, err)
			assert.IsType(t, []uint32{}, result)
			assert.Equal(t, []uint32{1, 2, 3}, result)
			_, err = caster.CastSlice([]string{"not", "uints"})
			assert.Error(t, err)
		})
	})
}

// TestCastFloatCaster tests the FloatCaster to ensure it correctly casts various
// input types to float32 and handles edge cases and errors appropriately.
func TestCastFloatCaster(t *testing.T) {
	t.Run("Float32", func(t *testing.T) {
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

		t.Run("CastSlice", func(t *testing.T) {
			input := []float64{1.1, 2.2, 3.3}
			result, err := caster.CastSlice(input)
			assert.NoError(t, err)
			assert.Equal(t, []float32{1.1, 2.2, 3.3}, result)

			_, err = caster.CastSlice([]string{"not", "floats"})
			assert.Error(t, err)
		})
	})
}

type CustomBinaryMarshaler struct {
	data []byte
}

func (c CustomBinaryMarshaler) MarshalBinary() ([]byte, error) {
	return c.data, nil
}

// TestCastBytesCaster tests the BytesCaster to ensure it correctly casts various
// input types to []byte and handles edge cases and errors appropriately.
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

// TestCastI128Caster tests the I128Caster to ensure it correctly casts various
// input types to num.Int128 and handles edge cases and errors appropriately.
func TestCastI128Caster(t *testing.T) {
	caster := I128Caster{}

	tests := []struct {
		name     string
		input    interface{}
		expected num.Int128
		hasError bool
	}{
		{"Int", 42, num.Int128FromInt64(42), false},
		{"Int64", int64(42), num.Int128FromInt64(42), false},
		{"String", "42", num.Int128{}, true},
		{"MaxInt64", int64(math.MaxInt64), num.Int128FromInt64(math.MaxInt64), false},
		{"MinInt64", int64(math.MinInt64), num.Int128FromInt64(math.MinInt64), false},
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

	t.Run("CastSlice", func(t *testing.T) {
		input := []int64{1, 2, 3}
		result, err := caster.CastSlice(input)
		assert.NoError(t, err)
		assert.IsType(t, []num.Int128{}, result)
		expected := []num.Int128{num.Int128FromInt64(1), num.Int128FromInt64(2), num.Int128FromInt64(3)}
		assert.Equal(t, expected, result)

		_, err = caster.CastSlice([]string{"not", "int128s"})
		assert.Error(t, err)
	})
}

// TestCastI256Caster tests the I256Caster to ensure it correctly casts various
// input types to num.Int256 and handles edge cases and errors appropriately.
func TestCastI256Caster(t *testing.T) {
	caster := I256Caster{}

	tests := []struct {
		name     string
		input    interface{}
		expected num.Int256
		hasError bool
	}{
		{"Int", 42, num.Int256FromInt64(42), false},
		{"Int64", int64(42), num.Int256FromInt64(42), false},
		{"String", "42", num.Int256{}, true},
		{"MaxInt64", int64(math.MaxInt64), num.Int256FromInt64(math.MaxInt64), false},
		{"MinInt64", int64(math.MinInt64), num.Int256FromInt64(math.MinInt64), false},
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

	t.Run("CastSlice", func(t *testing.T) {
		input := []int64{1, 2, 3}
		result, err := caster.CastSlice(input)
		assert.NoError(t, err)
		assert.IsType(t, []num.Int256{}, result)
		expected := []num.Int256{num.Int256FromInt64(1), num.Int256FromInt64(2), num.Int256FromInt64(3)}
		assert.Equal(t, expected, result)

		_, err = caster.CastSlice([]string{"not", "int256s"})
		assert.Error(t, err)
	})
}
