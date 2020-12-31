// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//
package pack

import (
	"fmt"
	"time"

	. "blockwatch.cc/knoxdb/encoding/decimal"
	. "blockwatch.cc/knoxdb/vec"
)

// Note: may evolve into a CAST function
func (t FieldType) CastType(val interface{}, f Field) (interface{}, error) {
	var ok bool
	res := val
	switch t {
	case FieldTypeBytes:
		_, ok = val.([]byte)
	case FieldTypeString:
		_, ok = val.(string)
	case FieldTypeDatetime:
		_, ok = val.(time.Time)
	case FieldTypeBoolean:
		_, ok = val.(bool)
	case FieldTypeInt128:
		switch v := val.(type) {
		case int:
			res, ok = int64(v), true
		case int64:
			res, ok = int64(v), true
		case int32:
			res, ok = int64(v), true
		case int16:
			res, ok = int64(v), true
		case int8:
			res, ok = int64(v), true
		case *Decimal32:
			res, ok = v.RoundToInt64(), true
		case *Decimal64:
			res, ok = v.RoundToInt64(), true
		case *Decimal128:
			res, ok = v.RoundToInt64(), true
		case *Decimal256:
			res, ok = v.RoundToInt64(), true
		default:
			ok = false
		}
	case FieldTypeInt64:
		switch v := val.(type) {
		case int:
			res, ok = int64(v), true
		case int64:
			res, ok = int64(v), true
		case int32:
			res, ok = int64(v), true
		case int16:
			res, ok = int64(v), true
		case int8:
			res, ok = int64(v), true
		case Decimal32:
			res, ok = v.RoundToInt64(), true
		case Decimal64:
			res, ok = v.RoundToInt64(), true
		case Decimal128:
			res, ok = v.RoundToInt64(), true
		case Decimal256:
			res, ok = v.RoundToInt64(), true
		case Int128:
			res, ok = v.Int64(), true
		case Int256:
			res, ok = v.Int64(), true
		default:
			ok = false
		}
	case FieldTypeInt32:
		switch v := val.(type) {
		case int:
			res, ok = int32(v), true
		case int64:
			res, ok = int32(v), true
		case int32:
			res, ok = int32(v), true
		case int16:
			res, ok = int64(v), true
		case int8:
			res, ok = int64(v), true
		case Decimal32:
			res, ok = int32(v.RoundToInt64()), true
		case Decimal64:
			res, ok = int32(v.RoundToInt64()), true
		case Decimal128:
			res, ok = int32(v.RoundToInt64()), true
		case Decimal256:
			res, ok = int32(v.RoundToInt64()), true
		case Int128:
			res, ok = int32(v.Int64()), true
		case Int256:
			res, ok = int32(v.Int64()), true
		default:
			ok = false
		}
	case FieldTypeInt16:
		switch v := val.(type) {
		case int:
			res, ok = int16(v), true
		case int64:
			res, ok = int16(v), true
		case int32:
			res, ok = int16(v), true
		case int16:
			res, ok = int16(v), true
		case int8:
			res, ok = int16(v), true
		case Decimal32:
			res, ok = int16(v.RoundToInt64()), true
		case Decimal64:
			res, ok = int16(v.RoundToInt64()), true
		case Decimal128:
			res, ok = int16(v.RoundToInt64()), true
		case Decimal256:
			res, ok = int16(v.RoundToInt64()), true
		case Int128:
			res, ok = int16(v.Int64()), true
		case Int256:
			res, ok = int16(v.Int64()), true
		default:
			ok = false
		}
	case FieldTypeInt8:
		switch v := val.(type) {
		case int:
			res, ok = int8(v), true
		case int64:
			res, ok = int8(v), true
		case int32:
			res, ok = int8(v), true
		case int16:
			res, ok = int8(v), true
		case int8:
			res, ok = int8(v), true
		case Decimal32:
			res, ok = int8(v.RoundToInt64()), true
		case Decimal64:
			res, ok = int8(v.RoundToInt64()), true
		case Decimal128:
			res, ok = int8(v.RoundToInt64()), true
		case Decimal256:
			res, ok = int8(v.RoundToInt64()), true
		case Int128:
			res, ok = int8(v.Int64()), true
		case Int256:
			res, ok = int8(v.Int64()), true
		default:
			ok = false
		}
	case FieldTypeUint64:
		switch v := val.(type) {
		case uint:
			res, ok = uint64(v), true
		case uint64:
			res, ok = uint64(v), true
		case uint32:
			res, ok = uint64(v), true
		case uint16:
			res, ok = uint64(v), true
		case uint8:
			res, ok = uint64(v), true
		case Decimal32:
			res, ok = uint64(v.RoundToInt64()), true
		case Decimal64:
			res, ok = uint64(v.RoundToInt64()), true
		case Decimal128:
			res, ok = uint64(v.RoundToInt64()), true
		case Decimal256:
			res, ok = uint64(v.RoundToInt64()), true
		case Int128:
			res, ok = uint64(v.Int64()), true
		case Int256:
			res, ok = uint64(v.Int64()), true
		default:
			ok = false
		}
	case FieldTypeUint32:
		switch v := val.(type) {
		case uint:
			res, ok = uint32(v), true
		case uint64:
			res, ok = uint32(v), true
		case uint32:
			res, ok = uint32(v), true
		case uint16:
			res, ok = uint32(v), true
		case uint8:
			res, ok = uint32(v), true
		case Decimal32:
			res, ok = uint32(v.RoundToInt64()), true
		case Decimal64:
			res, ok = uint32(v.RoundToInt64()), true
		case Decimal128:
			res, ok = uint32(v.RoundToInt64()), true
		case Decimal256:
			res, ok = uint32(v.RoundToInt64()), true
		case Int128:
			res, ok = uint32(v.Int64()), true
		case Int256:
			res, ok = uint32(v.Int64()), true
		default:
			ok = false
		}
	case FieldTypeUint16:
		switch v := val.(type) {
		case uint:
			res, ok = uint16(v), true
		case uint64:
			res, ok = uint16(v), true
		case uint32:
			res, ok = uint16(v), true
		case uint16:
			res, ok = uint16(v), true
		case uint8:
			res, ok = uint16(v), true
		case Decimal32:
			res, ok = uint16(v.RoundToInt64()), true
		case Decimal64:
			res, ok = uint16(v.RoundToInt64()), true
		case Decimal128:
			res, ok = uint16(v.RoundToInt64()), true
		case Decimal256:
			res, ok = uint16(v.RoundToInt64()), true
		case Int128:
			res, ok = uint16(v.Int64()), true
		case Int256:
			res, ok = uint16(v.Int64()), true
		default:
			ok = false
		}
	case FieldTypeUint8:
		switch v := val.(type) {
		case uint:
			res, ok = uint8(v), true
		case uint64:
			res, ok = uint8(v), true
		case uint32:
			res, ok = uint8(v), true
		case uint16:
			res, ok = uint8(v), true
		case uint8:
			res, ok = uint8(v), true
		case Decimal32:
			res, ok = uint8(v.RoundToInt64()), true
		case Decimal64:
			res, ok = uint8(v.RoundToInt64()), true
		case Decimal128:
			res, ok = uint8(v.RoundToInt64()), true
		case Decimal256:
			res, ok = uint8(v.RoundToInt64()), true
		case Int128:
			res, ok = uint8(v.Int64()), true
		case Int256:
			res, ok = uint8(v.Int64()), true
		default:
			ok = false
		}
	case FieldTypeFloat64:
		switch v := val.(type) {
		case float64:
			res, ok = float64(v), true
		case float32:
			res, ok = float64(v), true
		case Decimal32:
			res, ok = v.Float64(), true
		case Decimal64:
			res, ok = v.Float64(), true
		case Decimal128:
			res, ok = v.Float64(), true
		case Decimal256:
			res, ok = v.Float64(), true
		case Int128:
			res, ok = v.Float64(), true
		case Int256:
			res, ok = v.Float64(), true
		default:
			ok = false
		}
	case FieldTypeFloat32:
		switch v := val.(type) {
		case float64:
			res, ok = float32(v), true
		case float32:
			res, ok = float32(v), true
		case Decimal32:
			res, ok = float32(v.Float64()), true
		case Decimal64:
			res, ok = float32(v.Float64()), true
		case Decimal128:
			res, ok = float32(v.Float64()), true
		case Decimal256:
			res, ok = float32(v.Float64()), true
		case Int128:
			res, ok = float32(v.Float64()), true
		case Int256:
			res, ok = float32(v.Float64()), true
		default:
			ok = false
		}
	case FieldTypeDecimal32:
		dec := NewDecimal32(0, 0)
		switch v := val.(type) {
		case int:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case int64:
			err := dec.SetInt64(v, 0)
			res, ok = dec, err == nil
		case int32:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case int16:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case int8:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case float64:
			err := dec.SetFloat64(v, f.Scale)
			res, ok = dec, err == nil
		case float32:
			err := dec.SetFloat64(float64(v), f.Scale)
			res, ok = dec, err == nil
		case Decimal64:
			err := dec.SetInt64(v.Int64(), v.Scale())
			res, ok = dec.Quantize(f.Scale), err == nil
		case Decimal32:
			res, ok = v, true
		case Decimal128:
			err := dec.SetInt64(v.Int64(), v.Scale())
			res, ok = dec.Quantize(f.Scale), err == nil
		case Decimal256:
			err := dec.SetInt64(v.Int64(), v.Scale())
			res, ok = dec.Quantize(f.Scale), err == nil
		case Int128:
			err := dec.SetInt64(v.Int64(), f.Scale)
			res, ok = dec, err == nil
		case Int256:
			err := dec.SetInt64(v.Int64(), f.Scale)
			res, ok = dec, err == nil
		default:
			ok = false
		}
	case FieldTypeDecimal64:
		dec := NewDecimal64(0, 0)
		switch v := val.(type) {
		case int:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case int64:
			err := dec.SetInt64(v, 0)
			res, ok = dec, err == nil
		case int32:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case int16:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case int8:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case float64:
			err := dec.SetFloat64(v, f.Scale)
			res, ok = dec, err == nil
		case float32:
			err := dec.SetFloat64(float64(v), f.Scale)
			res, ok = dec, err == nil
		case Decimal32:
			err := dec.SetInt64(v.Int64(), v.Scale())
			res, ok = dec.Quantize(f.Scale), err == nil
		case Decimal64:
			res, ok = v, true
		case Decimal128:
			err := dec.SetInt64(v.Int64(), v.Scale())
			res, ok = dec.Quantize(f.Scale), err == nil
		case Decimal256:
			err := dec.SetInt64(v.Int64(), v.Scale())
			res, ok = dec.Quantize(f.Scale), err == nil
		case Int128:
			err := dec.SetInt64(v.Int64(), f.Scale)
			res, ok = dec, err == nil
		case Int256:
			err := dec.SetInt64(v.Int64(), f.Scale)
			res, ok = dec, err == nil
		default:
			ok = false
		}
	case FieldTypeDecimal128:
		dec := NewDecimal128(ZeroInt128, 0)
		switch v := val.(type) {
		case int:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case int64:
			err := dec.SetInt64(v, 0)
			res, ok = dec, err == nil
		case int32:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case int16:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case int8:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case float64:
			err := dec.SetFloat64(v, f.Scale)
			res, ok = dec, err == nil
		case float32:
			err := dec.SetFloat64(float64(v), f.Scale)
			res, ok = dec, err == nil
		case Decimal32:
			err := dec.SetInt64(v.Int64(), v.Scale())
			res, ok = dec.Quantize(f.Scale), err == nil
		case Decimal64:
			err := dec.SetInt64(v.Int64(), v.Scale())
			res, ok = dec.Quantize(f.Scale), err == nil
		case Decimal128:
			res, ok = v, true
		case Decimal256:
			err := dec.SetInt128(v.Int128(), v.Scale())
			res, ok = v.Quantize(f.Scale), err == nil
		case Int128:
			res, ok = dec.SetInt128(v, f.Scale), true
		case Int256:
			err := dec.SetInt128(v.Int128(), f.Scale)
			res, ok = v, err == nil
		default:
			ok = false
		}
	case FieldTypeDecimal256:
		dec := NewDecimal256(ZeroInt256, 0)
		switch v := val.(type) {
		case int:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case int64:
			err := dec.SetInt64(v, 0)
			res, ok = dec, err == nil
		case int32:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case int16:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case int8:
			err := dec.SetInt64(int64(v), 0)
			res, ok = dec, err == nil
		case float64:
			err := dec.SetFloat64(v, f.Scale)
			res, ok = dec, err == nil
		case float32:
			err := dec.SetFloat64(float64(v), f.Scale)
			res, ok = dec, err == nil
		case Decimal32:
			err := dec.SetInt64(v.Int64(), v.Scale())
			res, ok = dec.Quantize(f.Scale), err == nil
		case Decimal64:
			err := dec.SetInt64(v.Int64(), v.Scale())
			res, ok = dec.Quantize(f.Scale), err == nil
		case Decimal128:
			err := dec.SetInt128(v.Int128(), v.Scale())
			res, ok = v.Quantize(f.Scale), err == nil
		case Decimal256:
			res, ok = v, true
		case Int128:
			err := dec.SetInt128(v, f.Scale)
			res, ok = v, err == nil
		case Int256:
			err := dec.SetInt256(v, f.Scale)
			res, ok = v, err == nil
		default:
			ok = false
		}
	}
	if !ok {
		return res, fmt.Errorf("pack: unexpected value type %T for %s condition", val, t)
	}
	return res, nil
}

// Note: can evolve into a CAST function
func (t FieldType) CastSliceType(val interface{}, f Field) (interface{}, error) {
	var ok bool
	res := val
	switch t {
	case FieldTypeBytes:
		_, ok = val.([][]byte)
	case FieldTypeString:
		_, ok = val.([]string)
	case FieldTypeDatetime:
		_, ok = val.([]time.Time)
	case FieldTypeBoolean:
		_, ok = val.([]bool)
	case FieldTypeInt64:
		switch v := val.(type) {
		case []int:
			cp := make([]int64, len(v))
			for i := range v {
				cp[i] = int64(v[i])
			}
			res, ok = cp, true
		case []int64:
			res, ok = val, true
		case []int32:
			cp := make([]int64, len(v))
			for i := range v {
				cp[i] = int64(v[i])
			}
			res, ok = cp, true
		case []int16:
			cp := make([]int64, len(v))
			for i := range v {
				cp[i] = int64(v[i])
			}
			res, ok = cp, true
		case []int8:
			cp := make([]int64, len(v))
			for i := range v {
				cp[i] = int64(v[i])
			}
			res, ok = cp, true
		// TODO: float types
		// TODO: decimal types
		// TODO: int128/256 types
		default:
			ok = false
		}
	case FieldTypeInt32:
		switch v := val.(type) {
		case []int:
			cp := make([]int32, len(v))
			for i := range v {
				cp[i] = int32(v[i])
			}
			res, ok = cp, true
		case []int64:
			cp := make([]int32, len(v))
			for i := range v {
				cp[i] = int32(v[i])
			}
			res, ok = cp, true
		case []int32:
			res, ok = val, true
		case []int16:
			cp := make([]int32, len(v))
			for i := range v {
				cp[i] = int32(v[i])
			}
			res, ok = cp, true
		case []int8:
			cp := make([]int32, len(v))
			for i := range v {
				cp[i] = int32(v[i])
			}
			res, ok = cp, true
		// TODO: float types
		// TODO: decimal types
		// TODO: int128/256 types
		default:
			ok = false
		}
	case FieldTypeInt16:
		switch v := val.(type) {
		case []int:
			cp := make([]int16, len(v))
			for i := range v {
				cp[i] = int16(v[i])
			}
			res, ok = cp, true
		case []int64:
			cp := make([]int16, len(v))
			for i := range v {
				cp[i] = int16(v[i])
			}
			res, ok = cp, true
		case []int32:
			cp := make([]int16, len(v))
			for i := range v {
				cp[i] = int16(v[i])
			}
			res, ok = cp, true
		case []int16:
			res, ok = val, true
		case []int8:
			cp := make([]int16, len(v))
			for i := range v {
				cp[i] = int16(v[i])
			}
			res, ok = cp, true
		// TODO: float types
		// TODO: decimal types
		// TODO: int128/256 types
		default:
			ok = false
		}
	case FieldTypeInt8:
		switch v := val.(type) {
		case []int:
			cp := make([]int8, len(v))
			for i := range v {
				cp[i] = int8(v[i])
			}
			res, ok = cp, true
		case []int64:
			cp := make([]int8, len(v))
			for i := range v {
				cp[i] = int8(v[i])
			}
			res, ok = cp, true
		case []int32:
			cp := make([]int8, len(v))
			for i := range v {
				cp[i] = int8(v[i])
			}
			res, ok = cp, true
		case []int16:
			cp := make([]int8, len(v))
			for i := range v {
				cp[i] = int8(v[i])
			}
			res, ok = cp, true
		case []int8:
			res, ok = val, true
		// TODO: float types
		// TODO: decimal types
		// TODO: int128/256 types
		default:
			ok = false
		}
	case FieldTypeUint64:
		switch v := val.(type) {
		case []uint:
			cp := make([]uint64, len(v))
			for i := range v {
				cp[i] = uint64(v[i])
			}
			res, ok = cp, true
		case []uint64:
			res, ok = val, true
		case []uint32:
			cp := make([]uint64, len(v))
			for i := range v {
				cp[i] = uint64(v[i])
			}
			res, ok = cp, true
		case []uint16:
			cp := make([]uint64, len(v))
			for i := range v {
				cp[i] = uint64(v[i])
			}
			res, ok = cp, true
		case []uint8:
			cp := make([]uint64, len(v))
			for i := range v {
				cp[i] = uint64(v[i])
			}
			res, ok = cp, true
		// TODO: float types
		// TODO: decimal types
		// TODO: int128/256 types
		default:
			ok = false
		}
	case FieldTypeUint32:
		switch v := val.(type) {
		case []uint:
			cp := make([]uint32, len(v))
			for i := range v {
				cp[i] = uint32(v[i])
			}
			res, ok = cp, true
		case []uint64:
			cp := make([]uint32, len(v))
			for i := range v {
				cp[i] = uint32(v[i])
			}
			res, ok = cp, true
		case []uint32:
			res, ok = val, true
		case []uint16:
			cp := make([]uint32, len(v))
			for i := range v {
				cp[i] = uint32(v[i])
			}
			res, ok = cp, true
		case []uint8:
			cp := make([]uint32, len(v))
			for i := range v {
				cp[i] = uint32(v[i])
			}
			res, ok = cp, true
		// TODO: float types
		// TODO: decimal types
		// TODO: int128/256 types
		default:
			ok = false
		}
	case FieldTypeUint16:
		switch v := val.(type) {
		case []uint:
			cp := make([]uint16, len(v))
			for i := range v {
				cp[i] = uint16(v[i])
			}
			res, ok = cp, true
		case []uint64:
			cp := make([]uint16, len(v))
			for i := range v {
				cp[i] = uint16(v[i])
			}
			res, ok = cp, true
		case []uint32:
			cp := make([]uint16, len(v))
			for i := range v {
				cp[i] = uint16(v[i])
			}
			res, ok = cp, true
		case []uint16:
			res, ok = val, true
		case []uint8:
			cp := make([]uint16, len(v))
			for i := range v {
				cp[i] = uint16(v[i])
			}
			res, ok = cp, true
		// TODO: float types
		// TODO: decimal types
		// TODO: int128/256 types
		default:
			ok = false
		}
	case FieldTypeUint8:
		switch v := val.(type) {
		case []uint:
			cp := make([]uint8, len(v))
			for i := range v {
				cp[i] = uint8(v[i])
			}
			res, ok = cp, true
		case []uint64:
			cp := make([]uint8, len(v))
			for i := range v {
				cp[i] = uint8(v[i])
			}
			res, ok = cp, true
		case []uint32:
			cp := make([]uint8, len(v))
			for i := range v {
				cp[i] = uint8(v[i])
			}
			res, ok = cp, true
		case []uint16:
			cp := make([]uint8, len(v))
			for i := range v {
				cp[i] = uint8(v[i])
			}
			res, ok = cp, true
		case []uint8:
			res, ok = val, true
		// TODO: float types
		// TODO: decimal types
		// TODO: int128/256 types
		default:
			ok = false
		}
	// TODO: casts
	case FieldTypeFloat64:
		_, ok = val.([]float64)
	case FieldTypeFloat32:
		_, ok = val.([]float32)
	case FieldTypeDecimal32:
		_, ok = val.([]int32)
	case FieldTypeDecimal64:
		_, ok = val.([]int64)
	case FieldTypeDecimal128:
		_, ok = val.([]Int128)
	case FieldTypeDecimal256:
		_, ok = val.([]Int256)
	case FieldTypeInt128:
		_, ok = val.([]Int128)
	case FieldTypeInt256:
		_, ok = val.([]Int256)
	}
	if !ok {
		return res, fmt.Errorf("pack: unexpected value type %T for %s slice condition", val, t)
	}
	return res, nil
}

func (t FieldType) CheckType(val interface{}) error {
	var ok bool
	switch t {
	case FieldTypeBytes:
		_, ok = val.([]byte)
	case FieldTypeString:
		_, ok = val.(string)
	case FieldTypeDatetime:
		_, ok = val.(time.Time)
	case FieldTypeBoolean:
		_, ok = val.(bool)
	case FieldTypeInt64:
		_, ok = val.(int64)
	case FieldTypeInt32:
		_, ok = val.(int32)
	case FieldTypeInt16:
		_, ok = val.(int16)
	case FieldTypeInt8:
		_, ok = val.(int8)
	case FieldTypeUint64:
		_, ok = val.(uint64)
	case FieldTypeUint32:
		_, ok = val.(uint32)
	case FieldTypeUint16:
		_, ok = val.(uint16)
	case FieldTypeUint8:
		_, ok = val.(uint8)
	case FieldTypeFloat64:
		_, ok = val.(float64)
	case FieldTypeFloat32:
		_, ok = val.(float32)
	case FieldTypeInt128:
		_, ok = val.(Int128)
	case FieldTypeInt256:
		_, ok = val.(Int256)
	case FieldTypeDecimal256:
		_, ok = val.(Decimal256)
	case FieldTypeDecimal128:
		_, ok = val.(Decimal128)
	case FieldTypeDecimal64:
		_, ok = val.(Decimal64)
	case FieldTypeDecimal32:
		_, ok = val.(Decimal32)
	}
	if !ok {
		return fmt.Errorf("pack: unexpected value type %T for %s condition", val, t)
	}
	return nil
}

func (t FieldType) CheckSliceType(val interface{}) error {
	var ok bool
	switch t {
	case FieldTypeBytes:
		_, ok = val.([][]byte)
	case FieldTypeString:
		_, ok = val.([]string)
	case FieldTypeDatetime:
		_, ok = val.([]time.Time)
	case FieldTypeBoolean:
		_, ok = val.([]bool)
	case FieldTypeInt64:
		_, ok = val.([]int64)
	case FieldTypeInt32:
		_, ok = val.([]int32)
	case FieldTypeInt16:
		_, ok = val.([]int16)
	case FieldTypeInt8:
		_, ok = val.([]int8)
	case FieldTypeUint64:
		_, ok = val.([]uint64)
	case FieldTypeUint32:
		_, ok = val.([]uint32)
	case FieldTypeUint16:
		_, ok = val.([]uint16)
	case FieldTypeUint8:
		_, ok = val.([]uint8)
	case FieldTypeFloat64:
		_, ok = val.([]float64)
	case FieldTypeFloat32:
		_, ok = val.([]float32)
	case FieldTypeInt128:
		_, ok = val.([]Int128)
	case FieldTypeInt256:
		_, ok = val.([]Int256)
	case FieldTypeDecimal256:
		_, ok = val.([]Decimal256)
	case FieldTypeDecimal128:
		_, ok = val.([]Decimal128)
	case FieldTypeDecimal64:
		_, ok = val.([]Decimal64)
	case FieldTypeDecimal32:
		_, ok = val.([]Decimal32)
	}
	if !ok {
		return fmt.Errorf("pack: unexpected value type %T for %s slice condition", val, t)
	}
	return nil
}

func (t FieldType) CopySliceType(val interface{}) (interface{}, error) {
	switch t {
	case FieldTypeBytes:
		if slice, ok := val.([][]byte); ok {
			cp := make([][]byte, len(slice))
			for i, v := range slice {
				buf := make([]byte, len(v))
				copy(buf, v)
				cp[i] = buf
			}
			return cp, nil
		}
	case FieldTypeString:
		if slice, ok := val.([]string); ok {
			cp := make([]string, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeDatetime:
		if slice, ok := val.([]time.Time); ok {
			cp := make([]time.Time, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeBoolean:
		if slice, ok := val.([]time.Time); ok {
			cp := make([]time.Time, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeInt256:
		if slice, ok := val.([]Int256); ok {
			cp := make([]Int256, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeInt128:
		if slice, ok := val.([]Int128); ok {
			cp := make([]Int128, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeInt64:
		if slice, ok := val.([]int64); ok {
			cp := make([]int64, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeInt32:
		if slice, ok := val.([]int32); ok {
			cp := make([]int32, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeInt16:
		if slice, ok := val.([]int16); ok {
			cp := make([]int16, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeInt8:
		if slice, ok := val.([]int8); ok {
			cp := make([]int8, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeUint64:
		if slice, ok := val.([]uint64); ok {
			cp := make([]uint64, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeUint32:
		if slice, ok := val.([]uint32); ok {
			cp := make([]uint32, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeUint16:
		if slice, ok := val.([]uint16); ok {
			cp := make([]uint16, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeUint8:
		if slice, ok := val.([]uint8); ok {
			cp := make([]uint8, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeFloat64:
		if slice, ok := val.([]float64); ok {
			cp := make([]float64, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeFloat32:
		if slice, ok := val.([]float32); ok {
			cp := make([]float32, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeDecimal256:
		// support both Decimal type and underlying storage type
		if slice, ok := val.([]Int256); ok {
			cp := make([]Int256, len(slice))
			copy(cp, slice)
			return cp, nil
		}
		if slice, ok := val.([]Decimal256); ok {
			cp := make([]Decimal256, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeDecimal128:
		// support both Decimal type and underlying storage type
		if slice, ok := val.([]Int128); ok {
			cp := make([]Int128, len(slice))
			copy(cp, slice)
			return cp, nil
		}
		if slice, ok := val.([]Decimal128); ok {
			cp := make([]Decimal128, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeDecimal64:
		// support both Decimal type and underlying storage type
		if slice, ok := val.([]int64); ok {
			cp := make([]int64, len(slice))
			copy(cp, slice)
			return cp, nil
		}
		if slice, ok := val.([]Decimal64); ok {
			cp := make([]Decimal64, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	case FieldTypeDecimal32:
		// support both Decimal type and underlying storage type
		if slice, ok := val.([]int32); ok {
			cp := make([]int32, len(slice))
			copy(cp, slice)
			return cp, nil
		}
		if slice, ok := val.([]Decimal32); ok {
			cp := make([]Decimal32, len(slice))
			copy(cp, slice)
			return cp, nil
		}
	default:
		return nil, fmt.Errorf("pack: slice copy on unsupported field type %s", t)
	}
	return nil, fmt.Errorf("pack: slice copy mismatched value type %T for %s field", val, t)
}