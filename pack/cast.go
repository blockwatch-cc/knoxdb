// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
package pack

import (
	"encoding"
	"fmt"
	"reflect"
	"time"

	. "blockwatch.cc/knoxdb/encoding/bignum"
	. "blockwatch.cc/knoxdb/encoding/decimal"
)

func (t FieldType) CastType(val interface{}, f *Field) (interface{}, error) {
	var ok bool
	res := val
	switch t {
	case FieldTypeBytes:
		if vv, ok2 := val.(encoding.BinaryMarshaler); ok2 {
			r, err := vv.MarshalBinary()
			if err != nil {
				return nil, err
			}
			res = r
			ok = true
		} else {
			_, ok = val.([]byte)
		}
	case FieldTypeString:
		if vv, ok2 := val.(encoding.TextMarshaler); ok2 {
			r, err := vv.MarshalText()
			if err != nil {
				return nil, err
			}
			res = r
			ok = true
		} else {
			_, ok = val.(string)
		}
	case FieldTypeDatetime:
		_, ok = val.(time.Time)
	case FieldTypeBoolean:
		switch v := val.(type) {
		case bool:
			res, ok = v, true
		case int:
			res, ok = v > 0, true
		case int64:
			res, ok = v > 0, true
		case int32:
			res, ok = v > 0, true
		case int16:
			res, ok = v > 0, true
		case int8:
			res, ok = v > 0, true
		case string:
			res, ok = len(v) > 0, true
		default:
			// type aliases
			vv := reflect.Indirect(reflect.ValueOf(val))
			switch vv.Kind() {
			case reflect.Bool:
				res, ok = vv.Bool(), true
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				res, ok = int(vv.Int()) > 0, true
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				res, ok = int(vv.Uint()) > 0, true
			case reflect.String, reflect.Slice, reflect.Array:
				res, ok = vv.Len() > 0, true
			}
		}
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
			// type aliases
			vv := reflect.Indirect(reflect.ValueOf(val))
			switch vv.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				res, ok = int64(vv.Int()), true
			}
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
			res, ok = int32(v), true
		case int8:
			res, ok = int32(v), true
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
			// type aliases
			vv := reflect.Indirect(reflect.ValueOf(val))
			switch vv.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				res, ok = int32(vv.Int()), true
			}
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
			// type aliases
			vv := reflect.Indirect(reflect.ValueOf(val))
			switch vv.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				res, ok = int16(vv.Int()), true
			}
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
			// type aliases
			vv := reflect.Indirect(reflect.ValueOf(val))
			switch vv.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				res, ok = int8(vv.Int()), true
			}
		}
	case FieldTypeUint64:
		switch v := val.(type) {
		case int:
			res, ok = uint64(v), true
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
			// type aliases
			vv := reflect.Indirect(reflect.ValueOf(val))
			switch vv.Kind() {
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				res, ok = uint64(vv.Uint()), true
			}
		}
	case FieldTypeUint32:
		switch v := val.(type) {
		case int:
			res, ok = uint32(v), true
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
			// type aliases
			vv := reflect.Indirect(reflect.ValueOf(val))
			switch vv.Kind() {
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				res, ok = uint32(vv.Uint()), true
			}
		}
	case FieldTypeUint16:
		switch v := val.(type) {
		case int:
			res, ok = uint16(v), true
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
			// type aliases
			vv := reflect.Indirect(reflect.ValueOf(val))
			switch vv.Kind() {
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				res, ok = uint16(vv.Uint()), true
			}
		}
	case FieldTypeUint8:
		switch v := val.(type) {
		case int:
			res, ok = uint8(v), true
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
			// type aliases
			vv := reflect.Indirect(reflect.ValueOf(val))
			switch vv.Kind() {
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				res, ok = uint8(vv.Uint()), true
			}
		}
	case FieldTypeFloat64:
		switch v := val.(type) {
		case int:
			res, ok = float64(v), true
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
		}
	case FieldTypeFloat32:
		switch v := val.(type) {
		case int:
			res, ok = float32(v), true
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
		}
	}
	if !ok {
		return res, fmt.Errorf("pack: cast unexpected value type %T for %s condition", val, t)
	}
	return res, nil
}

func (t FieldType) CastSliceType(val interface{}, f *Field) (interface{}, error) {
	var (
		ok  bool
		err error
	)
	res := val
	switch t {
	case FieldTypeBytes:
		_, ok = val.([][]byte)
		if !ok {
			// must use reflect to convert to interface
			v := reflect.ValueOf(val)
			if v.Kind() == reflect.Slice {
				slice := make([][]byte, v.Len())
				if v.Len() == 0 {
					res = slice
					ok = true
				} else if v.Index(0).CanInterface() && v.Index(0).Type().Implements(binaryMarshalerType) {
					for i := 0; i < v.Len(); i++ {
						slice[i], err = v.Index(i).Interface().(encoding.BinaryMarshaler).MarshalBinary()
						if err != nil {
							return nil, err
						}
					}
					res = slice
					ok = true
				}
			}
		}
	case FieldTypeString:
		_, ok = val.([]string)
		if !ok {
			// must use reflect to convert to interface
			v := reflect.ValueOf(val)
			if v.Kind() == reflect.Slice {
				slice := make([]string, v.Len())
				if v.Len() == 0 {
					res = slice
					ok = true
				} else if v.Index(0).CanInterface() && v.Index(0).Type().Implements(textMarshalerType) {
					for i := 0; i < v.Len(); i++ {
						str, err := v.Index(i).Interface().(encoding.TextMarshaler).MarshalText()
						if err != nil {
							return nil, err
						}
						slice[i] = string(str)
					}
					res = slice
					ok = true
				}
			}
		}
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
			// convert enum types
			vv := reflect.Indirect(reflect.ValueOf(val))
			if vv.Kind() == reflect.Slice {
				switch vv.Type().Elem().Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					cp := make([]int64, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = int64(vv.Index(i).Int())
					}
					res, ok = cp, true
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					cp := make([]int64, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = int64(vv.Index(i).Int())
					}
					res, ok = cp, true
				}
			}
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
			// convert enum types
			vv := reflect.Indirect(reflect.ValueOf(val))
			if vv.Kind() == reflect.Slice {
				switch vv.Type().Elem().Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					cp := make([]int32, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = int32(vv.Index(i).Int())
					}
					res, ok = cp, true
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					cp := make([]int32, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = int32(vv.Index(i).Int())
					}
					res, ok = cp, true
				}
			}
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
			// convert enum types
			vv := reflect.Indirect(reflect.ValueOf(val))
			if vv.Kind() == reflect.Slice {
				switch vv.Type().Elem().Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					cp := make([]int16, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = int16(vv.Index(i).Int())
					}
					res, ok = cp, true
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					cp := make([]int16, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = int16(vv.Index(i).Int())
					}
					res, ok = cp, true
				}
			}
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
			// convert enum types
			vv := reflect.Indirect(reflect.ValueOf(val))
			if vv.Kind() == reflect.Slice {
				switch vv.Type().Elem().Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					cp := make([]int8, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = int8(vv.Index(i).Int())
					}
					res, ok = cp, true
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					cp := make([]int8, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = int8(vv.Index(i).Int())
					}
					res, ok = cp, true
				}
			}
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
			// convert enum types
			vv := reflect.Indirect(reflect.ValueOf(val))
			if vv.Kind() == reflect.Slice {
				switch vv.Type().Elem().Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					cp := make([]uint64, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = uint64(vv.Index(i).Uint())
					}
					res, ok = cp, true
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					cp := make([]uint64, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = uint64(vv.Index(i).Uint())
					}
					res, ok = cp, true
				}
			}
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
			// convert enum types
			vv := reflect.Indirect(reflect.ValueOf(val))
			if vv.Kind() == reflect.Slice {
				switch vv.Type().Elem().Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					cp := make([]uint32, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = uint32(vv.Index(i).Uint())
					}
					res, ok = cp, true
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					cp := make([]uint32, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = uint32(vv.Index(i).Uint())
					}
					res, ok = cp, true
				}
			}
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
			// convert enum types
			vv := reflect.Indirect(reflect.ValueOf(val))
			if vv.Kind() == reflect.Slice {
				switch vv.Type().Elem().Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					cp := make([]uint16, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = uint16(vv.Index(i).Uint())
					}
					res, ok = cp, true
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					cp := make([]uint16, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = uint16(vv.Index(i).Uint())
					}
					res, ok = cp, true
				}
			}
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
			// convert enum types
			vv := reflect.Indirect(reflect.ValueOf(val))
			if vv.Kind() == reflect.Slice {
				switch vv.Type().Elem().Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					cp := make([]uint8, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = uint8(vv.Index(i).Uint())
					}
					res, ok = cp, true
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					cp := make([]uint8, vv.Len())
					for i, l := 0, vv.Len(); i < l; i++ {
						cp[i] = uint8(vv.Index(i).Uint())
					}
					res, ok = cp, true
				}
			}
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
		return res, fmt.Errorf("pack: cast unexpected value type %T for %s slice condition", val, t)
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
		return fmt.Errorf("pack: check unexpected value type %T for %s condition", val, t)
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
		return fmt.Errorf("pack: check unexpected value type %T for %s slice condition", val, t)
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
