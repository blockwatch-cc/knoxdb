// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cast

import (
	"reflect"
	"strconv"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// float caster
type FloatCaster[T schema.Float] struct{}

func (c FloatCaster[T]) CastValue(val any) (res any, err error) {
	var ok bool
	res = val
	switch v := val.(type) {
	case int:
		res, ok = T(v), true
	case int64:
		res, ok = T(v), true
	case int32:
		res, ok = T(v), true
	case int16:
		res, ok = T(v), true
	case int8:
		res, ok = T(v), true
	case uint:
		res, ok = T(v), true
	case uint64:
		res, ok = T(v), true
	case uint32:
		res, ok = T(v), true
	case uint16:
		res, ok = T(v), true
	case uint8:
		res, ok = T(v), true
	case float64:
		res, ok = T(v), true
	case float32:
		res, ok = T(v), true
	case num.Decimal32:
		res, ok = T(v.Float64()), true
	case num.Decimal64:
		res, ok = T(v.Float64()), true
	case num.Decimal128:
		res, ok = T(v.Float64()), true
	case num.Decimal256:
		res, ok = T(v.Float64()), true
	case num.Int128:
		res, ok = T(v.Float64()), true
	case num.Int256:
		res, ok = T(v.Float64()), true
	case num.Big:
		res, ok = T(v.Float64(0)), true
	default:
		// type aliases
		vv := reflect.Indirect(reflect.ValueOf(val))
		switch vv.Kind() {
		case reflect.Float32, reflect.Float64:
			res, ok = T(vv.Float()), true
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			res, ok = T(vv.Int()), true
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			res, ok = T(vv.Uint()), true
		}
	}
	if !ok {
		err = CastError(val, "float"+strconv.Itoa(reflect.TypeFor[T]().Bits()))
	}
	return
}

func (c FloatCaster[T]) CastSlice(val any) (res any, err error) {
	var ok bool
	res = val
	switch v := val.(type) {
	case []float64:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i])
		}
		res, ok = cp, true
	case []float32:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i])
		}
		res, ok = cp, true
	case []int:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i])
		}
		res, ok = cp, true
	case []int64:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i])
		}
		res, ok = cp, true
	case []int32:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i])
		}
		res, ok = cp, true
	case []int16:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i])
		}
		res, ok = cp, true
	case []int8:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i])
		}
		res, ok = cp, true
	case []uint:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i])
		}
		res, ok = cp, true
	case []uint64:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i])
		}
		res, ok = cp, true
	case []uint32:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i])
		}
		res, ok = cp, true
	case []uint16:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i])
		}
		res, ok = cp, true
	case []uint8:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i])
		}
		res, ok = cp, true
	case []num.Decimal32:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i].Float64())
		}
		res, ok = cp, true
	case []num.Decimal64:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i].Float64())
		}
		res, ok = cp, true
	case []num.Decimal128:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i].Float64())
		}
		res, ok = cp, true
	case []num.Decimal256:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i].Float64())
		}
		res, ok = cp, true
	case []num.Int128:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i].Float64())
		}
		res, ok = cp, true
	case []num.Int256:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i].Float64())
		}
		res, ok = cp, true
	case []num.Big:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i].Float64(0))
		}
		res, ok = cp, true
	default:
		// convert enum types
		vv := reflect.Indirect(reflect.ValueOf(val))
		if vv.Kind() == reflect.Slice {
			switch vv.Type().Elem().Kind() {
			case reflect.Float32, reflect.Float64:
				cp := make([]T, vv.Len())
				for i, l := 0, vv.Len(); i < l; i++ {
					cp[i] = T(vv.Index(i).Float())
				}
				res, ok = cp, true
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				cp := make([]T, vv.Len())
				for i, l := 0, vv.Len(); i < l; i++ {
					cp[i] = T(vv.Index(i).Int())
				}
				res, ok = cp, true
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				cp := make([]T, vv.Len())
				for i, l := 0, vv.Len(); i < l; i++ {
					cp[i] = T(vv.Index(i).Uint())
				}
				res, ok = cp, true
			}
		}
	}
	if !ok {
		err = CastError(val, "float"+strconv.Itoa(reflect.TypeFor[T]().Bits()))
	}
	return
}
