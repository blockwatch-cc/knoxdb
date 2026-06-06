// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cast

import (
	"math"
	"reflect"
	"strconv"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

// uint caster
type UintCaster[T schema.Unsigned] struct{}

func (c UintCaster[T]) CastValue(val any) (res any, err error) {
	var ok bool
	width := util.SizeOf[T]() * 8
	res = val
	switch v := val.(type) {
	case int:
		res, ok = T(v), v>>(width-1) == 0
	case int64:
		res, ok = T(v), v>>(width-1) == 0
	case int32:
		res, ok = T(v), v>>(width-1) == 0
	case int16:
		res, ok = T(v), v>>(width-1) == 0
	case int8:
		res, ok = T(v), v>>(width-1) == 0
	case uint:
		res, ok = T(v), v>>width == 0
	case uint64:
		res, ok = T(v), v>>width == 0
	case uint32:
		res, ok = T(v), v>>width == 0
	case uint16:
		res, ok = T(v), v>>width == 0
	case uint8:
		res, ok = T(v), v>>width == 0
	case float32:
		res, ok = T(v), !math.Signbit(float64(v)) && uint32(v)>>width == 0
	case float64:
		res, ok = T(v), !math.Signbit(v) && uint64(v)>>width == 0
	case num.Decimal32:
		res, ok = T(v.Int64()), v.Scale() == 0 && v.Int32()>>(width-1) == 0
	case num.Decimal64:
		res, ok = T(v.Int64()), v.Scale() == 0 && v.Int64()>>(width-1) == 0
	case num.Decimal128:
		res, ok = T(v.Int64()), v.Scale() == 0 && v.Int128().IsInt64() && v.Int64()>>(width-1) == 0
	case num.Decimal256:
		res, ok = T(v.Int64()), v.Scale() == 0 && v.Int256().IsInt64() && v.Int64()>>(width-1) == 0
	case num.Int128:
		res, ok = T(v.Int64()), v.IsInt64() && v.Int64()>>(width-1) == 0
	case num.Int256:
		res, ok = T(v.Int64()), v.IsInt64() && v.Int64()>>(width-1) == 0
	case num.Big:
		res, ok = T(v.Int64()), v.Big().IsInt64() && v.Int64()>>(width-1) == 0
	default:
		// type aliases
		vv := reflect.Indirect(reflect.ValueOf(val))
		switch vv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			res, ok = T(vv.Int()), vv.Int()>>(width-1) == 0
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			res, ok = T(vv.Uint()), vv.Uint()>>width == 0
		}
	}
	if !ok {
		err = CastError(val, "uint"+strconv.Itoa(width))
	}
	return
}

func (c UintCaster[T]) CastSlice(val any) (res any, err error) {
	ok := true
	width := util.SizeOf[T]() * 8
	res = val
	switch v := val.(type) {
	case []int:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []int64:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []int32:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []int16:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []int8:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []uint:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>(width) == 0
		}
		if ok {
			res = cp
		}
	case []uint64:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>(width) == 0
		}
		if ok {
			res = cp
		}
	case []uint32:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>(width) == 0
		}
		if ok {
			res = cp
		}
	case []uint16:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>(width) == 0
		}
		if ok {
			res = cp
		}
	case []uint8:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>(width) == 0
		}
		if ok {
			res = cp
		}
	case []float32:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && !math.Signbit(float64(v[i])) && uint32(v[i])>>width == 0
		}
		if ok {
			res = cp
		}
	case []float64:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && !math.Signbit(float64(v[i])) && uint64(v[i])>>width == 0
		}
		if ok {
			res = cp
		}
	case []num.Decimal32:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i].Int64()), ok && v[i].Scale() == 0 && v[i].Int32()>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []num.Decimal64:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i].Int64()), ok && v[i].Scale() == 0 && v[i].Int64()>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []num.Decimal128:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i].Int64()), ok && v[i].Scale() == 0 && v[i].Int128().IsInt64() && v[i].Int64()>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []num.Decimal256:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i].Int64()), ok && v[i].Scale() == 0 && v[i].Int256().IsInt64() && v[i].Int64()>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []num.Int128:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i].Int64()), ok && v[i].IsInt64() && v[i].Int64()>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []num.Int256:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i].Int64()), ok && v[i].IsInt64() && v[i].Int64()>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []num.Big:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i].Int64()), ok && v[i].Big().IsInt64() && v[i].Int64()>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	default:
		// convert enum types
		vv := reflect.Indirect(reflect.ValueOf(val))
		if vv.Kind() == reflect.Slice {
			switch vv.Type().Elem().Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				cp := make([]T, vv.Len())
				for i, l := 0, vv.Len(); i < l; i++ {
					cp[i], ok = T(vv.Index(i).Int()), ok && vv.Index(i).Int()>>(width-1) == 0
				}
				if ok {
					res = cp
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				cp := make([]T, vv.Len())
				for i, l := 0, vv.Len(); i < l; i++ {
					cp[i], ok = T(vv.Index(i).Uint()), ok && vv.Index(i).Uint()>>width == 0
				}
				if ok {
					res = cp
				}
			default:
				ok = false
			}
		} else {
			ok = false
		}
	}
	if !ok {
		err = CastError(val, "uint"+strconv.Itoa(width))
	}
	return
}
