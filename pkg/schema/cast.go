// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
	"golang.org/x/exp/constraints"
)

// ValueCasters have the purpose of converting Go types used in programmatic
// queries (written in Go) to block types. This is required since inputs for
// comparison functions accept interfaces and will perform unchecked type
// conversions. We use ValueCaster during query compilation to ensure these
// interface to type conversions don't panic.
//
// The type of a ValueCaster defines the output (target) type which must
// be equal to the underlying block type for a given field.

type ValueCaster interface {
	CastValue(any) (any, error)
	CastSlice(any) (any, error)
}

func castError(val any, kind string) error {
	return fmt.Errorf("cast: unexpected value type %T for %s condition", val, kind)
}

func NewCaster(typ types.FieldType) ValueCaster {
	switch typ {
	case types.FieldTypeDatetime:
		return TimeCaster{}
	case types.FieldTypeBoolean:
		return BoolCaster{}
	case types.FieldTypeString:
		return StringCaster{} // MarshalText, stringer, ToString
	case types.FieldTypeBytes:
		return BytesCaster{} // MarshalBinary
	case types.FieldTypeInt8:
		return IntCaster[int8]{}
	case types.FieldTypeInt16:
		return IntCaster[int16]{}
	case types.FieldTypeInt32:
		return IntCaster[int32]{}
	case types.FieldTypeInt64:
		return IntCaster[int64]{}
	case types.FieldTypeUint8:
		return UintCaster[uint8]{}
	case types.FieldTypeUint16:
		return UintCaster[uint16]{}
	case types.FieldTypeUint32:
		return UintCaster[uint32]{}
	case types.FieldTypeUint64:
		return UintCaster[uint64]{}
	case types.FieldTypeFloat32:
		return FloatCaster[float32]{}
	case types.FieldTypeFloat64:
		return FloatCaster[float64]{}
	case types.FieldTypeInt128:
		return I128Caster{}
	case types.FieldTypeInt256:
		return I256Caster{}
	case types.FieldTypeDecimal32:
		return IntCaster[int32]{}
	case types.FieldTypeDecimal64:
		return IntCaster[int64]{}
	case types.FieldTypeDecimal128:
		return I128Caster{}
	case types.FieldTypeDecimal256:
		return I256Caster{}
	default:
		panic(fmt.Errorf("caster: unsupported field type %s %d", typ, typ))
	}
}

// caster
type IntCaster[T constraints.Signed] struct{}

func (c IntCaster[T]) CastValue(val any) (res any, err error) {
	var ok bool
	width := unsafe.Sizeof(T(0)) * 8
	switch v := val.(type) {
	case int:
		res, ok = T(v), v>>width == 0
	case int64:
		res, ok = T(v), v>>width == 0
	case int32:
		res, ok = T(v), v>>width == 0
	case int16:
		res, ok = T(v), v>>width == 0
	case int8:
		res, ok = T(v), true
	case uint:
		res, ok = T(v), v>>(width-1) == 0
	case uint64:
		res, ok = T(v), v>>(width-1) == 0
	case uint32:
		res, ok = T(v), v>>(width-1) == 0
	case uint16:
		res, ok = T(v), v>>(width-1) == 0
	case uint8:
		res, ok = T(v), v>>(width-1) == 0
	case float32:
		res, ok = T(v), math.Round(float64(v)) == float64(v) && uint32(v)>>(width-1) == 0
	case float64:
		res, ok = T(v), math.Round(v) == v && uint64(v)>>(width-1) == 0
	case num.Decimal32:
		res, ok = T(v.Int64()), v.Scale() == 0 && v.Int32()>>width == 0
	case num.Decimal64:
		res, ok = T(v.Int64()), v.Scale() == 0 && v.Int64()>>width == 0
	case num.Decimal128:
		res, ok = T(v.Int64()), v.Scale() == 0 && v.Int128().IsInt64() && v.Int64()>>width == 0
	case num.Decimal256:
		res, ok = T(v.Int64()), v.Scale() == 0 && v.Int256().IsInt64() && v.Int64()>>width == 0
	case num.Int128:
		res, ok = T(v.Int64()), v.IsInt64() && v.Int64()>>width == 0
	case num.Int256:
		res, ok = T(v.Int64()), v.IsInt64() && v.Int64()>>width == 0
	default:
		// type aliases
		vv := reflect.Indirect(reflect.ValueOf(val))
		switch vv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			res, ok = T(vv.Int()), vv.Int()>>width == 0
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			res, ok = T(vv.Uint()), vv.Uint()>>(width-1) == 0
		}
	}
	if !ok {
		var t T
		err = castError(val, "int"+strconv.Itoa(int(unsafe.Sizeof(t)*8)))
	}
	return
}

func (c IntCaster[T]) CastSlice(val any) (res any, err error) {
	ok := true
	width := unsafe.Sizeof(T(0)) * 8
	switch v := val.(type) {
	case []int:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>width == 0
		}
		if ok {
			res = cp
		}
	case []int64:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>width == 0
		}
		if ok {
			res = cp
		}
	case []int32:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>width == 0
		}
		if ok {
			res = cp
		}
	case []int16:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>width == 0
		}
		if ok {
			res = cp
		}
	case []int8:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i])
		}
		res, ok = cp, true
	case []uint:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []uint64:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []uint32:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []uint16:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []uint8:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && v[i]>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []float32:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && math.Round(float64(v[i])) == float64(v[i]) && uint32(v[i])>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []float64:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i]), ok && math.Round(v[i]) == v[i] && uint64(v[i])>>(width-1) == 0
		}
		if ok {
			res = cp
		}
	case []num.Decimal32:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i].Int64()), ok && v[i].Scale() == 0 && v[i].Int32()>>width == 0
		}
		if ok {
			res = cp
		}
	case []num.Decimal64:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i].Int64()), ok && v[i].Scale() == 0 && v[i].Int64()>>width == 0
		}
		if ok {
			res = cp
		}
	case []num.Decimal128:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i].Int64()), ok && v[i].Scale() == 0 && v[i].Int128().IsInt64() && v[i].Int64()>>width == 0
		}
		if ok {
			res = cp
		}
	case []num.Decimal256:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i].Int64()), ok && v[i].Scale() == 0 && v[i].Int256().IsInt64() && v[i].Int64()>>width == 0
		}
		if ok {
			res = cp
		}
	case []num.Int128:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i].Int64()), ok && v[i].IsInt64() && v[i].Int64()>>width == 0
		}
		if ok {
			res = cp
		}
	case []num.Int256:
		cp := make([]T, len(v))
		for i := range v {
			cp[i], ok = T(v[i].Int64()), ok && v[i].IsInt64() && v[i].Int64()>>width == 0
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
					cp[i], ok = T(vv.Index(i).Int()), ok && vv.Index(i).Int()>>width == 0
				}
				if ok {
					res = cp
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				cp := make([]T, vv.Len())
				for i, l := 0, vv.Len(); i < l; i++ {
					cp[i], ok = T(vv.Index(i).Uint()), ok && vv.Index(i).Uint()>>(width-1) == 0
				}
				if ok {
					res = cp
				}
			}
		}
	}
	if !ok {
		var t T
		err = castError(val, "int"+strconv.Itoa(int(unsafe.Sizeof(t)*8)))
	}
	return
}

// uint caster
type UintCaster[T constraints.Unsigned] struct{}

func (c UintCaster[T]) CastValue(val any) (res any, err error) {
	var ok bool
	res = val
	switch v := val.(type) {
	case int:
		res, ok = T(v), true // v>>(width-1) == 0
	case int64:
		res, ok = T(v), true
	case int32:
		res, ok = T(v), true
	case int16:
		res, ok = T(v), true
	case int8:
		res, ok = T(v), true
	case uint:
		res, ok = T(v), true // v>>width == 0
	case uint64:
		res, ok = T(v), true
	case uint32:
		res, ok = T(v), true
	case uint16:
		res, ok = T(v), true
	case uint8:
		res, ok = T(v), true
	case float32:
		res, ok = T(v), true // !math.Signbit(v) && ..
	case float64:
		res, ok = T(v), true // !math.Signbit(v) && ..
	case num.Decimal32:
		res, ok = T(v.Int64()), true // width - 1 !! because v can be negative!
	case num.Decimal64:
		res, ok = T(v.Int64()), true
	case num.Decimal128:
		res, ok = T(v.Int64()), true
	case num.Decimal256:
		res, ok = T(v.Int64()), true
	case num.Int128:
		res, ok = T(v.Int64()), true
	case num.Int256:
		res, ok = T(v.Int64()), true
	default:
		// type aliases
		vv := reflect.Indirect(reflect.ValueOf(val))
		switch vv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			res, ok = T(vv.Int()), true
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			res, ok = T(vv.Uint()), true
		}
	}
	if !ok {
		var t T
		err = castError(val, "uint"+strconv.Itoa(int(unsafe.Sizeof(t)*8)))
	}
	return
}

func (c UintCaster[T]) CastSlice(val any) (res any, err error) {
	var ok bool
	res = val
	switch v := val.(type) {
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
		res, ok = val, true
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
	case []float32:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i])
		}
		res, ok = cp, true
	case []float64:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i])
		}
		res, ok = cp, true
	case []num.Decimal32:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i].Int64())
		}
		res, ok = cp, true
	case []num.Decimal64:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i].Int64())
		}
		res, ok = cp, true
	case []num.Decimal128:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i].Int64())
		}
		res, ok = cp, true
	case []num.Decimal256:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i].Int64())
		}
		res, ok = cp, true
	case []num.Int128:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i].Int64())
		}
		res, ok = cp, true
	case []num.Int256:
		cp := make([]T, len(v))
		for i := range v {
			cp[i] = T(v[i].Int64())
		}
		res, ok = cp, true
	default:
		// convert enum types
		vv := reflect.Indirect(reflect.ValueOf(val))
		if vv.Kind() == reflect.Slice {
			switch vv.Type().Elem().Kind() {
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
		var t T
		err = castError(val, "uint"+strconv.Itoa(int(unsafe.Sizeof(t)*8)))
	}
	return
}

// float caster
type FloatCaster[T constraints.Float] struct{}

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
		var t T
		err = castError(val, "float"+strconv.Itoa(int(unsafe.Sizeof(t)*8)))
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
		var t T
		err = castError(val, "float"+strconv.Itoa(int(unsafe.Sizeof(t)*8)))
	}
	return
}

// time caster
type TimeCaster struct{}

func (c TimeCaster) CastValue(val any) (res any, err error) {
	v, ok := val.(time.Time)
	if !ok {
		err = castError(val, "time")
	} else {
		res = v.UnixNano()
	}
	return
}

func (c TimeCaster) CastSlice(val any) (res any, err error) {
	v, ok := val.([]time.Time)
	if !ok {
		err = castError(val, "time")
	} else {
		r := make([]int64, len(v))
		for i := range v {
			r[i] = v[i].UnixNano()
		}
		res = r
	}
	return
}

// bool caster
type BoolCaster struct{}

func (c BoolCaster) CastValue(val any) (res any, err error) {
	var ok bool
	res = val
	switch v := val.(type) {
	case int:
		res, ok = v > 0, true
	case bool:
		ok = true
	}
	if !ok {
		err = castError(val, "bool")
	}
	return
}

func (c BoolCaster) CastSlice(val any) (res any, err error) {
	var ok bool
	res = val
	switch v := val.(type) {
	case []int:
		cp := make([]bool, len(v))
		for i := range v {
			cp[i] = v[i] > 0
		}
		res, ok = cp, true
	case bool:
		ok = true
	}
	if !ok {
		err = castError(val, "bool")
	}
	return
}

// string caster
type StringCaster struct{}

func (c StringCaster) CastValue(val any) (res any, err error) {
	var ok bool
	switch v := val.(type) {
	case string:
		res, ok = v, true
	case []byte:
		res, ok = string(v), true
	default:
		res, ok = util.ToString(val), true
	}
	if !ok {
		err = castError(val, "string")
	}
	return
}

func (c StringCaster) CastSlice(val any) (res any, err error) {
	var ok bool
	res = val
	switch v := val.(type) {
	case []string:
		res, ok = v, true
	case [][]byte:
		cp := make([]string, len(v))
		for i := range v {
			cp[i] = string(v[i])
		}
		res, ok = cp, true
	default:
		rv := reflect.ValueOf(val)
		if rv.Kind() == reflect.Slice {
			cp := make([]string, rv.Len())
			for i := range cp {
				cp[i] = util.ToString(rv.Index(i))
			}
			res, ok = cp, true
		}
	}
	if !ok {
		err = castError(val, "string")
	}
	return
}

// bytes caster
type BytesCaster struct{}

func (c BytesCaster) CastValue(val any) (res any, err error) {
	var (
		ok bool
		b  [8]byte
	)
	switch v := val.(type) {
	case int:
		binary.BigEndian.PutUint64(b[:], uint64(v))
		res, ok = b[:], true
	case int64:
		binary.BigEndian.PutUint64(b[:], uint64(v))
		res, ok = b[:], true
	case int32:
		binary.BigEndian.PutUint32(b[:], uint32(v))
		res, ok = b[:4], true
	case int16:
		binary.BigEndian.PutUint16(b[:], uint16(v))
		res, ok = b[:2], true
	case int8:
		res, ok = byte(v), true
	case uint:
		binary.BigEndian.PutUint64(b[:], uint64(v))
		res, ok = b[:], true
	case uint64:
		binary.BigEndian.PutUint64(b[:], uint64(v))
		res, ok = b[:], true
	case uint32:
		binary.BigEndian.PutUint32(b[:], uint32(v))
		res, ok = b[:4], true
	case uint16:
		binary.BigEndian.PutUint16(b[:], uint16(v))
		res, ok = b[:2], true
	case uint8:
		res, ok = byte(v), true
	case float64:
		binary.BigEndian.PutUint64(b[:], math.Float64bits(v))
		res, ok = b[:], true
	case float32:
		binary.BigEndian.PutUint32(b[:], math.Float32bits(v))
		res, ok = b[:4], true
	case num.Decimal32:
		binary.BigEndian.PutUint32(b[:], uint32(v.Int32()))
		res, ok = b[:4], true
	case num.Decimal64:
		binary.BigEndian.PutUint64(b[:], uint64(v.Int64()))
		res, ok = b[:], true
	case num.Decimal128:
		b := v.Int128().Bytes16()
		res, ok = b[:], true
	case num.Decimal256:
		b := v.Int256().Bytes32()
		res, ok = b[:], true
	case num.Int128:
		b := v.Bytes16()
		res, ok = b[:], true
	case num.Int256:
		b := v.Bytes32()
		res, ok = b[:], true
	case string:
		res, ok = []byte(v), true
	default:
		// binary marshaler
		if v, ok2 := val.(encoding.BinaryMarshaler); ok2 {
			res, err = v.MarshalBinary()
			ok = err == nil
		} else {
			// type aliases
			vv := reflect.Indirect(reflect.ValueOf(val))
			switch vv.Kind() {
			case reflect.Float32:
				binary.BigEndian.PutUint32(b[:], math.Float32bits(float32(vv.Float())))
				res, ok = b[:4], true
			case reflect.Float64:
				binary.BigEndian.PutUint64(b[:], math.Float64bits(vv.Float()))
				res, ok = b[:], true
			case reflect.Int, reflect.Int64:
				binary.BigEndian.PutUint64(b[:], uint64(vv.Int()))
				res, ok = b[:vv.Type().Size()], true
			case reflect.Int32:
				binary.BigEndian.PutUint32(b[:], uint32(vv.Int()))
				res, ok = b[:4], true
			case reflect.Int16:
				binary.BigEndian.PutUint16(b[:], uint16(vv.Int()))
				res, ok = b[:2], true
			case reflect.Int8:
				res, ok = byte(vv.Int()), true
			case reflect.Uint, reflect.Uint64:
				binary.BigEndian.PutUint64(b[:], uint64(vv.Uint()))
				res, ok = b[:], true
			case reflect.Uint32:
				binary.BigEndian.PutUint32(b[:], uint32(vv.Uint()))
				res, ok = b[:4], true
			case reflect.Uint16:
				binary.BigEndian.PutUint16(b[:], uint16(vv.Uint()))
				res, ok = b[:2], true
			case reflect.Uint8:
				res, ok = byte(vv.Uint()), true
			}
		}
	}
	if !ok {
		err = castError(val, "byte")
	}
	return
}

func (c BytesCaster) CastSlice(val any) (res any, err error) {
	var ok bool
	rv := reflect.ValueOf(val)
	if rv.Kind() == reflect.Slice {
		cp := make([][]byte, rv.Len())
		for i := range cp {
			v, err := c.CastValue(rv.Index(i))
			if err != nil {
				break
			}
			cp[i] = v.([]byte)
		}
		res, ok = cp, true
	}
	if !ok {
		err = castError(val, "byte")
	}
	return
}

// int128 caster
type I128Caster struct{}

func (c I128Caster) CastValue(val any) (res any, err error) {
	var ok bool
	res = val
	switch v := val.(type) {
	case int:
		res, ok = num.Int128FromInt64(int64(v)), true
	case int64:
		res, ok = num.Int128FromInt64(v), true
	case int32:
		res, ok = num.Int128FromInt64(int64(v)), true
	case int16:
		res, ok = num.Int128FromInt64(int64(v)), true
	case int8:
		res, ok = num.Int128FromInt64(int64(v)), true
	case uint:
		res, ok = num.Int128FromInt64(int64(v)), true
	case uint64:
		res, ok = num.Int128FromInt64(int64(v)), true
	case uint32:
		res, ok = num.Int128FromInt64(int64(v)), true
	case uint16:
		res, ok = num.Int128FromInt64(int64(v)), true
	case uint8:
		res, ok = num.Int128FromInt64(int64(v)), true
	case float32:
		var i128 num.Int128
		acc := i128.SetFloat64(float64(v))
		res, ok = i128, acc == num.Exact
	case float64:
		var i128 num.Int128
		acc := i128.SetFloat64(v)
		res, ok = i128, acc == num.Exact
	case num.Decimal32:
		res, ok = num.Int128FromInt64(v.Int64()), true
	case num.Decimal64:
		res, ok = num.Int128FromInt64(v.Int64()), true
	case num.Decimal128:
		res, ok = v.Int128(), true
	case num.Decimal256:
		res, ok = v.Int256(), v.Int256().IsInt128()
	case num.Int128:
		res, ok = v, true
	case num.Int256:
		res, ok = v.Int128(), v.IsInt128()
	}
	if !ok {
		err = castError(val, "int128")
	}
	return
}

func (c I128Caster) CastSlice(val any) (res any, err error) {
	var ok bool
	rv := reflect.ValueOf(val)
	if rv.Kind() == reflect.Slice {
		cp := make([]num.Int128, rv.Len())
		for i := range cp {
			v, err := c.CastValue(rv.Index(i))
			if err != nil {
				break
			}
			cp[i] = v.(num.Int128)
		}
		res, ok = cp, true
	}
	if !ok {
		err = castError(val, "int128")
	}
	return
}

// int256 caster
type I256Caster struct{}

func (c I256Caster) CastValue(val any) (res any, err error) {
	var ok bool
	res = val
	switch v := val.(type) {
	case int:
		res, ok = num.Int256FromInt64(int64(v)), true
	case int64:
		res, ok = num.Int256FromInt64(v), true
	case int32:
		res, ok = num.Int256FromInt64(int64(v)), true
	case int16:
		res, ok = num.Int256FromInt64(int64(v)), true
	case int8:
		res, ok = num.Int256FromInt64(int64(v)), true
	case uint:
		res, ok = num.Int256FromInt64(int64(v)), true
	case uint64:
		res, ok = num.Int256FromInt64(int64(v)), true
	case uint32:
		res, ok = num.Int256FromInt64(int64(v)), true
	case uint16:
		res, ok = num.Int256FromInt64(int64(v)), true
	case uint8:
		res, ok = num.Int256FromInt64(int64(v)), true
	case float32:
		var i256 num.Int256
		i256.SetFloat64(float64(v))
		res, ok = i256, true
	case float64:
		var i256 num.Int256
		i256.SetFloat64(v)
		res, ok = i256, true
	case num.Decimal32:
		res, ok = num.Int256FromInt64(v.Int64()), true
	case num.Decimal64:
		res, ok = num.Int256FromInt64(v.Int64()), true
	case num.Decimal128:
		res, ok = v.Int256(), true
	case num.Decimal256:
		res, ok = v.Int256(), true
	case num.Int128:
		res, ok = v.Int256(), true
	case num.Int256:
		res, ok = v, true
	}
	if !ok {
		err = castError(val, "int256")
	}
	return
}

func (c I256Caster) CastSlice(val any) (res any, err error) {
	var ok bool
	rv := reflect.ValueOf(val)
	if rv.Kind() == reflect.Slice {
		cp := make([]num.Int128, rv.Len())
		for i := range cp {
			v, err := c.CastValue(rv.Index(i))
			if err != nil {
				break
			}
			cp[i] = v.(num.Int128)
		}
		res, ok = cp, true
	}
	if !ok {
		err = castError(val, "int128")
	}
	return
}
