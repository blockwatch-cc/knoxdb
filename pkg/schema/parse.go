// Copyright (c) 2024-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
	"golang.org/x/exp/constraints"
)

type ValueParser interface {
	ParseValue(string) (any, error)
	ParseSlice(string) (any, error)
}

func NewParser(typ types.FieldType, scale uint8, enum *EnumDictionary) ValueParser {
	switch typ {
	case FT_TIMESTAMP:
		return TimeParser{scale: TimeScale(scale), isTimeOnly: false}
	case FT_TIME:
		return TimeParser{scale: TimeScale(scale), isTimeOnly: true}
	case FT_DATE:
		return TimeParser{scale: TIME_SCALE_DAY, isTimeOnly: false}
	case FT_BOOL:
		return BoolParser{}
	case FT_STRING:
		return StringParser{}
	case FT_BYTES:
		return BytesParser{}
	case FT_I8:
		return IntParser[int8]{8}
	case FT_I16:
		return IntParser[int16]{16}
	case FT_I32:
		return IntParser[int32]{32}
	case FT_I64:
		return IntParser[int64]{64}
	case FT_U8:
		return UintParser[uint8]{8}
	case FT_U16:
		if enum == nil {
			return UintParser[uint16]{16}
		} else {
			return enum
		}
	case FT_U32:
		return UintParser[uint32]{32}
	case FT_U64:
		return UintParser[uint64]{64}
	case FT_F32:
		return FloatParser[float32]{32}
	case FT_F64:
		return FloatParser[float64]{64}
	case FT_I128:
		return I128Parser{}
	case FT_I256:
		return I256Parser{}
	case FT_D32:
		return D32Parser{scale}
	case FT_D64:
		return D64Parser{scale}
	case FT_D128:
		return D128Parser{scale}
	case FT_D256:
		return D256Parser{scale}
	case FT_BIGINT:
		return BigIntParser{}
	default:
		panic(fmt.Errorf("parser: unsupported field type %s %d", typ, typ))
	}
}

// int parser
type IntParser[T constraints.Signed] struct {
	bitsize int
}

func (p IntParser[T]) ParseValue(s string) (any, error) {
	val, err := strconv.ParseInt(s, 0, p.bitsize)
	return T(val), err
}

func (p IntParser[T]) ParseSlice(s string) (any, error) {
	vv := strings.Split(s, ",")
	slice := make([]T, len(vv))
	for i, v := range vv {
		j, err := strconv.ParseInt(v, 0, p.bitsize)
		if err != nil {
			return nil, err
		}
		slice[i] = T(j)
	}
	return slice, nil
}

// uint parser
type UintParser[T constraints.Unsigned] struct {
	bitsize int
}

func (p UintParser[T]) ParseValue(s string) (any, error) {
	val, err := strconv.ParseUint(s, 0, p.bitsize)
	return T(val), err
}

func (p UintParser[T]) ParseSlice(s string) (any, error) {
	vv := strings.Split(s, ",")
	slice := make([]T, len(vv))
	for i, v := range vv {
		j, err := strconv.ParseUint(v, 0, p.bitsize)
		if err != nil {
			return nil, err
		}
		slice[i] = T(j)
	}
	return slice, nil
}

// float parser
type FloatParser[T constraints.Float] struct {
	bitsize int
}

func (p FloatParser[T]) ParseValue(s string) (any, error) {
	val, err := strconv.ParseFloat(s, p.bitsize)
	return T(val), err
}

func (p FloatParser[T]) ParseSlice(s string) (any, error) {
	vv := strings.Split(s, ",")
	slice := make([]T, len(vv))
	for i, v := range vv {
		j, err := strconv.ParseFloat(v, p.bitsize)
		if err != nil {
			return nil, err
		}
		slice[i] = T(j)
	}
	return slice, nil
}

// i128 parser
type I128Parser struct{}

func (_ I128Parser) ParseValue(s string) (any, error) {
	return num.ParseInt128(s)
}

func (_ I128Parser) ParseSlice(s string) (any, error) {
	vv := strings.Split(s, ",")
	slice := make([]num.Int128, len(vv))
	for i, v := range vv {
		var err error
		slice[i], err = num.ParseInt128(v)
		if err != nil {
			return nil, err
		}
	}
	return slice, nil
}

// i256 parser
type I256Parser struct{}

func (_ I256Parser) ParseValue(s string) (any, error) {
	return num.ParseInt256(s)
}

func (_ I256Parser) ParseSlice(s string) (any, error) {
	vv := strings.Split(s, ",")
	slice := make([]num.Int256, len(vv))
	for i, v := range vv {
		var err error
		slice[i], err = num.ParseInt256(v)
		if err != nil {
			return nil, err
		}
	}
	return slice, nil
}

// Decimal32 parser
type D32Parser struct {
	scale uint8
}

func (p D32Parser) ParseValue(s string) (any, error) {
	d, err := num.ParseDecimal32(s)
	return d.Quantize(p.scale).Int32(), err
}

func (p D32Parser) ParseSlice(s string) (any, error) {
	vv := strings.Split(s, ",")
	slice := make([]int32, len(vv))
	for i, v := range vv {
		d, err := num.ParseDecimal32(v)
		if err != nil {
			return nil, err
		}
		slice[i] = d.Quantize(p.scale).Int32()
	}
	return slice, nil
}

// Decimal64 parser
type D64Parser struct {
	scale uint8
}

func (p D64Parser) ParseValue(s string) (any, error) {
	d, err := num.ParseDecimal64(s)
	return d.Quantize(p.scale).Int64(), err
}

func (p D64Parser) ParseSlice(s string) (any, error) {
	vv := strings.Split(s, ",")
	slice := make([]int64, len(vv))
	for i, v := range vv {
		d, err := num.ParseDecimal64(v)
		if err != nil {
			return nil, err
		}
		slice[i] = d.Quantize(p.scale).Int64()
	}
	return slice, nil
}

// Decimal128 parser
type D128Parser struct {
	scale uint8
}

func (p D128Parser) ParseValue(s string) (any, error) {
	d, err := num.ParseDecimal128(s)
	return d.Quantize(p.scale).Int128(), err
}

func (p D128Parser) ParseSlice(s string) (any, error) {
	vv := strings.Split(s, ",")
	slice := make([]num.Int128, len(vv))
	for i, v := range vv {
		d, err := num.ParseDecimal128(v)
		if err != nil {
			return nil, err
		}
		slice[i] = d.Quantize(p.scale).Int128()
	}
	return slice, nil
}

// Decimal256 parser
type D256Parser struct {
	scale uint8
}

func (p D256Parser) ParseValue(s string) (any, error) {
	d, err := num.ParseDecimal256(s)
	return d.Quantize(p.scale).Int256(), err
}

func (p D256Parser) ParseSlice(s string) (any, error) {
	vv := strings.Split(s, ",")
	slice := make([]num.Int256, len(vv))
	for i, v := range vv {
		d, err := num.ParseDecimal256(v)
		if err != nil {
			return nil, err
		}
		slice[i] = d.Quantize(p.scale).Int256()
	}
	return slice, nil
}

// string parser
type StringParser struct{}

func (_ StringParser) ParseValue(s string) (any, error) {
	return []byte(s), nil
}

func (p StringParser) ParseSlice(s string) (any, error) {
	return bytes.Split([]byte(s), []byte(",")), nil
}

// bytes parser
type BytesParser struct{}

func (_ BytesParser) ParseValue(s string) (any, error) {
	if strings.HasPrefix(s, "0x") {
		return hex.DecodeString(s[2:])
	}
	return util.UnsafeGetBytes(s), nil
}

func (_ BytesParser) ParseSlice(s string) (any, error) {
	if len(s) == 0 {
		return nil, nil
	}
	vv := strings.Split(s, ",")
	slice := make([][]byte, len(vv))
	if strings.HasPrefix(vv[0], "0x") {
		var err error
		for i, v := range vv {
			slice[i], err = hex.DecodeString(v[2:])
			if err != nil {
				return nil, err
			}
		}
	} else {
		for i, v := range vv {
			slice[i] = util.UnsafeGetBytes(v)
		}
	}
	return slice, nil
}

// time parser
type TimeParser struct {
	scale      TimeScale
	isTimeOnly bool
}

func (p TimeParser) ParseValue(s string) (any, error) {
	return p.scale.Parse(s, p.isTimeOnly)
}

func (p TimeParser) ParseSlice(s string) (any, error) {
	vv := strings.Split(s, ",")
	slice := make([]int64, len(vv))
	for i, v := range vv {
		tm, err := p.scale.Parse(v, p.isTimeOnly)
		if err != nil {
			return nil, err
		}
		slice[i] = tm
	}
	return slice, nil
}

// bool parser
type BoolParser struct{}

func (_ BoolParser) ParseValue(s string) (any, error) {
	return strconv.ParseBool(s)
}

func (p BoolParser) ParseSlice(s string) (any, error) {
	vv := strings.Split(s, ",")
	slice := make([]bool, len(vv))
	for i, v := range vv {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, err
		}
		slice[i] = b
	}
	return slice, nil
}

// bigint parser
type BigIntParser struct{}

func (_ BigIntParser) ParseValue(s string) (any, error) {
	return num.ParseBig(s)
}

func (p BigIntParser) ParseSlice(s string) (any, error) {
	vv := strings.Split(s, ",")
	slice := make([]num.Big, len(vv))
	for i, v := range vv {
		b, err := num.ParseBig(v)
		if err != nil {
			return nil, err
		}
		slice[i] = b
	}
	return slice, nil
}
