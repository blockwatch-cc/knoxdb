// Copyright (c) 2024-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package parse

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

type ValueParser interface {
	ParseValue(string) (any, error)
	ParseSlice(string) (any, error)
}

func NewParser(typ schema.FieldType, scale uint8, enum ValueParser) ValueParser {
	switch typ {
	case schema.Timestamp:
		return TimeParser{scale: schema.TimeScale(scale), isTimeOnly: false}
	case schema.Duration:
		return DurationParser{scale: schema.TimeScale(scale)}
	case schema.Time:
		return TimeParser{scale: schema.TimeScale(scale), isTimeOnly: true}
	case schema.Date:
		return TimeParser{scale: schema.TIME_SCALE_DAY, isTimeOnly: false}
	case schema.Boolean:
		return BoolParser{}
	case schema.String:
		if scale > 0 {
			return StringParser{int(scale), int(scale)}
		}
		return StringParser{0, 255}
	case schema.Bytes:
		if scale > 0 {
			return BytesParser{int(scale), int(scale)}
		}
		return BytesParser{0, 255}
	case schema.Text:
		return StringParser{}
	case schema.Binary:
		return BytesParser{}
	case schema.Int8:
		return IntParser[int8]{8}
	case schema.Int16:
		return IntParser[int16]{16}
	case schema.Int32:
		return IntParser[int32]{32}
	case schema.Int64:
		return IntParser[int64]{64}
	case schema.Uint8:
		return UintParser[uint8]{8}
	case schema.Uint16:
		return UintParser[uint16]{16}
	case schema.Uint32:
		return UintParser[uint32]{32}
	case schema.Uint64:
		return UintParser[uint64]{64}
	case schema.Float32:
		return FloatParser[float32]{32}
	case schema.Float64:
		return FloatParser[float64]{64}
	case schema.Int128:
		return I128Parser{}
	case schema.Int256:
		return I256Parser{}
	case schema.Decimal32:
		return D32Parser{scale}
	case schema.Decimal64:
		return D64Parser{scale}
	case schema.Decimal128:
		return D128Parser{scale}
	case schema.Decimal256:
		return D256Parser{scale}
	case schema.Bigint:
		return BigIntParser{}
	case schema.Enum:
		return enum
	default:
		panic(fmt.Errorf("parser: unsupported field type %s %d", typ, typ))
	}
}

// int parser
type IntParser[T schema.Signed] struct {
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
type UintParser[T schema.Unsigned] struct {
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
type FloatParser[T schema.Float] struct {
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

func (I128Parser) ParseValue(s string) (any, error) {
	return num.ParseInt128(s)
}

func (I128Parser) ParseSlice(s string) (any, error) {
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

func (I256Parser) ParseValue(s string) (any, error) {
	return num.ParseInt256(s)
}

func (I256Parser) ParseSlice(s string) (any, error) {
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
type StringParser struct {
	minLen int
	maxLen int
}

func (p StringParser) ensureLength(n int) error {
	if p.minLen > 0 && n < p.minLen {
		return fmt.Errorf("string len %d < min %d", n, p.minLen)
	}
	if p.maxLen > 0 && n > p.maxLen {
		return fmt.Errorf("string len %d > max %d", n, p.maxLen)
	}
	return nil
}

func (p StringParser) ParseValue(s string) (any, error) {
	if err := p.ensureLength(len(s)); err != nil {
		return nil, err
	}
	return []byte(s), nil
}

func (p StringParser) ParseSlice(s string) (any, error) {
	res := bytes.Split([]byte(s), []byte(","))
	if p.maxLen > 0 || p.minLen > 0 {
		for _, v := range res {
			if err := p.ensureLength(len(v)); err != nil {
				return nil, err
			}
		}
	}
	return res, nil
}

// bytes parser
type BytesParser struct {
	minLen int
	maxLen int
}

func (p BytesParser) ensureLength(n int) error {
	if p.minLen > 0 && n < p.minLen {
		return fmt.Errorf("bytes len %d < min %d", n, p.minLen)
	}
	if p.maxLen > 0 && n > p.maxLen {
		return fmt.Errorf("bytes len %d > max %d", n, p.maxLen)
	}
	return nil
}

func (p BytesParser) ParseValue(s string) (any, error) {
	if strings.HasPrefix(s, "0x") {
		if err := p.ensureLength(len(s)/2 - 1); err != nil {
			return nil, err
		}
		return hex.DecodeString(s[2:])
	} else {
		if err := p.ensureLength(len(s)); err != nil {
			return nil, err
		}
		return util.UnsafeGetBytes(s), nil
	}
}

func (p BytesParser) ParseSlice(s string) (any, error) {
	if len(s) == 0 {
		return nil, nil
	}
	vv := strings.Split(s, ",")
	slice := make([][]byte, len(vv))
	if strings.HasPrefix(vv[0], "0x") {
		var err error
		for i, v := range vv {
			if err = p.ensureLength(len(s)/2 - 1); err != nil {
				return nil, err
			}
			slice[i], err = hex.DecodeString(v[2:])
			if err != nil {
				return nil, err
			}
		}
	} else {
		for i, v := range vv {
			if err := p.ensureLength(len(s)); err != nil {
				return nil, err
			}
			slice[i] = util.UnsafeGetBytes(v)
		}
	}
	return slice, nil
}

// time parser
type TimeParser struct {
	scale      schema.TimeScale
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

// duration parser
type DurationParser struct {
	scale schema.TimeScale
}

func (p DurationParser) ParseValue(s string) (any, error) {
	d, err := p.scale.ParseDuration(s)
	if err != nil {
		return nil, err
	}
	return p.scale.Int64(d), nil
}

func (p DurationParser) ParseSlice(s string) (any, error) {
	vv := strings.Split(s, ",")
	slice := make([]int64, len(vv))
	for i, v := range vv {
		d, err := p.scale.ParseDuration(v)
		if err != nil {
			return nil, err
		}
		slice[i] = p.scale.Int64(d)
	}
	return slice, nil
}

// bool parser
type BoolParser struct{}

func (BoolParser) ParseValue(s string) (any, error) {
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

func (BigIntParser) ParseValue(s string) (any, error) {
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
