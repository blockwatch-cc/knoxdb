// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"strings"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
)

type FieldType byte

const (
	FieldTypeInvalid FieldType = iota
	FieldTypeTimestamp
	FieldTypeInt64
	FieldTypeUint64
	FieldTypeFloat64
	FieldTypeBoolean
	FieldTypeString
	FieldTypeBytes
	FieldTypeInt32
	FieldTypeInt16
	FieldTypeInt8
	FieldTypeUint32
	FieldTypeUint16
	FieldTypeUint8
	FieldTypeFloat32
	FieldTypeInt256
	FieldTypeInt128
	FieldTypeDecimal256
	FieldTypeDecimal128
	FieldTypeDecimal64
	FieldTypeDecimal32
	FieldTypeBigint
	FieldTypeDate
	FieldTypeTime
)

var (
	fieldTypeString  = "__timestamp_int64_uint64_float64_boolean_string_bytes_int32_int16_int8_uint32_uint16_uint8_float32_int256_int128_decimal256_decimal128_decimal64_decimal32_bigint_date_time"
	fieldTypeIdx     = [...]int{0, 2, 12, 18, 25, 33, 41, 48, 54, 60, 66, 71, 78, 85, 91, 99, 106, 113, 124, 135, 145, 155, 162, 167, 172}
	fieldTypeReverse = map[string]FieldType{}

	fieldTypeWireSize = [...]int{
		FieldTypeInvalid:    0,
		FieldTypeTimestamp:  8, // i64
		FieldTypeInt64:      8,
		FieldTypeUint64:     8,
		FieldTypeFloat64:    8,
		FieldTypeBoolean:    1,
		FieldTypeString:     4, // minimum uint32 for size
		FieldTypeBytes:      4, // minimum uint32 for size
		FieldTypeInt32:      4,
		FieldTypeInt16:      2,
		FieldTypeInt8:       1,
		FieldTypeUint32:     4,
		FieldTypeUint16:     2,
		FieldTypeUint8:      1,
		FieldTypeFloat32:    4,
		FieldTypeInt256:     32,
		FieldTypeInt128:     16,
		FieldTypeDecimal256: 32,
		FieldTypeDecimal128: 16,
		FieldTypeDecimal64:  8,
		FieldTypeDecimal32:  4,
		FieldTypeBigint:     4, // stored as var bytes
		FieldTypeDate:       8, // i64
		FieldTypeTime:       8, // i64
	}
)

func init() {
	for t := FieldTypeInvalid; t <= FieldTypeTime; t++ {
		fieldTypeReverse[t.String()] = t
	}
}

func (t FieldType) IsValid() bool {
	return t > FieldTypeInvalid && t <= FieldTypeTime
}

func (t FieldType) String() string {
	return fieldTypeString[fieldTypeIdx[t] : fieldTypeIdx[t+1]-1]
}

func (t FieldType) Zero() any {
	switch t {
	case FieldTypeTimestamp, FieldTypeDate, FieldTypeTime:
		var t time.Time
		return t.UTC()
	case FieldTypeInt64:
		return int64(0)
	case FieldTypeUint64:
		return uint64(0)
	case FieldTypeFloat64:
		return float64(0)
	case FieldTypeBoolean:
		return false
	case FieldTypeString:
		return ""
	case FieldTypeBytes:
		return []byte{}
	case FieldTypeInt32:
		return int32(0)
	case FieldTypeInt16:
		return int16(0)
	case FieldTypeInt8:
		return int8(0)
	case FieldTypeUint32:
		return uint32(0)
	case FieldTypeUint16:
		return uint16(0)
	case FieldTypeUint8:
		return uint8(0)
	case FieldTypeFloat32:
		return float32(0)
	case FieldTypeInt256:
		return num.ZeroInt256
	case FieldTypeInt128:
		return num.ZeroInt128
	case FieldTypeDecimal256:
		return num.ZeroDecimal256
	case FieldTypeDecimal128:
		return num.ZeroDecimal128
	case FieldTypeDecimal64:
		return num.ZeroDecimal64
	case FieldTypeDecimal32:
		return num.ZeroDecimal32
	case FieldTypeBigint:
		return num.BigZero
	default:
		return nil
	}
}

func ParseFieldType(s string) FieldType {
	return fieldTypeReverse[s]
}

func (t FieldType) Size() int {
	return fieldTypeWireSize[t]
}

func (t FieldType) BlockType() BlockType {
	return BlockTypes[t]
}

type FieldFlags byte

const (
	FieldFlagPrimary FieldFlags = 1 << iota
	FieldFlagIndexed
	FieldFlagEnum
	FieldFlagDeleted
	FieldFlagInternal
	FieldFlagNullable
)

var (
	fieldFlagNames = "primary_indexed_enum_deleted_internal_nullable"
	fieldFlagIdx   = [...]int{0, 8, 16, 21, 29, 38, 47}
)

func (i FieldFlags) Is(f FieldFlags) bool {
	return i&f > 0
}

func (i FieldFlags) String() string {
	if i == 0 {
		return ""
	}
	var b strings.Builder
	for p, k := 0, FieldFlags(1); p < 7; p, k = p+1, k<<1 {
		if i.Is(k) {
			start, end := fieldFlagIdx[p], len(fieldFlagNames)
			if k < FieldFlagInternal {
				end = fieldFlagIdx[p+1] - 1
			}
			if b.Len() > 0 {
				b.WriteString(", ")
			}
			b.WriteString(fieldFlagNames[start:end])
		}
	}
	return b.String()
}

type IfaceFlags byte

const (
	IfaceBinaryMarshaler IfaceFlags = 1 << iota
	IfaceBinaryUnmarshaler
	IfaceTextMarshaler
	IfaceTextUnmarshaler
	IfaceStringer
)

func (i IfaceFlags) Is(f IfaceFlags) bool {
	return i&f > 0
}
