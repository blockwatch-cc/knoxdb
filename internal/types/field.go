// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import "strings"

type FieldType byte

const (
	FieldTypeInvalid FieldType = iota
	FieldTypeDatetime
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
)

var (
	fieldTypeString  = "__datetime_int64_uint64_float64_boolean_string_bytes_int32_int16_int8_uint32_uint16_uint8_float32_int256_int128_decimal256_decimal128_decimal64_decimal32"
	fieldTypeIdx     = [...]int{0, 2, 11, 17, 24, 32, 40, 47, 53, 59, 65, 70, 77, 84, 90, 98, 105, 112, 123, 134, 144, 154}
	fieldTypeReverse = map[string]FieldType{}

	fieldTypeDataSize = [...]int{
		FieldTypeInvalid:    0,
		FieldTypeDatetime:   8,
		FieldTypeInt64:      8,
		FieldTypeUint64:     8,
		FieldTypeFloat64:    8,
		FieldTypeBoolean:    1,
		FieldTypeString:     16,
		FieldTypeBytes:      24,
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
	}

	fieldTypeWireSize = [...]int{
		FieldTypeInvalid:    0,
		FieldTypeDatetime:   8,
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
	}
)

func init() {
	for t := FieldTypeInvalid; t <= FieldTypeDecimal32; t++ {
		fieldTypeReverse[t.String()] = t
	}
}

func (t FieldType) IsValid() bool {
	return t > FieldTypeInvalid && t <= FieldTypeDecimal32
}

func (t FieldType) String() string {
	return fieldTypeString[fieldTypeIdx[t] : fieldTypeIdx[t+1]-1]
}

func ParseFieldType(s string) FieldType {
	return fieldTypeReverse[s]
}

func (t FieldType) Size() int {
	return fieldTypeWireSize[t]
}

type FieldFlags byte

const (
	FieldFlagPrimary FieldFlags = 1 << iota
	FieldFlagIndexed
	FieldFlagEnum
	FieldFlagDeleted
	FieldFlagInternal
)

var (
	fieldFlagNames = "primary_indexed_enum_deleted_internal"
	fieldFlagIdx   = [...]int{0, 8, 16, 21, 29, 38}
)

func (i FieldFlags) Is(f FieldFlags) bool {
	return i&f > 0
}

func (i FieldFlags) String() string {
	if i == 0 {
		return ""
	}
	var b strings.Builder
	for p, k := 0, FieldFlags(1); p < 6; p, k = p+1, k<<1 {
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

type FieldCompression byte

const (
	FieldCompressNone FieldCompression = iota
	FieldCompressSnappy
	FieldCompressLZ4
	FieldCompressZstd
)

func (i FieldCompression) Is(f FieldCompression) bool {
	return i&f > 0
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
