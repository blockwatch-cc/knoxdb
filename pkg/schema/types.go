// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"fmt"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
)

type FieldType byte

const (
	Invalid FieldType = iota
	Timestamp
	Int64
	Uint64
	Float64
	Boolean
	String
	Bytes
	Int32
	Int16
	Int8
	Uint32
	Uint16
	Uint8
	Float32
	Int256
	Int128
	Decimal256
	Decimal128
	Decimal64
	Decimal32
	Bigint
	Date
	Time
	Text
	Binary
	List
	Map

	// TODO: new types
	// Duration
	// Union
)

const (
	MAX_NAME   = 1<<8 - 1  // 255
	MAX_ARRAY  = 1<<8 - 1  // 255
	MAX_STRING = 1<<8 - 1  // 255
	MAX_BYTES  = 1<<8 - 1  // 255
	MAX_TEXT   = 1<<32 - 1 // 4G
	MAX_BLOB   = 1<<32 - 1 // 4G
)

var (
	fieldTypeString  = "__timestamp_int64_uint64_float64_boolean_string_bytes_int32_int16_int8_uint32_uint16_uint8_float32_int256_int128_decimal256_decimal128_decimal64_decimal32_bigint_date_time_text_blob_list_map"
	fieldTypeIdx     = [...]uint8{0, 2, 12, 18, 25, 33, 41, 48, 54, 60, 66, 71, 78, 85, 91, 99, 106, 113, 124, 135, 145, 155, 162, 167, 172, 177, 182, 187, 191}
	fieldTypeReverse = map[string]FieldType{}

	fieldTypeWireSize = [...]int{
		Invalid:    0,
		Timestamp:  8, // i64
		Int64:      8,
		Uint64:     8,
		Float64:    8,
		Boolean:    1,
		String:     1, // 1 byte size
		Bytes:      1, // 1 byte size
		Int32:      4,
		Int16:      2,
		Int8:       1,
		Uint32:     4,
		Uint16:     2,
		Uint8:      1,
		Float32:    4,
		Int256:     32,
		Int128:     16,
		Decimal256: 32,
		Decimal128: 16,
		Decimal64:  8,
		Decimal32:  4,
		Bigint:     1, // 1 byte size + var bytes
		Date:       8, // i64
		Time:       8, // i64
		Text:       4, // 4 byte size
		Binary:     4, // 4 byte size
		List:       4, // 4 byte size
		Map:        4, // 4 byte size
	}
)

func init() {
	for t := range FieldType(len(fieldTypeIdx) - 1) {
		fieldTypeReverse[t.String()] = t
	}
	for f := range FieldFlags(len(fieldFlagIdx) - 1) {
		fieldFlagReverse[f.String()] = f
	}
}

func (t FieldType) IsValid() bool {
	return t > Invalid && t <= Map
}

func (t FieldType) String() string {
	return fieldTypeString[fieldTypeIdx[t] : fieldTypeIdx[t+1]-1]
}

func (t FieldType) Zero() any {
	switch t {
	case Timestamp, Date, Time:
		var t time.Time
		return t.UTC()
	case Int64:
		return int64(0)
	case Uint64:
		return uint64(0)
	case Float64:
		return float64(0)
	case Boolean:
		return false
	case String, Text:
		return ""
	case Bytes, Binary, List, Map:
		return []byte{}
	case Int32:
		return int32(0)
	case Int16:
		return int16(0)
	case Int8:
		return int8(0)
	case Uint32:
		return uint32(0)
	case Uint16:
		return uint16(0)
	case Uint8:
		return uint8(0)
	case Float32:
		return float32(0)
	case Int256:
		return num.ZeroInt256
	case Int128:
		return num.ZeroInt128
	case Decimal256:
		return num.ZeroDecimal256
	case Decimal128:
		return num.ZeroDecimal128
	case Decimal64:
		return num.ZeroDecimal64
	case Decimal32:
		return num.ZeroDecimal32
	case Bigint:
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

type FieldFlags byte

const (
	FlagPrimary  FieldFlags = 1 << iota // primary key
	FlagArray                           // fixed length string/byte array
	FlagEnum                            // enumeration
	FlagDeleted                         // is deleted, hide
	FlagMetadata                        // field is metadata
	FlagNullable                        // can be null
	FlagTimebase                        // event time timestamp
	FlagAction                          // field is CDC action metadata
)

var (
	fieldFlagNames   = "primary_array_enum_deleted_metadata_nullable_timebase_action"
	fieldFlagIdx     = [...]int8{0, 8, 14, 19, 27, 36, 45, 54, 61}
	fieldFlagReverse = map[string]FieldFlags{}
)

func (i FieldFlags) String() string {
	if i == 0 {
		return ""
	}
	var b strings.Builder
	for p, k := 0, FieldFlags(1); p < 7; p, k = p+1, k<<1 {
		if i&k > 0 {
			start, end := fieldFlagIdx[p], fieldFlagIdx[p+1]-1
			if b.Len() > 0 {
				b.WriteString(",")
			}
			b.WriteString(fieldFlagNames[start:end])
		}
	}
	return b.String()
}

func ParseFieldFlag(s string) FieldFlags {
	return fieldFlagReverse[s]
}

func validateInt(name string, n, minVal, maxVal int) error {
	if n < minVal || (maxVal > 0 && n > maxVal) {
		return fmt.Errorf("%s %d out of bounds [%d..%d]", name, n, minVal, maxVal)
	}
	return nil
}
