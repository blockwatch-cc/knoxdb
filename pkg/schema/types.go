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
	Invalid    FieldType = iota // 0
	Timestamp                   // 1
	Duration                    // 2
	Date                        // 3
	Time                        // 4
	Uint64                      // 5
	Uint32                      // 6
	Uint16                      // 7
	Uint8                       // 8
	Int64                       // 9
	Int32                       // 10
	Int16                       // 11
	Int8                        // 12
	Boolean                     // 13
	Float64                     // 14
	Float32                     // 15
	Int256                      // 16
	Int128                      // 17
	Decimal256                  // 18
	Decimal128                  // 19
	Decimal64                   // 20
	Decimal32                   // 21
	Bigint                      // 22
	String                      // 23
	Text                        // 24
	Bytes                       // 25
	Binary                      // 26
	List                        // 27
	Map                         // 28
	Union                       // 29
	Variant                     // 30
)

const (
	MAX_NAME     = 1<<8 - 1  // 255
	MAX_ARRAY    = 1<<8 - 1  // 255
	MAX_STRING   = 1<<8 - 1  // 255
	MAX_BYTES    = 1<<8 - 1  // 255
	MAX_VARIANTS = 1<<8 - 1  // 255
	MAX_TEXT     = 1<<32 - 1 // 4G
	MAX_BINARY   = 1<<32 - 1 // 4G
)

var (
	fieldTypeString  = "__timestamp_duration_date_time_u64_u32_u16_u8_i64_i32_i16_i8_bool_f64_f32_i256_i128_d256_d128_d64_d32_bigint_string_text_bytes_binary_list_map_union_variant"
	fieldTypeIdx     = [...]uint8{0, 2, 12, 21, 26, 31, 35, 39, 43, 46, 50, 54, 58, 61, 66, 70, 74, 79, 84, 89, 94, 98, 102, 109, 116, 121, 127, 134, 139, 143, 149, 157}
	fieldTypeReverse = map[string]FieldType{}

	// fixed / minimum wire sizes in bytes for record encoding
	fieldTypeWireSize = [...]int{
		Invalid:    0,
		Timestamp:  8, // i64
		Duration:   8, // i64
		Date:       8, // i64
		Time:       8, // i64
		Uint64:     8,
		Uint32:     4,
		Uint16:     2,
		Uint8:      1,
		Int64:      8,
		Int32:      4,
		Int16:      2,
		Int8:       1,
		Boolean:    1,
		Float64:    8,
		Float32:    4,
		Int256:     32,
		Int128:     16,
		Decimal256: 32,
		Decimal128: 16,
		Decimal64:  8,
		Decimal32:  4,
		Bigint:     1, // 1 byte size + var bytes
		String:     1, // 1 byte size
		Text:       4, // 4 byte size
		Bytes:      1, // 1 byte size
		Binary:     4, // 4 byte size
		List:       4, // 4 byte size
		Map:        4, // 4 byte size
		Union:      1, // 1 byte size + var bytes
		Variant:    5, // 4 byte size + 1 byte typeid + var bytes
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
	return t > Invalid && t <= FieldType(len(fieldTypeIdx))
}

func (t FieldType) NullableDefault() bool {
	switch t {
	case Binary, Bytes, Union:
		return true
	default:
		return false
	}
}

func (t FieldType) String() string {
	return fieldTypeString[fieldTypeIdx[t] : fieldTypeIdx[t+1]-1]
}

func (t FieldType) Zero() any {
	switch t {
	case Timestamp, Date, Time:
		var t time.Time
		return t.UTC()
	case Duration:
		return time.Duration(0)
	case Uint64:
		return uint64(0)
	case Uint32:
		return uint32(0)
	case Uint16:
		return uint16(0)
	case Uint8:
		return uint8(0)
	case Int64:
		return int64(0)
	case Int32:
		return int32(0)
	case Int16:
		return int16(0)
	case Int8:
		return int8(0)
	case Boolean:
		return false
	case Float64:
		return float64(0)
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
	case String, Text:
		return ""
	case Bytes, Binary, List, Map, Variant:
		return []byte{}
	case Union:
		return UnionValue{}
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
