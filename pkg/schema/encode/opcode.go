// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"strconv"
	"sync"

	"blockwatch.cc/knoxdb/pkg/schema"
)

type OpCode byte

const (
	OC_INVALID   OpCode = iota // 0x0  0
	OC_I8                      // 0x1  1
	OC_I16                     // 0x2  2
	OC_I32                     // 0x3  3
	OC_I64                     // 0x4  4
	OC_U8                      // 0x5  5
	OC_U16                     // 0x6  6
	OC_U32                     // 0x7  7
	OC_U64                     // 0x8  8
	OC_F32                     // 0x9  9
	OC_F64                     // 0xA  10
	OC_BOOL                    // 0xB  11
	OC_FIXBYTES                // 0xC  12
	OC_FIXSTRING               // 0xD  13
	OC_STRING                  // 0xE  14
	OC_BYTES                   // 0xF  15
	OC_TIMESTAMP               // 0x10 16
	OC_TIME                    // 0x11 17
	OC_DATE                    // 0x12 18
	OC_I128                    // 0x13 19
	OC_I256                    // 0x14 20
	OC_D32                     // 0x15 21
	OC_D64                     // 0x16 22
	OC_D128                    // 0x17 23
	OC_D256                    // 0x18 24
	OC_BIGINT                  // 0x19 25
	OC_ENUM                    // 0x1A 26
	OC_SKIP                    // 0x1B 27
	OC_TEXT                    // 0x1C 28
	OC_BLOB                    // 0x1D 29
	OC_LIST                    // 0x1E 30
	OC_MAP                     // 0x1F 31 (unused)
	OC_DURATION                // 0x20 32
	OC_UNION                   // 0x21 33
)

var (
	opCodeStrings = "__i8_i16_i32_i64_u8_u16_u32_u64_f32_f64_bool_fixbyte_fixstr_str_byte_timestamp_time_date_i128_i256_d32_d64_d128_d256_bigint_enum_skip_text_blob_list_map_duration_union"
	opCodeIdx     = [...]uint8{
		0,                           // invalid
		2, 5, 9, 13, 17, 20, 24, 28, // int/uint
		32, 36, // float
		40,     // bool
		45, 53, // fixed
		60, 64, // string, bytes
		69, 79, 84, // datetime
		89, 94, // i128/256
		99, 103, 107, 112, // decimals
		117,      // bigint
		124,      // enum
		129,      // skip
		134,      // text
		139,      // blob
		144, 149, // list, map
		153, // duration
		162, // union
		168, // end-of-string
	}

	ft2oc = map[schema.FieldType]OpCode{
		schema.Timestamp:  OC_TIMESTAMP,
		schema.Duration:   OC_DURATION,
		schema.Date:       OC_DATE,
		schema.Time:       OC_TIME,
		schema.Uint64:     OC_U64,
		schema.Uint32:     OC_U32,
		schema.Uint16:     OC_U16,
		schema.Uint8:      OC_U8,
		schema.Int64:      OC_I64,
		schema.Int32:      OC_I32,
		schema.Int16:      OC_I16,
		schema.Int8:       OC_I8,
		schema.Boolean:    OC_BOOL,
		schema.Float64:    OC_F64,
		schema.Float32:    OC_F32,
		schema.Int256:     OC_I256,
		schema.Int128:     OC_I128,
		schema.Decimal256: OC_D256,
		schema.Decimal128: OC_D128,
		schema.Decimal64:  OC_D64,
		schema.Decimal32:  OC_D32,
		schema.Bigint:     OC_BIGINT,
		schema.String:     OC_STRING,
		schema.Text:       OC_TEXT,
		schema.Bytes:      OC_BYTES,
		schema.Binary:     OC_BLOB,
		schema.List:       OC_LIST,
		schema.Map:        OC_MAP, // unsupported, throws error
		schema.Union:      OC_UNION,
	}
)

func (c OpCode) String() string {
	if int(c) >= len(opCodeIdx)-1 {
		return "opcode_" + strconv.Itoa(int(c))
	}
	return opCodeStrings[opCodeIdx[c] : opCodeIdx[c+1]-1]
}

var (
	ocRegistryLock sync.RWMutex
	ocRegistry     = map[uint64][]OpCode{}
)

func CompileCodecs(s *schema.Schema) (enc []OpCode) {
	// lookup in global type registry first
	ocRegistryLock.RLock()
	enc, ok := ocRegistry[s.Hash]
	ocRegistryLock.RUnlock()
	if ok {
		return enc
	}

	// if not found, create
	enc = make([]OpCode, len(s.Fields))
	// skip every field not on teh first field's level
	lvl := s.Fields[0].Level
	for i, f := range s.Fields {
		if f.Level != lvl {
			enc[i] = OC_SKIP
		} else {
			enc[i] = CodecFor(f)
		}
	}

	// register
	ocRegistryLock.Lock()
	ocRegistry[s.Hash] = enc
	ocRegistryLock.Unlock()

	return
}

func CodecFor(f *schema.Field) OpCode {
	if !f.IsVisible() {
		return OC_SKIP
	}

	oc, ok := ft2oc[f.Type]
	if !ok {
		return OC_INVALID
	}

	if f.IsArray() {
		if f.Type == schema.String {
			return OC_FIXSTRING
		}
		if f.Type == schema.Bytes {
			return OC_FIXBYTES
		}
	}

	if f.IsEnum() {
		return OC_ENUM
	}

	return oc
}
