// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import "strconv"

type OpCode byte

const (
	OpCodeInvalid         OpCode = iota // 0x0  0
	OpCodeInt8                          // 0x1  1
	OpCodeInt16                         // 0x2  2
	OpCodeInt32                         // 0x3  3
	OpCodeInt64                         // 0x4  4
	OpCodeUint8                         // 0x5  5
	OpCodeUint16                        // 0x6  6
	OpCodeUint32                        // 0x7  7
	OpCodeUint64                        // 0x8  8
	OpCodeFloat32                       // 0x9  9
	OpCodeFloat64                       // 0xA  10
	OpCodeBool                          // 0xB  11
	OpCodeFixedBytes                    // 0xC  12
	OpCodeFixedString                   // 0xD  13
	OpCodeString                        // 0xE  14
	OpCodeBytes                         // 0xF  15
	OpCodeTimestamp                     // 0x10 16
	OpCodeTime                          // 0x11 17
	OpCodeDate                          // 0x12 18
	OpCodeInt128                        // 0x13 19
	OpCodeInt256                        // 0x14 20
	OpCodeDecimal32                     // 0x15 21
	OpCodeDecimal64                     // 0x16 22
	OpCodeDecimal128                    // 0x17 23
	OpCodeDecimal256                    // 0x18 24
	OpCodeMarshalBinary                 // 0x19 25
	OpCodeMarshalText                   // 0x1A 26
	OpCodeStringer                      // 0x1B 27
	OpCodeUnmarshalBinary               // 0x1C 28
	OpCodeUnmarshalText                 // 0x1D 29
	OpCodeEnum                          // 0x1E 30
	OpCodeSkip                          // 0x1F 31
	OpCodeBigInt                        // 0x20 32
)

var (
	opCodeStrings = "__i8_i16_i32_i64_u8_u16_u32_u64_f32_f64_bool_fixbyte_fixstr_str_byte_timestamp_time_date_i128_i256_d32_d64_d128_d256_mshbin_mshtxt_mshstr_ushbin_ushtxt_enum_skip_bigint"
	opCodeIdx     = [...]int{
		0,                           // invalid
		2, 5, 9, 13, 17, 20, 24, 28, // int/uint
		32, 36, // float
		40,     // bool
		45, 53, // fixed
		60, 64, // string, bytes
		69, 79, 84, // datetime
		89, 94, // i128/256
		98, 103, 107, 112, // decimals
		117, 124, 131, // marshalers
		138, 145, // unmarshalers
		152, // enum
		157, // skip
		162, // bigint
		169, // end-of-string
	}
)

func (c OpCode) String() string {
	if int(c) >= len(opCodeIdx)-1 {
		return "opcode_" + strconv.Itoa(int(c))
	}
	return opCodeStrings[opCodeIdx[c] : opCodeIdx[c+1]-1]
}

func (c OpCode) NeedsInterface() bool {
	return c >= OpCodeMarshalBinary && c <= OpCodeUnmarshalText
}
