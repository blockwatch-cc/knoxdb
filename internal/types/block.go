// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

type BlockType byte

const (
	BlockInvalid BlockType = iota // 0
	BlockInt64                    // 1
	BlockInt32                    // 2
	BlockInt16                    // 3
	BlockInt8                     // 4
	BlockUint64                   // 5
	BlockUint32                   // 6
	BlockUint16                   // 7
	BlockUint8                    // 8
	BlockFloat64                  // 9
	BlockFloat32                  // 10
	BlockBool                     // 11
	BlockBytes                    // 12
	BlockInt128                   // 13
	BlockInt256                   // 14
)

var (
	blockTypeNames    = "__i64_i32_i16_i8_u64_u32_u16_u8_f64_f32_bool_bytes_i128_i256"
	blockTypeNamesOfs = [...]int8{0, 2, 6, 10, 14, 17, 21, 25, 29, 32, 36, 40, 45, 51, 56, 61}

	blockTypeDataSize = [...]int{
		BlockInvalid: 0,
		BlockInt64:   8,
		BlockInt32:   4,
		BlockInt16:   2,
		BlockInt8:    1,
		BlockUint64:  8,
		BlockUint32:  4,
		BlockUint16:  2,
		BlockUint8:   1,
		BlockFloat64: 8,
		BlockFloat32: 4,
		BlockBool:    1,
		BlockBytes:   0,
		BlockInt128:  16,
		BlockInt256:  32,
		15:           0, // fill to 16 entries
	}

	BlockTypes = [...]BlockType{
		0:            BlockInvalid, // 0
		FT_TIMESTAMP: BlockInt64,   // 1
		FT_I64:       BlockInt64,   // 2
		FT_U64:       BlockUint64,  // 3
		FT_F64:       BlockFloat64, // 4
		FT_BOOL:      BlockBool,    // 5
		FT_STRING:    BlockBytes,   // 6
		FT_BYTES:     BlockBytes,   // 7
		FT_I32:       BlockInt32,   // 8
		FT_I16:       BlockInt16,   // 9
		FT_I8:        BlockInt8,    // 10
		FT_U32:       BlockUint32,  // 11
		FT_U16:       BlockUint16,  // 12
		FT_U8:        BlockUint8,   // 13
		FT_F32:       BlockFloat32, // 14
		FT_I256:      BlockInt256,  // 15
		FT_I128:      BlockInt128,  // 16
		FT_D256:      BlockInt256,  // 17
		FT_D128:      BlockInt128,  // 18
		FT_D64:       BlockInt64,   // 19
		FT_D32:       BlockInt32,   // 20
		FT_BIGINT:    BlockBytes,   // 21
		FT_DATE:      BlockInt64,   // 22
		FT_TIME:      BlockInt64,   // 23
		FT_TEXT:      BlockBytes,   // 24
		FT_BLOB:      BlockBytes,   // 25
		FT_LIST:      BlockUint32,  // 26 offset map
		FT_MAP:       BlockUint32,  // 27 offset map
		31:           0,            // fill to 32 entries
	}
)

func ToBlockType(t FieldType) BlockType {
	return BlockTypes[t]
}

func (t BlockType) IsValid() bool {
	return t > 0 && t <= BlockInt256
}

func (t BlockType) String() string {
	if !t.IsValid() {
		return "invalid block type"
	}
	return blockTypeNames[blockTypeNamesOfs[t] : blockTypeNamesOfs[t+1]-1]
}

func (t BlockType) Size() int {
	if int(t) < len(blockTypeDataSize) {
		return blockTypeDataSize[t]
	}
	return 0
}
