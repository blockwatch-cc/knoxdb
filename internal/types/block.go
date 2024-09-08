// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

// Note: uses 5 bit encoding (max 32 values)
type BlockType byte

const (
	BlockTime    BlockType = iota // 0
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
	BlockString                   // 12
	BlockBytes                    // 13
	BlockInt128                   // 14
	BlockInt256                   // 15
)

var (
	blockTypeNames    = "time_int64_int32_int16_int8_uint64_uint32_uint16_uint8_float64_float32_bool_string_bytes_int128_int256"
	blockTypeNamesOfs = []int{0, 5, 11, 17, 23, 28, 35, 42, 49, 55, 63, 71, 76, 83, 89, 96, 103}
)

func (t BlockType) IsValid() bool {
	return t >= 0 && t <= BlockInt256
}

func (t BlockType) String() string {
	if !t.IsValid() {
		return "invalid block type"
	}
	return blockTypeNames[blockTypeNamesOfs[t] : blockTypeNamesOfs[t+1]-1]
}

func (t BlockType) IsInt() bool {
	switch t {
	case BlockInt64, BlockInt32, BlockInt16, BlockInt8,
		BlockUint64, BlockUint32, BlockUint16, BlockUint8:
		return true
	default:
		return false
	}
}

func (t BlockType) IsFloat() bool {
	switch t {
	case BlockFloat32, BlockFloat64:
		return true
	default:
		return false
	}
}

func (t BlockType) IsSigned() bool {
	switch t {
	case BlockInt64, BlockInt32, BlockInt16, BlockInt8:
		return true
	default:
		return false
	}
}

func (t BlockType) IsUnsigned() bool {
	switch t {
	case BlockUint64, BlockUint32, BlockUint16, BlockUint8:
		return true
	default:
		return false
	}
}
