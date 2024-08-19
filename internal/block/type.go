// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"encoding/binary"
	"reflect"
	"sync"
)

// Note: uses 5 bit encoding (max 32 values)
type BlockType byte

const (
	BlockTime    BlockType = iota // 0
	BlockInt64                    // 1
	BlockUint64                   // 2
	BlockFloat64                  // 3
	BlockBool                     // 4
	BlockString                   // 5
	BlockBytes                    // 6
	BlockInt32                    // 7
	BlockInt16                    // 8
	BlockInt8                     // 9
	BlockUint32                   // 10
	BlockUint16                   // 11
	BlockUint8                    // 12
	BlockFloat32                  // 13
	BlockInt128                   // 14
	BlockInt256                   // 15
)

var (
	blockPool = &sync.Pool{
		New: func() any { return &Block{} },
	}

	bigEndian = binary.BigEndian

	BlockSz = int(reflect.TypeOf(Block{}).Size())

	blockTypeNames    = "timeint64int32int16int8uint64uint32uint16uint8float64float32boolstringbytesint128int256"
	blockTypeNamesOfs = []int{0, 4, 9, 14, 19, 23, 29, 35, 41, 46, 53, 60, 64, 70, 75, 81, 87}

	blockTypeDataSize = [...]int{
		BlockTime:    8,
		BlockInt64:   8,
		BlockUint64:  8,
		BlockFloat64: 8,
		BlockBool:    1,
		BlockString:  0, // variable
		BlockBytes:   0, // variable
		BlockInt32:   4,
		BlockInt16:   2,
		BlockInt8:    1,
		BlockUint32:  4,
		BlockUint16:  2,
		BlockUint8:   1,
		BlockFloat32: 4,
		BlockInt256:  32,
		BlockInt128:  16,
	}
)

func (t BlockType) IsValid() bool {
	return t >= 0 && t <= BlockInt256
}

func (t BlockType) String() string {
	if !t.IsValid() {
		return "invalid block type"
	}
	return blockTypeNames[blockTypeNamesOfs[t]:blockTypeNamesOfs[t+1]]
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
