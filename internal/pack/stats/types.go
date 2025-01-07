// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"encoding/binary"
	"reflect"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
)

var BE = binary.BigEndian

var (
	szStatsIndex  = int(reflect.TypeOf(StatsIndex{}).Size())
	szPackStats   = int(reflect.TypeOf(PackStats{}).Size())
	szBlockStats  = int(reflect.TypeOf(BlockStats{}).Size())
	szBloomFilter = int(reflect.TypeOf(bloom.Filter{}).Size())
	szBitset      = int(reflect.TypeOf(bitset.Bitset{}).Size())
)

type BlockType = block.BlockType

const (
	BlockTime    = block.BlockTime
	BlockInt64   = block.BlockInt64
	BlockInt32   = block.BlockInt32
	BlockInt16   = block.BlockInt16
	BlockInt8    = block.BlockInt8
	BlockUint64  = block.BlockUint64
	BlockUint32  = block.BlockUint32
	BlockUint16  = block.BlockUint16
	BlockUint8   = block.BlockUint8
	BlockFloat64 = block.BlockFloat64
	BlockFloat32 = block.BlockFloat32
	BlockBool    = block.BlockBool
	BlockBytes   = block.BlockBytes
	BlockInt128  = block.BlockInt128
	BlockInt256  = block.BlockInt256
)
