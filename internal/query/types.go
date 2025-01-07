// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/types"
)

type (
	FilterMode = types.FilterMode
	BlockType  = block.BlockType
)

const (
	FilterModeInvalid  = types.FilterModeInvalid  // 0
	FilterModeEqual    = types.FilterModeEqual    // 1
	FilterModeNotEqual = types.FilterModeNotEqual // 2
	FilterModeGt       = types.FilterModeGt       // 3
	FilterModeGe       = types.FilterModeGe       // 4
	FilterModeLt       = types.FilterModeLt       // 5
	FilterModeLe       = types.FilterModeLe       // 6
	FilterModeIn       = types.FilterModeIn       // 7
	FilterModeNotIn    = types.FilterModeNotIn    // 8
	FilterModeRange    = types.FilterModeRange    // 9
	FilterModeRegexp   = types.FilterModeRegexp   // 10
	FilterModeTrue     = types.FilterModeTrue     // 11
	FilterModeFalse    = types.FilterModeFalse    // 12
)

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

var (
	BlockTypes = [...]block.BlockType{
		types.FieldTypeDatetime:   block.BlockTime,
		types.FieldTypeBoolean:    block.BlockBool,
		types.FieldTypeString:     block.BlockBytes,
		types.FieldTypeBytes:      block.BlockBytes,
		types.FieldTypeInt8:       block.BlockInt8,
		types.FieldTypeInt16:      block.BlockInt16,
		types.FieldTypeInt32:      block.BlockInt32,
		types.FieldTypeInt64:      block.BlockInt64,
		types.FieldTypeInt128:     block.BlockInt128,
		types.FieldTypeInt256:     block.BlockInt256,
		types.FieldTypeUint8:      block.BlockUint8,
		types.FieldTypeUint16:     block.BlockUint16,
		types.FieldTypeUint32:     block.BlockUint32,
		types.FieldTypeUint64:     block.BlockUint64,
		types.FieldTypeDecimal32:  block.BlockInt32,
		types.FieldTypeDecimal64:  block.BlockInt64,
		types.FieldTypeDecimal128: block.BlockInt128,
		types.FieldTypeDecimal256: block.BlockInt256,
		types.FieldTypeFloat32:    block.BlockFloat32,
		types.FieldTypeFloat64:    block.BlockFloat64,
	}
)
