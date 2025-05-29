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
	BlockInvalid = types.BlockInvalid
	BlockInt64   = types.BlockInt64
	BlockInt32   = types.BlockInt32
	BlockInt16   = types.BlockInt16
	BlockInt8    = types.BlockInt8
	BlockUint64  = types.BlockUint64
	BlockUint32  = types.BlockUint32
	BlockUint16  = types.BlockUint16
	BlockUint8   = types.BlockUint8
	BlockFloat64 = types.BlockFloat64
	BlockFloat32 = types.BlockFloat32
	BlockBool    = types.BlockBool
	BlockBytes   = types.BlockBytes
	BlockInt128  = types.BlockInt128
	BlockInt256  = types.BlockInt256
)
