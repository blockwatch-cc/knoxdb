// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/types"
)

var (
	BlockTypes = [...]block.BlockType{
		types.FieldTypeDatetime:   block.BlockTime,
		types.FieldTypeBoolean:    block.BlockBool,
		types.FieldTypeString:     block.BlockString,
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

	FieldTypes = [...]types.FieldType{
		block.BlockTime:    types.FieldTypeDatetime,
		block.BlockBool:    types.FieldTypeBoolean,
		block.BlockString:  types.FieldTypeString,
		block.BlockBytes:   types.FieldTypeBytes,
		block.BlockInt8:    types.FieldTypeInt8,
		block.BlockInt16:   types.FieldTypeInt16,
		block.BlockInt32:   types.FieldTypeInt32,
		block.BlockInt64:   types.FieldTypeInt64,
		block.BlockInt128:  types.FieldTypeInt128,
		block.BlockInt256:  types.FieldTypeInt256,
		block.BlockUint8:   types.FieldTypeUint8,
		block.BlockUint16:  types.FieldTypeUint16,
		block.BlockUint32:  types.FieldTypeUint32,
		block.BlockUint64:  types.FieldTypeUint64,
		block.BlockFloat32: types.FieldTypeFloat32,
		block.BlockFloat64: types.FieldTypeFloat64,
	}
)
