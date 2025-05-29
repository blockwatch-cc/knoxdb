// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"blockwatch.cc/knoxdb/internal/types"
)

var (
	BlockTypes = types.BlockTypes

	FieldTypes = [...]types.FieldType{
		types.BlockBool:    types.FieldTypeBoolean,
		types.BlockBytes:   types.FieldTypeBytes,
		types.BlockInt8:    types.FieldTypeInt8,
		types.BlockInt16:   types.FieldTypeInt16,
		types.BlockInt32:   types.FieldTypeInt32,
		types.BlockInt64:   types.FieldTypeInt64,
		types.BlockInt128:  types.FieldTypeInt128,
		types.BlockInt256:  types.FieldTypeInt256,
		types.BlockUint8:   types.FieldTypeUint8,
		types.BlockUint16:  types.FieldTypeUint16,
		types.BlockUint32:  types.FieldTypeUint32,
		types.BlockUint64:  types.FieldTypeUint64,
		types.BlockFloat32: types.FieldTypeFloat32,
		types.BlockFloat64: types.FieldTypeFloat64,
	}
)
