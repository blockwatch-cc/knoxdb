// Copyright (c) 2013 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	UnsafeGetString = util.UnsafeGetString
	UnsafeGetBytes  = util.UnsafeGetBytes
	Min             = util.Min[int]
)

const (
	FieldTypeInvalid    = schema.FieldTypeInvalid
	FieldTypeDatetime   = schema.FieldTypeDatetime
	FieldTypeBoolean    = schema.FieldTypeBoolean
	FieldTypeString     = schema.FieldTypeString
	FieldTypeBytes      = schema.FieldTypeBytes
	FieldTypeInt8       = schema.FieldTypeInt8
	FieldTypeInt16      = schema.FieldTypeInt16
	FieldTypeInt32      = schema.FieldTypeInt32
	FieldTypeInt64      = schema.FieldTypeInt64
	FieldTypeInt128     = schema.FieldTypeInt128
	FieldTypeInt256     = schema.FieldTypeInt256
	FieldTypeUint8      = schema.FieldTypeUint8
	FieldTypeUint16     = schema.FieldTypeUint16
	FieldTypeUint32     = schema.FieldTypeUint32
	FieldTypeUint64     = schema.FieldTypeUint64
	FieldTypeDecimal32  = schema.FieldTypeDecimal32
	FieldTypeDecimal64  = schema.FieldTypeDecimal64
	FieldTypeDecimal128 = schema.FieldTypeDecimal128
	FieldTypeDecimal256 = schema.FieldTypeDecimal256
	FieldTypeFloat32    = schema.FieldTypeFloat32
	FieldTypeFloat64    = schema.FieldTypeFloat64
)
