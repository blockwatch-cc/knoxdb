// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitpack

import (
	"blockwatch.cc/knoxdb/internal/encode/bitpack/generic"
)

type (
	PackFunc   = generic.PackFunc
	UnpackFunc = generic.UnpackFunc
)

var (
	Pack     = generic.Pack
	Unpack   = generic.Unpack
	Packer   = generic.Packer
	Unpacker = generic.Unpacker

	EncodeInt64 = generic.Encode[int64]
	EncodeInt32 = generic.Encode[int32]
	EncodeInt16 = generic.Encode[int16]
	EncodeInt8  = generic.Encode[int8]

	EncodeUint64 = generic.Encode[uint64]
	EncodeUint32 = generic.Encode[uint32]
	EncodeUint16 = generic.Encode[uint16]
	EncodeUint8  = generic.Encode[uint8]

	// Decode   = generic.Decode

	Equal        = generic.Equal
	NotEqual     = generic.NotEqual
	Less         = generic.Less
	LessEqual    = generic.LessEqual
	Greater      = generic.Greater
	GreaterEqual = generic.GreaterEqual
	Between      = generic.Between
)
