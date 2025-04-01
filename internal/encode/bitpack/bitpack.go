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
	// single
	Pack     = generic.Pack
	Unpack   = generic.Unpack
	Packer   = generic.Packer
	Unpacker = generic.Unpacker

	// encode
	EncodeInt64 = generic.Encode[int64]
	EncodeInt32 = generic.Encode[int32]
	EncodeInt16 = generic.Encode[int16]
	EncodeInt8  = generic.Encode[int8]

	EncodeUint64 = generic.Encode[uint64]
	EncodeUint32 = generic.Encode[uint32]
	EncodeUint16 = generic.Encode[uint16]
	EncodeUint8  = generic.Encode[uint8]

	// decode
	DecodeInt64 = generic.Decode[int64]
	DecodeInt32 = generic.Decode[int32]
	DecodeInt16 = generic.Decode[int16]
	DecodeInt8  = generic.Decode[int8]

	DecodeUint64 = generic.Decode[uint64]
	DecodeUint32 = generic.Decode[uint32]
	DecodeUint16 = generic.Decode[uint16]
	DecodeUint8  = generic.Decode[uint8]

	// cmp
	Equal        = generic.Equal
	NotEqual     = generic.NotEqual
	Less         = generic.Less
	LessEqual    = generic.LessEqual
	Greater      = generic.Greater
	GreaterEqual = generic.GreaterEqual
	Between      = generic.Between
)

func EstimateMaxSize(width, n int) int {
	return (width*n + 7) / 8
}
