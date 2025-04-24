// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64
// +build !amd64

package avx2

import (
	"blockwatch.cc/knoxdb/internal/encode/s8b/generic"
)

var (
	DecodeUint64 = generic.Decode[uint64]
	DecodeUint32 = generic.Decode[uint32]
	DecodeUint16 = generic.Decode[uint16]
	DecodeUint8  = generic.Decode[uint8]
	DecodeInt64  = generic.Decode[int64]
	DecodeInt32  = generic.Decode[int32]
	DecodeInt16  = generic.Decode[int16]
	DecodeInt8   = generic.Decode[int8]
	CountValues  = generic.CountValues
)
