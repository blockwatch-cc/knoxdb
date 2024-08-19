// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package s8b

import (
	"blockwatch.cc/knoxdb/internal/s8b/avx2"
	"blockwatch.cc/knoxdb/internal/s8b/avx512"
	"blockwatch.cc/knoxdb/internal/s8b/generic"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	EncodeUint64 = generic.EncodeUint64
	DecodeUint64 = generic.DecodeUint64
	DecodeUint32 = generic.DecodeUint32
	DecodeUint16 = generic.DecodeUint16
	DecodeUint8  = generic.DecodeUint8
	CountValues  = generic.CountValues

	MaxValue   = uint64((1 << 60) - 1)
	MaxValue32 = uint64((1 << 30) - 1)
	MaxValue16 = uint64((1 << 15) - 1)
	NewEncoder = generic.NewEncoder
)

type (
	// test use only
	Encoder = generic.Encoder
	Decoder = generic.Decoder
)

func init() {
	if util.UseAVX2 {
		DecodeUint64 = avx2.DecodeUint64
		DecodeUint32 = avx2.DecodeUint32
		DecodeUint16 = avx2.DecodeUint16
		DecodeUint8 = avx2.DecodeUint8
		CountValues = avx2.CountValues
	}
	if util.UseAVX512_F {
		DecodeUint64 = avx512.DecodeUint64
	}
}
