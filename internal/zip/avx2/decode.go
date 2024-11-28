// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package avx2

import "blockwatch.cc/knoxdb/internal/zip/generic"

// Go package exports
var (
	// not implemented yet
	// ZzDeltaEncodeUint64 = generic.ZzDeltaEncodeUint64
	// ZzDeltaEncodeUint32 = generic.ZzDeltaEncodeUint32
	// ZzDeltaEncodeUint16 = generic.ZzDeltaEncodeUint16
	// ZzDeltaEncodeUint8  = generic.ZzDeltaEncodeUint8

	ZzDeltaDecodeInt64  = generic.ZzDeltaDecodeInt64
	ZzDeltaDecodeInt32  = generic.ZzDeltaDecodeInt32
	ZzDeltaDecodeInt16  = generic.ZzDeltaDecodeInt16
	ZzDeltaDecodeInt8   = generic.ZzDeltaDecodeInt8
	ZzDeltaDecodeUint64 = generic.ZzDeltaDecodeUint64
	ZzDeltaDecodeTime   = generic.ZzDeltaDecodeTime
	DeltaDecodeTime     = generic.DeltaDecodeTime
)
