// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package avx2

import (
	"blockwatch.cc/knoxdb/internal/s8b/generic"
)

var (
	DecodeUint64 = generic.DecodeUint64
	DecodeUint32 = generic.DecodeUint32
	DecodeUint16 = generic.DecodeUint16
	DecodeUint8  = generic.DecodeUint8
	CountValues  = generic.CountValues

	// legacy use only
	// DecodeBytesBigEndian = generic.DecodeBytesBigEndian
	// CountValuesBigEndian = generic.CountValuesBigEndian
)
