// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package avx2

import "blockwatch.cc/knoxdb/internal/encode/analyze/generic"

// Go package exports
var (
	AnalyzeInt64 = generic.Analyze[int64]
	AnalyzeInt32 = generic.Analyze[int32]
	AnalyzeInt16 = generic.Analyze[int16]
	AnalyzeInt8  = generic.Analyze[int8]

	AnalyzeUint64 = generic.Analyze[uint64]
	AnalyzeUint32 = generic.Analyze[uint32]
	AnalyzeUint16 = generic.Analyze[uint16]
	AnalyzeUint8  = generic.Analyze[uint8]
)
