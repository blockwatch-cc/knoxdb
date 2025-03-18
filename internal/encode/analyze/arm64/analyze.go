// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !arm64 || appengine || gccgo
// +build !arm64 appengine gccgo

package arm64

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
