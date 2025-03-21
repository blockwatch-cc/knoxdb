// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package analyze

import (
	"blockwatch.cc/knoxdb/internal/encode/analyze/avx2"
	"blockwatch.cc/knoxdb/internal/encode/analyze/generic"
	"blockwatch.cc/knoxdb/pkg/util"
	"golang.org/x/sys/cpu"
)

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

func init() {
	switch {
	case util.UseAVX2:
		AnalyzeInt64 = avx2.AnalyzeInt64
		AnalyzeInt32 = avx2.AnalyzeInt32
		AnalyzeInt16 = avx2.AnalyzeInt16
		AnalyzeInt8 = avx2.AnalyzeInt8
		AnalyzeUint64 = avx2.AnalyzeUint64
		AnalyzeUint32 = avx2.AnalyzeUint32
		AnalyzeUint16 = avx2.AnalyzeUint16
		AnalyzeUint8 = avx2.AnalyzeUint8
	case cpu.ARM64.HasASIMD:
		// AnalyzeInt64 = arm64.AnalyzeInt64
		// 	AnalyzeInt32 = arm64.AnalyzeInt32
		// 	AnalyzeInt16 = arm64.AnalyzeInt16
		// 	AnalyzeInt8 = arm64.AnalyzeInt8
		// 	AnalyzeUint64 = arm64.AnalyzeUint64
		// 	AnalyzeUint32 = arm64.AnalyzeUint32
		// 	AnalyzeUint16 = arm64.AnalyzeUint16
		// 	AnalyzeUint8 = arm64.AnalyzeUint8
	}
}
