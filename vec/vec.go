// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"log"
)

var (
	useAVX2 bool

	useAVX512_F            bool // AVX-512 Foundation Instructions
	useAVX512_DQ           bool // AVX-512 Doubleword & Quadword Instrs
	useAVX512_IFMA         bool // AVX-512 Integer Fused Multiply Add
	useAVX512_PF           bool // AVX-512 Prefetch Instructions
	useAVX512_ER           bool // AVX-512 Exponent & Reciprocal Instrs
	useAVX512_CD           bool // AVX-512 Conflict Detection Instrs
	useAVX512_BW           bool // AVX-512 Byte and Word Instructions
	useAVX512_VL           bool // AVX-512 Vector Length Extensions
	useAVX512_VBMI         bool // AVX-512 Vector Byte Manipulation Instrs
	useAVX512_BITALG       bool // Support for VPOPCNT[B,W] and VPSHUFBITQMB
	useAVX512_VPOPCNTDQ    bool // POPCNT for vectors of DW/QW
	useAVX512_4VNNIW       bool // AVX512 Neural Network Instructions
	useAVX512_4FMAPS       bool // AVX512 Multiply Accumulation Single Precision
	useAVX512_VP2INTERSECT bool // VP2INTERSECT{D,Q} insns
	useAVX512_BF16         bool // AVX512 BFloat16 Instructions
)

func LogAVXFeatures(l *log.Logger) {
	l.Printf("AVX2 %t", useAVX2)
	l.Printf("AVX512-F %t", useAVX512_F)
	l.Printf("AVX512-DQ %t", useAVX512_DQ)
	l.Printf("AVX512-IFMA %t", useAVX512_IFMA)
	l.Printf("AVX512-PF %t", useAVX512_PF)
	l.Printf("AVX512-ER %t", useAVX512_ER)
	l.Printf("AVX512-CD %t", useAVX512_CD)
	l.Printf("AVX512-BW %t", useAVX512_BW)
	l.Printf("AVX512-VL %t", useAVX512_VL)
	l.Printf("AVX512-VBMI %t", useAVX512_VBMI)
	l.Printf("AVX512-BITALG %t", useAVX512_BITALG)
	l.Printf("AVX512-VPOPCNTDQ %t", useAVX512_VPOPCNTDQ)
	l.Printf("AVX512-4VNNIW %t", useAVX512_4VNNIW)
	l.Printf("AVX512-4FMAPS %t", useAVX512_4FMAPS)
	l.Printf("AVX512-VP2INTERSECT %t", useAVX512_VP2INTERSECT)
	l.Printf("AVX512-BF16 %t", useAVX512_BF16)
}
