// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"fmt"
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

func printFlags() {
	fmt.Printf("AVX2 %t\n", useAVX2)
	fmt.Printf("AVX512-F %t\n", useAVX512_F)
	fmt.Printf("AVX512-DQ %t\n", useAVX512_DQ)
	fmt.Printf("AVX512-IFMA %t\n", useAVX512_IFMA)
	fmt.Printf("AVX512-PF %t\n", useAVX512_PF)
	fmt.Printf("AVX512-ER %t\n", useAVX512_ER)
	fmt.Printf("AVX512-CD %t\n", useAVX512_CD)
	fmt.Printf("AVX512-BW %t\n", useAVX512_BW)
	fmt.Printf("AVX512-VL %t\n", useAVX512_VL)
	fmt.Printf("AVX512-VBMI %t\n", useAVX512_VBMI)
	fmt.Printf("AVX512-BITALG %t\n", useAVX512_BITALG)
	fmt.Printf("AVX512-VPOPCNTDQ %t\n", useAVX512_VPOPCNTDQ)
	fmt.Printf("AVX512-4VNNIW %t\n", useAVX512_4VNNIW)
	fmt.Printf("AVX512-4FMAPS %t\n", useAVX512_4FMAPS)
	fmt.Printf("AVX512-VP2INTERSECT %t\n", useAVX512_VP2INTERSECT)
	fmt.Printf("AVX512-BF16 %t\n", useAVX512_BF16)
}
