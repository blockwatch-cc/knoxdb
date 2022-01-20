// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package util

import (
	"log"

	"golang.org/x/sys/cpu"
)

//go:noescape
func cpuid(eaxArg, ecxArg uint32) (eax, ebx, ecx, edx uint32)

func init() {
	detectAVX()
}

func detectAVX() {
	UseAVX2 = cpu.X86.HasAVX2

	maxID, _, _, _ := cpuid(0, 0)
	if maxID < 7 {
		return
	}

	eax7, ebx7, ecx7, edx7 := cpuid(7, 0)

	/* Intel-defined CPU features, CPUID level 0x00000007:0.ebx, word 5 */
	UseAVX512_F = isSet(16, ebx7)
	UseAVX512_DQ = isSet(17, ebx7)
	UseAVX512_IFMA = isSet(21, ebx7)
	UseAVX512_PF = isSet(26, ebx7)
	UseAVX512_ER = isSet(27, ebx7)
	UseAVX512_CD = isSet(28, ebx7)
	UseAVX512_BW = isSet(30, ebx7)
	UseAVX512_VL = isSet(31, ebx7)

	/* Intel-defined CPU features, CPUID level 0x00000007:0.ecx, word 6 */
	UseAVX512_VBMI = isSet(1, ecx7)
	UseAVX512_BITALG = isSet(12, ecx7)
	UseAVX512_VPOPCNTDQ = isSet(14, ecx7)

	/* Intel-defined CPU features, CPUID level 0x00000007:0.edx, word 9 */
	UseAVX512_4VNNIW = isSet(2, edx7)
	UseAVX512_4FMAPS = isSet(3, edx7)
	UseAVX512_VP2INTERSECT = isSet(8, edx7)

	/* Intel-defined CPU features, CPUID level 0x00000007:1.eax, word 10 */
	UseAVX512_BF16 = isSet(5, eax7)
}

func isSet(bitpos uint, value uint32) bool {
	return value&(1<<bitpos) != 0
}

func DisableAVX2() {
	UseAVX2 = false
}

func DisableAVX512() {
	UseAVX512_F = false
	UseAVX512_DQ = false
	UseAVX512_IFMA = false
	UseAVX512_PF = false
	UseAVX512_ER = false
	UseAVX512_CD = false
	UseAVX512_BW = false
	UseAVX512_VL = false
	UseAVX512_VBMI = false
	UseAVX512_BITALG = false
	UseAVX512_VPOPCNTDQ = false
	UseAVX512_4VNNIW = false
	UseAVX512_4FMAPS = false
	UseAVX512_VP2INTERSECT = false
	UseAVX512_BF16 = false
}

func EnableAVX() {
	detectAVX()
}

var (
	UseAVX2 bool

	UseAVX512_F            bool // AVX-512 Foundation Instructions
	UseAVX512_DQ           bool // AVX-512 Doubleword & Quadword Instrs
	UseAVX512_IFMA         bool // AVX-512 Integer Fused Multiply Add
	UseAVX512_PF           bool // AVX-512 Prefetch Instructions
	UseAVX512_ER           bool // AVX-512 Exponent & Reciprocal Instrs
	UseAVX512_CD           bool // AVX-512 Conflict Detection Instrs
	UseAVX512_BW           bool // AVX-512 Byte and Word Instructions
	UseAVX512_VL           bool // AVX-512 Vector Length Extensions
	UseAVX512_VBMI         bool // AVX-512 Vector Byte Manipulation Instrs
	UseAVX512_BITALG       bool // Support for VPOPCNT[B,W] and VPSHUFBITQMB
	UseAVX512_VPOPCNTDQ    bool // POPCNT for vectors of DW/QW
	UseAVX512_4VNNIW       bool // AVX512 Neural Network Instructions
	UseAVX512_4FMAPS       bool // AVX512 Multiply Accumulation Single Precision
	UseAVX512_VP2INTERSECT bool // VP2INTERSECT{D,Q} insns
	UseAVX512_BF16         bool // AVX512 BFloat16 Instructions
)

func LogCPUFeatures(l *log.Logger) {
	l.Printf("AMD64 CPU detected")
	l.Printf(" AVX2 %t", UseAVX2)
	l.Printf(" AVX512-F %t", UseAVX512_F)
	l.Printf(" AVX512-DQ %t", UseAVX512_DQ)
	l.Printf(" AVX512-IFMA %t", UseAVX512_IFMA)
	l.Printf(" AVX512-PF %t", UseAVX512_PF)
	l.Printf(" AVX512-ER %t", UseAVX512_ER)
	l.Printf(" AVX512-CD %t", UseAVX512_CD)
	l.Printf(" AVX512-BW %t", UseAVX512_BW)
	l.Printf(" AVX512-VL %t", UseAVX512_VL)
	l.Printf(" AVX512-VBMI %t", UseAVX512_VBMI)
	l.Printf(" AVX512-BITALG %t", UseAVX512_BITALG)
	l.Printf(" AVX512-VPOPCNTDQ %t", UseAVX512_VPOPCNTDQ)
	l.Printf(" AVX512-4VNNIW %t", UseAVX512_4VNNIW)
	l.Printf(" AVX512-4FMAPS %t", UseAVX512_4FMAPS)
	l.Printf(" AVX512-VP2INTERSECT %t", UseAVX512_VP2INTERSECT)
	l.Printf(" AVX512-BF16 %t", UseAVX512_BF16)
}
