// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package util

import "golang.org/x/sys/cpu"

//go:noescape
func cpuid(eaxArg, ecxArg uint32) (eax, ebx, ecx, edx uint32)

func init() {
	detectAVX()
	EnableAVX = detectAVX
	DisableAVX2 = disableAVX2
	DisableAVX512 = disableAVX512
}

func detectAVX() {
	IsAMD64 = true
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

func disableAVX2() {
	UseAVX2 = false
}

func disableAVX512() {
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
