// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

package vec

import "golang.org/x/sys/cpu"

func init() {
	detectAVX()
}

func detectAVX() {
	useAVX2 = cpu.X86.HasAVX2

	maxID, _, _, _ := cpuid(0, 0)
	if maxID < 7 {
		return
	}

	eax7, ebx7, ecx7, edx7 := cpuid(7, 0)

	/* Intel-defined CPU features, CPUID level 0x00000007:0.ebx, word 5 */
	useAVX512_F = isSet(16, ebx7)
	useAVX512_DQ = isSet(17, ebx7)
	useAVX512_IFMA = isSet(21, ebx7)
	useAVX512_PF = isSet(26, ebx7)
	useAVX512_ER = isSet(27, ebx7)
	useAVX512_CD = isSet(28, ebx7)
	useAVX512_BW = isSet(30, ebx7)
	useAVX512_VL = isSet(31, ebx7)

	/* Intel-defined CPU features, CPUID level 0x00000007:0.ecx, word 6 */
	useAVX512_VBMI = isSet(1, ecx7)
	useAVX512_BITALG = isSet(12, ecx7)
	useAVX512_VPOPCNTDQ = isSet(14, ecx7)

	/* Intel-defined CPU features, CPUID level 0x00000007:0.edx, word 9 */
	useAVX512_4VNNIW = isSet(2, edx7)
	useAVX512_4FMAPS = isSet(3, edx7)
	useAVX512_VP2INTERSECT = isSet(8, edx7)

	/* Intel-defined CPU features, CPUID level 0x00000007:1.eax, word 10 */
	useAVX512_BF16 = isSet(5, eax7)
}

//go:noescape
func cpuid(eaxArg, ecxArg uint32) (eax, ebx, ecx, edx uint32)

func isSet(bitpos uint, value uint32) bool {
	return value&(1<<bitpos) != 0
}

func DisableAVX2() {
	useAVX2 = false
}

func DisableAVX512() {
	useAVX512_F = false
	useAVX512_DQ = false
	useAVX512_IFMA = false
	useAVX512_PF = false
	useAVX512_ER = false
	useAVX512_CD = false
	useAVX512_BW = false
	useAVX512_VL = false
	useAVX512_VBMI = false
	useAVX512_BITALG = false
	useAVX512_VPOPCNTDQ = false
	useAVX512_4VNNIW = false
	useAVX512_4FMAPS = false
	useAVX512_VP2INTERSECT = false
	useAVX512_BF16 = false
}

func EnableAVX() {
	detectAVX()
}
