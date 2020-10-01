// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants.h"

// func matchFloat64EqualAVX2(src []float64, val float64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y7 = vector data
TEXT ·matchFloat64EqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $32      // slices smaller than 32 byte are handled separately
	JB		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0            // load val into AVX2 reg
	VMOVDQA		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQA		shuffle<>+0x00(SB), Y10    // load shuffle control mask

// works for >= 32 float64 (i.e. 256 bytes of data)
loop_avx2:
	VCMPPD		$0, 0(SI), Y0, Y1          // imm8 = $0 (equal, nosignal)
	VCMPPD		$0, 32(SI), Y0, Y2
	VCMPPD		$0, 64(SI), Y0, Y3
	VCMPPD		$0, 96(SI), Y0, Y4
	VCMPPD		$0, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VCMPPD		$0, 160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VCMPPD		$0, 192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VCMPPD		$0, 224(SI), Y0, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	LEAQ		256(SI), SI
	LEAQ		4(DI), DI
	SUBQ		$32, BX
	CMPQ		BX, $32
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	VMOVSD	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float64
scalar:
	VCOMISD	(SI), X0
	JPS	    scalar_shift    // sets partity flag when either value is NaN
	SETEQ	R10
	ADDL	R10, R9
	ORL	 	R10, AX
scalar_shift:
	SHLL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	SHLL	CX, AX        // fill 32bits by shifting
	BSWAPL	AX            // swap bytes into place for big endian output
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET


// func matchFloat64NotEqualAVX2(src []float64, val float64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y7 = vector data
TEXT ·matchFloat64NotEqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $32      // slices smaller than 32 byte are handled separately
	JB		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0            // load val into AVX2 reg
	VMOVDQA		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQA		shuffle<>+0x00(SB), Y10    // load shuffle control mask

// works for >= 32 float64 (i.e. 256 bytes of data)
loop_avx2:
	VCMPPD		$0x04, 0(SI), Y0, Y1          // imm8 = $0x04 (not equal, nosignal)
	VCMPPD		$0x04, 32(SI), Y0, Y2
	VCMPPD		$0x04, 64(SI), Y0, Y3
	VCMPPD		$0x04, 96(SI), Y0, Y4
	VCMPPD		$0x04, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VCMPPD		$0x04, 160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VCMPPD		$0x04, 192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VCMPPD		$0x04, 224(SI), Y0, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	LEAQ		256(SI), SI
	LEAQ		4(DI), DI
	SUBQ		$32, BX
	CMPQ		BX, $32
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	VMOVSD	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float64
scalar:
	VUCOMISD	(SI), X0    // sets partity flag when either value is NaN
	SETNE	R10
	JNP	    scalar_shift
    MOVQ    $1, R10         // NaN is always not equal
scalar_shift:
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	SHLL	CX, AX        // fill 32bits by shifting
	BSWAPL	AX            // swap bytes into place for big endian output
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func matchFloat64LessThanAVX2(src []float64, val float64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·matchFloat64LessThanAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $32      // slices smaller than 32 byte are handled separately
	JB		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0            // load val into AVX2 reg
	VMOVDQA		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQA		shuffle<>+0x00(SB), Y10    // load shuffle control mask

// works for >= 32 float64 (i.e. 256 bytes of data)
// Note: we switch operand order and use the opposite
// test (GT instead of LT) to save one op per vector
loop_avx2:
	VCMPPD		$0x1e, 0(SI), Y0, Y1          // imm8 = $0x1e (greater than, nosignal)
	VCMPPD		$0x1e, 32(SI), Y0, Y2
	VCMPPD		$0x1e, 64(SI), Y0, Y3
	VCMPPD		$0x1e, 96(SI), Y0, Y4
	VCMPPD		$0x1e, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VCMPPD		$0x1e, 160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VCMPPD		$0x1e, 192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VCMPPD		$0x1e, 224(SI), Y0, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	LEAQ		256(SI), SI
	LEAQ		4(DI), DI
	SUBQ		$32, BX
	CMPQ		BX, $32
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	VMOVSD	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float64
scalar:
	VCMPSD  	$0x1e, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	SHLL		$1, AX
	LEAQ		8(SI), SI
	DECL		BX
	JZ	 		scalar_done
	JMP	 		scalar

scalar_done:
	SHLL	CX, AX        // fill 32bits by shifting
	BSWAPL	AX            // swap bytes into place for big endian output
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func matchFloat64LessThanEqualAVX2(src []float64, val float64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·matchFloat64LessThanEqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $32      // slices smaller than 32 byte are handled separately
	JB		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0            // load val into AVX2 reg
	VMOVDQA		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQA		shuffle<>+0x00(SB), Y10    // load shuffle control mask

// works for >= 32 float64 (i.e. 256 bytes of data)
// Note: we switch operand order and use the opposite
// test (GTE instead of LTE) to save one op per vector
loop_avx2:
	VCMPPD		$0x1d, 0(SI), Y0, Y1          // imm8 = $0x1d (GTE, nosignal)
	VCMPPD		$0x1d, 32(SI), Y0, Y2
	VCMPPD		$0x1d, 64(SI), Y0, Y3
	VCMPPD		$0x1d, 96(SI), Y0, Y4
	VCMPPD		$0x1d, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VCMPPD		$0x1d, 160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VCMPPD		$0x1d, 192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VCMPPD		$0x1d, 224(SI), Y0, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	LEAQ		256(SI), SI
	LEAQ		4(DI), DI
	SUBQ		$32, BX
	CMPQ		BX, $32
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	VMOVSD	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float64
scalar:
	VCMPSD  	$0x1d, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	SHLL		$1, AX
	LEAQ		8(SI), SI
	DECL		BX
	JZ	 		scalar_done
	JMP	 		scalar

scalar_done:
	SHLL	CX, AX        // fill 32bits by shifting
	BSWAPL	AX            // swap bytes into place for big endian output
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET


// func matchFloat64GreaterThanAVX2(src []float64, val float64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·matchFloat64GreaterThanAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $32      // slices smaller than 32 byte are handled separately
	JB		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0            // load val into AVX2 reg
	VMOVDQA		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQA		shuffle<>+0x00(SB), Y10    // load shuffle control mask

// works for >= 32 float64 (i.e. 256 bytes of data)
// Note: we switch operand order and use the opposite
// test (LT instead of GT) to save one op per vector
loop_avx2:
	VCMPPD		$0x11, 0(SI), Y0, Y1          // imm8 = $x11 (LT, nosignal)
	VCMPPD		$0x11, 32(SI), Y0, Y2
	VCMPPD		$0x11, 64(SI), Y0, Y3
	VCMPPD		$0x11, 96(SI), Y0, Y4
	VCMPPD		$0x11, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VCMPPD		$0x11, 160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VCMPPD		$0x11, 192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VCMPPD		$0x11, 224(SI), Y0, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	LEAQ		256(SI), SI
	LEAQ		4(DI), DI
	SUBQ		$32, BX
	CMPQ		BX, $32
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	VMOVSD	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float64
scalar:
	VCMPSD  	$0x11, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	SHLL		$1, AX
	LEAQ		8(SI), SI
	DECL		BX
	JZ	 		scalar_done
	JMP	 		scalar

scalar_done:
	SHLL	CX, AX        // fill 32bits by shifting
	BSWAPL	AX            // swap bytes into place for big endian output
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func matchFloat64GreaterThanEqualAVX2(src []float64, val float64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·matchFloat64GreaterThanEqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $32      // slices smaller than 32 byte are handled separately
	JB		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0            // load val into AVX2 reg
	VMOVDQA		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQA		shuffle<>+0x00(SB), Y10    // load shuffle control mask

// works for >= 32 float64 (i.e. 256 bytes of data)
// Note: we switch operand order and use the opposite
// test (LTE instead of GTE) to save one op per vector
loop_avx2:
	VCMPPD		$0x12, 0(SI), Y0, Y1          // imm8 = $0x12 (LTE, nosignal)
	VCMPPD		$0x12, 32(SI), Y0, Y2
	VCMPPD		$0x12, 64(SI), Y0, Y3
	VCMPPD		$0x12, 96(SI), Y0, Y4
	VCMPPD		$0x12, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VCMPPD		$0x12, 160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VCMPPD		$0x12, 192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VCMPPD		$0x12, 224(SI), Y0, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	LEAQ		256(SI), SI
	LEAQ		4(DI), DI
	SUBQ		$32, BX
	CMPQ		BX, $32
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	VMOVSD	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float64
scalar:
	VCMPSD  	$0x12, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	SHLL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	SHLL	CX, AX        // fill 32bits by shifting
	BSWAPL	AX            // swap bytes into place for big endian output
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func matchFloat64BetweenAVX2(src []float64, a, b float64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   R10 = lower comparison value for scalar
//   R11 = upper comparison value for scalar
//   Y0 = lower comparison value for AVX2
//   Y11 = upper comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
//   Y12-15 = intermediate aggregation
TEXT ·matchFloat64BetweenAVX2(SB), NOSPLIT, $0-72
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+40(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $32      // slices smaller than 32 byte are handled separately
	JB		prep_scalar

prep_avx2:
	VBROADCASTSD a+24(FP), Y0            // load val a into AVX2 reg
	VBROADCASTSD b+32(FP), Y11           // load val b into AVX2 reg
	VMOVDQA		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQA		shuffle<>+0x00(SB), Y10    // load shuffle control mask

// works for >= 32 float64 (i.e. 256 bytes of data)
// Note: we load values into vector registers because we need
// to perform two comparisons and merge their results with AND
// because there is no simple range check formula or instruction
// for float64 vector data
loop_avx2:
	VMOVAPD		0(SI), Y1
	VMOVAPD		32(SI), Y2
	VMOVAPD		64(SI), Y3
	VMOVAPD		96(SI), Y4
	VMOVAPD		128(SI), Y5
	VCMPPD		$0x1d, Y0, Y1, Y12         // imm8 = $0x12 (GTE, nosignal)
	VCMPPD		$0x1d, Y0, Y2, Y13
	VCMPPD		$0x1d, Y0, Y3, Y14
	VCMPPD		$0x1d, Y0, Y4, Y15
	VCMPPD		$0x12, Y11, Y1, Y1         // imm8 = $0x12 (LTE, nosignal)
	VCMPPD		$0x12, Y11, Y2, Y2
	VCMPPD		$0x12, Y11, Y3, Y3
	VCMPPD		$0x12, Y11, Y4, Y4
	VPAND		Y12, Y1, Y1
	VPAND		Y13, Y2, Y2
	VPAND		Y14, Y3, Y3
	VPAND		Y15, Y4, Y4
	VCMPPD		$0x1d, Y0, Y5, Y12
	VCMPPD		$0x12, Y11, Y5, Y5
	VPAND		Y12, Y5, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VMOVAPD		160(SI), Y6
	VCMPPD		$0x1d, Y0, Y6, Y12
	VCMPPD		$0x12, Y11, Y6, Y6
	VPAND		Y12, Y6, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VMOVAPD		192(SI), Y7
	VCMPPD		$0x1d, Y0, Y7, Y13
	VCMPPD		$0x12, Y11, Y7, Y7
	VPAND		Y13, Y7, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VMOVAPD		224(SI), Y8
	VCMPPD		$0x1d, Y0, Y8, Y14
	VCMPPD		$0x12, Y11, Y8, Y8
	VPAND		Y14, Y8, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	LEAQ		256(SI), SI
	LEAQ		4(DI), DI
	SUBQ		$32, BX
	CMPQ		BX, $32
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	VMOVSD	a+24(FP), X0   // load val a for comparison
	VMOVSD	b+32(FP), X1   // load val b for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float64
scalar:
	VMOVSD		(SI), X2
	VCMPSD  	$0x1d, X0, X2, X3
	VCMPSD  	$0x12, X1, X2, X2
	VPAND		X3, X2, X2
	VPMOVMSKB	X2, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	SHLL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	SHLL	CX, AX        // fill 32bits by shifting
	BSWAPL	AX            // swap bytes into place for big endian output
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+64(FP)
	RET


