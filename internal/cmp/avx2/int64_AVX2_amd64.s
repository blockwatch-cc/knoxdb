// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !appengine && gc && !purego && !noasm

#include "textflag.h"
#include "constants_AVX2.h"

// func cmp_i64_eq_x2(src []int64, val int64, bits []byte) int64
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
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_i64_eq_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPBROADCASTQ val+24(FP), Y0            // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10    // load shuffle control mask
	CMPQ	BX, $63      // slices smaller than 64 byte are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_big:
	VPCMPEQQ	0(SI), Y0, Y1
	VPCMPEQQ	32(SI), Y0, Y2
	VPCMPEQQ	64(SI), Y0, Y3
	VPCMPEQQ	96(SI), Y0, Y4
	VPCMPEQQ	128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VPCMPEQQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPEQQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPEQQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPEQQ	256(SI), Y0, Y1
	VPCMPEQQ	288(SI), Y0, Y2
	VPCMPEQQ	320(SI), Y0, Y3
	VPCMPEQQ	352(SI), Y0, Y4
	VPCMPEQQ	384(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VPCMPEQQ	416(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPEQQ	448(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPEQQ	480(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32,DX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VPCMPEQQ	0(SI), Y0, Y1
	VPCMPEQQ	32(SI), Y0, Y2
	VPCMPEQQ	64(SI), Y0, Y3
	VPCMPEQQ	96(SI), Y0, Y4
	VPCMPEQQ	128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VPCMPEQQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPEQQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPEQQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$4, DI
    SUBQ        $32, BX

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETEQ	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
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

// func cmp_i64_ne_x2(src []int64, val int64, bits []byte) int64
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
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_i64_ne_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPBROADCASTQ val+24(FP), Y0            // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10    // load shuffle control mask
	CMPQ	BX, $63      // slices smaller than 64 byte are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_big:
	VPCMPEQQ	0(SI), Y0, Y1
	VPCMPEQQ	32(SI), Y0, Y2
	VPCMPEQQ	64(SI), Y0, Y3
	VPCMPEQQ	96(SI), Y0, Y4
	VPCMPEQQ	128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VPCMPEQQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPEQQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPEQQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPEQQ	256(SI), Y0, Y1
	VPCMPEQQ	288(SI), Y0, Y2
	VPCMPEQQ	320(SI), Y0, Y3
	VPCMPEQQ	352(SI), Y0, Y4
	VPCMPEQQ	384(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VPCMPEQQ	416(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPEQQ	448(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPEQQ	480(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32,DX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VPCMPEQQ	0(SI), Y0, Y1
	VPCMPEQQ	32(SI), Y0, Y2
	VPCMPEQQ	64(SI), Y0, Y3
	VPCMPEQQ	96(SI), Y0, Y4
	VPCMPEQQ	128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VPCMPEQQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPEQQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPEQQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    NOTL        AX
	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$4, DI
    SUBQ        $32, BX

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETNE	R10
	ADDL	R10, R9
	ORL 	R10, AX
	RORL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
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

// func cmp_i64_lt_x2(src []int64, val int64, bits []byte) int64
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
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_i64_lt_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPBROADCASTQ 	val+24(FP), Y0                  // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9            // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $63      // slices smaller than 64 byte are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_big:
	VPCMPGTQ	0(SI), Y0, Y1   // check using GT with switched operands
	VPCMPGTQ	32(SI), Y0, Y2
	VPCMPGTQ	64(SI), Y0, Y3
	VPCMPGTQ	96(SI), Y0, Y4
	VPCMPGTQ	128(SI), Y0, Y5
    VPACKSSDW	Y1, Y2, Y1
	VPCMPGTQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
  	VPCMPGTQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPGTQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPGTQ	256(SI), Y0, Y1     
	VPCMPGTQ	288(SI), Y0, Y2
	VPCMPGTQ	320(SI), Y0, Y3
	VPCMPGTQ	352(SI), Y0, Y4
	VPCMPGTQ	384(SI), Y0, Y5
    VPACKSSDW	Y1, Y2, Y1
	VPCMPGTQ	416(SI), Y0, Y6     
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPGTQ	448(SI), Y0, Y7     
	VPACKSSDW	Y5, Y6, Y5
	VPCMPGTQ	480(SI), Y0, Y8     
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32,DX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
    VPCMPGTQ	0(SI), Y0, Y1   // check using GT with switched operands
	VPCMPGTQ	32(SI), Y0, Y2
	VPCMPGTQ	64(SI), Y0, Y3
	VPCMPGTQ	96(SI), Y0, Y4
	VPCMPGTQ	128(SI), Y0, Y5
    VPACKSSDW	Y1, Y2, Y1
	VPCMPGTQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPGTQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPGTQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$4, DI
    SUBQ        $32, BX

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETLT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
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

// func cmp_i64_le_x2(src []int64, val int64, bits []byte) int64
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
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_i64_le_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPBROADCASTQ 	val+24(FP), Y0                   // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10    // load shuffle control mask
	CMPQ	BX, $63      // slices smaller than 64 byte are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_big:
	VMOVDQU		0(SI), Y1       // load values (necessary to switch operands)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
	VPCMPGTQ	Y0, Y1, Y1     // signed compare
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		160(SI), Y6     
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		192(SI), Y7     
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		224(SI), Y8     
	VPCMPGTQ	Y0, Y8, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VMOVDQU		256(SI), Y1       // load values (necessary to switch operands)
	VMOVDQU		288(SI), Y2
	VMOVDQU		320(SI), Y3
	VMOVDQU		352(SI), Y4
	VMOVDQU		384(SI), Y5
	VPCMPGTQ	Y0, Y1, Y1     // signed compare
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		416(SI), Y6     
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		448(SI), Y7     
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		480(SI), Y8     
	VPCMPGTQ	Y0, Y8, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32,DX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU		0(SI), Y1       // load values (necessary to switch operands)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
	VPCMPGTQ	Y0, Y1, Y1     // signed compare
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		160(SI), Y6     
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		192(SI), Y7     
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		224(SI), Y8     
	VPCMPGTQ	Y0, Y8, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    NOTL        AX
	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$4, DI
    SUBQ        $32, BX

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETLE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar
    
scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
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

// func cmp_i64_ge_x2(src []int64, val int64, bits []byte) int64
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
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_i64_ge_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPBROADCASTQ 	val+24(FP), Y0                  // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9            // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $63      // slices smaller than 64 byte are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_big:
	VPCMPGTQ	0(SI), Y0, Y1   // check using GT with switched operands
	VPCMPGTQ	32(SI), Y0, Y2
	VPCMPGTQ	64(SI), Y0, Y3
	VPCMPGTQ	96(SI), Y0, Y4
	VPCMPGTQ	128(SI), Y0, Y5
    VPACKSSDW	Y1, Y2, Y1
	VPCMPGTQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
  	VPCMPGTQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPGTQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPGTQ	256(SI), Y0, Y1      // load values (necessary to flip sign bit)
	VPCMPGTQ	288(SI), Y0, Y2
	VPCMPGTQ	320(SI), Y0, Y3
	VPCMPGTQ	352(SI), Y0, Y4
	VPCMPGTQ	384(SI), Y0, Y5
    VPACKSSDW	Y1, Y2, Y1
	VPCMPGTQ	416(SI), Y0, Y6     
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPGTQ	448(SI), Y0, Y7     
	VPACKSSDW	Y5, Y6, Y5
	VPCMPGTQ	480(SI), Y0, Y8     
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32,DX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
    VPCMPGTQ	0(SI), Y0, Y1   // check using GT with switched operands
	VPCMPGTQ	32(SI), Y0, Y2
	VPCMPGTQ	64(SI), Y0, Y3
	VPCMPGTQ	96(SI), Y0, Y4
	VPCMPGTQ	128(SI), Y0, Y5
    VPACKSSDW	Y1, Y2, Y1
	VPCMPGTQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPGTQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPGTQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    NOTL        AX
	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$4, DI
    SUBQ        $32, BX

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETGE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
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

// func cmp_i64_gt_x2(src []int64, val int64, bits []byte) int64
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
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_i64_gt_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPBROADCASTQ 	val+24(FP), Y0                   // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10    // load shuffle control mask
	CMPQ	BX, $63      // slices smaller than 64 byte are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_big:
	VMOVDQU		0(SI), Y1       // load values (necessary to switch operands)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
	VPCMPGTQ	Y0, Y1, Y1     // signed compare
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		160(SI), Y6     
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		192(SI), Y7     
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		224(SI), Y8     
	VPCMPGTQ	Y0, Y8, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VMOVDQU		256(SI), Y1       // load values (necessary to switch operands)
	VMOVDQU		288(SI), Y2
	VMOVDQU		320(SI), Y3
	VMOVDQU		352(SI), Y4
	VMOVDQU		384(SI), Y5
	VPCMPGTQ	Y0, Y1, Y1     // signed compare
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		416(SI), Y6     
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		448(SI), Y7     
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		480(SI), Y8     
	VPCMPGTQ	Y0, Y8, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32,DX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU		0(SI), Y1       // load values (necessary to switch operands)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
	VPCMPGTQ	Y0, Y1, Y1     // signed compare
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		160(SI), Y6     
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		192(SI), Y7     
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		224(SI), Y8     
	VPCMPGTQ	Y0, Y8, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$4, DI
    SUBQ        $32, BX

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETGT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar
    
scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
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

// func cmp_i64_bw_x2(src []int64, a, b int64, bits []byte) int64
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
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_i64_bw_x2(SB), NOSPLIT, $0-72
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+40(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled separately
	JBE		prep_scalar

// works for >= 32 int64 (i.e. 256 bytes of data)
// check is using GT with switched operands and add
// the diff method to avoid jumps:
// 	diff := b - a + 1
//  v-a < diff
prep_avx:
	VPCMPEQQ		Y11, Y11, Y11                   // create 0x8000.. mask
	VPSLLQ			$63, Y11, Y11                   // create 0x8000.. mask
	VPCMPEQQ		Y13, Y13, Y13                   // create 1 for adding
	VPSRLQ			$63, Y13, Y13
	VPBROADCASTQ 	a+24(FP), Y12                   // load val a into AVX2 reg
	VPBROADCASTQ 	b+32(FP), Y0                    // load val b into AVX2 reg
	VPSUBQ			Y12, Y0, Y0                     // compute diff
	VPADDQ			Y13, Y0, Y0
	VPXOR			Y11, Y0, Y0                     // flip sign bit
	VMOVDQU			crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU			shuffle64<>+0x00(SB), Y10       // load shuffle control mask

	CMPQ	BX, $63      // slices smaller than 64 byte are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_big:
	VMOVDQU		0(SI), Y1      // load values (necessary to flip sign bit)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
	VPSUBQ		Y12, Y1, Y1
	VPSUBQ		Y12, Y2, Y2
	VPSUBQ		Y12, Y3, Y3
	VPSUBQ		Y12, Y4, Y4
	VPSUBQ		Y12, Y5, Y5
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPXOR		Y11, Y5, Y5
	VPCMPGTQ	Y1, Y0, Y1     // signed compare
	VPCMPGTQ	Y2, Y0, Y2
	VPCMPGTQ	Y3, Y0, Y3
	VPCMPGTQ	Y4, Y0, Y4
	VPCMPGTQ	Y5, Y0, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		160(SI), Y6     
	VPSUBQ		Y12, Y6, Y6
	VPXOR		Y11, Y6, Y6
	VPCMPGTQ	Y6, Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		192(SI), Y7     
	VPSUBQ		Y12, Y7, Y7
	VPXOR		Y11, Y7, Y7
	VPCMPGTQ	Y7, Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		224(SI), Y8     
	VPSUBQ		Y12, Y8, Y8
	VPXOR		Y11, Y8, Y8
	VPCMPGTQ	Y8, Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VMOVDQU		256(SI), Y1      // load values (necessary to flip sign bit)
	VMOVDQU		288(SI), Y2
	VMOVDQU		320(SI), Y3
	VMOVDQU		352(SI), Y4
	VMOVDQU		384(SI), Y5
	VPSUBQ		Y12, Y1, Y1
	VPSUBQ		Y12, Y2, Y2
	VPSUBQ		Y12, Y3, Y3
	VPSUBQ		Y12, Y4, Y4
	VPSUBQ		Y12, Y5, Y5
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPXOR		Y11, Y5, Y5
	VPCMPGTQ	Y1, Y0, Y1     // signed compare
	VPCMPGTQ	Y2, Y0, Y2
	VPCMPGTQ	Y3, Y0, Y3
	VPCMPGTQ	Y4, Y0, Y4
	VPCMPGTQ	Y5, Y0, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		416(SI), Y6     
	VPSUBQ		Y12, Y6, Y6
	VPXOR		Y11, Y6, Y6
	VPCMPGTQ	Y6, Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		448(SI), Y7     
	VPSUBQ		Y12, Y7, Y7
	VPXOR		Y11, Y7, Y7
	VPCMPGTQ	Y7, Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		480(SI), Y8     
	VPSUBQ		Y12, Y8, Y8
	VPXOR		Y11, Y8, Y8
	VPCMPGTQ	Y8, Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32,DX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU		0(SI), Y1      // load values (necessary to flip sign bit)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
	VPSUBQ		Y12, Y1, Y1
	VPSUBQ		Y12, Y2, Y2
	VPSUBQ		Y12, Y3, Y3
	VPSUBQ		Y12, Y4, Y4
	VPSUBQ		Y12, Y5, Y5
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPXOR		Y11, Y5, Y5
	VPCMPGTQ	Y1, Y0, Y1     // signed compare
	VPCMPGTQ	Y2, Y0, Y2
	VPCMPGTQ	Y3, Y0, Y3
	VPCMPGTQ	Y4, Y0, Y4
	VPCMPGTQ	Y5, Y0, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		160(SI), Y6     
	VPSUBQ		Y12, Y6, Y6
	VPXOR		Y11, Y6, Y6
	VPCMPGTQ	Y6, Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		192(SI), Y7     
	VPSUBQ		Y12, Y7, Y7
	VPXOR		Y11, Y7, Y7
	VPCMPGTQ	Y7, Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		224(SI), Y8     
	VPSUBQ		Y12, Y8, Y8
	VPXOR		Y11, Y8, Y8
	VPCMPGTQ	Y8, Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$4, DI
    SUBQ        $32, BX

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	a+24(FP), R13   // load val a
	MOVQ	b+32(FP), DX    // load val b
	SUBQ	R13, DX
	INCQ	DX
	MOVQ    $1, R12          // create 0x80... mask
	SHLQ    $63, R12
	XORQ    R12, DX          // flip sign bit
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	SUBQ	R13, R8          // v - a < diff
	XORQ    R12, R8          // flip sign bit
	CMPQ	R8, DX
	SETLT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
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
