// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// +build amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants_AVX2.h"

// func cmp_f64_eq_x2(src []float64, val float64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   X0 = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_f64_eq_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD val+24(FP), Y0            // load val into AVX2 reg
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

// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VCMPPD		$0, 0(SI), Y0, Y1          // imm8 = $0 (equal, nosignal)
	VCMPPD		$0, 32(SI), Y0, Y2
	VCMPPD		$0, 64(SI), Y0, Y3
	VCMPPD		$0, 96(SI), Y0, Y4
	VCMPPD	    $0, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0, 160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0, 192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0, 224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPD	    $0, 256(SI), Y0, Y1
	VCMPPD	    $0, 288(SI), Y0, Y2
	VCMPPD	    $0, 320(SI), Y0, Y3
	VCMPPD	    $0, 352(SI), Y0, Y4
	VCMPPD	    $0, 384(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0, 416(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0, 448(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0, 480(SI), Y0, Y8
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
	VCMPPD	    $0, 0(SI), Y0, Y1
	VCMPPD	    $0, 32(SI), Y0, Y2
	VCMPPD	    $0, 64(SI), Y0, Y3
	VCMPPD	    $0, 96(SI), Y0, Y4
	VCMPPD	    $0, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0, 160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0, 192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0, 224(SI), Y0, Y8
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
	VMOVSD	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float64
scalar:
	VCOMISD	(SI), X0
	JPS	    scalar_shift    // sets partity flag when either value is NaN
	SETEQ	R10
	ADDL	R10, R9
	ORL	 	R10, AX
scalar_shift:
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

// func cmp_f64_ne_x2(src []float64, val float64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   X0 = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_f64_ne_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD val+24(FP), Y0            // load val into AVX2 reg
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

// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VCMPPD		$0x04, 0(SI), Y0, Y1          // imm8 = $0x04 (not equal, nosignal)
	VCMPPD		$0x04, 32(SI), Y0, Y2
	VCMPPD		$0x04, 64(SI), Y0, Y3
	VCMPPD		$0x04, 96(SI), Y0, Y4
	VCMPPD	    $0x04, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0x04, 160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0x04, 192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0x04, 224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPD	    $0x04, 256(SI), Y0, Y1
	VCMPPD	    $0x04, 288(SI), Y0, Y2
	VCMPPD	    $0x04, 320(SI), Y0, Y3
	VCMPPD	    $0x04, 352(SI), Y0, Y4
	VCMPPD	    $0x04, 384(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0x04, 416(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0x04, 448(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0x04, 480(SI), Y0, Y8
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
	VCMPPD	    $0x04, 0(SI), Y0, Y1
	VCMPPD	    $0x04, 32(SI), Y0, Y2
	VCMPPD	    $0x04, 64(SI), Y0, Y3
	VCMPPD	    $0x04, 96(SI), Y0, Y4
	VCMPPD	    $0x04, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0x04, 160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0x04, 192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0x04, 224(SI), Y0, Y8
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
	VMOVSD	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
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

// func cmp_f64_lt_x2(src []float64, val float64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   X0 = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_f64_lt_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD 	val+24(FP), Y0                   // load val into AVX2 reg
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

// works for >= 64 float64 (i.e. 512 bytes of data)
// Note: we switch operand order and use the opposite
// test (GT instead of LT) to save one op per vector
loop_big:
	VCMPPD		$0x1e, 0(SI), Y0, Y1          // imm8 = $0x1e (greater than, nosignal)
	VCMPPD		$0x1e, 32(SI), Y0, Y2
	VCMPPD		$0x1e, 64(SI), Y0, Y3
	VCMPPD		$0x1e, 96(SI), Y0, Y4
	VCMPPD	    $0x1e, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0x1e, 160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0x1e, 192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0x1e, 224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPD	    $0x1e, 256(SI), Y0, Y1
	VCMPPD	    $0x1e, 288(SI), Y0, Y2
	VCMPPD	    $0x1e, 320(SI), Y0, Y3
	VCMPPD	    $0x1e, 352(SI), Y0, Y4
	VCMPPD	    $0x1e, 384(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0x1e, 416(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0x1e, 448(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0x1e, 480(SI), Y0, Y8
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
	VCMPPD	    $0x1e, 0(SI), Y0, Y1
	VCMPPD	    $0x1e, 32(SI), Y0, Y2
	VCMPPD	    $0x1e, 64(SI), Y0, Y3
	VCMPPD	    $0x1e, 96(SI), Y0, Y4
	VCMPPD	    $0x1e, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0x1e, 160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0x1e, 192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0x1e, 224(SI), Y0, Y8
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
	VMOVSD	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float64
scalar:
	VCMPSD  	$0x1e, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	RORL		$1, AX
	LEAQ		8(SI), SI
	DECL		BX
	JZ	 		scalar_done
	JMP	 		scalar

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

// func cmp_f64_le_x2(src []float64, val float64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   X0 = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_f64_le_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD 	val+24(FP), Y0                   // load val into AVX2 reg
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

// works for >= 64 float64 (i.e. 512 bytes of data)
// Note: we switch operand order and use the opposite
// test (GTE instead of LTE) to save one op per vector
loop_big:
	VCMPPD		$0x1d, 0(SI), Y0, Y1          // imm8 = $0x1d (GTE, nosignal)
	VCMPPD		$0x1d, 32(SI), Y0, Y2
	VCMPPD		$0x1d, 64(SI), Y0, Y3
	VCMPPD		$0x1d, 96(SI), Y0, Y4
	VCMPPD	    $0x1d, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0x1d, 160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0x1d, 192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0x1d, 224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPD	    $0x1d, 256(SI), Y0, Y1
	VCMPPD	    $0x1d, 288(SI), Y0, Y2
	VCMPPD	    $0x1d, 320(SI), Y0, Y3
	VCMPPD	    $0x1d, 352(SI), Y0, Y4
	VCMPPD	    $0x1d, 384(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0x1d, 416(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0x1d, 448(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0x1d, 480(SI), Y0, Y8
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
	VCMPPD	    $0x1d, 0(SI), Y0, Y1
	VCMPPD	    $0x1d, 32(SI), Y0, Y2
	VCMPPD	    $0x1d, 64(SI), Y0, Y3
	VCMPPD	    $0x1d, 96(SI), Y0, Y4
	VCMPPD	    $0x1d, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0x1d, 160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0x1d, 192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0x1d, 224(SI), Y0, Y8
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
	VMOVSD	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float64
scalar:
	VCMPSD  	$0x1d, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	RORL		$1, AX
	LEAQ		8(SI), SI
	DECL		BX
	JZ	 		scalar_done
	JMP	 		scalar
    
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

// func cmp_f64GreaterThanlAVX2(src []float64, val float64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   X0 = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_f64_gt_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD 	val+24(FP), Y0                   // load val into AVX2 reg
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

// works for >= 64 float64 (i.e. 512 bytes of data)
// Note: we switch operand order and use the opposite
// test (LT instead of GT) to save one op per vector
loop_big:
	VCMPPD		$0x11, 0(SI), Y0, Y1          // imm8 = $x11 (LT, nosignal)
	VCMPPD		$0x11, 32(SI), Y0, Y2
	VCMPPD		$0x11, 64(SI), Y0, Y3
	VCMPPD		$0x11, 96(SI), Y0, Y4
	VCMPPD	    $0x11, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0x11, 160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0x11, 192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0x11, 224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPD	    $0x11, 256(SI), Y0, Y1
	VCMPPD	    $0x11, 288(SI), Y0, Y2
	VCMPPD	    $0x11, 320(SI), Y0, Y3
	VCMPPD	    $0x11, 352(SI), Y0, Y4
	VCMPPD	    $0x11, 384(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0x11, 416(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0x11, 448(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0x11, 480(SI), Y0, Y8
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
	VCMPPD	    $0x11, 0(SI), Y0, Y1
	VCMPPD	    $0x11, 32(SI), Y0, Y2
	VCMPPD	    $0x11, 64(SI), Y0, Y3
	VCMPPD	    $0x11, 96(SI), Y0, Y4
	VCMPPD	    $0x11, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0x11, 160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0x11, 192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0x11, 224(SI), Y0, Y8
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
	VMOVSD	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float64
scalar:
	VCMPSD  	$0x11, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	RORL		$1, AX
	LEAQ		8(SI), SI
	DECL		BX
	JZ	 		scalar_done
	JMP	 		scalar
    
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

// func cmp_f64_ge_x2(src []float64, val float64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   X0 = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_f64_ge_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD 	val+24(FP), Y0                   // load val into AVX2 reg
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

// works for >= 64 float64 (i.e. 512 bytes of data)
// Note: we switch operand order and use the opposite
// test (LTE instead of GTE) to save one op per vector
loop_big:
	VCMPPD		$0x12, 0(SI), Y0, Y1          // imm8 = $0x12 (LTE, nosignal)
	VCMPPD		$0x12, 32(SI), Y0, Y2
	VCMPPD		$0x12, 64(SI), Y0, Y3
	VCMPPD		$0x12, 96(SI), Y0, Y4
	VCMPPD	    $0x12, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0x12, 160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0x12, 192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0x12, 224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPD	    $0x12, 256(SI), Y0, Y1
	VCMPPD	    $0x12, 288(SI), Y0, Y2
	VCMPPD	    $0x12, 320(SI), Y0, Y3
	VCMPPD	    $0x12, 352(SI), Y0, Y4
	VCMPPD	    $0x12, 384(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0x12, 416(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0x12, 448(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0x12, 480(SI), Y0, Y8
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
	VCMPPD	    $0x12, 0(SI), Y0, Y1
	VCMPPD	    $0x12, 32(SI), Y0, Y2
	VCMPPD	    $0x12, 64(SI), Y0, Y3
	VCMPPD	    $0x12, 96(SI), Y0, Y4
	VCMPPD	    $0x12, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VCMPPD	    $0x12, 160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VCMPPD	    $0x12, 192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VCMPPD	    $0x12, 224(SI), Y0, Y8
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
	VMOVSD	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float64
scalar:
	VCMPSD  	$0x12, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	RORL		$1, AX
	LEAQ		8(SI), SI
	DECL		BX
	JZ	 		scalar_done
	JMP	 		scalar

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

// func cmp_f64_bw_x2(src []float64, a, b float64, bits []byte) int64
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
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_f64_bw_x2(SB), NOSPLIT, $0-72
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+40(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled separately
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD a+24(FP), Y0            // load val a into AVX2 reg
	VBROADCASTSD b+32(FP), Y11           // load val b into AVX2 reg
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

// works for >= 64 float64 (i.e. 512 bytes of data)
// Note: we load values into vector registers because we need
// to perform two comparisons and merge their results with AND
// because there is no simple range check formula or instruction
// for float64 vector data
loop_big:
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
	VPACKSSDW	Y1, Y2, Y1
	VMOVAPD		160(SI), Y6
	VCMPPD		$0x1d, Y0, Y6, Y12
	VCMPPD		$0x12, Y11, Y6, Y6
	VPAND		Y12, Y6, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVAPD		192(SI), Y7
	VCMPPD		$0x1d, Y0, Y7, Y13
	VCMPPD		$0x12, Y11, Y7, Y7
	VPAND		Y13, Y7, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVAPD		224(SI), Y8
	VCMPPD		$0x1d, Y0, Y8, Y14
	VCMPPD		$0x12, Y11, Y8, Y8
	VPAND		Y14, Y8, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VMOVAPD		256(SI), Y1
	VMOVAPD		288(SI), Y2
	VMOVAPD		320(SI), Y3
	VMOVAPD		352(SI), Y4
	VMOVAPD		384(SI), Y5
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
	VPACKSSDW	Y1, Y2, Y1
	VMOVAPD		416(SI), Y6
	VCMPPD		$0x1d, Y0, Y6, Y12
	VCMPPD		$0x12, Y11, Y6, Y6
	VPAND		Y12, Y6, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVAPD		448(SI), Y7
	VCMPPD		$0x1d, Y0, Y7, Y13
	VCMPPD		$0x12, Y11, Y7, Y7
	VPAND		Y13, Y7, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVAPD		480(SI), Y8
	VCMPPD		$0x1d, Y0, Y8, Y14
	VCMPPD		$0x12, Y11, Y8, Y8
	VPAND		Y14, Y8, Y8
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
	VPACKSSDW	Y1, Y2, Y1
	VMOVAPD		160(SI), Y6
	VCMPPD		$0x1d, Y0, Y6, Y12
	VCMPPD		$0x12, Y11, Y6, Y6
	VPAND		Y12, Y6, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVAPD		192(SI), Y7
	VCMPPD		$0x1d, Y0, Y7, Y13
	VCMPPD		$0x12, Y11, Y7, Y7
	VPAND		Y13, Y7, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVAPD		224(SI), Y8
	VCMPPD		$0x1d, Y0, Y8, Y14
	VCMPPD		$0x12, Y11, Y8, Y8
	VPAND		Y14, Y8, Y8
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
	VMOVSD	a+24(FP), X0   // load val a for comparison
	VMOVSD	b+32(FP), X1   // load val b for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
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
