// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants_AVX2.h"

// func cmp_u16_eq_x2(src []uint16, val uint16, bits []byte) int64
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
TEXT ·cmp_u16_eq_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPBROADCASTW val+24(FP), Y0            // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU		shuffle16<>+0x00(SB), Y10    // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff00, CX     // number of values processed in big blocks
    ANDQ    $0xff, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_big:
	VPCMPEQW	0(SI), Y0, Y1
	VPCMPEQW	32(SI), Y0, Y2
	VPCMPEQW	64(SI), Y0, Y3
	VPCMPEQW	96(SI), Y0, Y4

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y3, Y4, Y3
  	VPERMD		Y3, Y9, Y3
	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, (DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPCMPEQW	128(SI), Y0, Y5
	VPCMPEQW	160(SI), Y0, Y6
	VPCMPEQW	192(SI), Y0, Y7
	VPCMPEQW	224(SI), Y0, Y8

	VPACKSSWB	Y5, Y6, Y5
  	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5
	VPMOVMSKB	Y5, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y7, Y8, Y7
  	VPERMD		Y7, Y9, Y7
	VPSHUFB		Y10, Y7, Y7
	VPMOVMSKB	Y7, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 8(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPCMPEQW	256(SI), Y0, Y1
	VPCMPEQW	288(SI), Y0, Y2
	VPCMPEQW	320(SI), Y0, Y3
	VPCMPEQW	352(SI), Y0, Y4

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y3, Y4, Y3
  	VPERMD		Y3, Y9, Y3
	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 16(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VPCMPEQW	384(SI), Y0, Y5
	VPCMPEQW	416(SI), Y0, Y6
	VPCMPEQW	448(SI), Y0, Y7
	VPCMPEQW	480(SI), Y0, Y8

	VPACKSSWB	Y5, Y6, Y5
  	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5
	VPMOVMSKB	Y5, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y7, Y8, Y7
  	VPERMD		Y7, Y9, Y7
	VPSHUFB		Y10, Y7, Y7
	VPMOVMSKB	Y7, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 24(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$32, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VPCMPEQW	0(SI), Y0, Y1
	VPCMPEQW	32(SI), Y0, Y2

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$64, SI    
	ADDQ		$4, DI
    SUBQ        $32, BX
    CMPQ        BX, $32
	JB		 	exit_small
	JMP		 	loop_small

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVW	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int16
scalar:
	MOVW	(SI), R8
	CMPW	R8, DX
	SETEQ	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	2(SI), SI
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

// func cmp_u16_ne_x2(src []uint16, val uint16, bits []byte) int64
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
TEXT ·cmp_u16_ne_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPBROADCASTW val+24(FP), Y0            // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU		shuffle16<>+0x00(SB), Y10    // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff00, CX     // number of values processed in big blocks
    ANDQ    $0xff, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_big:
	VPCMPEQW	0(SI), Y0, Y1
	VPCMPEQW	32(SI), Y0, Y2
	VPCMPEQW	64(SI), Y0, Y3
	VPCMPEQW	96(SI), Y0, Y4

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y3, Y4, Y3
  	VPERMD		Y3, Y9, Y3
	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, (DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPCMPEQW	128(SI), Y0, Y5
	VPCMPEQW	160(SI), Y0, Y6
	VPCMPEQW	192(SI), Y0, Y7
	VPCMPEQW	224(SI), Y0, Y8

	VPACKSSWB	Y5, Y6, Y5
  	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5
	VPMOVMSKB	Y5, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y7, Y8, Y7
  	VPERMD		Y7, Y9, Y7
	VPSHUFB		Y10, Y7, Y7
	VPMOVMSKB	Y7, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, 8(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPCMPEQW	256(SI), Y0, Y1
	VPCMPEQW	288(SI), Y0, Y2
	VPCMPEQW	320(SI), Y0, Y3
	VPCMPEQW	352(SI), Y0, Y4

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y3, Y4, Y3
  	VPERMD		Y3, Y9, Y3
	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, 16(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VPCMPEQW	384(SI), Y0, Y5
	VPCMPEQW	416(SI), Y0, Y6
	VPCMPEQW	448(SI), Y0, Y7
	VPCMPEQW	480(SI), Y0, Y8

	VPACKSSWB	Y5, Y6, Y5
  	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5
	VPMOVMSKB	Y5, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y7, Y8, Y7
  	VPERMD		Y7, Y9, Y7
	VPSHUFB		Y10, Y7, Y7
	VPMOVMSKB	Y7, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, 24(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$32, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VPCMPEQW	0(SI), Y0, Y1
	VPCMPEQW	32(SI), Y0, Y2

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
    NOTL        AX
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$64, SI    
	ADDQ		$4, DI
    SUBQ        $32, BX
    CMPQ        BX, $32
	JB		 	exit_small
	JMP		 	loop_small

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVW	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int16
scalar:
	MOVW	(SI), R8
	CMPW	R8, DX
	SETNE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	2(SI), SI
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

// func cmp_u16_lt_x2(src []uint16, val uint16, bits []byte) int64
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
TEXT ·cmp_u16_lt_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPCMPEQW		Y11, Y11, Y11                   // create 0x8000.. mask
	VPSLLW			$15, Y11, Y11                   // create 0x8000.. mask
	VPBROADCASTW 	val+24(FP), Y0                  // load val into AVX2 reg
	VPXOR			Y11, Y0, Y0                     // flip sign bit
	VMOVDQU		crosslane<>+0x00(SB), Y9            // load permute control mask
	VMOVDQU		shuffle16<>+0x00(SB), Y10           // load shuffle control mask
	CMPQ	BX, $255                                // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff00, CX     // number of values processed in big blocks
    ANDQ    $0xff, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_big:
	VMOVDQU		0(SI), Y1 
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTW	Y1, Y0, Y1     // signed compare
	VPCMPGTW	Y2, Y0, Y2
	VPCMPGTW	Y3, Y0, Y3
	VPCMPGTW	Y4, Y0, Y4

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y3, Y4, Y3
  	VPERMD		Y3, Y9, Y3
	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		128(SI), Y5 
	VMOVDQU		160(SI), Y6
	VMOVDQU		192(SI), Y7
	VMOVDQU		224(SI), Y8
	VPXOR		Y11, Y5, Y5    // flip sign bits
	VPXOR		Y11, Y6, Y6
	VPXOR		Y11, Y7, Y7
	VPXOR		Y11, Y8, Y8
	VPCMPGTW	Y5, Y0, Y5     // signed compare
	VPCMPGTW	Y6, Y0, Y6
	VPCMPGTW	Y7, Y0, Y7
	VPCMPGTW	Y8, Y0, Y8

	VPACKSSWB	Y5, Y6, Y5
  	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5
	VPMOVMSKB	Y5, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y7, Y8, Y7
  	VPERMD		Y7, Y9, Y7
	VPSHUFB		Y10, Y7, Y7
	VPMOVMSKB	Y7, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 8(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		256(SI), Y1 
	VMOVDQU		288(SI), Y2
	VMOVDQU		320(SI), Y3
	VMOVDQU		352(SI), Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTW	Y1, Y0, Y1     // signed compare
	VPCMPGTW	Y2, Y0, Y2
	VPCMPGTW	Y3, Y0, Y3
	VPCMPGTW	Y4, Y0, Y4

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y3, Y4, Y3
  	VPERMD		Y3, Y9, Y3
	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 16(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		384(SI), Y5 
	VMOVDQU		416(SI), Y6
	VMOVDQU		448(SI), Y7
	VMOVDQU		480(SI), Y8
	VPXOR		Y11, Y5, Y5    // flip sign bits
	VPXOR		Y11, Y6, Y6
	VPXOR		Y11, Y7, Y7
	VPXOR		Y11, Y8, Y8
	VPCMPGTW	Y5, Y0, Y5     // signed compare
	VPCMPGTW	Y6, Y0, Y6
	VPCMPGTW	Y7, Y0, Y7
	VPCMPGTW	Y8, Y0, Y8

	VPACKSSWB	Y5, Y6, Y5
  	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5
	VPMOVMSKB	Y5, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y7, Y8, Y7
  	VPERMD		Y7, Y9, Y7
	VPSHUFB		Y10, Y7, Y7
	VPMOVMSKB	Y7, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 24(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$32, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU		0(SI), Y1 
	VMOVDQU		32(SI), Y2
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPCMPGTW	Y1, Y0, Y1     // signed compare
	VPCMPGTW	Y2, Y0, Y2

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$64, SI    
	ADDQ		$4, DI
    SUBQ        $32, BX
    CMPQ        BX, $32
	JB		 	exit_small
	JMP		 	loop_small

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVW	val+24(FP), DX   // load val for comparison
	MOVQ    $1, R12          // create 0x80... mask
	SHLW    $15, R12
	XORW    R12, DX          // flip sign bit
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int16
scalar:
	MOVW	(SI), R8
	XORW    R12, R8          // flip sign bit
	CMPW	R8, DX
	SETLT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	2(SI), SI
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

// func cmp_u16_le_x2(src []uint16, val uint16, bits []byte) int64
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
TEXT ·cmp_u16_le_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPCMPEQW		Y11, Y11, Y11                   // create 0x8000.. mask
	VPSLLW			$15, Y11, Y11                   // create 0x8000.. mask
	VPBROADCASTW 	val+24(FP), Y0                  // load val into AVX2 reg
	VPXOR			Y11, Y0, Y0                     // flip sign bit
	VMOVDQU		crosslane<>+0x00(SB), Y9            // load permute control mask
	VMOVDQU		shuffle16<>+0x00(SB), Y10           // load shuffle control mask
	CMPQ	BX, $255                                // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff00, CX     // number of values processed in big blocks
    ANDQ    $0xff, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_big:
	VMOVDQU		0(SI), Y1 
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTW	Y0, Y1, Y1     // signed compare
	VPCMPGTW	Y0, Y2, Y2
	VPCMPGTW	Y0, Y3, Y3
	VPCMPGTW	Y0, Y4, Y4

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y3, Y4, Y3
  	VPERMD		Y3, Y9, Y3
	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, (DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		128(SI), Y5 
	VMOVDQU		160(SI), Y6
	VMOVDQU		192(SI), Y7
	VMOVDQU		224(SI), Y8
	VPXOR		Y11, Y5, Y5    // flip sign bits
	VPXOR		Y11, Y6, Y6
	VPXOR		Y11, Y7, Y7
	VPXOR		Y11, Y8, Y8
	VPCMPGTW	Y0, Y5, Y5     // signed compare
	VPCMPGTW	Y0, Y6, Y6
	VPCMPGTW	Y0, Y7, Y7
	VPCMPGTW	Y0, Y8, Y8

	VPACKSSWB	Y5, Y6, Y5
  	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5
	VPMOVMSKB	Y5, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y7, Y8, Y7
  	VPERMD		Y7, Y9, Y7
	VPSHUFB		Y10, Y7, Y7
	VPMOVMSKB	Y7, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, 8(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		256(SI), Y1 
	VMOVDQU		288(SI), Y2
	VMOVDQU		320(SI), Y3
	VMOVDQU		352(SI), Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTW	Y0, Y1, Y1     // signed compare
	VPCMPGTW	Y0, Y2, Y2
	VPCMPGTW	Y0, Y3, Y3
	VPCMPGTW	Y0, Y4, Y4

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y3, Y4, Y3
  	VPERMD		Y3, Y9, Y3
	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, 16(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		384(SI), Y5 
	VMOVDQU		416(SI), Y6
	VMOVDQU		448(SI), Y7
	VMOVDQU		480(SI), Y8
	VPXOR		Y11, Y5, Y5    // flip sign bits
	VPXOR		Y11, Y6, Y6
	VPXOR		Y11, Y7, Y7
	VPXOR		Y11, Y8, Y8
	VPCMPGTW	Y0, Y5, Y5     // signed compare
	VPCMPGTW	Y0, Y6, Y6
	VPCMPGTW	Y0, Y7, Y7
	VPCMPGTW	Y0, Y8, Y8

	VPACKSSWB	Y5, Y6, Y5
  	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5
	VPMOVMSKB	Y5, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y7, Y8, Y7
  	VPERMD		Y7, Y9, Y7
	VPSHUFB		Y10, Y7, Y7
	VPMOVMSKB	Y7, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, 24(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$32, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU		0(SI), Y1 
	VMOVDQU		32(SI), Y2
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPCMPGTW	Y0, Y1, Y1     // signed compare
	VPCMPGTW	Y0, Y2, Y2

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
    NOTL        AX
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$64, SI    
	ADDQ		$4, DI
    SUBQ        $32, BX
    CMPQ        BX, $32
	JB		 	exit_small
	JMP		 	loop_small

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVW	val+24(FP), DX   // load val for comparison
	MOVQ    $1, R12          // create 0x80... mask
	SHLW    $15, R12
	XORW    R12, DX          // flip sign bit
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int16
scalar:
	MOVW	(SI), R8
	XORW    R12, R8          // flip sign bit
	CMPW	R8, DX
	SETLE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	2(SI), SI
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
    
// func cmp_u16_gt_x2(src []uint16, val uint16, bits []byte) int64
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
TEXT ·cmp_u16_gt_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPCMPEQW		Y11, Y11, Y11                   // create 0x8000.. mask
	VPSLLW			$15, Y11, Y11                   // create 0x8000.. mask
	VPBROADCASTW 	val+24(FP), Y0                  // load val into AVX2 reg
	VPXOR			Y11, Y0, Y0                     // flip sign bit
	VMOVDQU		crosslane<>+0x00(SB), Y9            // load permute control mask
	VMOVDQU		shuffle16<>+0x00(SB), Y10           // load shuffle control mask
	CMPQ	BX, $255                                // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff00, CX     // number of values processed in big blocks
    ANDQ    $0xff, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_big:
	VMOVDQU		0(SI), Y1 
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTW	Y0, Y1, Y1     // signed compare
	VPCMPGTW	Y0, Y2, Y2
	VPCMPGTW	Y0, Y3, Y3
	VPCMPGTW	Y0, Y4, Y4

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y3, Y4, Y3
  	VPERMD		Y3, Y9, Y3
	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		128(SI), Y5 
	VMOVDQU		160(SI), Y6
	VMOVDQU		192(SI), Y7
	VMOVDQU		224(SI), Y8
	VPXOR		Y11, Y5, Y5    // flip sign bits
	VPXOR		Y11, Y6, Y6
	VPXOR		Y11, Y7, Y7
	VPXOR		Y11, Y8, Y8
	VPCMPGTW	Y0, Y5, Y5     // signed compare
	VPCMPGTW	Y0, Y6, Y6
	VPCMPGTW	Y0, Y7, Y7
	VPCMPGTW	Y0, Y8, Y8

	VPACKSSWB	Y5, Y6, Y5
  	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5
	VPMOVMSKB	Y5, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y7, Y8, Y7
  	VPERMD		Y7, Y9, Y7
	VPSHUFB		Y10, Y7, Y7
	VPMOVMSKB	Y7, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 8(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		256(SI), Y1 
	VMOVDQU		288(SI), Y2
	VMOVDQU		320(SI), Y3
	VMOVDQU		352(SI), Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTW	Y0, Y1, Y1     // signed compare
	VPCMPGTW	Y0, Y2, Y2
	VPCMPGTW	Y0, Y3, Y3
	VPCMPGTW	Y0, Y4, Y4

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y3, Y4, Y3
  	VPERMD		Y3, Y9, Y3
	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 16(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		384(SI), Y5 
	VMOVDQU		416(SI), Y6
	VMOVDQU		448(SI), Y7
	VMOVDQU		480(SI), Y8
	VPXOR		Y11, Y5, Y5    // flip sign bits
	VPXOR		Y11, Y6, Y6
	VPXOR		Y11, Y7, Y7
	VPXOR		Y11, Y8, Y8
	VPCMPGTW	Y0, Y5, Y5     // signed compare
	VPCMPGTW	Y0, Y6, Y6
	VPCMPGTW	Y0, Y7, Y7
	VPCMPGTW	Y0, Y8, Y8

	VPACKSSWB	Y5, Y6, Y5
  	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5
	VPMOVMSKB	Y5, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y7, Y8, Y7
  	VPERMD		Y7, Y9, Y7
	VPSHUFB		Y10, Y7, Y7
	VPMOVMSKB	Y7, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 24(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$32, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU		0(SI), Y1 
	VMOVDQU		32(SI), Y2
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPCMPGTW	Y0, Y1, Y1     // signed compare
	VPCMPGTW	Y0, Y2, Y2

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$64, SI    
	ADDQ		$4, DI
    SUBQ        $32, BX
    CMPQ        BX, $32
	JB		 	exit_small
	JMP		 	loop_small

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVW	val+24(FP), DX   // load val for comparison
	MOVQ    $1, R12          // create 0x80... mask
	SHLW    $15, R12
	XORW    R12, DX          // flip sign bit
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int16
scalar:
	MOVW	(SI), R8
	XORW    R12, R8          // flip sign bit
	CMPW	R8, DX
	SETGT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	2(SI), SI
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

// func cmp_u16_ge_x2(src []uint16, val uint16, bits []byte) int64
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
TEXT ·cmp_u16_ge_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VPCMPEQW		Y11, Y11, Y11                   // create 0x8000.. mask
	VPSLLW			$15, Y11, Y11                   // create 0x8000.. mask
	VPBROADCASTW 	val+24(FP), Y0                  // load val into AVX2 reg
	VPXOR			Y11, Y0, Y0                     // flip sign bit
	VMOVDQU		crosslane<>+0x00(SB), Y9            // load permute control mask
	VMOVDQU		shuffle16<>+0x00(SB), Y10           // load shuffle control mask
	CMPQ	BX, $255                                // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff00, CX     // number of values processed in big blocks
    ANDQ    $0xff, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_big:
	VMOVDQU		0(SI), Y1 
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTW	Y1, Y0, Y1     // signed compare
	VPCMPGTW	Y2, Y0, Y2
	VPCMPGTW	Y3, Y0, Y3
	VPCMPGTW	Y4, Y0, Y4

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y3, Y4, Y3
  	VPERMD		Y3, Y9, Y3
	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, (DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		128(SI), Y5 
	VMOVDQU		160(SI), Y6
	VMOVDQU		192(SI), Y7
	VMOVDQU		224(SI), Y8
	VPXOR		Y11, Y5, Y5    // flip sign bits
	VPXOR		Y11, Y6, Y6
	VPXOR		Y11, Y7, Y7
	VPXOR		Y11, Y8, Y8
	VPCMPGTW	Y5, Y0, Y5     // signed compare
	VPCMPGTW	Y6, Y0, Y6
	VPCMPGTW	Y7, Y0, Y7
	VPCMPGTW	Y8, Y0, Y8

	VPACKSSWB	Y5, Y6, Y5
  	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5
	VPMOVMSKB	Y5, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y7, Y8, Y7
  	VPERMD		Y7, Y9, Y7
	VPSHUFB		Y10, Y7, Y7
	VPMOVMSKB	Y7, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, 8(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		256(SI), Y1 
	VMOVDQU		288(SI), Y2
	VMOVDQU		320(SI), Y3
	VMOVDQU		352(SI), Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTW	Y1, Y0, Y1     // signed compare
	VPCMPGTW	Y2, Y0, Y2
	VPCMPGTW	Y3, Y0, Y3
	VPCMPGTW	Y4, Y0, Y4

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y3, Y4, Y3
  	VPERMD		Y3, Y9, Y3
	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, 16(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		384(SI), Y5 
	VMOVDQU		416(SI), Y6
	VMOVDQU		448(SI), Y7
	VMOVDQU		480(SI), Y8
	VPXOR		Y11, Y5, Y5    // flip sign bits
	VPXOR		Y11, Y6, Y6
	VPXOR		Y11, Y7, Y7
	VPXOR		Y11, Y8, Y8
	VPCMPGTW	Y5, Y0, Y5     // signed compare
	VPCMPGTW	Y6, Y0, Y6
	VPCMPGTW	Y7, Y0, Y7
	VPCMPGTW	Y8, Y0, Y8

	VPACKSSWB	Y5, Y6, Y5
  	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5
	VPMOVMSKB	Y5, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y7, Y8, Y7
  	VPERMD		Y7, Y9, Y7
	VPSHUFB		Y10, Y7, Y7
	VPMOVMSKB	Y7, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, 24(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$32, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU		0(SI), Y1 
	VMOVDQU		32(SI), Y2
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPCMPGTW	Y1, Y0, Y1     // signed compare
	VPCMPGTW	Y2, Y0, Y2

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
    NOTL        AX
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$64, SI    
	ADDQ		$4, DI
    SUBQ        $32, BX
    CMPQ        BX, $32
	JB		 	exit_small
	JMP		 	loop_small

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVW	val+24(FP), DX   // load val for comparison
	MOVQ    $1, R12          // create 0x80... mask
	SHLW    $15, R12
	XORW    R12, DX          // flip sign bit
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int16
scalar:
	MOVW	(SI), R8
	XORW    R12, R8          // flip sign bit
	CMPW	R8, DX
	SETGE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	2(SI), SI
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

// func cmp_u16_bw_x2(src []uint16, a, b uint16, bits []byte) int64
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
TEXT ·cmp_u16_bw_x2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

// check is using GT with switched operands and add
// the diff method to avoid jumps:
// 	diff := b - a + 1
//  v-a < diff
prep_avx:
	VPCMPEQQ		Y11, Y11, Y11                   // create 0x8000.. mask
	VPSLLW			$15, Y11, Y11                   // create 0x8000.. mask
	VPCMPEQW		Y13, Y13, Y13                   // create 1 for adding
	VPSRLW			$15, Y13, Y13
	VPBROADCASTW 	a+24(FP), Y12                   // load val a into AVX2 reg
	VPBROADCASTW 	b+26(FP), Y0                    // load val b into AVX2 reg
	VPSUBW			Y12, Y0, Y0                     // compute diff
	VPADDW			Y13, Y0, Y0
	VPXOR			Y11, Y0, Y0                     // flip sign bit
	VMOVDQU		crosslane<>+0x00(SB), Y9            // load permute control mask
	VMOVDQU		shuffle16<>+0x00(SB), Y10           // load shuffle control mask
	CMPQ	BX, $255                                // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff00, CX     // number of values processed in big blocks
    ANDQ    $0xff, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_big:
	VMOVDQU		0(SI), Y1     
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VPSUBW		Y12, Y1, Y1
	VPSUBW		Y12, Y2, Y2
	VPSUBW		Y12, Y3, Y3
	VPSUBW		Y12, Y4, Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTW	Y1, Y0, Y1     // signed compare
	VPCMPGTW	Y2, Y0, Y2
	VPCMPGTW	Y3, Y0, Y3
	VPCMPGTW	Y4, Y0, Y4

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y3, Y4, Y3
  	VPERMD		Y3, Y9, Y3
	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		128(SI), Y5 
	VMOVDQU		160(SI), Y6
	VMOVDQU		192(SI), Y7
	VMOVDQU		224(SI), Y8
	VPSUBW		Y12, Y5, Y5
	VPSUBW		Y12, Y6, Y6
	VPSUBW		Y12, Y7, Y7
	VPSUBW		Y12, Y8, Y8
	VPXOR		Y11, Y5, Y5    // flip sign bits
	VPXOR		Y11, Y6, Y6
	VPXOR		Y11, Y7, Y7
	VPXOR		Y11, Y8, Y8
	VPCMPGTW	Y5, Y0, Y5     // signed compare
	VPCMPGTW	Y6, Y0, Y6
	VPCMPGTW	Y7, Y0, Y7
	VPCMPGTW	Y8, Y0, Y8

	VPACKSSWB	Y5, Y6, Y5
  	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5
	VPMOVMSKB	Y5, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y7, Y8, Y7
  	VPERMD		Y7, Y9, Y7
	VPSHUFB		Y10, Y7, Y7
	VPMOVMSKB	Y7, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 8(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		256(SI), Y1 
	VMOVDQU		288(SI), Y2
	VMOVDQU		320(SI), Y3
	VMOVDQU		352(SI), Y4
	VPSUBW		Y12, Y1, Y1
	VPSUBW		Y12, Y2, Y2
	VPSUBW		Y12, Y3, Y3
	VPSUBW		Y12, Y4, Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTW	Y1, Y0, Y1     // signed compare
	VPCMPGTW	Y2, Y0, Y2
	VPCMPGTW	Y3, Y0, Y3
	VPCMPGTW	Y4, Y0, Y4

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y3, Y4, Y3
  	VPERMD		Y3, Y9, Y3
	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 16(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VMOVDQU		384(SI), Y5 
	VMOVDQU		416(SI), Y6
	VMOVDQU		448(SI), Y7
	VMOVDQU		480(SI), Y8
	VPSUBW		Y12, Y5, Y5
	VPSUBW		Y12, Y6, Y6
	VPSUBW		Y12, Y7, Y7
	VPSUBW		Y12, Y8, Y8
	VPXOR		Y11, Y5, Y5    // flip sign bits
	VPXOR		Y11, Y6, Y6
	VPXOR		Y11, Y7, Y7
	VPXOR		Y11, Y8, Y8
	VPCMPGTW	Y5, Y0, Y5     // signed compare
	VPCMPGTW	Y6, Y0, Y6
	VPCMPGTW	Y7, Y0, Y7
	VPCMPGTW	Y8, Y0, Y8

	VPACKSSWB	Y5, Y6, Y5
  	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5
	VPMOVMSKB	Y5, DX      // move per byte MSBs into packed bitmask to r32 or r64

	VPACKSSWB	Y7, Y8, Y7
  	VPERMD		Y7, Y9, Y7
	VPSHUFB		Y10, Y7, Y7
	VPMOVMSKB	Y7, AX      // move per byte MSBs into packed bitmask to r32 or r64
    
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 24(DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$32, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU		0(SI), Y1 
	VMOVDQU		32(SI), Y2
	VPSUBW		Y12, Y1, Y1
	VPSUBW		Y12, Y2, Y2
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPCMPGTW	Y1, Y0, Y1     // signed compare
	VPCMPGTW	Y2, Y0, Y2

	VPACKSSWB	Y1, Y2, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$64, SI    
	ADDQ		$4, DI
    SUBQ        $32, BX
    CMPQ        BX, $32
	JB		 	exit_small
	JMP		 	loop_small

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVW	a+24(FP), R13   // load val a
	MOVW	b+26(FP), DX    // load val b
	SUBW	R13, DX
	INCW	DX
	MOVQ    $1, R12          // create 0x80... mask
	SHLW    $15, R12
	XORW    R12, DX          // flip sign bit
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int16
scalar:
	MOVW	(SI), R8
	XORW    R12, R8          // flip sign bit
	SUBW	R13, R8          // v - a < diff
	CMPW	R8, DX
	SETLT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	2(SI), SI
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
