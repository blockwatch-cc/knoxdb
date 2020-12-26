// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants.h"

// func matchInt8EqualAVX2(src []int8, val int8, bits []byte) int64
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
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·matchInt8EqualAVX2(SB), NOSPLIT, $0-57
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar
	JBE		prep_scalar

prep_avx:
	VPBROADCASTB    val+24(FP), Y0              // load val into AVX2 reg
	VMOVDQU		    shuffle8<>+0x00(SB), Y10    // load shuffle control mask
 	CMPQ	        BX, $511                    // slices smaller than 511 values are handled in small loop
	JBE		        prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xfffffffffffffe00, CX     // number of values processed in big blocks
    ANDQ    $0x1ff, BX                  // number of values processed in small blocks/scalar
    ADDQ    CX, SI                      // move SI to the end of the array
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
loop_big:
    VPCMPEQB	(SI)(CX*8), Y0, Y1
    VPCMPEQB	32(SI)(CX*8), Y0, Y2
    VPCMPEQB	64(SI)(CX*8), Y0, Y3
    VPCMPEQB	96(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, (DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 8(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    VPCMPEQB	128(SI)(CX*8), Y0, Y1
    VPCMPEQB	160(SI)(CX*8), Y0, Y2
    VPCMPEQB	192(SI)(CX*8), Y0, Y3
    VPCMPEQB	224(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 16(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 24(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	
    VPCMPEQB	256(SI)(CX*8), Y0, Y1
    VPCMPEQB	288(SI)(CX*8), Y0, Y2
    VPCMPEQB	320(SI)(CX*8), Y0, Y3
    VPCMPEQB	352(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 32(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 40(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    VPCMPEQB	384(SI)(CX*8), Y0, Y1
    VPCMPEQB	416(SI)(CX*8), Y0, Y2
    VPCMPEQB	448(SI)(CX*8), Y0, Y3
    VPCMPEQB	480(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 48(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 56(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    
	ADDQ		$64, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VPCMPEQB	0(SI), Y0, Y1

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$32, SI    
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
	MOVB	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int8
scalar:
	MOVB	(SI), R8
	CMPB	R8, DX
	SETEQ	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	1(SI), SI
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

// func matchInt8NotEqualAVX2(src []int8, val int8, bits []byte) int64
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
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·matchInt8NotEqualAVX2(SB), NOSPLIT, $0-57
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar
	JBE		prep_scalar

prep_avx:
	VPBROADCASTB    val+24(FP), Y0              // load val into AVX2 reg
	VMOVDQU		    shuffle8<>+0x00(SB), Y10    // load shuffle control mask
 	CMPQ	        BX, $511                    // slices smaller than 511 values are handled in small loop
	JBE		        prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xfffffffffffffe00, CX     // number of values processed in big blocks
    ANDQ    $0x1ff, BX                  // number of values processed in small blocks/scalar
    ADDQ    CX, SI                      // move SI to the end of the array
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
loop_big:
    VPCMPEQB	(SI)(CX*8), Y0, Y1
    VPCMPEQB	32(SI)(CX*8), Y0, Y2
    VPCMPEQB	64(SI)(CX*8), Y0, Y3
    VPCMPEQB	96(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, (DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 8(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    VPCMPEQB	128(SI)(CX*8), Y0, Y1
    VPCMPEQB	160(SI)(CX*8), Y0, Y2
    VPCMPEQB	192(SI)(CX*8), Y0, Y3
    VPCMPEQB	224(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 16(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 24(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	
    VPCMPEQB	256(SI)(CX*8), Y0, Y1
    VPCMPEQB	288(SI)(CX*8), Y0, Y2
    VPCMPEQB	320(SI)(CX*8), Y0, Y3
    VPCMPEQB	352(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 32(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 40(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    VPCMPEQB	384(SI)(CX*8), Y0, Y1
    VPCMPEQB	416(SI)(CX*8), Y0, Y2
    VPCMPEQB	448(SI)(CX*8), Y0, Y3
    VPCMPEQB	480(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 48(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 56(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    
	ADDQ		$64, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VPCMPEQB	0(SI), Y0, Y1

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    NOTL        AX
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$32, SI    
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
	MOVB	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int8
scalar:
	MOVB	(SI), R8
	CMPB	R8, DX
	SETNE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	1(SI), SI
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

// func matchInt8LessThanAVX2(src []int8, val int8, bits []byte) int64
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
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·matchInt8LessThanAVX2(SB), NOSPLIT, $0-57
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar
	JBE		prep_scalar

prep_avx:
	VPBROADCASTB    val+24(FP), Y0                  // load val into AVX2 reg
	VMOVDQU		    shuffle8<>+0x00(SB), Y10        // load shuffle control mask
 	CMPQ	        BX, $511                        // slices smaller than 511 values are handled in small loop
	JBE		        prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xfffffffffffffe00, CX     // number of values processed in big blocks
    ANDQ    $0x1ff, BX                  // number of values processed in small blocks/scalar
    ADDQ    CX, SI                      // move SI to the end of the array
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
loop_big:
    VPCMPGTB	(SI)(CX*8), Y0, Y1
    VPCMPGTB	32(SI)(CX*8), Y0, Y2
    VPCMPGTB	64(SI)(CX*8), Y0, Y3
    VPCMPGTB	96(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, (DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 8(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    VPCMPGTB	128(SI)(CX*8), Y0, Y1
    VPCMPGTB	160(SI)(CX*8), Y0, Y2
    VPCMPGTB	192(SI)(CX*8), Y0, Y3
    VPCMPGTB	224(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 16(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 24(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	
    VPCMPGTB	256(SI)(CX*8), Y0, Y1
    VPCMPGTB	288(SI)(CX*8), Y0, Y2
    VPCMPGTB	320(SI)(CX*8), Y0, Y3
    VPCMPGTB	352(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 32(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 40(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    VPCMPGTB	384(SI)(CX*8), Y0, Y1
    VPCMPGTB	416(SI)(CX*8), Y0, Y2
    VPCMPGTB	448(SI)(CX*8), Y0, Y3
    VPCMPGTB	480(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 48(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 56(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    
	ADDQ		$64, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VPCMPGTB	    0(SI), Y0, Y1

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$32, SI    
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
	MOVB	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int8
scalar:
	MOVB	(SI), R8
	CMPB	R8, DX
	SETLT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	1(SI), SI
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

// func matchInt8LessThanEqualAVX2(src []int8, val int8, bits []byte) int64
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
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·matchInt8LessThanEqualAVX2(SB), NOSPLIT, $0-57
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar
	JBE		prep_scalar

prep_avx:
	VPBROADCASTB    val+24(FP), Y0                  // load val into AVX2 reg
	VMOVDQU		    shuffle8<>+0x00(SB), Y10        // load shuffle control mask
 	CMPQ	        BX, $511                        // slices smaller than 511 values are handled in small loop
	JBE		        prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xfffffffffffffe00, CX     // number of values processed in big blocks
    ANDQ    $0x1ff, BX                  // number of values processed in small blocks/scalar
    ADDQ    CX, SI                      // move SI to the end of the array
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
loop_big:
    VMOVDQU	(SI)(CX*8), Y1
    VMOVDQU	32(SI)(CX*8), Y2
    VMOVDQU	64(SI)(CX*8), Y3
    VMOVDQU	96(SI)(CX*8), Y4
	VPCMPGTB	Y0, Y1, Y1     // signed compare
	VPCMPGTB	Y0, Y2, Y2
	VPCMPGTB	Y0, Y3, Y3
	VPCMPGTB	Y0, Y4, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, (DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 8(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    VMOVDQU	128(SI)(CX*8), Y1
    VMOVDQU	160(SI)(CX*8), Y2
    VMOVDQU	192(SI)(CX*8), Y3
    VMOVDQU	224(SI)(CX*8), Y4
	VPCMPGTB	Y0, Y1, Y1     // signed compare
	VPCMPGTB	Y0, Y2, Y2
	VPCMPGTB	Y0, Y3, Y3
	VPCMPGTB	Y0, Y4, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 16(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 24(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	
    VMOVDQU	256(SI)(CX*8), Y1
    VMOVDQU	288(SI)(CX*8), Y2
    VMOVDQU	320(SI)(CX*8), Y3
    VMOVDQU	352(SI)(CX*8), Y4
	VPCMPGTB	Y0, Y1, Y1     // signed compare
	VPCMPGTB	Y0, Y2, Y2
	VPCMPGTB	Y0, Y3, Y3
	VPCMPGTB	Y0, Y4, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 32(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 40(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    VMOVDQU	384(SI)(CX*8), Y1
    VMOVDQU	416(SI)(CX*8), Y2
    VMOVDQU	448(SI)(CX*8), Y3
    VMOVDQU	480(SI)(CX*8), Y4
	VPCMPGTB	Y0, Y1, Y1     // signed compare
	VPCMPGTB	Y0, Y2, Y2
	VPCMPGTB	Y0, Y3, Y3
	VPCMPGTB	Y0, Y4, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 48(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 56(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    
	ADDQ		$64, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU	    0(SI), Y1
	VPCMPGTB	Y0, Y1, Y1     // signed compare

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    NOTL        AX
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$32, SI    
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
	MOVB	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int8
scalar:
	MOVB	(SI), R8
	CMPB	R8, DX
	SETLE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	1(SI), SI
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

// func matchInt8GreaterThanAVX2(src []int8, val int8, bits []byte) int64
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
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·matchInt8GreaterThanAVX2(SB), NOSPLIT, $0-57
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar
	JBE		prep_scalar

prep_avx:
	VPBROADCASTB    val+24(FP), Y0                  // load val into AVX2 reg
	VMOVDQU		    shuffle8<>+0x00(SB), Y10        // load shuffle control mask
 	CMPQ	        BX, $511                        // slices smaller than 511 values are handled in small loop
	JBE		        prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xfffffffffffffe00, CX     // number of values processed in big blocks
    ANDQ    $0x1ff, BX                  // number of values processed in small blocks/scalar
    ADDQ    CX, SI                      // move SI to the end of the array
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
loop_big:
    VMOVDQU	(SI)(CX*8), Y1
    VMOVDQU	32(SI)(CX*8), Y2
    VMOVDQU	64(SI)(CX*8), Y3
    VMOVDQU	96(SI)(CX*8), Y4
	VPCMPGTB	Y0, Y1, Y1     // signed compare
	VPCMPGTB	Y0, Y2, Y2
	VPCMPGTB	Y0, Y3, Y3
	VPCMPGTB	Y0, Y4, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, (DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 8(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    VMOVDQU	128(SI)(CX*8), Y1
    VMOVDQU	160(SI)(CX*8), Y2
    VMOVDQU	192(SI)(CX*8), Y3
    VMOVDQU	224(SI)(CX*8), Y4
	VPCMPGTB	Y0, Y1, Y1     // signed compare
	VPCMPGTB	Y0, Y2, Y2
	VPCMPGTB	Y0, Y3, Y3
	VPCMPGTB	Y0, Y4, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 16(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 24(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	
    VMOVDQU	256(SI)(CX*8), Y1
    VMOVDQU	288(SI)(CX*8), Y2
    VMOVDQU	320(SI)(CX*8), Y3
    VMOVDQU	352(SI)(CX*8), Y4
	VPCMPGTB	Y0, Y1, Y1     // signed compare
	VPCMPGTB	Y0, Y2, Y2
	VPCMPGTB	Y0, Y3, Y3
	VPCMPGTB	Y0, Y4, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 32(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 40(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    VMOVDQU	384(SI)(CX*8), Y1
    VMOVDQU	416(SI)(CX*8), Y2
    VMOVDQU	448(SI)(CX*8), Y3
    VMOVDQU	480(SI)(CX*8), Y4
	VPCMPGTB	Y0, Y1, Y1     // signed compare
	VPCMPGTB	Y0, Y2, Y2
	VPCMPGTB	Y0, Y3, Y3
	VPCMPGTB	Y0, Y4, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 48(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 56(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    
	ADDQ		$64, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU	    0(SI), Y1
	VPCMPGTB	Y0, Y1, Y1     // signed compare

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$32, SI    
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
	MOVB	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int8
scalar:
	MOVB	(SI), R8
	CMPB	R8, DX
	SETGT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	1(SI), SI
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

// func matchInt8GreaterThanEqualAVX2(src []int8, val int8, bits []byte) int64
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
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·matchInt8GreaterThanEqualAVX2(SB), NOSPLIT, $0-57
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar
	JBE		prep_scalar

prep_avx:
	VPBROADCASTB    val+24(FP), Y0                  // load val into AVX2 reg
	VMOVDQU		    shuffle8<>+0x00(SB), Y10        // load shuffle control mask
 	CMPQ	        BX, $511                        // slices smaller than 511 values are handled in small loop
	JBE		        prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xfffffffffffffe00, CX     // number of values processed in big blocks
    ANDQ    $0x1ff, BX                  // number of values processed in small blocks/scalar
    ADDQ    CX, SI                      // move SI to the end of the array
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
loop_big:
    VPCMPGTB	(SI)(CX*8), Y0, Y1
    VPCMPGTB	32(SI)(CX*8), Y0, Y2
    VPCMPGTB	64(SI)(CX*8), Y0, Y3
    VPCMPGTB	96(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, (DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 8(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    VPCMPGTB	128(SI)(CX*8), Y0, Y1
    VPCMPGTB	160(SI)(CX*8), Y0, Y2
    VPCMPGTB	192(SI)(CX*8), Y0, Y3
    VPCMPGTB	224(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 16(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 24(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	
    VPCMPGTB	256(SI)(CX*8), Y0, Y1
    VPCMPGTB	288(SI)(CX*8), Y0, Y2
    VPCMPGTB	320(SI)(CX*8), Y0, Y3
    VPCMPGTB	352(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 32(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 40(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    VPCMPGTB	384(SI)(CX*8), Y0, Y1
    VPCMPGTB	416(SI)(CX*8), Y0, Y2
    VPCMPGTB	448(SI)(CX*8), Y0, Y3
    VPCMPGTB	480(SI)(CX*8), Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 48(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

    NOTQ        AX
	MOVQ		AX, 56(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    
	ADDQ		$64, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VPCMPGTB	    0(SI), Y0, Y1

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    NOTL        AX
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$32, SI    
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
	MOVB	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int8
scalar:
	MOVB	(SI), R8
	CMPB	R8, DX
	SETGE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	1(SI), SI
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

// func matchInt8BetweenAVX2(src []int8, a, b int8, bits []byte) int64
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
TEXT ·matchInt8BetweenAVX2(SB), NOSPLIT, $0-60
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar
	JBE		prep_scalar

// check is using GT with switched operands and add
// the diff method to avoid jumps:
// 	diff := b - a + 1
//  v-a < diff
prep_avx:
	VPBROADCASTB    const_0x80<>+0x00(SB), Y11      // create 0x8000.. mask
	VPBROADCASTB    const_0x01<>+0x00(SB), Y13      // create 1 for adding
	VPBROADCASTB 	a+24(FP), Y12                   // load val a into AVX2 reg
	VPBROADCASTB 	b+25(FP), Y0                    // load val b into AVX2 reg
	VPSUBB			Y12, Y0, Y0                     // compute diff
	VPADDB			Y13, Y0, Y0
	VPXOR			Y11, Y0, Y0                     // flip sign bit
	VMOVDQU		    shuffle8<>+0x00(SB), Y10        // load shuffle control mask
 	CMPQ	        BX, $511                        // slices smaller than 511 values are handled in small loop
	JBE		        prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xfffffffffffffe00, CX     // number of values processed in big blocks
    ANDQ    $0x1ff, BX                  // number of values processed in small blocks/scalar
    ADDQ    CX, SI                      // move SI to the end of the array
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
loop_big:
    VMOVDQU	(SI)(CX*8), Y1
    VMOVDQU	32(SI)(CX*8), Y2
    VMOVDQU	64(SI)(CX*8), Y3
    VMOVDQU	96(SI)(CX*8), Y4
	VPSUBB		Y12, Y1, Y1
	VPSUBB		Y12, Y2, Y2
	VPSUBB		Y12, Y3, Y3
	VPSUBB		Y12, Y4, Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTB	Y1, Y0, Y1     // signed compare
	VPCMPGTB	Y2, Y0, Y2
	VPCMPGTB	Y3, Y0, Y3
	VPCMPGTB	Y4, Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, (DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 8(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    VMOVDQU	128(SI)(CX*8), Y1
    VMOVDQU	160(SI)(CX*8), Y2
    VMOVDQU	192(SI)(CX*8), Y3
    VMOVDQU	224(SI)(CX*8), Y4
	VPSUBB		Y12, Y1, Y1
	VPSUBB		Y12, Y2, Y2
	VPSUBB		Y12, Y3, Y3
	VPSUBB		Y12, Y4, Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTB	Y1, Y0, Y1     // signed compare
	VPCMPGTB	Y2, Y0, Y2
	VPCMPGTB	Y3, Y0, Y3
	VPCMPGTB	Y4, Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 16(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 24(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	
    VMOVDQU	256(SI)(CX*8), Y1
    VMOVDQU	288(SI)(CX*8), Y2
    VMOVDQU	320(SI)(CX*8), Y3
    VMOVDQU	352(SI)(CX*8), Y4
	VPSUBB		Y12, Y1, Y1
	VPSUBB		Y12, Y2, Y2
	VPSUBB		Y12, Y3, Y3
	VPSUBB		Y12, Y4, Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTB	Y1, Y0, Y1     // signed compare
	VPCMPGTB	Y2, Y0, Y2
	VPCMPGTB	Y3, Y0, Y3
	VPCMPGTB	Y4, Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 32(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 40(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    VMOVDQU	384(SI)(CX*8), Y1
    VMOVDQU	416(SI)(CX*8), Y2
    VMOVDQU	448(SI)(CX*8), Y3
    VMOVDQU	480(SI)(CX*8), Y4
	VPSUBB		Y12, Y1, Y1
	VPSUBB		Y12, Y2, Y2
	VPSUBB		Y12, Y3, Y3
	VPSUBB		Y12, Y4, Y4
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPCMPGTB	Y1, Y0, Y1     // signed compare
	VPCMPGTB	Y2, Y0, Y2
	VPCMPGTB	Y3, Y0, Y3
	VPCMPGTB	Y4, Y0, Y4

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 48(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, DX      // move per byte MSBs into packed bitmask to r32 or r64
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, AX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32, AX
    ORQ         DX, AX

	MOVQ		AX, 56(DI)(CX*1)    // write 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

    
	ADDQ		$64, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU	    0(SI), Y1
	VPSUBB		Y12, Y1, Y1
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPCMPGTB	Y1, Y0, Y1     // signed compare

	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$32, SI    
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
	MOVB	a+24(FP), R13   // load val a
	MOVB	b+25(FP), DX    // load val b
	SUBB	R13, DX
	INCB	DX
	MOVQ    $1, R12          // create 0x80... mask
	SHLB    $7, R12
	XORB    R12, DX          // flip sign bit
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int8
scalar:
	MOVB	(SI), R8
	XORB    R12, R8          // flip sign bit
	SUBB	R13, R8          // v - a < diff
	CMPB	R8, DX
	SETLT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	1(SI), SI
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
