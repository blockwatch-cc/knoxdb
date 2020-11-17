// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants.h"

// func matchUint8EqualAVX2(src []uint8, val uint8, bits []byte) int64
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
TEXT ·matchUint8EqualAVX2(SB), NOSPLIT, $0-57
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled separately
	JBE		prep_scalar
    MOVQ    BX, CX
    ANDQ    $0x1f, BX
    ANDQ    $0xffffffffffffffe0, CX
    ADDQ    CX, SI
    SHRQ    $3, CX
    ADDQ    CX, DI
    NEGQ    CX
    
prep_avx2:
	VPBROADCASTB val+24(FP), Y0            // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU		shuffle8<>+0x00(SB), Y10    // load shuffle control mask

// works for >= 32 int8 (i.e. 32 bytes of data)
loop_avx2:
	VPCMPEQB	(SI)(CX*8), Y0, Y1


	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)(CX*1)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	//LEAQ		32(SI), SI
//	LEAQ		4(DI), DI
//	SUBQ		$32, BX
	ADDQ		$4, CX
//	CMPQ		BX, $32
	JZ		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
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

// for remainders of <32 int16
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

// func matchUint8EqualAVX512(src []uint8, val uint8, bits []byte) int64
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
TEXT ·matchUint8EqualAVX512(SB), NOSPLIT, $0-57
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $63      // slices smaller than 32 byte are handled separately
	JBE		prep_scalar
    MOVQ    BX, CX
    ANDQ    $0x3f, BX
    ANDQ    $0xffffffffffffffc0, CX
    ADDQ    CX, SI
    SHRQ    $3, CX
    ADDQ    CX, DI
    NEGQ    CX
    
prep_avx2:
	VPBROADCASTB val+24(FP), Z0            // load val into AVX2 reg
	//VMOVDQU64		crosslane_512<>+0x00(SB), Z9   // load permute control mask
	VMOVDQU64		shuffle8_512<>+0x00(SB), Z10    // load shuffle control mask

// works for >= 64 int8 (i.e. 64 bytes of data)
loop_avx2:
    VMOVDQU64   (SI)(CX*8), Z1
	VPSHUFB		Z10, Z1, Z1
	VPCMPEQB	Z1, Z0, K1
    
    
	//VPCMPEQB	(SI)(CX*8), Z0, K1



	//VPMOVMSKB	Z1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	//VPMOVB2M	Z1, K1      // move per byte MSBs into packed bitmask to r32 or r64
	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
    KMOVQ       K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$8, CX
	JZ		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
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

// for remainders of <32 int16
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

// func matchUint8EqualAVX2Opt(src []uint8, val uint8, bits []byte) int64
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
TEXT ·matchUint8EqualAVX2Opt(SB), NOSPLIT, $0-57
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $127      // slices smaller than 32 byte are handled separately
	JBE		prep_scalar

prep_avx2:
	VPBROADCASTB val+24(FP), Y0            // load val into AVX2 reg
//	VMOVDQU		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU		shuffle8<>+0x00(SB), Y10    // load shuffle control mask

// works for >= 32 int8 (i.e. 32 bytes of data)
loop_avx2:
	VPCMPEQB	32(SI), Y0, Y2
	VPSHUFB		Y10, Y2, Y2
	VPMOVMSKB	Y2, DX      // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32,DX

	VPCMPEQB	0(SI), Y0, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	//MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	//POPCNTQ		AX, AX      // count 1 bits
	//ADDQ		AX, R9
    ORQ         DX, AX
	MOVQ		AX, 0(DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	VPCMPEQB	96(SI), Y0, Y4
	VPSHUFB		Y10, Y4, Y4
	VPMOVMSKB	Y4, DX      // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32,DX
//	MOVL		DX, 12(DI)    // write the lower 32 bits to the output slice
//	POPCNTQ		DX, DX      // count 1 bits
//	ADDQ		DX, R9

	VPCMPEQB	64(SI), Y0, Y3
	VPSHUFB		Y10, Y3, Y3
	VPMOVMSKB	Y3, AX      // move per byte MSBs into packed bitmask to r32 or r64
//	MOVL		AX, 8(DI)    // write the lower 32 bits to the output slice
//	POPCNTQ		AX, AX      // count 1 bits
//	ADDQ		AX, R9
    ORQ         DX, AX
	MOVQ		AX, 8(DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9


	LEAQ		128(SI), SI
	LEAQ		16(DI), DI
	SUBQ		$128, BX
	CMPQ		BX, $128
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
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

// for remainders of <32 int16
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

// func matchUint8NotEqualAVX2(src []uint64, val uint64, bits []byte) int64
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
TEXT ·matchUint8NotEqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled separately
	JBE		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0            // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU		shuffle<>+0x00(SB), Y10    // load shuffle control mask

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_avx2:
	VPCMPEQQ	0(SI), Y0, Y1
	VPCMPEQQ	32(SI), Y0, Y2
	VPCMPEQQ	64(SI), Y0, Y3
	VPCMPEQQ	96(SI), Y0, Y4
	VPCMPEQQ	128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPCMPEQQ	160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VPCMPEQQ	192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VPCMPEQQ	224(SI), Y0, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	XORQ	    $0xffffffff, AX // convert EQ to NE
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
	MOVQ	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETEQ	R10
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
	XORQ	$0xffffffff, AX // convert EQ to NE
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


// func matchUint8LessThanAVX2(src []uint64, val uint64, bits []byte) int64
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
//   Y11 = sign bit flip mask
//   Y1-Y8 = vector data
TEXT ·matchUint8LessThanAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled separately
	JBE		prep_scalar

// works for >= 32 int64 (i.e. 256 bytes of data)
// check using GT with switched operands and add
// 0x8000000000000000 to both a and b to use the
// signed compare for unsigned integers
prep_avx2:
	VPCMPEQQ		Y11, Y11, Y11                    // create 0x8000.. mask
	VPSLLQ			$63, Y11, Y11                    // create 0x8000.. mask
	VBROADCASTSD 	val+24(FP), Y0                   // load val into AVX2 reg
	VPXOR			Y11, Y0, Y0                      // flip sign bit
	VMOVDQU			crosslane<>+0x00(SB), Y9         // load permute control mask
	VMOVDQU			shuffle<>+0x00(SB), Y10          // load shuffle control mask

loop_avx2:
	VMOVDQU		0(SI), Y1      // load values (necessary to flip sign bit)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
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
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VMOVDQU		160(SI), Y6
	VPXOR		Y11, Y6, Y6
	VPCMPGTQ	Y6, Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VMOVDQU		192(SI), Y7
	VPXOR		Y11, Y7, Y7
	VPCMPGTQ	Y7, Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VMOVDQU		224(SI), Y8
	VPXOR		Y11, Y8, Y8
	VPCMPGTQ	Y8, Y0, Y8
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
	MOVQ	val+24(FP), DX   // load val for comparison
	MOVQ    $1, R12          // create 0x80... mask
	SHLQ    $63, R12
	XORQ    R12, DX          // flip sign bit
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	XORQ    R12, R8          // flip sign bit
	CMPQ	R8, DX
	SETLT	R10
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

// func matchUint8LessThanEqualAVX2(src []uint64, val uint64, bits []byte) int64
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
TEXT ·matchUint8LessThanEqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled separately
	JBE		prep_scalar

prep_avx2:
	VPCMPEQQ		Y11, Y11, Y11                // create 0x8000.. mask
	VPSLLQ			$63, Y11, Y11                // create 0x8000.. mask
	VBROADCASTSD 	val+24(FP), Y0               // load val into AVX2 reg
	VPXOR			Y11, Y0, Y0                  // flip sign bit
	VMOVDQU		 	crosslane<>+0x00(SB), Y9     // load permute control mask
	VMOVDQU		 	shuffle<>+0x00(SB), Y10      // load shuffle control mask

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_avx2:
	VMOVDQU		0(SI), Y1      // load values (necessary to switch operands & flip sign)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPXOR		Y11, Y5, Y5
	VPCMPGTQ	Y0, Y1, Y1
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VMOVDQU		160(SI), Y6
	VPXOR		Y11, Y6, Y6
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VMOVDQU		192(SI), Y7
	VPXOR		Y11, Y7, Y7
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VMOVDQU		224(SI), Y8
	VPXOR		Y11, Y8, Y8
	VPCMPGTQ	Y0, Y8, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	NOTL 		AX          // invert mask so GT translates into LTE
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
	MOVQ	val+24(FP), DX   // load val for comparison
	MOVQ    $1, R12          // create 0x80... mask
	SHLQ    $63, R12
	XORQ    R12, DX          // flip sign bit
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	XORQ    R12, R8          // flip sign bit
	CMPQ	R8, DX
	SETLE	R10
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


// func matchUint8GreaterThanAVX2(src []uint64, val uint64, bits []byte) int64
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
TEXT ·matchUint8GreaterThanAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled separately
	JBE		prep_scalar

prep_avx2:
	VPCMPEQQ		Y11, Y11, Y11                // create 0x8000.. mask
	VPSLLQ			$63, Y11, Y11                // create 0x8000.. mask
	VBROADCASTSD 	val+24(FP), Y0               // load val into AVX2 reg
	VPXOR			Y11, Y0, Y0                  // flip sign bit
	VMOVDQU			crosslane<>+0x00(SB), Y9     // load permute control mask
	VMOVDQU			shuffle<>+0x00(SB), Y10      // load shuffle control mask

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_avx2:
	VMOVDQU		0(SI), Y1      // load values to flip sign bits & compare switch order
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPXOR		Y11, Y5, Y5
	VPCMPGTQ	Y0, Y1, Y1
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VMOVDQU		160(SI), Y6
	VPXOR		Y11, Y6, Y6
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VMOVDQU		192(SI), Y7
	VPXOR		Y11, Y7, Y7
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VMOVDQU		224(SI), Y8
	VPXOR		Y11, Y8, Y8
	VPCMPGTQ	Y0, Y8, Y8
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
	MOVQ	val+24(FP), DX   // load val for comparison
	MOVQ    $1, R12          // create 0x80... mask
	SHLQ    $63, R12
	XORQ    R12, DX          // flip sign bit
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	XORQ    R12, R8          // flip sign bit
	CMPQ	R8, DX
	SETGT	R10
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

// func matchUint8GreaterThanEqualAVX2(src []uint64, val uint64, bits []byte) int64
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
TEXT ·matchUint8GreaterThanEqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled separately
	JBE		prep_scalar

prep_avx2:
	VPCMPEQQ		Y11, Y11, Y11                // create 0x8000.. mask
	VPSLLQ			$63, Y11, Y11                // create 0x8000.. mask
	VBROADCASTSD 	val+24(FP), Y0               // load val into AVX2 reg
	VPXOR			Y11, Y0, Y0                  // flip sign bit
	VMOVDQU			crosslane<>+0x00(SB), Y9     // load permute control mask
	VMOVDQU			shuffle<>+0x00(SB), Y10      // load shuffle control mask

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_avx2:
	VMOVDQU		0(SI), Y1      // load values (necessary to flip sign bit)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
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
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VMOVDQU		160(SI), Y6
	VPXOR		Y11, Y6, Y6
	VPCMPGTQ	Y6, Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VMOVDQU		192(SI), Y7
	VPXOR		Y11, Y7, Y7
	VPCMPGTQ	Y7, Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VMOVDQU		224(SI), Y8
	VPXOR		Y11, Y8, Y8
	VPCMPGTQ	Y8, Y0, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64
	NOTL		AX          // invert mask, so NOT LT becomes GTE
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
	MOVQ	val+24(FP), DX   // load val for comparison
	MOVQ    $1, R12          // create 0x80... mask
	SHLQ    $63, R12
	XORQ    R12, DX          // flip sign bit
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	XORQ    R12, R8          // flip sign bit
	CMPQ	R8, DX
	SETGE	R10
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

// func matchUint8BetweenAVX2(src []uint64, a, b uint64, bits []byte) int64
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
TEXT ·matchUint8BetweenAVX2(SB), NOSPLIT, $0-72
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
prep_avx2:
	VPCMPEQQ		Y11, Y11, Y11                    // create 0x8000.. mask
	VPSLLQ			$63, Y11, Y11                    // create 0x8000.. mask
	VPCMPEQQ		Y13, Y13, Y13                    // create 1 for adding
	VPSRLQ			$63, Y13, Y13
	VBROADCASTSD 	val+24(FP), Y12                  // load val a into AVX2 reg
	VBROADCASTSD 	val+32(FP), Y0                   // load val b into AVX2 reg
	VPSUBQ			Y12, Y0, Y0                      // compute diff
	VPADDQ			Y13, Y0, Y0
	VPXOR			Y11, Y0, Y0                      // flip sign bit
	VMOVDQU			crosslane<>+0x00(SB), Y9         // load permute control mask
	VMOVDQU			shuffle<>+0x00(SB), Y10          // load shuffle control mask

loop_avx2:
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
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VMOVDQU		160(SI), Y6
	VPSUBQ		Y12, Y6, Y6
	VPXOR		Y11, Y6, Y6
	VPCMPGTQ	Y6, Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VMOVDQU		192(SI), Y7
	VPSUBQ		Y12, Y7, Y7
	VPXOR		Y11, Y7, Y7
	VPCMPGTQ	Y7, Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VMOVDQU		224(SI), Y8
	VPSUBQ		Y12, Y8, Y8
	VPXOR		Y11, Y8, Y8
	VPCMPGTQ	Y8, Y0, Y8
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
	MOVQ	val+24(FP), R13   // load val a
	MOVQ	val+32(FP), DX    // load val b
	SUBQ	R13, DX
	INCQ	DX
	MOVQ    $1, R12          // create 0x80... mask
	SHLQ    $63, R12
	XORQ    R12, DX          // flip sign bit
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
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

