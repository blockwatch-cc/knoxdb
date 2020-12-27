// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants_AVX2.h"

// func matchFloat32EqualAVX2(src []float32, val float32, bits []byte) int64
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
TEXT ·matchFloat32EqualAVX2(SB), NOSPLIT, $0-60
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSS val+24(FP), Y0                 // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU		shuffle32<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff80, CX     // number of values processed in big blocks
    ANDQ    $0x7f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 128 float32 (i.e. 512 bytes of data)
loop_big:
	VCMPPS	    $0, 0(SI), Y0, Y1
	VCMPPS	    $0, 32(SI), Y0, Y2
	VCMPPS	    $0, 64(SI), Y0, Y3
	VCMPPS	    $0, 96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPS	    $0, 128(SI), Y0, Y5
	VCMPPS	    $0, 160(SI), Y0, Y6
	VCMPPS	    $0, 192(SI), Y0, Y7
	VCMPPS	    $0, 224(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)      // write the lower 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VCMPPS	    $0, 256(SI), Y0, Y1
	VCMPPS	    $0, 288(SI), Y0, Y2
	VCMPPS	    $0, 320(SI), Y0, Y3
	VCMPPS	    $0, 352(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPS	    $0, 384(SI), Y0, Y5
	VCMPPS	    $0, 416(SI), Y0, Y6
	VCMPPS	    $0, 448(SI), Y0, Y7
	VCMPPS	    $0, 480(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 8(DI)(CX*1)      // write the 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$16, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VCMPPS	    $0, 0(SI), Y0, Y1
	VCMPPS	    $0, 32(SI), Y0, Y2
	VCMPPS	    $0, 64(SI), Y0, Y3
	VCMPPS	    $0, 96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX              // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$128, SI    
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
	VMOVSS	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float32
scalar:
	VCOMISS	(SI), X0
	JPS	    scalar_shift    // sets partity flag when either value is NaN
	SETEQ	R10
	ADDL	R10, R9
	ORL	 	R10, AX
scalar_shift:
	SHLL	$1, AX
	LEAQ	4(SI), SI
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

// func matchFloat32NotEqualAVX2(src []float32, val float32, bits []byte) int64
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
TEXT ·matchFloat32NotEqualAVX2(SB), NOSPLIT, $0-60
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSS val+24(FP), Y0                 // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU		shuffle32<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff80, CX     // number of values processed in big blocks
    ANDQ    $0x7f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 128 float32 (i.e. 512 bytes of data)
loop_big:
	VCMPPS	    $0x04, 0(SI), Y0, Y1
	VCMPPS	    $0x04, 32(SI), Y0, Y2
	VCMPPS	    $0x04, 64(SI), Y0, Y3
	VCMPPS	    $0x04, 96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPS	    $0x04, 128(SI), Y0, Y5
	VCMPPS	    $0x04, 160(SI), Y0, Y6
	VCMPPS	    $0x04, 192(SI), Y0, Y7
	VCMPPS	    $0x04, 224(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)      // write the lower 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VCMPPS	    $0x04, 256(SI), Y0, Y1
	VCMPPS	    $0x04, 288(SI), Y0, Y2
	VCMPPS	    $0x04, 320(SI), Y0, Y3
	VCMPPS	    $0x04, 352(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPS	    $0x04, 384(SI), Y0, Y5
	VCMPPS	    $0x04, 416(SI), Y0, Y6
	VCMPPS	    $0x04, 448(SI), Y0, Y7
	VCMPPS	    $0x04, 480(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 8(DI)(CX*1)      // write the 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$16, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VCMPPS	    $0x04, 0(SI), Y0, Y1
	VCMPPS	    $0x04, 32(SI), Y0, Y2
	VCMPPS	    $0x04, 64(SI), Y0, Y3
	VCMPPS	    $0x04, 96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX              // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$128, SI    
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
	VMOVSS	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float32
scalar:
	VCOMISS	(SI), X0
	SETNE	R10
	JNP	    scalar_shift
    MOVQ    $1, R10         // NaN is always not equal
scalar_shift:
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	4(SI), SI
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

// func matchFloat32LessThanAVX2(src []float32, val float32, bits []byte) int64
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
TEXT ·matchFloat32LessThanAVX2(SB), NOSPLIT, $0-60
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSS val+24(FP), Y0                 // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU		shuffle32<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff80, CX     // number of values processed in big blocks
    ANDQ    $0x7f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 128 float32 (i.e. 512 bytes of data)
loop_big:
	VCMPPS	    $0x1e, 0(SI), Y0, Y1
	VCMPPS	    $0x1e, 32(SI), Y0, Y2
	VCMPPS	    $0x1e, 64(SI), Y0, Y3
	VCMPPS	    $0x1e, 96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPS	    $0x1e, 128(SI), Y0, Y5
	VCMPPS	    $0x1e, 160(SI), Y0, Y6
	VCMPPS	    $0x1e, 192(SI), Y0, Y7
	VCMPPS	    $0x1e, 224(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)      // write the lower 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VCMPPS	    $0x1e, 256(SI), Y0, Y1
	VCMPPS	    $0x1e, 288(SI), Y0, Y2
	VCMPPS	    $0x1e, 320(SI), Y0, Y3
	VCMPPS	    $0x1e, 352(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPS	    $0x1e, 384(SI), Y0, Y5
	VCMPPS	    $0x1e, 416(SI), Y0, Y6
	VCMPPS	    $0x1e, 448(SI), Y0, Y7
	VCMPPS	    $0x1e, 480(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 8(DI)(CX*1)      // write the 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$16, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VCMPPS	    $0x1e, 0(SI), Y0, Y1
	VCMPPS	    $0x1e, 32(SI), Y0, Y2
	VCMPPS	    $0x1e, 64(SI), Y0, Y3
	VCMPPS	    $0x1e, 96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX              // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$128, SI    
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
	VMOVSS	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float32
scalar:
	VCMPSS  	$0x1e, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	SHLL		$1, AX
	LEAQ		4(SI), SI
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

// func matchFloat32LessThanEqualAVX2(src []float32, val float32, bits []byte) int64
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
TEXT ·matchFloat32LessThanEqualAVX2(SB), NOSPLIT, $0-60
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSS val+24(FP), Y0                 // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU		shuffle32<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff80, CX     // number of values processed in big blocks
    ANDQ    $0x7f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 128 float32 (i.e. 512 bytes of data)
loop_big:
	VCMPPS	    $0x1d, 0(SI), Y0, Y1
	VCMPPS	    $0x1d, 32(SI), Y0, Y2
	VCMPPS	    $0x1d, 64(SI), Y0, Y3
	VCMPPS	    $0x1d, 96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPS	    $0x1d, 128(SI), Y0, Y5
	VCMPPS	    $0x1d, 160(SI), Y0, Y6
	VCMPPS	    $0x1d, 192(SI), Y0, Y7
	VCMPPS	    $0x1d, 224(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)      // write the lower 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VCMPPS	    $0x1d, 256(SI), Y0, Y1
	VCMPPS	    $0x1d, 288(SI), Y0, Y2
	VCMPPS	    $0x1d, 320(SI), Y0, Y3
	VCMPPS	    $0x1d, 352(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPS	    $0x1d, 384(SI), Y0, Y5
	VCMPPS	    $0x1d, 416(SI), Y0, Y6
	VCMPPS	    $0x1d, 448(SI), Y0, Y7
	VCMPPS	    $0x1d, 480(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 8(DI)(CX*1)      // write the 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$16, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VCMPPS	    $0x1d, 0(SI), Y0, Y1
	VCMPPS	    $0x1d, 32(SI), Y0, Y2
	VCMPPS	    $0x1d, 64(SI), Y0, Y3
	VCMPPS	    $0x1d, 96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX              // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$128, SI    
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
	VMOVSS	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float32
scalar:
	VCMPSS  	$0x1d, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	SHLL		$1, AX
	LEAQ		4(SI), SI
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

// func matchFloat32GreaterThanAVX2(src []float32, val float32, bits []byte) int64
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
TEXT ·matchFloat32GreaterThanAVX2(SB), NOSPLIT, $0-60
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSS val+24(FP), Y0                 // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU		shuffle32<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff80, CX     // number of values processed in big blocks
    ANDQ    $0x7f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 128 float32 (i.e. 512 bytes of data)
loop_big:
	VCMPPS	    $0x11, 0(SI), Y0, Y1
	VCMPPS	    $0x11, 32(SI), Y0, Y2
	VCMPPS	    $0x11, 64(SI), Y0, Y3
	VCMPPS	    $0x11, 96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPS	    $0x11, 128(SI), Y0, Y5
	VCMPPS	    $0x11, 160(SI), Y0, Y6
	VCMPPS	    $0x11, 192(SI), Y0, Y7
	VCMPPS	    $0x11, 224(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)      // write the lower 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VCMPPS	    $0x11, 256(SI), Y0, Y1
	VCMPPS	    $0x11, 288(SI), Y0, Y2
	VCMPPS	    $0x11, 320(SI), Y0, Y3
	VCMPPS	    $0x11, 352(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPS	    $0x11, 384(SI), Y0, Y5
	VCMPPS	    $0x11, 416(SI), Y0, Y6
	VCMPPS	    $0x11, 448(SI), Y0, Y7
	VCMPPS	    $0x11, 480(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 8(DI)(CX*1)      // write the 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$16, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VCMPPS	    $0x11, 0(SI), Y0, Y1
	VCMPPS	    $0x11, 32(SI), Y0, Y2
	VCMPPS	    $0x11, 64(SI), Y0, Y3
	VCMPPS	    $0x11, 96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX              // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$128, SI    
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
	VMOVSS	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float32
scalar:
	VCMPSS  	$0x11, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	SHLL		$1, AX
	LEAQ		4(SI), SI
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

// func matchFloat32GreaterThanEqualAVX2(src []float32, val float32, bits []byte) int64
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
TEXT ·matchFloat32GreaterThanEqualAVX2(SB), NOSPLIT, $0-60
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSS val+24(FP), Y0                 // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU		shuffle32<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff80, CX     // number of values processed in big blocks
    ANDQ    $0x7f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 128 float32 (i.e. 512 bytes of data)
loop_big:
	VCMPPS	    $0x12, 0(SI), Y0, Y1
	VCMPPS	    $0x12, 32(SI), Y0, Y2
	VCMPPS	    $0x12, 64(SI), Y0, Y3
	VCMPPS	    $0x12, 96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPS	    $0x12, 128(SI), Y0, Y5
	VCMPPS	    $0x12, 160(SI), Y0, Y6
	VCMPPS	    $0x12, 192(SI), Y0, Y7
	VCMPPS	    $0x12, 224(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)      // write the lower 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VCMPPS	    $0x12, 256(SI), Y0, Y1
	VCMPPS	    $0x12, 288(SI), Y0, Y2
	VCMPPS	    $0x12, 320(SI), Y0, Y3
	VCMPPS	    $0x12, 352(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VCMPPS	    $0x12, 384(SI), Y0, Y5
	VCMPPS	    $0x12, 416(SI), Y0, Y6
	VCMPPS	    $0x12, 448(SI), Y0, Y7
	VCMPPS	    $0x12, 480(SI), Y0, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 8(DI)(CX*1)      // write the 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$16, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VCMPPS	    $0x12, 0(SI), Y0, Y1
	VCMPPS	    $0x12, 32(SI), Y0, Y2
	VCMPPS	    $0x12, 64(SI), Y0, Y3
	VCMPPS	    $0x12, 96(SI), Y0, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, AX              // move per byte MSBs into packed bitmask to r32 or r64
	MOVL		AX, (DI)            // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$128, SI    
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
	VMOVSS	val+24(FP), X0   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float32
scalar:
	VCMPSS  	$0x12, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	SHLL		$1, AX
	LEAQ		4(SI), SI
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

// func matchFloat32BetweenAVX2(src []float32, a, b float32, bits []byte) int64
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
TEXT ·matchFloat32BetweenAVX2(SB), NOSPLIT, $0-72
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled separately
	JBE		prep_scalar

prep_avx:
	VBROADCASTSS a+24(FP), Y0            // load val a into AVX2 reg
	VBROADCASTSS b+28(FP), Y11           // load val b into AVX2 reg
	VMOVDQU			crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU			shuffle32<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $255                            // slices smaller than 256 values are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffff80, CX     // number of values processed in big blocks
    ANDQ    $0x7f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 64 float32 (i.e. 512 bytes of data)
// Note: we load values into vector registers because we need
// to perform two comparisons and merge their results with AND
// because there is no simple range check formula or instruction
// for float32 vector data
loop_big:
	VMOVAPS		0(SI), Y1
	VMOVAPS		32(SI), Y2
	VMOVAPS		64(SI), Y3
	VMOVAPS		96(SI), Y4
	VCMPPS		$0x1d, Y0, Y1, Y12         // imm8 = $0x1d (GTE, nosignal)
	VCMPPS		$0x1d, Y0, Y2, Y13
	VCMPPS		$0x1d, Y0, Y3, Y14
	VCMPPS		$0x1d, Y0, Y4, Y15
	VCMPPS		$0x12, Y11, Y1, Y1         // imm8 = $0x12 (LTE, nosignal)
	VCMPPS		$0x12, Y11, Y2, Y2
	VCMPPS		$0x12, Y11, Y3, Y3
	VCMPPS		$0x12, Y11, Y4, Y4
	VPAND		Y12, Y1, Y1
	VPAND		Y13, Y2, Y2
	VPAND		Y14, Y3, Y3
	VPAND		Y15, Y4, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VMOVAPS		128(SI), Y5
	VMOVAPS		160(SI), Y6
	VMOVAPS		192(SI), Y7
	VMOVAPS		224(SI), Y8
	VCMPPS		$0x1d, Y0, Y5, Y12         // imm8 = $0x1d (GTE, nosignal)
	VCMPPS		$0x1d, Y0, Y6, Y13
	VCMPPS		$0x1d, Y0, Y7, Y14
	VCMPPS		$0x1d, Y0, Y8, Y15
	VCMPPS		$0x12, Y11, Y5, Y5         // imm8 = $0x12 (LTE, nosignal)
	VCMPPS		$0x12, Y11, Y6, Y6
	VCMPPS		$0x12, Y11, Y7, Y7
	VCMPPS		$0x12, Y11, Y8, Y8
	VPAND		Y12, Y5, Y5
	VPAND		Y13, Y6, Y6
	VPAND		Y14, Y7, Y7
	VPAND		Y15, Y8, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)      // write the lower 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	VMOVAPS		256(SI), Y1
	VMOVAPS		288(SI), Y2
	VMOVAPS		320(SI), Y3
	VMOVAPS		352(SI), Y4
	VCMPPS		$0x1d, Y0, Y1, Y12         // imm8 = $0x1d (GTE, nosignal)
	VCMPPS		$0x1d, Y0, Y2, Y13
	VCMPPS		$0x1d, Y0, Y3, Y14
	VCMPPS		$0x1d, Y0, Y4, Y15
	VCMPPS		$0x12, Y11, Y1, Y1         // imm8 = $0x12 (LTE, nosignal)
	VCMPPS		$0x12, Y11, Y2, Y2
	VCMPPS		$0x12, Y11, Y3, Y3
	VCMPPS		$0x12, Y11, Y4, Y4
	VPAND		Y12, Y1, Y1
	VPAND		Y13, Y2, Y2
	VPAND		Y14, Y3, Y3
	VPAND		Y15, Y4, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1

	VPMOVMSKB	Y1, DX              // move per byte MSBs into packed bitmask to r32 or r64

	VMOVAPS		384(SI), Y5
	VMOVAPS		416(SI), Y6
	VMOVAPS		448(SI), Y7
	VMOVAPS		480(SI), Y8
	VCMPPS		$0x1d, Y0, Y5, Y12         // imm8 = $0x1d (GTE, nosignal)
	VCMPPS		$0x1d, Y0, Y6, Y13
	VCMPPS		$0x1d, Y0, Y7, Y14
	VCMPPS		$0x1d, Y0, Y8, Y15
	VCMPPS		$0x12, Y11, Y5, Y5         // imm8 = $0x12 (LTE, nosignal)
	VCMPPS		$0x12, Y11, Y6, Y6
	VCMPPS		$0x12, Y11, Y7, Y7
	VCMPPS		$0x12, Y11, Y8, Y8
	VPAND		Y12, Y5, Y5
	VPAND		Y13, Y6, Y6
	VPAND		Y14, Y7, Y7
	VPAND		Y15, Y8, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7

	VPACKSSWB	Y5, Y7, Y5
	VPERMD		Y5, Y9, Y5
	VPSHUFB		Y10, Y5, Y5

	VPMOVMSKB	Y5, AX              // move per byte MSBs into packed bitmask to r32 or r64
    SHLQ        $32, AX
    ORQ         DX, AX
	MOVQ		AX, 8(DI)(CX*1)      // write the 64 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI    
	ADDQ		$16, CX
	JB		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVAPS		0(SI), Y1
	VMOVAPS		32(SI), Y2
	VMOVAPS		64(SI), Y3
	VMOVAPS		96(SI), Y4
	VCMPPS		$0x1d, Y0, Y1, Y12         // imm8 = $0x1d (GTE, nosignal)
	VCMPPS		$0x1d, Y0, Y2, Y13
	VCMPPS		$0x1d, Y0, Y3, Y14
	VCMPPS		$0x1d, Y0, Y4, Y15
	VCMPPS		$0x12, Y11, Y1, Y1         // imm8 = $0x12 (LTE, nosignal)
	VCMPPS		$0x12, Y11, Y2, Y2
	VCMPPS		$0x12, Y11, Y3, Y3
	VCMPPS		$0x12, Y11, Y4, Y4
	VPAND		Y12, Y1, Y1
	VPAND		Y13, Y2, Y2
	VPAND		Y14, Y3, Y3
	VPAND		Y15, Y4, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3

	VPACKSSWB	Y1, Y3, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$128, SI    
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
	VMOVSS	a+24(FP), X0   // load val a for comparison
	VMOVSS	b+28(FP), X1   // load val b for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 float32
scalar:
	VMOVSS		(SI), X2
	VCMPSS  	$0x1d, X0, X2, X3
	VCMPSS  	$0x12, X1, X2, X2
	VPAND		X3, X2, X2
	VPMOVMSKB	X2, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	SHLL	$1, AX
	LEAQ	4(SI), SI
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
