// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants_AVX2.h"


#define BITSET_AVX2(_FUNC) \
	VMOVDQU		0(DI), Y0; \
	_FUNC		0(SI), Y0, Y0; \
	VMOVDQU		32(DI), Y1; \
	_FUNC		32(SI), Y1, Y1; \
	VMOVDQU		64(DI), Y2; \
	_FUNC		64(SI), Y2, Y2; \
	VMOVDQU		96(DI), Y3; \
	_FUNC		96(SI), Y3, Y3; \
	VMOVDQU		Y0, 0(SI); \
	VMOVDQU		Y1, 32(SI); \
	VMOVDQU		Y2, 64(SI); \
	VMOVDQU		Y3, 96(SI); \
	VMOVDQU		128(DI), Y4; \
	_FUNC		128(SI), Y4, Y4; \
	VMOVDQU		160(DI), Y5; \
	_FUNC		160(SI), Y5, Y5; \
	VMOVDQU		192(DI), Y6; \
	_FUNC		192(SI), Y6, Y6; \
	VMOVDQU		224(DI), Y7; \
	_FUNC		224(SI), Y7, Y7; \
	VMOVDQU		Y4, 128(SI); \
	VMOVDQU		Y5, 160(SI); \
	VMOVDQU		Y6, 192(SI); \
	VMOVDQU		Y7, 224(SI);

#define BITSET_AVX(_FUNC) \
	VMOVDQU		0(DI), Y0; \
	_FUNC		0(SI), Y0, Y0; \
	VMOVDQU		Y0, 0(SI);

#define BITSET_I32(_FUNC) \
	MOVL	0(DI), AX; \
	_FUNC	0(SI), AX; \
	MOVL	AX, 0(SI);

#define BITSET_I8(_FUNC) \
	MOVB	0(DI), AX; \
	_FUNC	0(SI), AX; \
	MOVB	AX, 0(SI);

// func bitsetAndAVX2(dst, src []byte)
//
TEXT ·bitsetAndAVX2(SB), NOSPLIT, $0-48
	MOVQ	dst_base+0(FP), SI
	MOVQ	dst_len+8(FP), BX
	MOVQ	src_base+24(FP), DI

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $256     // slices smaller than 256 byte are handled separately
	JB		prep_avx

	// works for data size 256 byte
loop_avx2:
	BITSET_AVX2(VPAND)
	LEAQ		256(DI), DI
	LEAQ		256(SI), SI
	SUBQ		$256, BX
	CMPQ		BX, $256
	JB			exit_avx2
	JMP			loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ	BX, BX
	JLE		done

prep_avx:
	CMPQ	BX, $32
	JB		prep_i32

	// works for data size 16 byte
loop_avx:
	BITSET_AVX(VPAND)
	LEAQ		32(SI), SI
	LEAQ		32(DI), DI
	SUBL		$32, BX
	CMPL		BX, $32
	JB			prep_i32
	JMP			loop_avx

	// works for data size 15 down to single byte
prep_i32:
	TESTQ	BX, BX
	JLE		done
	XORQ	AX, AX
	CMPL	BX, $4
	JB		prep_i8

loop_i32:
	BITSET_I32(ANDL)
	LEAQ	4(SI), SI
	LEAQ	4(DI), DI
	SUBL	$4, BX
	CMPL	BX, $4
	JB		prep_i8
	JMP		loop_i32

prep_i8:
	TESTQ	BX, BX
	JLE		done
	XORL	AX, AX

loop_i8:
	BITSET_I8(ANDB)
	INCQ	DI
	INCQ	SI
	DECL	BX
	JZ		done
	JMP		loop_i8

done:
	RET

// func bitsetAndNotAVX2(dst, src []byte)
//
TEXT ·bitsetAndNotAVX2(SB), NOSPLIT, $0-48
	MOVQ	dst_base+0(FP), SI
	MOVQ	dst_len+8(FP), BX
	MOVQ	src_base+24(FP), DI

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $256     // slices smaller than 256 byte are handled separately
	JB		prep_avx

	// works for data size 256 byte
loop_avx2:
	BITSET_AVX2(VPANDN)
	LEAQ		256(DI), DI
	LEAQ		256(SI), SI
	SUBQ		$256, BX
	CMPQ		BX, $256
	JB			exit_avx2
	JMP			loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ	BX, BX
	JLE		done

prep_avx:
	CMPQ	BX, $32
	JB		prep_i32

	// works for data size 16 byte
loop_avx:
	BITSET_AVX(VPANDN)
	LEAQ		32(SI), SI
	LEAQ		32(DI), DI
	SUBL		$32, BX
	CMPL		BX, $32
	JB			prep_i32
	JMP			loop_avx

	// works for data size 15 down to single byte
prep_i32:
	TESTQ	BX, BX
	JLE		done
	XORQ	AX, AX
	CMPL	BX, $4
	JB		prep_i8

loop_i32:
	MOVL	0(DI), AX
	NOTL 	AX
	ANDL	0(SI), AX
	MOVL	AX, 0(SI)
	LEAQ	4(SI), SI
	LEAQ	4(DI), DI
	SUBL	$4, BX
	CMPL	BX, $4
	JB		prep_i8
	JMP		loop_i32

prep_i8:
	TESTQ	BX, BX
	JLE		done
	XORL	AX, AX

loop_i8:
	MOVB	0(DI), AX
	NOTB	AX
	ANDB	0(SI), AX
	MOVB	AX, 0(SI)
	INCQ	DI
	INCQ	SI
	DECL	BX
	JZ		done
	JMP		loop_i8

done:
	RET

// func bitsetOrAVX2(dst, src []byte)
//
TEXT ·bitsetOrAVX2(SB), NOSPLIT, $0-48
	MOVQ	dst_base+0(FP), SI
	MOVQ	dst_len+8(FP), BX
	MOVQ	src_base+24(FP), DI

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $256     // slices smaller than 256 byte are handled separately
	JB		prep_avx

	// works for data size 256 byte
loop_avx2:
	BITSET_AVX2(VPOR)
	LEAQ		256(DI), DI
	LEAQ		256(SI), SI
	SUBQ		$256, BX
	CMPQ		BX, $256
	JB			exit_avx2
	JMP			loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ	BX, BX
	JLE		done

prep_avx:
	CMPQ	BX, $32
	JB		prep_i32

	// works for data size 16 byte
loop_avx:
	BITSET_AVX(VPOR)
	LEAQ		32(DI), DI
	LEAQ		32(SI), SI
	SUBL		$32, BX
	CMPL		BX, $32
	JB			prep_i32
	JMP			loop_avx

	// works for data size 15 down to single byte
prep_i32:
	TESTQ	BX, BX
	JLE		done
	XORQ	AX, AX
	CMPL	BX, $4
	JB		prep_i8

loop_i32:
	BITSET_I32(ORL)
	LEAQ	4(DI), DI
	LEAQ	4(SI), SI
	SUBL	$4, BX
	CMPL	BX, $4
	JB		prep_i8
	JMP		loop_i32

prep_i8:
	TESTQ	BX, BX
	JLE		done
	XORL	AX, AX

loop_i8:
	BITSET_I8(ORB)
	INCQ	DI
	INCQ	SI
	DECL	BX
	JZ		done
	JMP		loop_i8

done:
	RET

// func bitsetXorAVX2(dst, src []byte)
//
TEXT ·bitsetXorAVX2(SB), NOSPLIT, $0-48
	MOVQ	dst_base+0(FP), SI
	MOVQ	dst_len+8(FP), BX
	MOVQ	src_base+24(FP), DI

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $256     // slices smaller than 256 byte are handled separately
	JB		prep_avx

	// works for data size 256 byte
loop_avx2:
	BITSET_AVX2(VPXOR)
	LEAQ		256(DI), DI
	LEAQ		256(SI), SI
	SUBQ		$256, BX
	CMPQ		BX, $256
	JB			exit_avx2
	JMP			loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ	BX, BX
	JLE		done

prep_avx:
	CMPQ	BX, $32
	JB		prep_i32

	// works for data size 16 byte
loop_avx:
	BITSET_AVX(VPXOR)
	LEAQ		32(DI), DI
	LEAQ		32(SI), SI
	SUBL		$32, BX
	CMPL		BX, $32
	JB			prep_i32
	JMP			loop_avx

	// works for data size 15 down to single byte
prep_i32:
	TESTQ	BX, BX
	JLE		done
	XORQ	AX, AX
	CMPL	BX, $4
	JB		prep_i8

loop_i32:
	BITSET_I32(XORL)
	LEAQ	4(DI), DI
	LEAQ	4(SI), SI
	SUBL	$4, BX
	CMPL	BX, $4
	JB		prep_i8
	JMP		loop_i32

prep_i8:
	TESTQ	BX, BX
	JLE		done
	XORL	AX, AX

loop_i8:
	BITSET_I8(XORB)
	INCQ	DI
	INCQ	SI
	DECL	BX
	JZ		done
	JMP		loop_i8

done:
	RET

#define BITSET_AVX2048_FLAG1(_FUNC) \
	VMOVDQU		0(DI), Y0; \
	_FUNC		0(SI), Y0, Y0; \
    VPOR        Y0, Y10, Y10 \
	VMOVDQU		32(DI), Y1; \
	_FUNC		32(SI), Y1, Y1; \
    VPOR        Y1, Y10, Y10 \
	VMOVDQU		64(DI), Y2; \
	_FUNC		64(SI), Y2, Y2; \
    VPOR        Y2, Y10, Y10 \
	VMOVDQU		96(DI), Y3; \
	_FUNC		96(SI), Y3, Y3; \
    VPOR        Y3, Y10, Y10 \
	VMOVDQU		Y0, 0(SI); \
	VMOVDQU		Y1, 32(SI); \
	VMOVDQU		Y2, 64(SI); \
	VMOVDQU		Y3, 96(SI); \
	VMOVDQU		128(DI), Y4; \
	_FUNC		128(SI), Y4, Y4; \
    VPOR        Y4, Y10, Y10 \
	VMOVDQU		160(DI), Y5; \
	_FUNC		160(SI), Y5, Y5; \
    VPOR        Y5, Y10, Y10 \
	VMOVDQU		192(DI), Y6; \
	_FUNC		192(SI), Y6, Y6; \
    VPOR        Y6, Y10, Y10 \
	VMOVDQU		224(DI), Y7; \
	_FUNC		224(SI), Y7, Y7; \
    VPOR        Y7, Y10, Y10 \
	VMOVDQU		Y4, 128(SI); \
	VMOVDQU		Y5, 160(SI); \
	VMOVDQU		Y6, 192(SI); \
	VMOVDQU		Y7, 224(SI);

#define BITSET_AVX256_FLAG1(_FUNC) \
	VMOVDQU		0(DI), Y0; \
	_FUNC		0(SI), Y0, Y0; \
    VPOR        Y0, Y10, Y10 \
	VMOVDQU		Y0, 0(SI);

#define BITSET_I32_FLAG1(_FUNC) \
	MOVL	0(DI), AX; \
	_FUNC	0(SI), AX; \
    ORL     AX, R10 \
	MOVL	AX, 0(SI);

#define BITSET_I8_FLAG1(_FUNC) \
	MOVB	0(DI), AX; \
	_FUNC	0(SI), AX; \
    ORB     AX, R10 \
	MOVB	AX, 0(SI);

// func bitsetAndAVX2Flag1(dst, src []byte) int
//
TEXT ·bitsetAndAVX2Flag1(SB), NOSPLIT, $0-48
	MOVQ	dst_base+0(FP), SI
	MOVQ	dst_len+8(FP), BX
	MOVQ	src_base+24(FP), DI
    VPXOR   Y10, Y10, Y10       // vector register for collecting ones

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $256     // slices smaller than 256 byte are handled separately
	JB		prep_avx

	// works for data size 256 byte
loop_avx2:
	BITSET_AVX2048_FLAG1(VPAND)
	LEAQ		256(DI), DI
	LEAQ		256(SI), SI
	SUBQ		$256, BX
	CMPQ		BX, $256
	JB			exit_avx2
	JMP			loop_avx2

exit_avx2:

prep_avx:
	CMPQ	BX, $32
	JB		prep_i32

// works for data size 16 byte
loop_avx:
	BITSET_AVX256_FLAG1(VPAND)
	LEAQ		32(SI), SI
	LEAQ		32(DI), DI
	SUBL		$32, BX
	CMPL		BX, $32
	JB			prep_i32
	JMP			loop_avx

exit_avx:
	VZEROUPPER

// works for data size 15 down to single byte
prep_i32:
    // move collected ones from AVX2 to x86 register
    VPXOR       Y11, Y11, Y11       // Y11 = 0
    VPCMPEQB	Y11, Y10, Y10       // for each byte of Y10: zero -> 0xff, not zero -> 0x00
	VPMOVMSKB	Y10, R10            // move per byte MSBs into packed bitmask to r32
    NOTL        R10

	CMPL	BX, $4
	JB		prep_i8

loop_i32:
	BITSET_I32_FLAG1(ANDL)
	LEAQ	4(SI), SI
	LEAQ	4(DI), DI
	SUBL	$4, BX
	CMPL	BX, $4
	JB		prep_i8
	JMP		loop_i32

prep_i8:
	TESTQ	BX, BX
	JLE		done
	XORQ	AX, AX

loop_i8:
	BITSET_I8_FLAG1(ANDB)
	INCQ	DI
	INCQ	SI
	DECL	BX
	JZ		done
	JMP		loop_i8

done:
    // collected ones are in R10
	MOVQ	R10, src_base+48(FP)
	RET

// func bitsetOrAVX2Flag1(dst, src []byte) int
//
TEXT ·bitsetOrAVX2Flag1(SB), NOSPLIT, $0-48
	MOVQ	dst_base+0(FP), SI
	MOVQ	dst_len+8(FP), BX
	MOVQ	src_base+24(FP), DI
    VPXOR   Y10, Y10, Y10       // vector register for collecting ones

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $256     // slices smaller than 256 byte are handled separately
	JB		prep_avx

	// works for data size 256 byte
loop_avx2:
	BITSET_AVX2048_FLAG1(VPOR)
	LEAQ		256(DI), DI
	LEAQ		256(SI), SI
	SUBQ		$256, BX
	CMPQ		BX, $256
	JB			exit_avx2
	JMP			loop_avx2

exit_avx2:

prep_avx:
	CMPQ	BX, $32
	JB		prep_i32

// works for data size 16 byte
loop_avx:
	BITSET_AVX256_FLAG1(VPOR)
	LEAQ		32(SI), SI
	LEAQ		32(DI), DI
	SUBL		$32, BX
	CMPL		BX, $32
	JB			prep_i32
	JMP			loop_avx

exit_avx:
	VZEROUPPER

// works for data size 15 down to single byte
prep_i32:
    // move collected ones from AVX2 to x86 register
    VPXOR       Y11, Y11, Y11       // Y11 = 0
    VPCMPEQB	Y11, Y10, Y10       // for each byte of Y10: zero -> 0xff, not zero -> 0x00
	VPMOVMSKB	Y10, R10            // move per byte MSBs into packed bitmask to r32
    NOTL        R10

	CMPL	BX, $4
	JB		prep_i8

loop_i32:
	BITSET_I32_FLAG1(ORL)
	LEAQ	4(SI), SI
	LEAQ	4(DI), DI
	SUBL	$4, BX
	CMPL	BX, $4
	JB		prep_i8
	JMP		loop_i32

prep_i8:
	TESTQ	BX, BX
	JLE		done
	XORQ	AX, AX

loop_i8:
	BITSET_I8_FLAG1(ORB)
	INCQ	DI
	INCQ	SI
	DECL	BX
	JZ		done
	JMP		loop_i8

done:
    // collected ones are in R10
	MOVQ	R10, src_base+48(FP)
	RET

// func bitsetNegAVX2(src []byte)
//
TEXT ·bitsetNegAVX2(SB), NOSPLIT, $0-24
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

	TESTQ		BX, BX
	JLE			done
	CMPQ		BX, $256     // slices smaller than 256 byte are handled separately
	JBE			prep_avx
	VPCMPEQD	Y8, Y8, Y8   // prepare 0xff.. vector for ones complement

	// works for data size 256 byte
loop_avx2:
	VPXOR		0(SI), Y8, Y0
	VPXOR		32(SI), Y8, Y1
	VPXOR		64(SI), Y8, Y2
	VPXOR		96(SI), Y8, Y3
	VMOVDQU		Y0, 0(SI)
	VMOVDQU		Y1, 32(SI)
	VMOVDQU		Y2, 64(SI)
	VMOVDQU		Y3, 96(SI)
	VPXOR		128(SI), Y8, Y4
	VPXOR		160(SI), Y8, Y5
	VPXOR		192(SI), Y8, Y6
	VPXOR		224(SI), Y8, Y7
	VMOVDQU		Y4, 128(SI)
	VMOVDQU		Y5, 160(SI)
	VMOVDQU		Y6, 192(SI)
	VMOVDQU		Y7, 224(SI)
	LEAQ		256(SI), SI
	SUBQ		$256, BX
	CMPQ		BX, $256
	JB			exit_avx2
	JMP			loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ	BX, BX
	JLE		done

prep_avx:
	CMPQ	BX, $16
	JBE		prep_i32
	VPCMPEQD	X8, X8, X8

	// works for data size 16 byte
loop_avx:
	VPXOR		0(SI), X8, X0
	VMOVDQU		X0, 0(SI)
	LEAQ		16(SI), SI
	SUBL		$16, BX
	CMPL		BX, $16
	JB			prep_i32
	JMP			loop_avx

	// works for data size 15 down to single byte
prep_i32:
	TESTQ	BX, BX
	JLE		done
	XORQ	AX, AX
	CMPL	BX, $4
	JBE		prep_i8

loop_i32:
	MOVL	0(SI), AX
	NOTL	AX
	MOVL	AX, 0(SI)
	LEAQ	4(SI), SI
	SUBL	$4, BX
	CMPL	BX, $4
	JBE		prep_i8
	JMP		loop_i32

prep_i8:
	TESTQ	BX, BX
	JLE		done
	XORL	AX, AX

loop_i8:
	MOVB	0(SI), AX
	NOTB 	AX
	MOVB	AX, 0(SI)
	INCQ	SI
	DECL	BX
	JZ		done
	JMP		loop_i8

done:
	RET

// func bitsetReverseAVX2(src []byte, bitsetReverseLut256 []uint8)
//
// input:
//   SI = src_base, loop counter from left
//   BX = src_len
//   BP = bitsetReverseLut256_base
// internal:
//   DI = loop count from right
//   Y9 = LUT for high nibble
//   Y10 = LUT for low nibble
//   Y11 = mask for low nibble
//   Y12 = nask for suffle bytes within qwords
//   Y13 = mask for permute qwords within YMM-register
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·bitsetReverseAVX2(SB), NOSPLIT, $0-24
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
    MOVQ    bitsetReverseLut256_base+24(FP), BP
    MOVQ    SI, DI
    ADDQ    BX, DI                          // now DI points to end of array

	TESTQ		BX, BX
	JLE			done

    VMOVDQU         LUT_reverse<>+0x00(SB), Y9      // LUT for high nibble
    VPSLLW          $4, Y9, Y10                     // LUT for low low nibble
    VPBROADCASTB    const_0x0f<>+0x00(SB), Y11      // low mask, 0x0f
    VMOVDQU         shuf_reverse<>+0x00(SB), Y12        // load byte shuffle mask
    VMOVDQU         perm_reverse<>+0x00(SB), Y13    // load byte shuffle mask

	CMPQ		BX, $63     // slices smaller than 64 byte are handled separately
	JBE			prep_avx


	// works for data size 64 byte
loop_avx2:
    SUBQ        $32, DI
	VMOVDQU		(SI), Y0               // load first 256 bit
	VMOVDQU		(DI), Y2               // load last 256 bit

    // revert Y0
    // first revert bits within bytes
    VPAND       Y0, Y11, Y1             // mask low nibble
    VPSHUFB     Y1, Y10, Y1             // lookup for low nibble (now Y1 contains high reversed nibble)
    VPSRLW      $4, Y0, Y0              // shift high nibble to low nibble
    VPAND       Y0, Y11, Y0             // mask low nibble
    VPSHUFB     Y0, Y9, Y0              // lookup for high nibble (now Y0 contains low reversed nibble)
    VPOR        Y0, Y1, Y0              // combine both nibbles
    // revert bytes within 128 bit lanes
    VPSHUFB     Y12, Y0, Y0
    // revert 128 bit lanes within YMM register
    VPERMD      Y0, Y13, Y0
	VMOVDQU		Y0, (DI)               // write it to the end

    // revert Y2
    // first revert bits within bytes
    VPAND       Y2, Y11, Y1             // mask low nibble
    VPSHUFB     Y1, Y10, Y1             // lookup for low nibble (now Y1 contains high reversed nibble)
    VPSRLW      $4, Y2, Y2              // shift high nibble to low nibble
    VPAND       Y2, Y11, Y2             // mask low nibble
    VPSHUFB     Y2, Y9, Y2              // lookup for high nibble (now Y0 contains low reversed nibble)
    VPOR        Y2, Y1, Y2              // combine both nibbles
    // revert bytes within 128 bit lanes
    VPSHUFB     Y12, Y2, Y2
    // revert 128 bit lanes within YMM register
    VPERMD      Y2, Y13, Y2
	VMOVDQU		Y2, (SI)               // write it to the begin

	ADDQ		$32, SI
    SUBQ		$64, BX
	CMPQ		BX, $64
	JB			exit_avx2
	JMP			loop_avx2

exit_avx2:
	VZEROUPPER

prep_avx:
	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31
	JBE		prep_i8

	// works for data size 16 byte, no loop because max 63 bytes -> max 2 XMM registers (32 bytes)
loop_avx:
    SUBQ        $16, DI
	VMOVDQU		(SI), X0               // load first 128 bit
	VMOVDQU		(DI), X2               // load last 128 bit

    // revert Y0
    // first revert bits within bytes
    VPAND       X0, X11, X1             // mask low nibble
    VPSHUFB     X1, X10, X1             // lookup for low nibble (now Y1 contains high reversed nibble)
    VPSRLW      $4, X0, X0              // shift high nibble to low nibble
    VPAND       X0, X11, X0             // mask low nibble
    VPSHUFB     X0, X9, X0              // lookup for high nibble (now Y0 contains low reversed nibble)
    VPOR        X0, X1, X0              // combine both nibbles
    // revert bytes within XMM register
    VPSHUFB     X12, X0, X0
	VMOVDQU		X0, (DI)               // write it to the end

    // revert Y2
    // first revert bits within bytes
    VPAND       X2, X11, X1             // mask low nibble
    VPSHUFB     X1, X10, X1             // lookup for low nibble (now Y1 contains high reversed nibble)
    VPSRLW      $4, X2, X2              // shift high nibble to low nibble
    VPAND       X2, X11, X2             // mask low nibble
    VPSHUFB     X2, X9, X2              // lookup for high nibble (now Y0 contains low reversed nibble)
    VPOR        X2, X1, X2              // combine both nibbles
    // revert bytes within XMM register
    VPSHUFB     X12, X2, X2
	VMOVDQU		X2, (SI)               // write it to the begin

	ADDQ		$16, SI
    SUBQ		$32, BX

exit_avx:
	VZEROUPPER

prep_i8:
    XORQ    R8, R8
    XORQ    R9, R9

	CMPQ	BX, $2
	JB		last_byte

loop_i8:
    SUBQ    $1, DI
	MOVB	(SI), R8
	MOVB	(DI), R9

    // revert R8 and R9
    MOVB    (BP)(R8*1),R10      // look up
    MOVB    (BP)(R9*1),R11      // look up
    MOVB    R10, 0(DI)
    MOVB    R11, 0(SI)

	ADDQ	$1, SI
	SUBQ	$2, BX
    CMPQ    BX, $2
	JB		last_byte
	JMP		loop_i8

last_byte:
	TESTQ	BX, BX
	JLE		done

	MOVB	(SI), R8
    // revert R8
    MOVB    (BP)(R8*1),R10      // look up
    MOVB    R10, (SI)

done:
	RET

// Helpers for PopCountAVX2
//
// void CSA(__m256i& h, __m256i& l, __m256i a, __m256i b, __m256i c)
// {
//   const __m256i u = a ^ b;
//   h = (a & b) | (u & c);
//   l = u ^ c;
// }
#define CSA(x, y, a, b, c) \
	VPAND	a, b, x; \
	VPXOR	a, b, b; \
	VPXOR	b, c, y; \
	VPAND	b, c, b; \
	VPOR 	x, b, x;

// func csaAVX2(h, l, a, b, c [4]uint64) [8]uint64
//TEXT ·csaAVX2(SB), NOSPLIT, $0-160
//	VMOVDQU	h+0(FP), Y0
//	VMOVDQU	l+32(FP), Y1
//	VMOVDQU	a+64(FP), Y2
//	VMOVDQU	b+96(FP), Y3
//	VMOVDQU	c+128(FP), Y4
//	CSA(Y0, Y1, Y2, Y3, Y4)
//	VMOVDQU	Y0, ret+160(FP)
//	VMOVDQU	Y1, ret+192(FP)
//	VZEROUPPER
//	RET


// Input == Output register
// Static: Y7(55), Y8(33), Y9(0F)
// Scratch: Y6
#define POPCOUNT(VAL) \
	VMOVDQU		VAL, Y6; \
	VPSRLW		$1, Y6, Y6; \
	VPAND		Y6, Y7, Y6; \
	VPSUBB		Y6, VAL, VAL; \
	VMOVDQU		VAL, Y6; \
	VPSRLW		$2, Y6, Y6; \
	VPAND		Y6, Y8, Y6; \
	VPAND		VAL, Y8, VAL; \
	VPADDB		VAL, Y6, VAL; \
	VMOVDQU		VAL, Y6; \
	VPSRLW		$4, Y6, Y6; \
	VPADDB		VAL, Y6, VAL; \
	VPAND		VAL, Y9, VAL; \
	VPXOR		Y6, Y6, Y6; \
	VPSADBW		VAL, Y6, VAL;

// func pop(v [4]uint64) [4]uint64
//TEXT ·popAVX2(SB), NOSPLIT, $0-32
//	VPBROADCASTB 	const_0x55<>+0x00(SB), Y7
//	VPBROADCASTB 	const_0x33<>+0x00(SB), Y8
//	VPBROADCASTB 	const_0x0f<>+0x00(SB), Y9
//	VMOVDQU	v+0(FP), Y0
//	POPCOUNT(Y0)
//	VMOVDQU	Y0, ret+32(FP)
//	VZEROUPPER
//	RET


// func bitsetPopCountAVX2(src []byte, size int) int64
//
// <32 byte: use CPU popcount on uint64 & uint8
// <512 byte: use AVX
// >=512 byte: use AVX2 harley seal
//
// References
// - https://github.com/WojciechMula/sse-popcount
// - https://en.wikipedia.org/wiki/Hamming_weight
//
// Input
// data   SI
// len    BX
//
// Static data
// 0x55      Y7
// 0x33      Y8
// 0x0f      Y9
//
// accumulators
// total     Y10
// ones      Y11
// twos      Y12
// fours     Y13
// eights    Y14
// sixteens  Y15
//
TEXT ·bitsetPopCountAVX2(SB), NOSPLIT, $0-32
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	XORQ	AX, AX

	TESTQ	BX, BX
	JLE		done
	MOVQ	BX, CX        // limit = size - size % 512;
	ANDQ	$15, CX
	NEGQ	CX
	ADDQ	BX, CX
	CMPQ	CX, $512      // for(; i < limit; i += 16) // addresses 16 x 32 bytes of data
	JBE		prep_avx

	// works for blocks of 512 byte
prep_avx2:
	VPBROADCASTB 	const_0x55<>+0x00(SB), Y7
	VPBROADCASTB 	const_0x33<>+0x00(SB), Y8
	VPBROADCASTB 	const_0x0f<>+0x00(SB), Y9
	VPXOR			Y10, Y10, Y10
	VPXOR			Y11, Y11, Y11
	VPXOR			Y12, Y12, Y12
	VPXOR			Y13, Y13, Y13
	VPXOR			Y14, Y14, Y14

loop_avx2:
	VMOVDQU		0(SI), Y0 		// CSA(twosA, ones, ones, data[i+0], data[i+1]);
	VMOVDQU		32(SI), Y1
	CSA(Y2, Y11, Y11, Y0, Y1)
	VMOVDQU		64(SI), Y0 		// CSA(twosB, ones, ones, data[i+2], data[i+3]);
	VMOVDQU		96(SI), Y1
	CSA(Y3, Y11, Y11, Y0, Y1)
	CSA(Y4, Y12, Y12, Y2, Y3)	// CSA(foursA, twos, twos, twosA, twosB);
	VMOVDQU		128(SI), Y0 	// CSA(twosA, ones, ones, data[i+4], data[i+5]);
	VMOVDQU		160(SI), Y1
	CSA(Y2, Y11, Y11, Y0, Y1)
	VMOVDQU		192(SI), Y0 	// CSA(twosB, ones, ones, data[i+6], data[i+7]);
	VMOVDQU		224(SI), Y1
	CSA(Y3, Y11, Y11, Y0, Y1)
	CSA(Y5, Y12, Y12, Y2, Y3) 	// CSA(foursB, twos, twos, twosA, twosB);
 	CSA(Y6, Y13, Y13, Y4, Y5)	// CSA(eightsA,fours, fours, foursA, foursB);
	VMOVDQU		256(SI), Y0 	// CSA(twosA, ones, ones, data[i+8], data[i+9]);
	VMOVDQU		288(SI), Y1
	CSA(Y2, Y11, Y11, Y0, Y1)
	VMOVDQU		320(SI), Y0 	// CSA(twosB, ones, ones, data[i+10], data[i+11]);
	VMOVDQU		352(SI), Y1
	CSA(Y3, Y11, Y11, Y0, Y1)
 	CSA(Y4, Y12, Y12, Y2, Y3)	// CSA(foursA, twos, twos, twosA, twosB);
	VMOVDQU		384(SI), Y0 	// CSA(twosA, ones, ones, data[i+12], data[i+13]);
	VMOVDQU		416(SI), Y1
	CSA(Y2, Y11, Y11, Y0, Y1)
	VMOVDQU		448(SI), Y0 	// CSA(twosB, ones, ones, data[i+14], data[i+15]);
	VMOVDQU		480(SI), Y1
	CSA(Y3, Y11, Y11, Y0, Y1)
	CSA(Y5, Y12, Y12, Y2, Y3)	// CSA(foursB, twos, twos, twosA, twosB);
	CSA(Y0, Y13, Y13, Y4, Y5)	// CSA(eightsB, fours, fours, foursA, foursB);
	CSA(Y15, Y14, Y14, Y6, Y0)	// CSA(sixteens, eights, eights, eightsA, eightsB);
	POPCOUNT(Y15)               // total = _mm256_add_epi64(total, popcount(sixteens));
	VPADDQ		Y15, Y10, Y10

	LEAQ		512(SI), SI
	SUBQ		$512, BX
	CMPQ		BX, $512
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
	VPSLLQ	$4, Y10, Y10	// total = _mm256_slli_epi64(total, 4);
	POPCOUNT(Y14)			// total = _mm256_add_epi64(total, _mm256_slli_epi64(popcount(eights), 3));
	VPSLLQ	$3, Y14, Y14
	VPADDQ	Y14, Y10, Y10
	POPCOUNT(Y13)			// total = _mm256_add_epi64(total, _mm256_slli_epi64(popcount(fours),  2));
	VPSLLQ	$2, Y13, Y13
	VPADDQ	Y13, Y10, Y10
	POPCOUNT(Y12)			// total = _mm256_add_epi64(total, _mm256_slli_epi64(popcount(twos),   1));
	VPSLLQ 	$1, Y12, Y12
	VPADDQ	Y12, Y10, Y10
	POPCOUNT(Y11)			// total = _mm256_add_epi64(total, popcount(ones));
	VPADDQ	Y11, Y10, Y10

	// horizontal sum  Y10[3] + Y10[2] + Y10[1] + Y10[0], all uint64
	VEXTRACTI128	$1, Y10, X0 	// move Y10[3,2] into X0[1,0]
 	VPADDQ 			X0, X10, X0     // vector add Y10[1,0] with X0[1,0]
 	VPEXTRQ			$1, X0, R8      // extract X0[1]
 	ADDQ			R8, AX
 	VPEXTRQ			$0, X0, R8		// extract X0[0]
 	ADDQ			R8, AX

	// exit early when data was multiple of 512 byte
	TESTQ	BX, BX
	JLE		done

	// works for blocks of size 32 byte
prep_avx:
	CMPQ	BX, $32
	JBE		prep_i64

loop_avx:
	VMOVDQU  	0(SI), X0
	VMOVDQU  	16(SI), X1
	VMOVHLPS 	X0, X2, X2
	VMOVHLPS 	X1, X3, X3
	VMOVQ    	X0, R8
	VMOVQ    	X1, R9
	POPCNTQ  	R8, R8
	ADDQ    	R8, AX
	POPCNTQ  	R9, R9
	ADDQ    	R9, AX
	VMOVQ    	X2, R10
	VMOVQ    	X3, R11
	POPCNTQ  	R10, R10
	ADDQ    	R10, AX
	POPCNTQ  	R11, R11
	ADDQ    	R11, AX

	LEAQ		32(SI), SI
	SUBL		$32, BX
	CMPL		BX, $32
	JB		 	prep_i64
	JMP		 	loop_avx

	// works for data size 31 down to single byte
prep_i64:
    VZEROUPPER
	TESTQ	BX, BX
	JLE		done
	CMPL	BX, $8
	JBE		prep_i8

loop_i64:
	POPCNTQ	(SI), R8
	ADDQ    R8, AX
	LEAQ	8(SI), SI
	SUBL	$8, BX
	CMPL	BX, $8
	JBE		prep_i8
	JMP		loop_i64

prep_i8:
	TESTQ	BX, BX
	JLE		done
	XORQ 	R8, R8

loop_i8:
	MOVB	(SI), R8
	POPCNTW R8, R8
	ADDQ    R8, AX
	INCQ	SI
	DECL	BX
	JZ	 	done
	JMP		loop_i8

done:
	VZEROUPPER
	MOVQ	AX, ret+24(FP)
	RET

// func bitsetIndexesAVX2FullCore(bitmap []byte, out []uint32, decodeTable []uint32, lengthTable []uint8) int
//
TEXT ·bitsetIndexesAVX2FullCore(SB), NOSPLIT, $0-104
	MOVQ	bitmap_base+0(FP), SI
	MOVQ	bitmap_len+8(FP), BX
	MOVQ	out_base+24(FP), DI
	MOVQ    DI, R15                     // save pointer for later
	MOVQ	decodeTable_base+48(FP), BP
	MOVQ	lengthTable_base+72(FP), DX

	TESTQ	BX, BX
	JLE		done


	VPCMPEQQ	    Y10, Y10, Y10           // Y10 = -1
    VPBROADCASTD    const_8<>(SB), Y12      // Y12 = 8

    ADDQ            BX, SI              // SI points to end of array
    NEGQ            BX                  // BX counts from neg to zero
    XORQ            CX, CX
    XORQ            R14, R14

loop_avx2:                             
    MOVB            (SI)(BX*1), CX      // CX = bitmap[BX]  
    LEAQ            (CX*8), R12         // Y0 = decodeTable[8*CX,...]
    VMOVDQU         (BP)(R12*4), Y0

    VPADDD          Y0, Y10, Y0         // Y0 = Y0 + Y10
    VPADDD          Y10, Y12, Y10       // Y10 = Y10 + 8

    VMOVDQU         Y0, (DI)            // [DI] = Y0
    MOVB            (DX)(CX*1), R14     // R14 = lengthTable[CX]
    LEAQ            (DI)(R14*4), DI     // DI = DI + 4*R14

    ADDQ            $1, BX
    JZ              exit_avx2
    JMP             loop_avx2

exit_avx2:
    VZEROUPPER
done:
    SUBQ        R15, DI
    SHRQ        $2, DI
    MOVQ        DI, ret+96(FP)
    RET

// func bitsetIndexesAVX2SkipCore(bitmap []byte, out []uint32, decodeTable []uint32, lengthTable []uint8) int
//
TEXT ·bitsetIndexesAVX2SkipCore(SB), NOSPLIT, $0-104
	MOVQ	bitmap_base+0(FP), SI
	MOVQ	bitmap_len+8(FP), CX
	MOVQ	out_base+24(FP), DI
	MOVQ    DI, R15                 // save pointer for later
	MOVQ	decodeTable_base+48(FP), BP
	MOVQ	lengthTable_base+72(FP), DX

	VPCMPEQQ	    Y10, Y10, Y10           // Y10 = -1
    VPBROADCASTD    const_8<>(SB), Y13      // Y13 = 8
    VPBROADCASTD    const_128<>(SB), Y15    // Y15 = 128
    VPBROADCASTD    const_2048<>(SB), Y14   // Y14 = 2048
	VPXOR		Y8, Y8, Y8   // prepare 0x00.. vector for comparison

start:
	// no more work?
	TESTQ		CX, CX
	JLE			done

	// super quick pre-check if the current byte qualifies already
    XORQ        AX, AX
    XORQ        BX, BX
	CMPB		0(SI)(BX*1), $0
	JNZ			found

	CMPQ		CX, $16		 // slices smaller than 16 byte are handled byte-wise
	JB			prep_i8
	CMPQ		CX, $256     // slices smaller than 256 byte are handled using AVX
	JB			prep_avx


	// works for data size 256 byte
loop_avx2:
    XORQ        BX, BX
	XORQ		AX, AX
	VPCMPEQB	0(SI), Y8, Y0    // set to FF on match (we`ll negate below)
	VPCMPEQB	32(SI), Y8, Y1
	VPCMPEQB	64(SI), Y8, Y2
	VPCMPEQB	96(SI), Y8, Y3
	VPMOVMSKB	Y1, R8
	SHLQ		$32, R8
	VPMOVMSKB	Y3, R9
	SHLQ		$32, R9
	VPMOVMSKB	Y0, R10
	ORQ			R10, R8
	VPMOVMSKB	Y2, R11
	ORQ			R11, R9
	NOTQ		R8			// negate the match mask
	NOTQ		R9
	TZCNTQ		R8, AX
	JNC			found       // CF is set to 1 if input was zero and cleared otherwise
	LEAQ		64(BX), BX
	TZCNTQ		R9, AX
	JNC			found
	LEAQ		64(BX), BX
	VPCMPEQB	128(SI), Y8, Y4
	VPCMPEQB	160(SI), Y8, Y5
	VPCMPEQB	192(SI), Y8, Y6
	VPCMPEQB	224(SI), Y8, Y7
	VPMOVMSKB	Y5, R12
	SHLQ		$32, R12
	VPMOVMSKB	Y7, R13
	SHLQ		$32, R13
	VPMOVMSKB	Y4, R14
	ORQ			R14, R12
	VPMOVMSKB	Y6, R8
	ORQ			R8, R13
	NOTQ		R12
	NOTQ		R13
	TZCNTQ		R12, AX
	JNC			found       // CF is set to 1 if input was zero and cleared otherwise
	LEAQ		64(BX), BX
	TZCNTQ		R13, AX
	JNC			found
	LEAQ		64(BX), BX

    VPADDD      Y10, Y14, Y10       // Y10 = Y10 + 2048
	ADDQ		$256, SI
	SUBQ		$256, CX
	CMPQ		CX, $256
	JB			exit_avx2
	JMP			loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ		CX, CX
	JLE			done
	CMPQ		CX, $16
	JBE			prep_i8

prep_avx:

	// works for data size 16 byte
loop_avx:
    XORQ        BX, BX
	XORQ		AX, AX
	VPCMPEQB	(SI), X8, X0
	VPMOVMSKB	X0, R8
	NOTW		R8
	TZCNTW		R8, AX
	JNC			found        // CF is set to 1 if input was zero and cleared otherwise

    VPADDD      Y10, Y15, Y10       // Y10 = Y10 + 128
	ADDQ		$16, SI
	SUBL		$16, CX
	CMPL		CX, $16
	JB			exit_avx
	JMP			loop_avx

exit_avx:
	VZEROUPPER
	TESTQ	CX, CX
	JLE		done

	// works for data size 15 down to single byte
prep_i8:
	XORQ	AX, AX
	XORQ	BX, BX

loop_i8:
	CMPB	(SI), $0
	JNZ		found

    VPADDD          Y10, Y13, Y10       // Y10 = Y10 + 8
    INCQ    SI
	DECL	CX
	JZ		done
	JMP		loop_i8

done:
    SUBQ        R15, DI
    SHRQ        $2, DI
    MOVQ        DI, ret+96(FP)
    RET

found:
	ADDQ	        AX, BX              // AX+BX is number of skipped zero bytes
    ADDQ            BX, SI
    SUBQ            BX, CX

    XORQ            R8, R8
    MOVB            (SI), R8            // R8 = bitmap[BX]  
    LEAQ            (R8*8), R12         // Y9 = decodeTable[8*R8,...]
    VMOVDQU         (BP)(R12*4), Y9

	VMOVD	        BX, X11              // no direct broadcast in AVX2
    VPSLLD          $3, X11, X11
    VPBROADCASTD    X11, Y12             // Y12 = 8*BX
    VPADDD          Y10, Y12, Y10       // Y10 = Y10 + 8*BX
    VPADDD          Y9, Y10, Y9         // Y9 = Y9 + Y10

    VMOVDQU         Y9, (DI)            // [DI] = Y9
    XORQ            R14, R14
    MOVB            (DX)(R8*1), R14     // R14 = lengthTable[R8]
    LEAQ            (DI)(R14*4), DI     // DI = DI + 4*R14
    
    VPADDD          Y10, Y13, Y10       // Y10 = Y10 + 8
    INCQ            SI
    SUBQ            $1, CX
    JMP             start

// func bitsetNextOneBitAVX2(src []byte, index uint64) uint64
//
TEXT ·bitsetNextOneBitAVX2(SB), NOSPLIT, $0-40
	MOVQ		src_base+0(FP), SI
	MOVQ		src_len+8(FP), CX
	MOVQ		index+24(FP), BX
	SUBQ		BX, CX
	XORQ		AX, AX

	// no more work?
	TESTQ		CX, CX
	JLE			done

	// super quick pre-check if the current byte qualifies already
	CMPB		0(SI)(BX*1), $0
	JNZ			found

	CMPQ		CX, $16		 // slices smaller than 16 byte are handled byte-wise
	JB			prep_i8
	CMPQ		CX, $256     // slices smaller than 256 byte are handled using AVX
	JBE			prep_avx

	VPXOR		Y8, Y8, Y8   // prepare 0x00.. vector for comparison

	// works for data size 256 byte
loop_avx2:
	VPCMPEQB	0(SI)(BX*1), Y8, Y0    // set to FF on match (we`ll negate below)
	VPCMPEQB	32(SI)(BX*1), Y8, Y1
	VPCMPEQB	64(SI)(BX*1), Y8, Y2
	VPCMPEQB	96(SI)(BX*1), Y8, Y3
	VPCMPEQB	128(SI)(BX*1), Y8, Y4
	VPCMPEQB	160(SI)(BX*1), Y8, Y5
	VPCMPEQB	192(SI)(BX*1), Y8, Y6
	VPCMPEQB	224(SI)(BX*1), Y8, Y7
	VPMOVMSKB	Y1, R8
	SHLQ		$32, R8
	VPMOVMSKB	Y3, R9
	SHLQ		$32, R9
	VPMOVMSKB	Y0, R10
	ORQ			R10, R8
	VPMOVMSKB	Y2, R11
	ORQ			R11, R9
	NOTQ		R8			// negate the match mask
	NOTQ		R9
	TZCNTQ		R8, AX
	JNC			found       // CF is set to 1 if input was zero and cleared otherwise
	LEAQ		64(BX), BX
	TZCNTQ		R9, AX
	JNC			found
	LEAQ		64(BX), BX
	VPMOVMSKB	Y5, R12
	SHLQ		$32, R12
	VPMOVMSKB	Y7, R13
	SHLQ		$32, R13
	VPMOVMSKB	Y4, R14
	ORQ			R14, R12
	VPMOVMSKB	Y6, R15
	ORQ			R15, R13
	NOTQ		R12
	NOTQ		R13
	TZCNTQ		R12, AX
	JNC			found       // CF is set to 1 if input was zero and cleared otherwise
	LEAQ		64(BX), BX
	TZCNTQ		R13, AX
	JNC			found
	LEAQ		64(BX), BX
	SUBQ		$256, CX
	CMPQ		CX, $256
	JB			exit_avx2
	JMP			loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ		CX, CX
	JLE			done
	CMPQ		CX, $16
	JBE			prep_i8

prep_avx:
	VPXOR		X8, X8, X8    // value to compare: 0x00...
	XORQ		R8, R8
	XORQ		AX, AX

	// works for data size 16 byte
loop_avx:
	VPCMPEQB	0(SI)(BX*1), X8, X0
	VPMOVMSKB	X0, R8
	NOTW		R8
	TZCNTW		R8, AX
	JNC			found        // CF is set to 1 if input was zero and cleared otherwise
	LEAQ		16(BX), BX
	SUBL		$16, CX
	CMPL		CX, $16
	JB			exit_avx
	JMP			loop_avx

exit_avx:
	VZEROUPPER
	TESTQ	CX, CX
	JLE		done

	// works for data size 15 down to single byte
prep_i8:
	XORQ	AX, AX

loop_i8:
	CMPB	0(SI)(BX*1), $0
	JNZ		found
	INCQ	BX
	DECL	CX
	JZ		done
	JMP		loop_i8

done:
	XORQ	AX, AX

found:
	ADDQ	BX, AX      // add position in slice
	MOVQ	AX, ret+32(FP)
	RET

// func bitsetNextZeroBitAVX2(src []byte, index uint64) uint64
//
TEXT ·bitsetNextZeroBitAVX2(SB), NOSPLIT, $0-40
	MOVQ		src_base+0(FP), SI
	MOVQ		src_len+8(FP), CX
	MOVQ		index+24(FP), BX
	SUBQ		BX, CX
	XORQ		AX, AX

	// no more work?
	TESTQ		CX, CX
	JLE			done

	// super quick pre-check if the current byte qualifies already
	CMPB		0(SI)(BX*1), $-1
	JL			found

	CMPL		CX, $16		 // slices smaller than 16 byte are handled byte-wise
	JB			prep_i8
	CMPQ		CX, $256     // slices smaller than 256 byte are handled using AVX
	JBE			prep_avx

	VPCMPEQQ	Y8, Y8, Y8   // prepare 0xff.. vector for comparison

	// works for data size 256 byte
loop_avx2:
	VPCMPEQB	0(SI)(BX*1), Y8, Y0    // set to FF on match (we`ll negate below)
	VPCMPEQB	32(SI)(BX*1), Y8, Y1
	VPCMPEQB	64(SI)(BX*1), Y8, Y2
	VPCMPEQB	96(SI)(BX*1), Y8, Y3
	VPCMPEQB	128(SI)(BX*1), Y8, Y4
	VPCMPEQB	160(SI)(BX*1), Y8, Y5
	VPCMPEQB	192(SI)(BX*1), Y8, Y6
	VPCMPEQB	224(SI)(BX*1), Y8, Y7
	VPMOVMSKB	Y1, R8
	SHLQ		$32, R8
	VPMOVMSKB	Y3, R9
	SHLQ		$32, R9
	VPMOVMSKB	Y0, R10
	ORQ			R10, R8
	VPMOVMSKB	Y2, R11
	ORQ			R11, R9
	NOTQ		R8
	NOTQ		R9
	TZCNTQ		R8, AX
	JNC			found     // CF is set to 1 if input was zero and cleared otherwise
	LEAQ		64(BX), BX
	TZCNTQ		R9, AX
	JNC			found
	LEAQ		64(BX), BX
	VPMOVMSKB	Y5, R12
	SHLQ		$32, R12
	VPMOVMSKB	Y7, R13
	SHLQ		$32, R13
	VPMOVMSKB	Y4, R14
	ORQ			R14, R12
	VPMOVMSKB	Y6, R15
	ORQ			R15, R13
	NOTQ		R12
	NOTQ		R13
	TZCNTQ		R12, AX
	JNC			found     // CF is set to 1 if input was zero and cleared otherwise
	LEAQ		64(BX), BX
	TZCNTQ		R13, AX
	JNC			found
	LEAQ		64(BX), BX
	SUBQ		$256, CX
	CMPQ		CX, $256
	JB			exit_avx2
	JMP			loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ		CX, CX
	JLE			done
	CMPQ		CX, $16
	JBE			prep_i8

prep_avx:
	VPCMPEQD	X8, X8, X8   // value to compare: 0xFF...
	XORQ		R8, R8
	XORQ		AX, AX

	// works for data size 16 byte
loop_avx:
	VPCMPEQB	0(SI)(BX*1), X8, X0
	VPMOVMSKB	X0, R8
	NOTW		R8
	TZCNTW		R8, AX
	JNC			found     // CF is set to 1 if input was zero and cleared otherwise
	LEAQ		16(BX), BX
	SUBL		$16, CX
	CMPL		CX, $16
	JB			exit_avx
	JMP			loop_avx

exit_avx:
	VZEROUPPER
	TESTQ		CX, CX
	JLE			done

	// works for data size 15 down to single byte
prep_i8:
	MOVB	$(-1), R8    // value to compare (0xFF)
	XORQ	AX, AX

loop_i8:
	CMPB	0(SI)(BX*1), R8
	JNZ		found
	INCQ	BX
	DECL	CX
	JZ		done
	JMP		loop_i8

done:
	XORQ	AX, AX

found:
	ADDQ	BX, AX            // add position in slice
	MOVQ	AX, ret+32(FP)
	RET
