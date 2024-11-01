// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants.h"

// func zzDeltaDecodeUint64AVX2Core(data []uint64)
//
// input:
//   SI = src_base
//   BX = src_len
TEXT ·zzDeltaDecodeUint64AVX2Core(SB), NOSPLIT, $0-24
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

    SHRQ    $2, BX              // calculate number of steps (divide by 4)

	TESTQ	BX, BX
	JLE		done

prep_avx:
	VPCMPEQQ	Y15, Y15, Y15
	VPSRLQ		$63, Y15, Y15	// Y15 = [1,1,...]
	VPXOR		Y14, Y14, Y14 	// Y14 = [0,0,...]
	VPXOR		Y0, Y0, Y0 		// start value Y0 = [0,0,...]

prep_big:

loop_big:
	VMOVDQU		(SI), Y4
    VPERMQ      $255, Y0, Y0

	// zigzag
	VPSRLQ		$1, Y4, Y1
	VPAND		Y4, Y15, Y4
	VPSUBQ		Y4, Y14, Y4
	VPXOR		Y4, Y1, Y1

	// delta
    VPERM2F128  $8, Y1, Y1, Y2
    VPALIGNR    $8, Y2, Y1, Y2
    VPADDQ      Y1, Y2, Y2
    VPERM2F128  $8, Y2, Y2, Y3
    VPADDQ      Y2, Y3, Y3

    VPADDQ      Y3, Y0, Y0
    
    VMOVDQU     Y0, (SI)

	ADDQ		$32, SI
	SUBQ		$1, BX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:

prep_small:

loop_small:

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
	RET

// func zzDeltaDecodeInt64AVX2Core(data []uint64)
//
// input:
//   SI = src_base
//   BX = src_len
TEXT ·zzDeltaDecodeInt64AVX2Core(SB), NOSPLIT, $0-24
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

    SHRQ    $2, BX              // calculate number of steps (divide by 4)

	TESTQ	BX, BX
	JLE		done

prep_avx:
	VPCMPEQQ	Y15, Y15, Y15
	VPSRLQ		$63, Y15, Y15	// Y15 = [1,1,...]
	VPXOR		Y14, Y14, Y14 	// Y14 = [0,0,...]
	VPXOR		Y0, Y0, Y0 		// start value Y0 = [0,0,...]

prep_big:
	VMOVDQU		(SI), Y4

	// zigzag
	VPSRLQ		$1, Y4, Y1
	VPAND		Y4, Y15, Y4
	VPSUBQ		Y4, Y14, Y4
	VPXOR		Y4, Y1, Y6

	// delta 1st part
    VPERM2F128  $8, Y6, Y6, Y7
    VPALIGNR    $8, Y7, Y6, Y7

	SUBQ		$1, BX
	JZ			exit_big

loop_big:
	VMOVDQU		32(SI), Y9

	// delta 2nd part
    VPADDQ      Y6, Y7, Y2
    VPERM2F128  $8, Y2, Y2, Y3

	// zigzag
	VPSRLQ		$1, Y9, Y6
	VPAND		Y9, Y15, Y9
	VPSUBQ		Y9, Y14, Y9
	VPXOR		Y9, Y6, Y6

    VPADDQ      Y2, Y3, Y3

    VPADDQ      Y3, Y0, Y0

	// delta 1st part
    VPERM2F128  $8, Y6, Y6, Y7
    VPALIGNR    $8, Y7, Y6, Y7

    VMOVDQU     Y0, (SI)
    VPERMQ      $255, Y0, Y0

	ADDQ		$32, SI
	SUBQ		$1, BX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	// delta 2nd part
    VPADDQ      Y6, Y7, Y2
    VPERM2F128  $8, Y2, Y2, Y3
    VPADDQ      Y2, Y3, Y3

    VPADDQ      Y3, Y0, Y0
    
    VMOVDQU     Y0, (SI)

prep_small:

loop_small:

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
	RET

// func zzDeltaDecodeInt32AVX2Core(data []int32)
//
// input:
//   SI = src_base
//   BX = src_len
TEXT ·zzDeltaDecodeInt32AVX2Core(SB), NOSPLIT, $0-24
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

    SHRQ    $3, BX              // calculate number of steps (divide by 8)

	TESTQ	BX, BX
	JLE		done

prep_avx:
	VPCMPEQQ	Y11, Y11, Y11
	VPCMPEQQ	X12, X12, X12
	VPXOR		Y11, Y12, Y12	// Y12 = [0,...,0xffffffff,...]

	VPCMPEQQ	Y15, Y15, Y15
	VPSRLD		$30, Y15, Y13	// Y13 = [3,3,...]
	VPSRLD		$29, Y15, Y11	// Y11 = [7,7,...]
	VPSRLD		$31, Y15, Y15	// Y15 = [1,1,...]
	VPXOR		Y14, Y14, Y14 	// Y14 = [0,0,...]
	VPXOR		Y0, Y0, Y0 		// start value Y0 = [0,0,...]

prep_big:
	VMOVDQU		(SI), Y4

	// zigzag
	VPSRLD		$1, Y4, Y1
	VPAND		Y4, Y15, Y4
	VPSUBD		Y4, Y14, Y4
	VPXOR		Y4, Y1, Y1

	// delta
	VPSLLDQ 	$8, Y1, Y2
	VPADDD		Y1, Y2, Y2
	VPSLLDQ 	$4, Y2, Y3
	VPADDD		Y2, Y3, Y8

	SUBQ		$1, BX
	JZ			exit_big

loop_big:
	VMOVDQU		32(SI), Y9

	VPERMD		Y8, Y13, Y4

	// zigzag
	VPSRLD		$1, Y9, Y6
	VPAND		Y9, Y15, Y9
	VPSUBD		Y9, Y14, Y9
	VPXOR		Y9, Y6, Y6

	VPAND		Y4, Y12, Y4
	VPADDD		Y4, Y8, Y3

	// delta
	VPSLLDQ 	$8, Y6, Y7
	VPADDD		Y6, Y7, Y7
	VPSLLDQ 	$4, Y7, Y8
	VPADDD		Y7, Y8, Y8

	VPADDD		Y0, Y3, Y0

    VMOVDQU     Y0, (SI)
	VPERMD		Y0, Y11, Y0

	ADDQ		$32, SI
	SUBQ		$1, BX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	VPERMD		Y8, Y13, Y4
	VPAND		Y4, Y12, Y4
	VPADDD		Y4, Y8, Y3

	VPADDD		Y0, Y3, Y0

    VMOVDQU     Y0, (SI)

prep_small:

loop_small:

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
	RET

// func zzDeltaDecodeInt16AVX2Core(data []int16)
//
// input:
//   SI = src_base
//   BX = src_len
TEXT ·zzDeltaDecodeInt16AVX2Core(SB), NOSPLIT, $0-24
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

	CMPQ	BX, $3			// slices smaller than 4 are not handled by the core function
	JBE		done

prep_avx:
	VPCMPEQQ	Y15, Y15, Y15
	VPSRLW		$15, Y15, Y15	// Y15 = [1,1,...]
	VPXOR		Y14, Y14, Y14 	// Y14 = [0,0,...]
	VPBROADCASTW		shuffle16<>+0x00(SB), Y13    // load shuffle control mask

	VPXOR		Y0, Y0, Y0 		// start value Y0 = [0,0,...]

    MOVQ    BX, CX
    ANDQ    $0xf, CX          	// number of values left
    SHRQ    $4, BX          	// calculate number of steps of big loop (divide by 32)
	JZ		prep_small

prep_big:
	VMOVDQU		(SI), Y4

	// zigzag
	VPSRLW		$1, Y4, Y1
	VPAND		Y4, Y15, Y4
	VPSUBW		Y4, Y14, Y4
	VPXOR		Y4, Y1, Y1

	// delta
	VPSLLQ  	$32, Y1, Y2
	VPADDW		Y1, Y2, Y2
	VPSLLQ  	$16, Y2, Y3
	VPADDW		Y2, Y3, Y8

	SUBQ		$1, BX
	JZ			exit_big

loop_big:
	VMOVDQU		32(SI), Y9

	VPSHUFLW	$255, Y8, Y4
	VPSLLDQ		$8, Y4, Y4
	VPADDW		Y4, Y8, Y3

	VPSHUFB		Y13, Y3, Y1
	VPERM2F128	$8, Y1, Y1, Y1
	VPADDW		Y3, Y1, Y3

	VPADDW		Y0, Y3, Y0

    VMOVDQU     Y0, (SI)
	VPERMQ		$255, Y0, Y0
	VPSHUFB		Y13, Y0, Y0

	// zigzag
	VPSRLW		$1, Y9, Y6
	VPAND		Y9, Y15, Y9
	VPSUBW		Y9, Y14, Y9
	VPXOR		Y9, Y6, Y6

	// delta
	VPSLLQ  	$32, Y6, Y7
	VPADDW		Y6, Y7, Y7
	VPSLLQ  	$16, Y7, Y8
	VPADDW		Y7, Y8, Y8

	ADDQ		$32, SI

	SUBQ		$1, BX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	VPSHUFLW	$255, Y8, Y4
	VPSLLDQ		$8, Y4, Y4
	VPADDW		Y4, Y8, Y3

	VPSHUFB		Y13, Y3, Y1
	VPERM2F128	$8, Y1, Y1, Y1
	VPADDW		Y3, Y1, Y3

	VPADDW		Y0, Y3, Y0

    VMOVDQU     Y0, (SI)

	VPERMQ		$255, Y0, Y0
	VPSHUFB		Y13, Y0, Y0

	ADDQ		$32, SI

prep_small:
	VPBROADCASTW		shuffle16_1<>+0x00(SB), X13   // load shuffle control mask

    SHRQ    $2, CX          	// calculate number of steps of small loop (divide by 4)
	JZ		exit_avx

loop_small:
	VMOVQ		(SI), X4
	VPSHUFB		X13, X0, X0

	// zigzag
	VPSRLW		$1, X4, X1
	VPAND		X4, X15, X4
	VPSUBW		X4, X14, X4
	VPXOR		X4, X1, X1

	// delta
	VPSLLQ  	$32, X1, X2
	VPADDW		X1, X2, X2
	VPSLLQ  	$16, X2, X3
	VPADDW		X2, X3, X3

	VPADDW		X0, X3, X0

    VMOVQ     	X0, (SI)

	ADDQ		$8, SI

	SUBQ		$1, CX
	JZ		 	exit_small
	JMP		 	loop_small

exit_small:

exit_avx:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
	RET

// func zzDeltaDecodeInt8AVX2Core(data []int8)
//
// input:
//   SI = src_base
//   BX = src_len
TEXT ·zzDeltaDecodeInt8AVX2Core(SB), NOSPLIT, $0-24
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

	CMPQ	BX, $7			// slices smaller than 8 are not handled by the core function
	JBE		done

prep_avx:
	VPBROADCASTB		const8_01<>+0x00(SB), Y15   // Y15 = [1,1,...]
	VPXOR				Y14, Y14, Y14				// Y14 = [0,0,...]
	VPBROADCASTB		shuffle8<>+0x00(SB), Y13   // load shuffle control mask
	VPBROADCASTB		const8_7f<>+0x00(SB), Y12	// Y12 = [0x7f,0x7f,...]
	VMOVDQU				shuffle81<>+0x00(SB), Y11   // load shuffle control mask
	VMOVDQU				shuffle82<>+0x00(SB), Y10   // load shuffle control mask

	VPXOR		Y0, Y0, Y0 		// start value Y0 = [0,0,...]

    MOVQ    BX, CX
    ANDQ    $0x1f, CX          	// number of values left
    SHRQ    $5, BX          	// calculate number of steps of big loop (divide by 32)
	JZ		prep_small

prep_big:

	VMOVDQU		(SI), Y4

	// zigzag
	VPSRLW		$1, Y4, Y1
	VPAND		Y1, Y12, Y1
	VPAND		Y4, Y15, Y4
	VPSUBB		Y4, Y14, Y4
	VPXOR		Y4, Y1, Y1

	// delta
	VPSLLD  	$16, Y1, Y2
	VPADDB		Y1, Y2, Y2
	VPSLLD  	$8, Y2, Y3
	VPADDB		Y2, Y3, Y8

	SUBQ		$1, BX
	JZ			exit_big

loop_big:
	VMOVDQU		32(SI), Y9

	VPSHUFB		Y11, Y8, Y4
	VPADDB		Y4, Y8, Y3

	// zigzag
	VPSRLW		$1, Y9, Y6
	VPAND		Y6, Y12, Y6
	VPAND		Y9, Y15, Y9
	VPSUBB		Y9, Y14, Y9
	VPXOR		Y9, Y6, Y6

	VPSHUFB		Y10, Y3, Y4
	VPADDB		Y4, Y3, Y3

	VPSHUFB		Y13, Y3, Y1
	VPERM2F128  $8, Y1, Y1, Y1
	VPADDB		Y1, Y3, Y3

	// delta
	VPSLLD  	$16, Y6, Y7
	VPADDB		Y6, Y7, Y7
	VPSLLD  	$8, Y7, Y8
	VPADDB		Y7, Y8, Y8

	VPADDB		Y0, Y3, Y0

    VMOVDQU		Y0, (SI)
	VPERMQ		$255, Y0, Y0
	VPSHUFB		Y13, Y0, Y0

	ADDQ		$32, SI
	SUBQ		$1, BX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	VPSHUFB		Y11, Y8, Y4
	VPADDB		Y4, Y8, Y3

	VPSHUFB		Y10, Y3, Y4
	VPADDB		Y4, Y3, Y3

	VPSHUFB		Y13, Y3, Y1
	VPERM2F128  $8, Y1, Y1, Y1
	VPADDB		Y1, Y3, Y3

	VPADDB		Y0, Y3, Y0

    VMOVDQU		Y0, (SI)

	VPERMQ		$255, Y0, Y0
	VPSHUFB		Y13, Y0, Y0

	ADDQ		$32, SI

prep_small:
	VPBROADCASTB		shuffle83<>+0x00(SB), X13   // load shuffle control mask

    SHRQ    $3, CX          	// calculate number of steps of small loop (divide by 8)
	JZ		exit_avx

loop_small:
	VMOVQ		(SI), X4
	VPSHUFB		X13, X0, X0

	// zigzag
	VPSRLW		$1, X4, X1
	VPAND		X1, X12, X1
	VPAND		X4, X15, X4
	VPSUBB		X4, X14, X4
	VPXOR		X4, X1, X1

	// delta
	VPSLLD  	$16, X1, X2
	VPADDB		X1, X2, X2
	VPSLLD  	$8, X2, X3
	VPADDB		X2, X3, X8

	VPSHUFB		X11, X8, X4
	VPADDB		X4, X8, X3

	VPADDB		X0, X3, X0

    VMOVQ		X0, (SI)

	ADDQ		$8, SI
	SUBQ		$1, CX
	JZ		 	exit_small
	JMP		 	loop_small

exit_small:

exit_avx:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
	RET

// func deltaDecodeTimeAVX2Core(src []uint64, mod uint64)
//
TEXT ·deltaDecodeTimeAVX2Core(SB), NOSPLIT, $0-32
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

	TESTQ	BX, BX
	JLE		done

    SHRQ    $2, BX      // number of loops (div by 4)
	JZ		done

prep_avx:
    VPBROADCASTQ    mod+24(FP), Y5      // Y5 = mod
    VPSLLQ          $32, Y5, Y6
    VPXOR           Y0, Y0, Y0          // Y0 = 0 (start value)

loop_avx:
    VMOVDQU     (SI), Y1

    // mult 64bit x 64bit
    VPMULLD     Y1, Y6, Y2
    VPMULUDQ    Y1, Y5, Y3
    VPADDQ      Y2, Y3, Y1

    // delta
    VPERMQ      $255, Y0, Y4
    VPERM2F128  $8, Y1, Y1, Y2
    VPALIGNR    $8, Y2, Y1, Y2
    VPADDQ      Y1, Y2, Y2
    VPERM2F128  $8, Y2, Y2, Y3
    VPADDQ      Y2, Y3, Y3
    VPADDQ      Y3, Y4, Y0

    VMOVDQU     Y0, (SI)

    ADDQ        $32, SI
    SUBQ        $1, BX
	JZ		 	exit_avx
	JMP		 	loop_avx

exit_avx:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

done:
	RET

// func zzDeltaDecodeTimeAVX2Core(src []uint64, mod uint64)
//
TEXT ·zzDeltaDecodeTimeAVX2Core(SB), NOSPLIT, $0-32
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

	TESTQ	BX, BX
	JLE		done

    SHRQ    $2, BX      // number of loops (div by 4)
	JZ		done

prep_avx:
	VPCMPEQQ	Y15, Y15, Y15
	VPSRLQ		$63, Y15, Y15	// Y15 = [1,1,...]
	VPXOR		Y14, Y14, Y14 	// Y14 = [0,0,...]

    VPBROADCASTQ    mod+24(FP), Y5      // Y5 = mod
    VPSLLQ          $32, Y5, Y6
    VPXOR           Y0, Y0, Y0          // Y0 = 0 (start value)

loop_avx:
    VMOVDQU     (SI), Y4

	// zigzag
	VPSRLQ		$1, Y4, Y1
	VPAND		Y4, Y15, Y4
	VPSUBQ		Y4, Y14, Y4
	VPXOR		Y4, Y1, Y1

    // mult 64bit x 64bit
    VPMULLD     Y1, Y6, Y2
    VPMULUDQ    Y1, Y5, Y3
    VPADDQ      Y2, Y3, Y1

    // delta
    VPERMQ      $255, Y0, Y4
    VPERM2F128  $8, Y1, Y1, Y2
    VPALIGNR    $8, Y2, Y1, Y2
    VPADDQ      Y1, Y2, Y2
    VPERM2F128  $8, Y2, Y2, Y3
    VPADDQ      Y2, Y3, Y3
    VPADDQ      Y3, Y4, Y0

    VMOVDQU     Y0, (SI)

    ADDQ        $32, SI
    SUBQ        $1, BX
	JZ		 	exit_avx
	JMP		 	loop_avx

exit_avx:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

done:
	RET

