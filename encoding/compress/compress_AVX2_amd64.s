// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"

// func zzDecodeInt64AVX2Core(data []int64)
//
// input:
//   SI = src_base
//   BX = src_len
TEXT ·zzDecodeInt64AVX2Core(SB), NOSPLIT, $0-24
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

    SHRQ    $2, BX              // calculate number of steps (divide by 4)

	TESTQ	BX, BX
	JLE		done

prep_avx:
	VPCMPEQQ	Y15, Y15, Y15
	VPSRLQ		$63, Y15, Y15	// Y15 = [1,1,...]
	VPXOR		Y14, Y14, Y14 	// Y14 = [0,0,...]

prep_big:

loop_big:
	VMOVDQU		(SI), Y0
	VPSRLQ		$1, Y0, Y1
	VPAND		Y0, Y15, Y0
	VPSUBQ		Y0, Y14, Y0
	VPXOR		Y0, Y1, Y0

	VMOVDQU		Y0, (SI)

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

// func zzDecodeUint64AVX2Core(data []int64)
//
// input:
//   SI = src_base
//   BX = src_len
TEXT ·zzDecodeUint64AVX2Core(SB), NOSPLIT, $0-24
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

    SHRQ    $2, BX              // calculate number of steps (divide by 4)

	TESTQ	BX, BX
	JLE		done

prep_avx:
	VPCMPEQQ	Y15, Y15, Y15
	VPSRLQ		$63, Y15, Y15	// Y15 = [1,1,...]
	VPXOR		Y14, Y14, Y14 	// Y14 = [0,0,...]

prep_big:

loop_big:
	VMOVDQU		(SI), Y0
	VPSRLQ		$1, Y0, Y1
	VPAND		Y0, Y15, Y0
	VPSUBQ		Y0, Y14, Y0
	VPXOR		Y0, Y1, Y0

	VMOVDQU		Y0, (SI)

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

// func deltaDecodeAVX2Core(data []int64)  
//
TEXT ·deltaDecodeInt64AVX2Core(SB), NOSPLIT, $0-24
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $3      // slices smaller than 4 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
    VPXOR       Y0, Y0, Y0          // Y0 = 0 (start value)
    
loop_avx:
    VMOVDQU     (SI), Y1
    VPERM2F128  $8, Y1, Y1, Y2
    VPALIGNR    $8, Y2, Y1, Y2
    VPADDQ      Y1, Y2, Y2
    VPERMQ      $255, Y0, Y4
    
    VPERM2F128  $8, Y2, Y2, Y3
    VPADDQ      Y2, Y3, Y3

    VPADDQ      Y3, Y4, Y0
    
    VMOVDQU     Y0, (SI)

    ADDQ        $32, SI
    SUBQ        $4, BX
    CMPQ        BX, $4
	JB		 	exit_avx
	JMP		 	loop_avx

exit_avx:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
done:
	RET

// func zzdeltaDecodeInt64AVX2Core(data []int64)
//
// input:
//   SI = src_base
//   BX = src_len
TEXT ·zzdeltaDecodeInt64AVX2Core(SB), NOSPLIT, $0-24
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
	VMOVDQU		(SI), Y5

	// zigzag
	VPSRLQ		$1, Y5, Y1
	VPAND		Y5, Y15, Y5
	VPSUBQ		Y5, Y14, Y5
	VPXOR		Y5, Y1, Y1

	// delta
    VPERM2F128  $8, Y1, Y1, Y2
    VPALIGNR    $8, Y2, Y1, Y2
    VPADDQ      Y1, Y2, Y2
    VPERMQ      $255, Y0, Y4
    
    VPERM2F128  $8, Y2, Y2, Y3
    VPADDQ      Y2, Y3, Y3

    VPADDQ      Y3, Y4, Y0
    
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

// func zzdeltaDecodeUint64AVX2Core(data []uint64)
//
// input:
//   SI = src_base
//   BX = src_len
TEXT ·zzdeltaDecodeUint64AVX2Core(SB), NOSPLIT, $0-24
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
	VMOVDQU		(SI), Y5

	// zigzag
	VPSRLQ		$1, Y5, Y1
	VPAND		Y5, Y15, Y5
	VPSUBQ		Y5, Y14, Y5
	VPXOR		Y5, Y1, Y1

	// delta
    VPERM2F128  $8, Y1, Y1, Y2
    VPALIGNR    $8, Y2, Y1, Y2
    VPADDQ      Y1, Y2, Y2
    VPERMQ      $255, Y0, Y4
    
    VPERM2F128  $8, Y2, Y2, Y3
    VPADDQ      Y2, Y3, Y3

    VPADDQ      Y3, Y4, Y0
    
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

// func delta8EncodeUint64AVX2Core(src []uint64) uint64
//
// input:
//   SI = src_base
//   BX = src_len
// internal:
//   R9 = maxdelta
//   Y14, Y15 = accumulators for maxdelta
//   Y0, Y1 = old vector data (to substract from)
//   Y2, Y3 = new vector data (to substract)
TEXT ·delta8EncodeUint64AVX2Core(SB), NOSPLIT, $0-32
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	XORQ	R9, R9

    LEAQ    (SI)(BX*8), SI      // move SI to end of array
    SHRQ    $3, BX              // calculate number of steps (divide by 8)
    SUBQ    $1, BX              // one less 

	TESTQ	BX, BX
	JLE		done

prep_avx:
    VPXOR   	Y15, Y15, Y15       // set maxdelta to 0
    VPXOR   	Y14, Y14, Y14       // set maxdelta to 0
    SUBQ        $64, SI
	VMOVDQU	    0(SI), Y0
	VMOVDQU 	32(SI), Y1

prep_big:

// works for >= 8 int64 (i.e. 64 bytes of data)
loop_big:
    SUBQ        $64, SI
	VMOVDQU	    0(SI), Y2
	VMOVDQU 	32(SI), Y3
    VPSUBQ      Y2, Y0, Y4
    VPSUBQ      Y3, Y1, Y5

	VMOVDQU		Y2, Y0
	VMOVDQU		Y3, Y1

    VPOR        Y4, Y14, Y14
    VPOR        Y5, Y15, Y15

	VMOVDQU		Y4, 64(SI)
	VMOVDQU		Y5, 96(SI)

	SUBQ		$1, BX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
    VPOR        Y14, Y15, Y15

prep_small:

loop_small:

exit_small:
	// this is an horizontal OR
	VPERM2I128		$0x81, Y15, Y15, Y0
	VPOR			Y15, Y0, Y0
	VEXTRACTF128	$0, Y0, X0
	VPEXTRQ			$0, X0, R8
	VPEXTRQ			$1, X0, R9
	ORQ				R8, R9

	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
	MOVQ	R9, ret+24(FP)
	RET

// func delta8DecodeUint64AVX2Core(src []uint64)
//
// input:
//   SI = src_base
//   BX = src_len
// internal:
//   R9 = maxdelta
//   Y0, Y1 = old vector data (to substract from)
//   Y2, Y3 = new vector data (to substract)
TEXT ·delta8DecodeUint64AVX2Core(SB), $0-24
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

    SHRQ    $3, BX              // calculate number of steps (divide by 8)
    SUBQ    $1, BX              // one less 

	TESTQ	BX, BX
	JLE		done

prep_avx:
	VMOVDQU	    0(SI), Y0
	VMOVDQU 	32(SI), Y1
    ADDQ        $64, SI

prep_big:

// works for >= 8 int64 (i.e. 64 bytes of data)
loop_big:
	VMOVDQU	    0(SI), Y2
	VMOVDQU 	32(SI), Y3
    VPADDQ      Y2, Y0, Y0
    VPADDQ      Y3, Y1, Y1

	VMOVDQU		Y0, 0(SI)
	VMOVDQU		Y1, 32(SI)

    ADDQ        $64, SI
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
