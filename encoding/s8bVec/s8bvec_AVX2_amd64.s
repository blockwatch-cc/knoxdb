// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefanx@blockwatch.cc

#include "textflag.h"
#include "constants_AVX2.h"

// func countValuesAVX2Core(src []byte) (count int)
//
// input:
//   SI = src_base
//   BX = src_len
// internal:
//   Y15 = LUT selector -> number of values
//   Y14 = selector mask for using 4x64bit vector
//   Y13 = selector mask for using 8x32bit vector
//   Y12, Y11 = sum registers
//   Y0-Y3 = vector data
//   BX = remaining bytes
//   CX = loop counter 
TEXT ·countValuesAVX2Core(SB), NOSPLIT, $0-32
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

        XORQ    AX, AX
        CMPQ    BX, $0
	JE	done

prep_avx:
        VPXOR           Y12, Y12, Y12                   // set sum to zero
        VPXOR           Y11, Y11, Y11                   // set sum to zero
	VPBROADCASTQ    sel_mask64<>+0x00(SB), Y14      // load selector mask
	VPBROADCASTD    sel_mask32<>+0x00(SB), Y13      // load selector mask
	VMOVDQU		sel_LUT<>+0x00(SB), Y15         // load LUT

prep_big:
        MOVQ    BX, CX
        ANDQ    $0x7f, BX                   // number of bytes left
        SHRQ    $7, CX                      // number of runs of big loop
        JZ      prep_small

loop_big:
        // for little endian 
        VMOVDQU           (SI), Y0 
        VMOVDQU           32(SI), Y1
        VMOVDQU           64(SI), Y2
        VMOVDQU           96(SI), Y3
	VPSRLQ	        $60, Y0, Y0      // determine selector
	VPSRLQ	        $60, Y1, Y1      // determine selector
	VPSRLQ	        $60, Y2, Y2      // determine selector
	VPSRLQ	        $60, Y3, Y3     // determine selector

	VPSLLQ	        $32, Y1, Y1      // combine selector vectors
        VPOR            Y1, Y0, Y0
	VPSLLQ	        $32, Y3, Y3      // combine selector vectors
        VPOR            Y3, Y2, Y2

	VPSHUFB	        Y0, Y15, Y0     // look up number of values
        VPAND           Y0, Y13, Y0     // clear unused values
	VPSHUFB	        Y2, Y15, Y2     // look up number of values
        VPAND           Y2, Y13, Y2     // clear unused values

        VPADDD          Y12, Y0, Y12    // add number of values
        VPADDD          Y11, Y2, Y11    // add number of values

	ADDQ		$128, SI
        SUBQ            $1, CX
	JZ	 	exit_big
	JMP	 	loop_big

exit_big:
        VPADDD          Y11, Y12, Y12

prep_small:
        MOVQ    BX, CX
        ANDQ    $0x1f, BX               // number of bytes left
        SHRQ    $5, CX                  // number of runs of small loop
        JZ      exit_small

loop_small:
        // for little endian 
        VMOVDQU           (SI), Y0 
	VPSRLQ	        $60, Y0, Y0      // determine selector

	VPSHUFB	        Y0, Y15, Y0     // look up number of values
        VPAND           Y0, Y14, Y0     // clear unused values

        VPADDD          Y12, Y0, Y12    // add number of values

	ADDQ		$32, SI
        SUBQ            $1, CX
	JZ	 	exit_small
	JMP	 	loop_small

exit_small:
        SHRQ            $3, BX                  // number of 64bit words
        VMOVDQU         countdown<>(SB), Y2
        MOVQ            BX, X0
        VPBROADCASTQ    X0, Y1                  // broadcast BX
        VPCMPGTQ        Y2, Y1, Y1              // mask remaining values

        VPMASKMOVQ      (SI), Y1, Y0            // load remaining values

        // for little endian 
	VPSRLQ	        $60, Y0, Y0      // determine selector

	VPSHUFB	        Y0, Y15, Y0     // look up number of values
        VPAND           Y0, Y14, Y0     // clear unused values
        VPAND           Y0, Y1, Y0      // cut vector

        VPADDD          Y12, Y0, Y12    // add number of values

        // finish: add all values in Y12
        VPHADDD         Y12, Y12, Y12
        VPHADDD         Y12, Y12, Y12
        VEXTRACTI128    $1, Y12, X0
        VPADDD          X0, X12, X12

	VMOVD	        X12, AX
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
        MOVQ            AX, count+24(FP)
        MOVQ            $100, AX
	RET

// func countValuesBigEndianAVX2Core(src []byte) (count int)
//
// input:
//   SI = src_base
//   BX = src_len
// internal:
//   Y15 = LUT selector -> number of values
//   Y14 = selector mask for using 4x64bit vector
//   Y13 = selector mask for using 8x32bit vector
//   Y12, Y11 = sum registers
//   Y0-Y3 = vector data
//   BX = remaining bytes
//   CX = loop counter 
TEXT ·countValuesBigEndianAVX2Core(SB), NOSPLIT, $0-32
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

        XORQ    AX, AX
        CMPQ    BX, $0
	JE	done

prep_avx:
        VPXOR           Y12, Y12, Y12                   // set sum to zero
        VPXOR           Y11, Y11, Y11                   // set sum to zero
	VPBROADCASTQ    sel_mask64<>+0x00(SB), Y14      // load selector mask
	VPBROADCASTD    sel_mask32<>+0x00(SB), Y13      // load selector mask
	VMOVDQU		sel_LUT<>+0x00(SB), Y15         // load LUT

prep_big:
        MOVQ    BX, CX
        ANDQ    $0x7f, BX                   // number of bytes left
        SHRQ    $7, CX                      // number of runs of big loop
        JZ      prep_small

loop_big:
        // for big endian
        VPAND           (SI), Y14, Y0   // determine selector
        VPAND           32(SI), Y14, Y1   // determine selector
        VPAND           64(SI), Y14, Y2   // determine selector
        VPAND           96(SI), Y14, Y3   // determine selector
	VPSRLQ	        $4, Y0, Y0      // determine selector
	VPSRLQ	        $4, Y1, Y1      // determine selector
	VPSRLQ	        $4, Y2, Y2      // determine selector
	VPSRLQ	        $4, Y3, Y3      // determine selector

	VPSLLQ	        $32, Y1, Y1      // combine selector vectors
        VPOR            Y1, Y0, Y0
	VPSLLQ	        $32, Y3, Y3      // combine selector vectors
        VPOR            Y3, Y2, Y2

	VPSHUFB	        Y0, Y15, Y0     // look up number of values
        VPAND           Y0, Y13, Y0     // clear unused values
	VPSHUFB	        Y2, Y15, Y2     // look up number of values
        VPAND           Y2, Y13, Y2     // clear unused values

        VPADDD          Y12, Y0, Y12    // add number of values
        VPADDD          Y11, Y2, Y11    // add number of values

	ADDQ		$128, SI
        SUBQ            $1, CX
	JZ	 	exit_big
	JMP	 	loop_big

exit_big:
        VPADDD          Y11, Y12, Y12

prep_small:
        MOVQ    BX, CX
        ANDQ    $0x1f, BX               // number of bytes left
        SHRQ    $5, CX                  // number of runs of small loop
        JZ      exit_small

loop_small:
        // for big endian
        VPAND           (SI), Y14, Y0   // determine selector
	VPSRLQ	        $4, Y0, Y0      // determine selector

	VPSHUFB	        Y0, Y15, Y0     // look up number of values
        VPAND           Y0, Y14, Y0     // clear unused values

        VPADDD          Y12, Y0, Y12    // add number of values

	ADDQ		$32, SI
        SUBQ            $1, CX
	JZ	 	exit_small
	JMP	 	loop_small

exit_small:
        SHRQ            $3, BX                  // number of 64bit words
        VMOVDQU         countdown<>(SB), Y2
        MOVQ            BX, X0
        VPBROADCASTQ    X0, Y1                  // broadcast BX
        VPCMPGTQ        Y2, Y1, Y1              // mask remaining values

        VPMASKMOVQ      (SI), Y1, Y0            // load remaining values

        // for big endian
        VPAND           Y0, Y14, Y0     // determine selector
	VPSRLQ	        $4, Y0, Y0      // determine selector

	VPSHUFB	        Y0, Y15, Y0     // look up number of values
        VPAND           Y0, Y14, Y0     // clear unused values
        VPAND           Y0, Y1, Y0      // cut vector

        VPADDD          Y12, Y0, Y12    // add number of values

        // finish: add all values in Y12
        VPHADDD         Y12, Y12, Y12
        VPHADDD         Y12, Y12, Y12
        VEXTRACTI128    $1, Y12, X0
        VPADDD          X0, X12, X12

	VMOVD	        X12, AX
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
        MOVQ            AX, count+24(FP)
        MOVQ            $100, AX
	RET
