// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants_AVX.h"

// func deltaScaleDecodeTimeAVX2Core(data []uint64, mod uint64)
//
TEXT ·deltaScaleDecodeTimeAVX2Core(SB), NOSPLIT, $0-24
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $3      // slices smaller than 4 values are handled in scalar loop
	JBE		prep_scalar

prep_avx:
    VPCMPEQQ        Y9, Y9, Y9   
    VPSLLQ          $32, Y9, Y9         // 0xffffffff00000000 mask

    VPBROADCASTQ    mod+24(FP), Y5      // Y3 = mod
    VPXOR           Y0, Y0, Y0          // Y0 = 0 (start value)
    
loop_avx:
    VMOVDQU     (SI), Y1

    VPSHUFD     $0xb1, Y5, Y2
    VPMULLD     Y1, Y2, Y2
    VPSLLQ      $32, Y2, Y15
    VPADDD      Y2, Y15, Y15
    VPAND       Y15, Y9, Y15
    VPMULUDQ    Y1, Y5, Y2
    VPADDQ      Y2, Y15, Y1 

    VPERMQ      $255, Y0, Y4

    VPERM2F128  $8, Y1, Y1, Y2
    VPALIGNR    $8, Y2, Y1, Y2
    VPADDQ      Y1, Y2, Y2
    
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
