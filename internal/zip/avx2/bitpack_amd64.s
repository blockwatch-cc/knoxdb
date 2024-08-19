// Copyright (c) 2022-2024 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"

// func packBytes32BitAVX2Core(src []uint64, dst []byte)
//
// input:
//   SI = src_base
//   BX = src_len
// internal:
//   R9 = maxdelta
//   Y14, Y15 = accumulators for maxdelta
//   Y0, Y1 = old vector data (to substract from)
//   Y2, Y3 = new vector data (to substract)
TEXT ·packBytes32BitAVX2Core(SB), $0-32
    MOVQ    src_base+0(FP), SI
    MOVQ    src_len+8(FP), BX
    MOVQ    dst_base+24(FP), DI
    XORQ    R9, R9

    SHRQ    $3, BX              // calculate number of steps (divide by 8)

    TESTQ   BX, BX
    JLE     done

prep_avx:

prep_big:

// works for >= 8 int64 (i.e. 64 bytes of data)
loop_big:
    VMOVDQU     0(SI), Y0
    VMOVDQU     32(SI), Y1

    VPSLLQ      $32, Y1, Y1
    VPOR        Y0, Y1, Y2

    VMOVDQU     Y2, (DI)

    ADDQ        $32, DI
    ADDQ        $64, SI
    SUBQ        $1, BX
    JZ          exit_big
    JMP         loop_big

exit_big:

prep_small:

loop_small:

exit_small:
    VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
    RET

// func unpackBytes32BitAVX2Core(src []byte, dst []uint64)
//
// input:
//   SI = src_base
//   BX = src_len
// internal:
//   R9 = maxdelta
//   Y14, Y15 = accumulators for maxdelta
//   Y0, Y1 = old vector data (to substract from)
//   Y2, Y3 = new vector data (to substract)
TEXT ·unpackBytes32BitAVX2Core(SB), $0-32
    MOVQ    src_base+0(FP), SI
    MOVQ    src_len+8(FP), BX
    MOVQ    dst_base+24(FP), DI
    XORQ    R9, R9

    SHRQ    $5, BX              // calculate number of steps (divide by 32)

    TESTQ   BX, BX
    JLE     done

prep_avx:
    VPCMPEQQ        Y12, Y12, Y12                    // create mask
    VPSRLQ          $32, Y12, Y12                    // create mask for 1st register

prep_big:

loop_big:
    VMOVDQU     0(SI), Y0
    VPAND       Y0, Y12, Y1
    VPSRLQ      $32, Y0, Y0

    VMOVDQU     Y1, 0(DI)
    VMOVDQU     Y0, 32(DI)

    ADDQ        $32, SI
    ADDQ        $64, DI
    SUBQ        $1, BX
    JZ          exit_big
    JMP         loop_big

exit_big:

prep_small:

loop_small:

exit_small:
    VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
    RET

// func packBytes16BitAVX2Core(src []uint64, dst []byte)
//
// input:
//   SI = src_base
//   BX = src_len
// internal:
//   R9 = maxdelta
//   Y14, Y15 = accumulators for maxdelta
//   Y0, Y1 = old vector data (to substract from)
//   Y2, Y3 = new vector data (to substract)
TEXT ·packBytes16BitAVX2Core(SB), $0-32
    MOVQ    src_base+0(FP), SI
    MOVQ    src_len+8(FP), BX
    MOVQ    dst_base+24(FP), DI
    XORQ    R9, R9

    SHRQ    $4, BX              // calculate number of steps (divide by 16)

    TESTQ   BX, BX
    JLE     done

prep_avx:

prep_big:

// works for >= 8 int64 (i.e. 64 bytes of data)
loop_big:
    VMOVDQU     0(SI), Y0
    VMOVDQU     32(SI), Y1
    VMOVDQU     64(SI), Y2
    VMOVDQU     96(SI), Y3

    VPSLLQ      $16, Y1, Y1
    VPSLLQ      $32, Y2, Y2
    VPSLLQ      $48, Y3, Y3

    VPOR        Y0, Y1, Y0
    VPOR        Y2, Y3, Y2
    VPOR        Y0, Y2, Y0

    VMOVDQU     Y0, (DI)

    ADDQ        $32, DI
    ADDQ        $128, SI
    SUBQ        $1, BX
    JZ          exit_big
    JMP         loop_big

exit_big:

prep_small:

loop_small:

exit_small:
    VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
    RET

// func unpackBytes16BitAVX2Core(src []byte, dst []uint64)
//
// input:
//   SI = src_base
//   BX = src_len
// internal:
//   R9 = maxdelta
//   Y14, Y15 = accumulators for maxdelta
//   Y0, Y1 = old vector data (to substract from)
//   Y2, Y3 = new vector data (to substract)
TEXT ·unpackBytes16BitAVX2Core(SB), $0-32
    MOVQ    src_base+0(FP), SI
    MOVQ    src_len+8(FP), BX
    MOVQ    dst_base+24(FP), DI
    XORQ    R9, R9

    SHRQ    $5, BX              // calculate number of steps (divide by 32)

    TESTQ   BX, BX
    JLE     done

prep_avx:
    VPCMPEQQ        Y12, Y12, Y12                    // create mask
    VPSRLQ          $48, Y12, Y12                    // create mask for 1st register

prep_big:

// works for >= 8 int64 (i.e. 64 bytes of data)
loop_big:
    VMOVDQU     0(SI), Y0

    VPSRLQ      $16, Y0, Y1
    VPAND       Y1, Y12, Y1
    VPSRLQ      $32, Y0, Y2
    VPAND       Y2, Y12, Y2
    VPSRLQ      $48, Y0, Y3
    VPAND       Y3, Y12, Y3
    VPAND       Y0, Y12, Y0

    VMOVDQU     Y0, 0(DI)
    VMOVDQU     Y1, 32(DI)
    VMOVDQU     Y2, 64(DI)
    VMOVDQU     Y3, 96(DI)

    ADDQ        $32, SI
    ADDQ        $128, DI
    SUBQ        $1, BX
    JZ          exit_big
    JMP         loop_big

exit_big:

prep_small:

loop_small:

exit_small:
    VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
    RET
