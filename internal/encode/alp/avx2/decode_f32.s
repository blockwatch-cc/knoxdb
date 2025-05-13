// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
// go:build amd64
//  +build amd64

#include "textflag.h"

DATA F10<>+0(SB)/4, $1.0
DATA F10<>+4(SB)/4, $10.0
DATA F10<>+8(SB)/4, $100.0
DATA F10<>+12(SB)/4, $1000.0
DATA F10<>+16(SB)/4, $10000.0
DATA F10<>+20(SB)/4, $100000.0
DATA F10<>+24(SB)/4, $1000000.0
DATA F10<>+28(SB)/4, $10000000.0
DATA F10<>+32(SB)/4, $100000000.0
DATA F10<>+36(SB)/4, $1000000000.0
DATA F10<>+40(SB)/4, $10000000000.0
GLOBL F10<>(SB), RODATA, $44

DATA IF10<>+0(SB)/4, $1.0
DATA IF10<>+4(SB)/4, $0.1
DATA IF10<>+8(SB)/4, $0.01
DATA IF10<>+12(SB)/4, $0.001
DATA IF10<>+16(SB)/4, $0.0001
DATA IF10<>+20(SB)/4, $0.00001
DATA IF10<>+24(SB)/4, $0.000001
DATA IF10<>+28(SB)/4, $0.0000001
DATA IF10<>+32(SB)/4, $0.00000001
DATA IF10<>+36(SB)/4, $0.000000001
DATA IF10<>+40(SB)/4, $0.0000000001
GLOBL IF10<>(SB), RODATA, $44

// Magic constants for int32_to_double_fast_precise
DATA MAGIC_I_LO<>+0(SB)/8, $0x4330000000000000
GLOBL MAGIC_I_LO<>(SB), RODATA, $8
DATA MAGIC_I_HI32<>+0(SB)/8, $0x4530000080000000
GLOBL MAGIC_I_HI32<>(SB), RODATA, $8
DATA MAGIC_I_ALL<>+0(SB)/8, $0x4530000080100000
GLOBL MAGIC_I_ALL<>(SB), RODATA, $8

// alp_f32_decode(src *int32, dst *float32, len int64, fx, ex uint8)
//
// Registers:
//   SI: src pointer
//   DI: dst pointer
//   BX: loop counter (i)
//   AX: temporary scalar
//   DX: temporary scalar
//   Y0: factor (float64)
//   Y1: inverse_factor (float64)
//   Y2: digit -> tmp_int -> tmp_dbl -> tmp_dbl_mlt
//
TEXT ·alp_f32_decode(SB), NOSPLIT, $0
    MOVQ src+0(FP), SI        // SI = *src
    MOVQ dst+8(FP), DI        // DI = *dst
    MOVQ len+16(FP), BX       // BX = len
    MOVBQZX fx+24(FP), AX     // AX = fac_idx
    MOVBQZX ex+25(FP), DX     // DX = exp_idx

    TESTQ   BX, BX
    JLE     done

    MOVQ $F10<>(SB), CX       // CX = *F10
    MOVL (CX)(AX*4), AX       // AX = F10[fx]
    VMOVD AX, X0              // X0 = factor
    VPBROADCASTD X0, Y0       // Y0 = factor

    MOVQ $IF10<>(SB), CX      // CX = *IF10
    MOVL (CX)(DX*4), DX       // DX = IF10[ex]
    VMOVD DX, X1              // X1 = inverse_factor
    VPBROADCASTD X1, Y1       // Y1 = inverse_factor

loop:
    VMOVDQU (SI), Y2          // Y2 = src[i]
    VCVTDQ2PS Y2, Y2          // convert int32 -> float32

    // ALP scaling
    VMULPS Y2, Y0, Y2         // Y5 *= factor
    VMULPS Y2, Y1, Y2         // Y5 *= inverse_factor
    VMOVUPS Y2, (DI)          // store

    ADDQ $32, SI
    ADDQ $32, DI
    SUBQ $8, BX               // i -= 8
    CMPQ BX, $7               // i > 7
    JG loop

done:
    VZEROUPPER
    RET

