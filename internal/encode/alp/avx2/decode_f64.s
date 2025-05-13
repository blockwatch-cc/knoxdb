// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
// go:build amd64
//  +build amd64

#include "textflag.h"

DATA F10<>+0(SB)/8, $1.0
DATA F10<>+8(SB)/8, $10.0
DATA F10<>+16(SB)/8, $100.0
DATA F10<>+24(SB)/8, $1000.0
DATA F10<>+32(SB)/8, $10000.0
DATA F10<>+40(SB)/8, $100000.0
DATA F10<>+48(SB)/8, $1000000.0
DATA F10<>+56(SB)/8, $10000000.0
DATA F10<>+64(SB)/8, $100000000.0
DATA F10<>+72(SB)/8, $1000000000.0
DATA F10<>+80(SB)/8, $10000000000.0
DATA F10<>+88(SB)/8, $100000000000.0
DATA F10<>+96(SB)/8, $1000000000000.0
DATA F10<>+104(SB)/8, $10000000000000.0
DATA F10<>+112(SB)/8, $100000000000000.0
DATA F10<>+120(SB)/8, $1000000000000000.0
DATA F10<>+128(SB)/8, $10000000000000000.0
DATA F10<>+136(SB)/8, $100000000000000000.0
DATA F10<>+144(SB)/8, $1000000000000000000.0
GLOBL F10<>(SB), RODATA, $152

DATA IF10<>+0(SB)/8, $1.0
DATA IF10<>+8(SB)/8, $0.1
DATA IF10<>+16(SB)/8, $0.01
DATA IF10<>+24(SB)/8, $0.001
DATA IF10<>+32(SB)/8, $0.0001
DATA IF10<>+40(SB)/8, $0.00001
DATA IF10<>+48(SB)/8, $0.000001
DATA IF10<>+56(SB)/8, $0.0000001
DATA IF10<>+64(SB)/8, $0.00000001
DATA IF10<>+72(SB)/8, $0.000000001
DATA IF10<>+80(SB)/8, $0.0000000001
DATA IF10<>+88(SB)/8, $0.00000000001
DATA IF10<>+96(SB)/8, $0.000000000001
DATA IF10<>+104(SB)/8, $0.0000000000001
DATA IF10<>+112(SB)/8, $0.00000000000001
DATA IF10<>+120(SB)/8, $0.000000000000001
DATA IF10<>+128(SB)/8, $0.0000000000000001
DATA IF10<>+136(SB)/8, $0.00000000000000001
DATA IF10<>+144(SB)/8, $0.000000000000000001
GLOBL IF10<>(SB), RODATA, $152

// Magic constants for int64_to_double_fast_precise
DATA MAGIC_I_LO<>+0(SB)/8, $0x4330000000000000
GLOBL MAGIC_I_LO<>(SB), RODATA, $8
DATA MAGIC_I_HI32<>+0(SB)/8, $0x4530000080000000
GLOBL MAGIC_I_HI32<>(SB), RODATA, $8
DATA MAGIC_I_ALL<>+0(SB)/8, $0x4530000080100000
GLOBL MAGIC_I_ALL<>(SB), RODATA, $8

// alp_f64_decode(src *int64, dst *float64, len int64, fx, ex uint8)
//
// Registers:
//   SI: src pointer
//   DI: dst pointer
//   BX: loop counter (i)
//   AX: temporary scalar
//   DX: temporary scalar
//   Y0: factor (float64)
//   Y1: inverse_factor (float64)
//   Y2: magic_i_lo
//   Y3: magic_i_hi32
//   Y4: magic_i_all
//   Y5: digit -> tmp_int -> tmp_dbl -> tmp_dbl_mlt
//   Y6: v_lo (int64 -> float64 conversion)
//   Y7: v_hi (int64 -> float64 conversion)
//
TEXT ·alp_f64_decode(SB), NOSPLIT, $0
    MOVQ src+0(FP), SI        // SI = *src
    MOVQ dst+8(FP), DI        // DI = *dst
    MOVQ len+16(FP), BX       // BX = len
    MOVBQZX fx+24(FP), AX     // AX = fac_idx
    MOVBQZX ex+25(FP), DX     // DX = exp_idx

    TESTQ   BX, BX
    JLE     done

    MOVQ $F10<>(SB), CX       // CX = *F10
    MOVQ (CX)(AX*8), AX       // AX = F10[fx]
    VMOVQ AX, X0              // X0 = factor
    VPBROADCASTQ X0, Y0       // Y0 = factor

    MOVQ $IF10<>(SB), CX      // CX = *IF10
    MOVQ (CX)(DX*8), DX       // DX = IF10[ex]
    VMOVQ DX, X1              // X1 = inverse_factor
    VPBROADCASTQ X1, Y1       // Y1 = inverse_factor

    // Load magic constants
    VPBROADCASTQ MAGIC_I_LO<>(SB), Y2   // Y2 = magic_i_lo
    VPBROADCASTQ MAGIC_I_HI32<>(SB), Y3 // Y3 = magic_i_hi32
    VPBROADCASTQ MAGIC_I_ALL<>(SB), Y4  // Y4 = magic_i_all

loop:
    VMOVDQU (SI), Y5    // Y5 = src[i]

    // int64_to_double_fast_precise
    VPBLENDD $0x55, Y5, Y2, Y6 // Y6 = v_lo 0b01010101
    VPSRLQ $32, Y5, Y7         // Y7 = v_hi
    VPXOR Y7, Y3, Y7           // Y7 = v_hi ^ magic_i_hi32
    VSUBPD Y4, Y7, Y7          // Y7 = v_hi_dbl = v_hi - magic_i_all
    VADDPD Y7, Y6, Y5          // Y5 = tmp_dbl = v_hi_dbl + v_lo

    // ALP scaling
    VMULPD Y5, Y0, Y5         // Y5 *= factor
    VMULPD Y5, Y1, Y5         // Y5 *= inverse_factor
    VMOVUPD Y5, (DI)          // store
    ADDQ $32, SI
    ADDQ $32, DI
    SUBQ $4, BX               // i -= 4
    CMPQ BX, $3               // i > 3
    JG loop

done:
    VZEROUPPER
    RET

// Magic constants for int64_to_double
DATA MAGIC_D<>+0(SB)/8, $0x4338000000000000
GLOBL MAGIC_D<>(SB), RODATA, $8

DATA MAGIC_I<>+0(SB)/8, $0x0018000000000000
GLOBL MAGIC_I<>(SB), RODATA, $8

// alp_f64_decode_safe(src *[1024]int64, dst *[1024]float64, fx, ex uint8)
//
// Only works for inputs in the range: [-2^51, 2^51]
//
// Registers:
//   SI: src pointer
//   DI: dst pointer
//   BX: loop counter (i)
//   AX: temporary scalar
//   DX: temporary scalar
//   Y0: factor (float64)
//   Y1: inverse_factor (float64)
//   Y2: magic_i_lo
//   Y3: magic_i_hi32
//   Y4: magic_i_all
//   Y5: digit -> tmp_int -> tmp_dbl -> tmp_dbl_mlt
//   Y6: v_lo (int64 -> float64 conversion)
//   Y7: v_hi (int64 -> float64 conversion)
//
TEXT ·alp_f64_decode_safe(SB), NOSPLIT, $0
    MOVQ src+0(FP), SI        // SI = *src
    MOVQ dst+8(FP), DI        // DI = *dst
    MOVQ len+16(FP), BX       // BX = len
    MOVBQZX fx+24(FP), AX     // AX = fac_idx
    MOVBQZX ex+25(FP), DX     // DX = exp_idx

    TESTQ   BX, BX
    JLE     done

    MOVQ $F10<>(SB), CX       // CX = *F10
    MOVQ (CX)(AX*8), AX       // AX = F10[fx]
    VMOVQ AX, X0              // X0 = factor_sse
    VPBROADCASTQ X0, Y0       // Y0 = factor_sse

    MOVQ $IF10<>(SB), CX      // CX = *IF10
    MOVQ (CX)(DX*8), DX       // DX = IF10[ex]
    VMOVQ DX, X1              // X1 = inverse_factor
    VPBROADCASTQ X1, Y1       // Y1 = inverse_factor

    VPBROADCASTQ MAGIC_I<>(SB), Y2 // Y2 = magic_i
    VPBROADCASTQ MAGIC_D<>(SB), Y3 // Y3 = magic_d

loop:
    VMOVDQU (SI), Y5          // Y5 = src[i]

    // int64_to_double
    VPADDQ Y5, Y3, Y5 // x = x + magic (integer math)
    VSUBPD Y3, Y5, Y5 // x = x - magic (float math)

    // ALP scaling
    VMULPD Y5, Y0, Y5         // Y5 *= factor
    VMULPD Y5, Y1, Y5         // Y5 *= inverse_factor
    VMOVUPD Y5, (DI)          // store

    ADDQ $32, SI
    ADDQ $32, DI
    SUBQ $4, BX               // i -= 4
    CMPQ BX, $3               // i > 3
    JG loop

done:
    VZEROUPPER
    RET
