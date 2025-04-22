// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
// go:build amd64
//  +build amd64

#include "textflag.h"

// Function declaration: analyze_f64_avx2(vals *float64, ctx *Context, len size_t)
TEXT ·analyze_f64_avx2(SB), NOSPLIT, $0-24
    MOVQ vals+0(FP), R8    // R8 = vals
    MOVQ ctx+8(FP), DI     // DI = ctx
    MOVQ len+16(FP), R9    // R9 = len

    // Handle empty or single-element case
    CMPQ R9, $0
    JE done
    MOVQ (R8), R13         // R13 = vals[0]
    VMOVQ R13, X4          // X4 = min (initial)
    VMOVDQA X4, X0         // X0 = max (initial)
    MOVQ $1, SI            // SI = num_runs

    CMPQ R9, $3
    JBE tail_loop_reset

    LEAQ -3(R9), R12       // R12 = len - 3
    XORQ BX, BX

    VPBROADCASTQ X4, Y4    // Y4 = min_vec
    VMOVDQA Y4, Y5         // Y5 = max_vec

first_loop:
    VMOVUPD (R8)(BX*8), Y1 // Load 4 float64s
    VMINPD Y1, Y4, Y4      // Update min_vec
    VMAXPD Y1, Y5, Y5      // Update max_vec

    // Create shifted vector
    VPERMQ $0x93, Y1, Y2   // Y2 = [b, c, d, a]
    VPINSRQ $0, R13, X2, X6 // X6 = [last_prev, 0]
    VINSERTI128 $0, X6, Y2, Y2 // Y2 = [last_prev, a, b, c]

    // Count runs
    VPCMPEQQ Y1, Y2, Y6    // Compare for equality
    VMOVMSKPD Y6, AX       // Extract mask
    NOTL AX                // Invert
    ANDL $0xE, AX          // Mask bit 0
    POPCNTL AX, AX         // Count runs
    ADDQ AX, SI            // Add to num_runs

    ADDQ $4, BX
    CMPQ BX, R12
    JAE reduce             // Jump to reduction if done with vectorized part

vector_loop:
    VMOVUPD (R8)(BX*8), Y1 // Load 4 float64s
    VMOVUPD -8(R8)(BX*8), Y2 // Load prev_vec
    VMINPD Y1, Y4, Y4      // Update min_vec
    VMAXPD Y1, Y5, Y5      // Update max_vec

    // Count runs
    VPCMPEQQ Y1, Y2, Y6
    VMOVMSKPD Y6, AX
    NOTL AX
    ANDL $0xF, AX
    POPCNTL AX, AX
    ADDQ AX, SI

    ADDQ $4, BX
    CMPQ BX, R12
    JB vector_loop

reduce:
    // Min reduction
    VPERMQ $0xB1, Y4, Y1
    VMINPD Y1, Y4, Y4
    VPERMQ $0x4E, Y4, Y1
    VMINPD Y1, Y4, Y4
    VEXTRACTI128 $0, Y4, X4 // X4 = scalar min

    // Max reduction
    VPERMQ $0xB1, Y5, Y0
    VMAXPD Y0, Y5, Y0
    VPERMQ $0x4E, Y0, Y1
    VMAXPD Y1, Y0, Y0
    VEXTRACTI128 $0, Y0, X0 // X0 = scalar max

    MOVQ -8(R8)(BX*8), R13 // load last_prev to init tail loop
    JMP tail_loop

tail_loop_reset:
    XORQ BX, BX

tail_loop:
    CMPQ BX, R9
    JAE tail_done
    MOVQ (R8)(BX*8), X1    // X1 = vals[i]
    MINSD X1, X4           // X4 = min(XMM1, X4)
    MAXSD X1, X0           // X0 = max(XMM1, X0)
    MOVQ X1, R12           // R12 = vals[i] for run counting
    TESTQ BX, BX
    JZ run_done
    CMPQ R12, R13
    JE run_done
    INCQ SI
run_done:
    INCQ BX
    MOVQ R12, R13
    JMP tail_loop

tail_done:
    MOVSD X4, (DI)         // ctx->Min
    MOVSD X0, 8(DI)        // ctx->Max
    MOVL SI, 24(DI)        // ctx->NumRuns
    VZEROUPPER
    RET

done:
    VZEROUPPER
    RET
