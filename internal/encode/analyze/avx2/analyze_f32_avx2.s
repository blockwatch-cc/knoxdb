// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
// go:build amd64
//  +build amd64

#include "textflag.h"
#include "constants.h"

// Function declaration: analyze_f32_avx2(vals *float32, ctx *Context, len size_t)
TEXT ·analyze_f32_avx2(SB), NOSPLIT, $0-24
    MOVQ vals+0(FP), R8    // R8 = vals
    MOVQ ctx+8(FP), DI     // DI = ctx
    MOVQ len+16(FP), R9    // R9 = len

    // Handle empty or single-element case
    CMPQ R9, $0
    JE done

    MOVL (R8), R13         // R13 = vals[0]
    VMOVD R13, X4          // X4 = min (initial)
    VMOVDQA X4, X0         // X0 = max (initial)
    VPBROADCASTD X4, Y4    // Y4 = min_vec
    VMOVDQA Y4, Y5         // Y5 = max_vec
    MOVQ $1, SI            // SI = num_runs

    // Load shuffle mask into Y6
    VMOVDQA shift_control<>+0(SB), Y6

    CMPQ R9, $7
    JBE tail_loop_reset

    LEAQ -7(R9), R12       // R12 = len - 7
    XORQ BX, BX

first_loop:
    VMOVUPS (R8)(BX*4), Y1 // Load 8 float32s
    VMINPS Y1, Y4, Y4      // Update min_vec
    VMAXPS Y1, Y5, Y5      // Update max_vec

    // Create shifted vector for run counting
    VPERMD Y1, Y6, Y2   // Y2 = [b, c, d, e, f, g, h, a]
    VPINSRD $0, R13, X2, X6 // X6 = [last_prev, 0]
    VINSERTI128 $0, X6, Y2, Y2 // Y2 = [last_prev, a, b, c, d, e, f, g]

    // Count runs
    VPCMPEQD Y1, Y2, Y6
    VMOVMSKPS Y6, AX
    NOTL AX
    ANDL $0xFE, AX         // Mask bit 0
    POPCNTL AX, AX
    ADDQ AX, SI

    ADDQ $8, BX
    CMPQ BX, R12
    JAE reduce             // Jump to reduction if done with vectorized part

vector_loop:
    VMOVUPS (R8)(BX*4), Y1 // Load 8 float32s
    VMOVUPS -4(R8)(BX*4), Y2 // Load prev_vec
    VMINPS Y1, Y4, Y4
    VMAXPS Y1, Y5, Y5

    // Count runs
    VPCMPEQD Y1, Y2, Y6
    VMOVMSKPS Y6, AX
    NOTL AX
    ANDL $0xFF, AX
    POPCNTL AX, AX
    ADDQ AX, SI

    ADDQ $8, BX
    CMPQ BX, R12
    JB vector_loop

reduce:
    // Min reduction
    VPSHUFD $0xB1, Y4, Y1  // Y1 = shuffled min_vec
    VMINPS Y1, Y4, Y4
    VPERMQ $0x4E, Y4, Y1   // Y1 = permuted min_vec
    VMINPS Y1, Y4, Y4
    VPSHUFD $0x1B, Y4, Y1  // Y1 = shuffled min_vec
    VMINPS Y1, Y4, Y4
    VEXTRACTI128 $0, Y4, X4 // X4 = scalar min

    // Max reduction
    VPSHUFD $0xB1, Y5, Y0  // Y0 = shuffled max_vec
    VMAXPS Y0, Y5, Y5
    VPERMQ $0x4E, Y5, Y0   // Y0 = permuted max_vec
    VMAXPS Y0, Y5, Y5
    VPSHUFD $0x1B, Y5, Y0  // Y0 = shuffled max_vec
    VMAXPS Y0, Y5, Y5
    VEXTRACTI128 $0, Y5, X0 // X0 = scalar max

    MOVL -4(R8)(BX*4), R13 // load last_prev to init tail loop
    JMP tail_loop

tail_loop_reset:
    XORQ BX, BX

tail_loop:
    CMPQ BX, R9
    JAE tail_done
    MOVL (R8)(BX*4), X1  // X1 = vals[i]
    MINSS X1, X4         // X4 = min(X1, X4)
    MAXSS X1, X0         // X0 = max(X1, X0)
    MOVL X1, R12         // R12 = vals[i] for run counting
    TESTQ BX, BX
    JZ run_done
    CMPL R12, R13
    JE run_done
    INCQ SI
run_done:
    INCQ BX
    MOVL R12, R13
    JMP tail_loop

tail_done:
    MOVSS X4, (DI)         // ctx->Min
    MOVSS X0, 4(DI)        // ctx->Max
    MOVL SI, 12(DI)        // ctx->NumRuns

done:
    VZEROUPPER
    RET
