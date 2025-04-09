// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
// go:build amd64
//  +build amd64

#include "textflag.h"

// Function declaration: analyze_i64_avx2(vals *int64, ctx *I64Context, len size_t)
TEXT ·analyze_i64_avx2(SB), NOSPLIT, $0-24
    MOVQ vals+0(FP), R8    // R8 = vals
    MOVQ ctx+8(FP), DI     // DI = ctx
    MOVQ len+16(FP), R9    // R9 = len

    MOVQ 16(DI), R10       // R10 = ctx->Delta
    CMPQ R9, $1
    SETNE AL
    TESTQ R10, R10
    SETNE R11B
    ANDB AL, R11B          // R11B = hasDelta

    MOVQ (R8), R13         // R13 = vals[0]
    VMOVQ R13, X7
    VPBROADCASTQ X7, Y4    // Y4 = min_vec
    VMOVDQA Y4, Y5         // Y5 = max_vec
    VMOVQ R10, X7
    VPBROADCASTQ X7, Y7    // Y7 = delta_vec
    MOVQ $1, SI            // SI = num_runs

    CMPQ R9, $3
    JBE tail_loop_reset

    LEAQ -3(R9), R12       // R12 = len - 3
    XORQ BX, BX

    // First iteration
first_loop:
    VMOVDQU (R8)(BX*8), Y1 // Load first 4 int64s
    VPCMPGTQ Y1, Y4, Y0    // Compare for min
    VPBLENDVB Y0, Y1, Y4, Y4 // Update min_vec
    VPCMPGTQ Y5, Y1, Y3    // Compare for max
    VPBLENDVB Y3, Y1, Y5, Y5 // Update max_vec

    // Create shifted vector
    VPERMQ $0x93, Y1, Y2   // Y2 = [b, c, d, a]
    VPINSRQ $0, R13, X2, X6 // X6 = [last_prev, 0]
    VINSERTI128 $0, X6, Y2, Y2 // Y2 = [last_prev, a, b, c]
    VMOVDQA Y1, Y8         // Save curr_vec

    // Count runs
    VPCMPEQQ Y1, Y2, Y6    // Runs comparison
    VMOVMSKPD Y6, AX       // Extract mask
    NOTL AX                // Invert
    ANDL $0xE, AX          // Mask bit 0
    POPCNTL AX, AX         // Count runs
    ADDQ AX, SI            // Add to num_runs

    // Delta check
    TESTB R11B, R11B       // hasDelta?
    JZ next_iter
    VPSUBQ Y2, Y8, Y1      // Differences
    VPCMPEQQ Y7, Y1, Y1    // Compare with delta_vec
    VMOVMSKPD Y1, DX       // Extract delta mask
    ANDL $0xE, DX          // Mask bit 0
    CMPL DX, $0xE          // Check against expected (masked)
    SETEQ R11B             // Set hasDelta if equal
    JMP next_iter

vector_loop:
    VMOVDQU (R8)(BX*8), Y1 // Y1 = curr_vec
    VMOVDQU -8(R8)(BX*8), Y2 // Y2 = load prev_vec (faster than shift)
    VPCMPGTQ Y1, Y4, Y0    // Y0 = (curr_vec > min_vec) for signed min
    VPBLENDVB Y0, Y1, Y4, Y4 // If curr_vec > min_vec, keep min_vec; else curr_vec
    VPCMPGTQ Y5, Y1, Y3    // Y3 = (max_vec > curr_vec) for signed max
    VPBLENDVB Y3, Y1, Y5, Y5 // If max_vec > curr_vec, keep curr_vec; else max_vec

    // count num_runs
    VPCMPEQQ Y1, Y2, Y6
    VMOVMSKPD Y6, AX
    NOTL AX
    ANDL $0xF, AX
    POPCNTL AX, AX
    ADDQ AX, SI

    // track delta
    TESTB R11B, R11B
    JZ next_iter           // Skip delta check if hasDelta is false
    VPSUBQ Y2, Y1, Y1
    VPCMPEQQ Y7, Y1, Y1
    VMOVMSKPD Y1, DX
    CMPL DX, $0xF          // Check against full mask
    SETEQ R11B             // Set hasDelta if equal

next_iter:
    ADDQ $4, BX
    CMPQ BX, R12
    JB vector_loop

    // Min reduction: Select smallest value
    VPERMQ $0xB1, Y4, Y1
    VPCMPGTQ Y1, Y4, Y0    // Y0 = (Y1 > Y4)
    VPBLENDVB Y0, Y1, Y4, Y4
    VPERMQ $0x4E, Y4, Y1
    VPCMPGTQ Y1, Y4, Y0
    VPBLENDVB Y0, Y1, Y4, Y4
    VMOVQ X4, AX           // Extract from Y4

    // Max reduction: Select largest value
    VPERMQ $0xB1, Y5, Y0
    VPCMPGTQ Y0, Y5, Y3    // Y3 = (Y0 > Y5)
    VPBLENDVB Y3, Y5, Y0, Y0
    VPERMQ $0x4E, Y0, Y1
    VPCMPGTQ Y1, Y0, Y3
    VPBLENDVB Y3, Y0, Y1, Y0
    VMOVQ X0, DX           // Extract from Y0
    MOVQ -8(R8)(BX*8), R13 // load last_prev to init tail loop
    JMP tail_loop

tail_loop_reset:
    XORQ BX, BX            // Reset BX for small arrays
    MOVQ R13, AX           // AX = initial min
    MOVQ R13, DX           // DX = initial max

tail_loop:
    CMPQ BX, R9
    JAE tail_done
    MOVQ (R8)(BX*8), R12   // R12 = vals[i]
    CMPQ R12, AX
    JGE min_done           // Signed comparison (R12 >= AX)
    MOVQ R12, AX
min_done:
    CMPQ R12, DX
    JLE max_done           // Signed comparison (R12 <= DX)
    MOVQ R12, DX
max_done:
    TESTQ BX, BX
    JZ run_done
    CMPQ R12, R13
    JE run_done
    INCQ SI
run_done:
    TESTB R11B, R11B
    JZ delta_done          // Skip delta check if hasDelta is false

    // delta compare to previous value
    TESTQ BX, BX           // skip on first vector element
    JZ delta_done
    MOVQ R12, R15
    SUBQ R13, R15          // R15 = vals[i] - vals[i-1]
    XORB R11B, R11B
    CMPQ R15, R10          // Compare with ctx->Delta
    SETEQ R11B

delta_done:
    INCQ BX
    MOVQ R12, R13          // save last value
    JMP tail_loop

tail_done:
    TESTB R11B, R11B
    JNZ delta_ok           // If hasDelta=1, keep R10
    XORQ R10, R10          // Otherwise, set R10=0
delta_ok:
    MOVQ AX, (DI)          // ctx->Min
    MOVQ DX, 8(DI)         // ctx->Max
    MOVQ R10, 16(DI)       // ctx->Delta (offset 8)
    MOVL SI, 24(DI)        // ctx->NumRuns (offset 12)

    VZEROUPPER
    RET
