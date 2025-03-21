// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
// go:build amd64
//  +build amd64

#include "textflag.h"
#include "constants.h"

TEXT ·analyze_u32_avx2(SB), NOSPLIT, $0-24
    MOVQ vals+0(FP), R8    // R8 = vals pointer (constant throughout)
    MOVQ ctx+8(FP), DI     // DI = ctx pointer (constant throughout)
    MOVQ len+16(FP), R9    // R9 = len (constant throughout)

    MOVL 8(DI), R10        // R10 = ctx->Delta (constant until tail_done)
    CMPQ R9, $1            // Compare len with 1
    SETNE AL               // AL = (len != 1)
    TESTQ R10, R10         // Check if Delta == 0
    SETNE R11B             // R11B = (Delta != 0)
    ANDB AL, R11B          // R11B = hasDelta (Delta != 0 && len > 1)

    MOVL (R8), R13         // R13 = vals[0] (uint32), used as last_prev
    VMOVD R13, X7          // X7 = vals[0] (temporary for broadcast)
    VPBROADCASTD X7, Y4    // Y4 = min_vec (updated in loop)
    VMOVDQA Y4, Y5         // Y5 = max_vec (updated in loop)
    MOVL R10, X7           // X7 = Delta (temporary for broadcast)
    VPBROADCASTD X7, Y7    // Y7 = delta_vec (constant throughout)
    MOVQ $1, SI            // SI = num_runs (accumulated throughout)

    // Load shuffle mask into Y6
    VMOVDQA shift_control<>+0(SB), Y6

    // Check if len <= 7 (vector loop threshold: 8 elements per iteration)
    CMPQ R9, $7
    JBE tail_loop_reset    // Jump if len <= 7

    LEAQ -7(R9), R12       // R12 = len - 7 (loop bound)
    XORQ BX, BX            // BX = index (updated each iteration)

    // First iteration
first_loop:
    VMOVDQU (R8)(BX*4), Y1 // Load first 8 uint32s
    VPMINUD Y1, Y4, Y4     // Update min_vec
    VPMAXUD Y1, Y5, Y5     // Update max_vec

    // Create shifted vector
    VPERMD Y1, Y6, Y2      // Y2 = [b, c, d, e, f, g, h, a]
    VPINSRD $0, R13, X2, X8 // X8 = [last_prev, 0, 0, 0]
    VINSERTI128 $0, X8, Y2, Y2 // Y2 = [last_prev, a, b, c, d, e, f, g]

    // Count runs
    VPCMPEQD Y1, Y2, Y8    // Runs comparison
    VMOVMSKPS Y8, AX       // Extract mask
    NOTL AX                // Invert
    ANDL $0xFF, AX         // Mask bit 0
    POPCNTL AX, AX         // Count runs
    ADDQ AX, SI            // Add to num_runs

    // Delta check
    TESTB R11B, R11B       // hasDelta?
    JZ next_iter
    VPSUBD Y2, Y1, Y1      // Differences
    VPCMPEQD Y7, Y1, Y1    // Compare with delta_vec
    VMOVMSKPS Y1, DX       // Extract delta mask
    ANDL $0xFE, DX         // Mask bit 0
    CMPL DX, $0xFE         // Check against expected (masked)
    SETEQ R11B             // Set hasDelta if equal
    JMP next_iter

vector_loop:
    VMOVDQU (R8)(BX*4), Y1 // Y1 = curr_vec (8x uint32)
    VPMINUD Y1, Y4, Y4     // Y4 = min(curr_vec, min_vec), unsigned
    VPMAXUD Y1, Y5, Y5     // Y5 = max(curr_vec, max_vec), unsigned

    // Create shifted vector for runs counting
    VPERMD Y1, Y6, Y2      // Y2 = shifted vector: [b, c, d, e, f, g, h, a]
    VPINSRD $0, R13, X2, X8 // X8 = temp with last_prev inserted
    VINSERTI128 $0, X8, Y2, Y2 // Y2 = [last_prev, a, b, c, d, e, f, g]

    // Count runs: compare curr_vec with shifted to detect transitions
    VPCMPEQD Y1, Y2, Y8    // Y8 = comparison result
    VMOVMSKPS Y8, AX       // AX = mask
    NOTL AX                // Invert mask
    ANDL $0xFF, AX         // Mask to 8 bits
    POPCNTL AX, AX         // Count transitions
    ADDQ AX, SI            // Accumulate num_runs

    // Delta check: verify consistent differences within block
    TESTB R11B, R11B
    JZ next_iter
    VPSUBD Y2, Y1, Y1      // Y1 = curr_vec - shifted (signed differences)
    VPCMPEQD Y7, Y1, Y1    // Y1 = comparison with delta_vec
    VMOVMSKPS Y1, DX       // DX = mask
    CMPL DX, $0xFF         // Check against full mask
    SETEQ R11B             // Set hasDelta if equal

next_iter:
    MOVL 28(R8)(BX*4), R13 // R13 = vals[i + 7], last_prev for next iteration
    ADDQ $8, BX
    CMPQ BX, R12
    JB vector_loop

    // Min reduction
    VPSHUFD $0xB1, Y4, Y1  // Y1 = shuffled min_vec
    VPMINUD Y1, Y4, Y4
    VPERMQ $0x4E, Y4, Y1   // Y1 = permuted min_vec
    VPMINUD Y1, Y4, Y4
    VPSHUFD $0x1B, Y4, Y1  // Y1 = shuffled min_vec
    VPMINUD Y1, Y4, Y4
    VMOVD X4, AX           // AX = final min

    // Max reduction
    VPSHUFD $0xB1, Y5, Y0  // Y0 = shuffled max_vec
    VPMAXUD Y0, Y5, Y5
    VPERMQ $0x4E, Y5, Y0   // Y0 = permuted max_vec
    VPMAXUD Y0, Y5, Y5
    VPSHUFD $0x1B, Y5, Y0  // Y0 = shuffled max_vec
    VPMAXUD Y0, Y5, Y5
    VMOVD X5, DX           // DX = final max
    JMP tail_loop

tail_loop_reset:
    XORQ BX, BX            // BX = index (reset for tail loop)
    MOVL R13, AX           // init min for tail loop only case
    MOVL R13, DX           // init max for tail loop only case

tail_loop:
    CMPQ BX, R9
    JAE tail_done
    MOVL (R8)(BX*4), R12   // R12 = vals[i] (uint32)
    CMPL R12, AX
    JAE min_done           // Unsigned comparison (R12 >= AX)
    MOVL R12, AX           // AX = updated min
min_done:
    CMPL R12, DX
    JBE max_done           // Unsigned comparison (R12 <= DX)
    MOVL R12, DX           // DX = updated max
max_done:
    TESTQ BX, BX           // skip on first vector element
    JZ run_done
    CMPL R12, R13          // vals[1] == vals[i-1]
    JE run_done
    INCQ SI                // Increment num_runs
run_done:
    TESTB R11B, R11B
    JZ delta_done

    // delta compare to previous value
    TESTQ BX, BX           // skip on first vector element
    JZ delta_done
    MOVL R12, R15
    SUBL R13, R15          // R15 = vals[i] - vals[i-1] (signed 32-bit)
    XORB R11B, R11B
    CMPL R15, R10          // Compare with ctx->Delta
    SETEQ R11B

delta_done:
    INCQ BX
    MOVL R12, R13          // save last value
    JMP tail_loop

tail_done:
    TESTB R11B, R11B
    JNZ delta_ok           // If hasDelta=1, keep R10
    XORQ R10, R10          // Otherwise, set R10=0
delta_ok:
    MOVL AX, (DI)          // ctx->Min
    MOVL DX, 4(DI)         // ctx->Max
    MOVL R10, 8(DI)        // ctx->Delta (offset 8)
    MOVL SI, 12(DI)        // ctx->NumRuns (offset 12)

    VZEROUPPER
    RET
