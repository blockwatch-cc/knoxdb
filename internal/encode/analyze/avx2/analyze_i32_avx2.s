// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
// go:build amd64
//  +build amd64

#include "textflag.h"
#include "constants.h"

TEXT ·analyze_i32_avx2(SB), NOSPLIT, $0-24
    // Load function arguments from stack (Go calling convention)
    MOVQ vals+0(FP), R8    // R8 = pointer to vals array (int32_t*)
    MOVQ ctx+8(FP), DI     // DI = pointer to ctx struct (I32Context*)
    MOVQ len+16(FP), R9    // R9 = length of vals array

    // Initialize context from input
    MOVL 8(DI), R10        // R10 = ctx->Delta (offset 8 in I32Context)
    CMPQ R9, $1            // Compare len with 1
    SETNE AL               // AL = 1 if len != 1 (has multiple elements)
    TESTQ R10, R10         // Check if Delta != 0
    SETNE R11B             // R11B = 1 if Delta != 0
    ANDB AL, R11B          // R11B = hasDelta (Delta != 0 && len > 1)

    // Initialize vector registers and scalars
    MOVL (R8), R13         // R13 = vals[0], initial last_prev
    VMOVD R13, X7          // X7 = vals[0] (32-bit scalar)
    VPBROADCASTD X7, Y4    // Y4 = min_vec, broadcast vals[0] to all 8 lanes
    VMOVDQA Y4, Y5         // Y5 = max_vec, copy min_vec
    MOVL R10, X7           // X7 = Delta
    VPBROADCASTD X7, Y7    // Y7 = delta_vec, broadcast Delta to all 8 lanes
    MOVQ $1, SI            // SI = num_runs, initialized to 1 (first run)

    // Load shuffle mask into Y6
    VMOVDQA shift_control<>+0(SB), Y6 // Y6 = [6, 5, 4, 3, 2, 1, 0, 7] (logical lane order)

    // Check if len <= 7 (vector loop threshold: 8 elements per iteration)
    CMPQ R9, $7
    JBE tail_loop_reset    // Jump to tail if len <= 7 (no vector loop needed)

    // Set up vector loop bounds
    LEAQ -7(R9), R12       // R12 = len - 7 (last index for full 8-element blocks)
    XORQ BX, BX            // BX = index i, start at 0

    // First iteration
first_loop:
    VMOVDQU (R8)(BX*4), Y1 // Load first 8 int32s
    VPMINSD Y1, Y4, Y4     // Update min_vec
    VPMAXSD Y1, Y5, Y5     // Update max_vec

    // Create shifted vector
    VPERMD Y1, Y6, Y2      // Y2 = [b, c, d, e, f, g, h, a]
    VPINSRD $0, R13, X2, X8 // X8 = [last_prev, 0, 0, 0]
    VINSERTI128 $0, X8, Y2, Y2 // Y2 = [last_prev, a, b, c, d, e, f, g]

    // Count runs
    VPCMPEQD Y1, Y2, Y8    // Runs comparison
    VMOVMSKPS Y8, AX       // Extract mask
    NOTL AX                // Invert
    ANDL $0xFE, AX         // Mask bit 0 (first element has no prev)
    POPCNTL AX, AX         // Count runs
    ADDQ AX, SI            // Add to num_runs

    // Delta check
    TESTB R11B, R11B       // hasDelta?
    JZ next_iter
    VPSUBD Y2, Y1, Y1      // Differences (using saved curr_vec)
    VPCMPEQD Y7, Y1, Y1    // Compare with delta_vec
    VMOVMSKPS Y1, DX       // Extract delta mask
    ANDL $0xFE, DX         // Mask bit 0
    CMPL DX, $0xFE         // Check against expected (masked)
    SETEQ R11B             // Set hasDelta if equal
    JMP next_iter

vector_loop:
    // Load 8x32-bit elements into curr_vec (256-bit YMM register)
    VMOVDQU (R8)(BX*4), Y1 // Y1 = curr_vec = vals[i:i+8]
    VPMINSD Y1, Y4, Y4     // Y4 = min(Y4, Y1), signed 32-bit min across 8 lanes
    VPMAXSD Y1, Y5, Y5     // Y5 = max(Y5, Y1), signed 32-bit max across 8 lanes

    // Create shifted vector for runs counting
    VPERMD Y1, Y6, Y2      // Y2 = permute(Y1) = [b, c, d, e, f, g, h, a] (left rotation)
    VPINSRD $0, R13, X2, X8 // X8 = [last_prev, 0, 0, 0], insert last_prev into lane 0
    VINSERTI128 $0, X8, Y2, Y2 // Y2 = [last_prev, a, b, c, d, e, f, g], replace lower 128 bits

    // Count runs: compare curr_vec with shifted to detect transitions
    VPCMPEQD Y1, Y2, Y8    // Y8 = equality mask (0xFFFFFFFF for equal, 0 for not equal)
    VMOVMSKPS Y8, AX       // AX = 8-bit mask from sign bits of Y8 (1 = equal, 0 = transition)
    NOTL AX                // Invert: 1 = transition, 0 = equal
    ANDL $0xFF, AX         // Mask to 8 bits (safety, though VMOVMSKPS is 8-bit)
    POPCNTL AX, AX         // Count 1s (transitions)
    ADDQ AX, SI            // Add to num_runs

    // Delta check: verify consistent differences within block
    TESTB R11B, R11B       // Check hasDelta flag
    JZ next_iter           // Skip if no delta to check
    VPSUBD Y2, Y1, Y1      // Y1 = curr_vec - shifted (signed differences)
    VPCMPEQD Y7, Y1, Y1    // Y1 = equality mask with delta_vec
    VMOVMSKPS Y1, DX       // DX = 8-bit mask of delta matches
    CMPL DX, $0xFF         // Check against full mask
    SETEQ R11B             // Set hasDelta if equal

next_iter:
    // Update last_prev for next iteration
    MOVL 28(R8)(BX*4), R13 // R13 = vals[i + 7], last element of current block
    ADDQ $8, BX            // Increment index by 8 (next block)
    CMPQ BX, R12           // Check if more full blocks remain
    JB vector_loop         // Loop if BX < len - 7

    // Reduce min_vec across lanes to scalar
    VPSHUFD $0xB1, Y4, Y1  // Shuffle: [2, 3, 0, 1, 6, 7, 4, 5]
    VPMINSD Y1, Y4, Y4     // Pairwise min
    VPERMQ $0x4E, Y4, Y1   // Permute: [1, 0, 3, 2]
    VPMINSD Y1, Y4, Y4
    VPSHUFD $0x1B, Y4, Y1  // Shuffle: [3, 2, 1, 0, 7, 6, 5, 4]
    VPMINSD Y1, Y4, Y4
    VMOVD X4, AX           // AX = final min (lane 0 of Y4)

    // Reduce max_vec across lanes to scalar
    VPSHUFD $0xB1, Y5, Y0  // Shuffle: [2, 3, 0, 1, 6, 7, 4, 5]
    VPMAXSD Y0, Y5, Y5     // Pairwise max
    VPERMQ $0x4E, Y5, Y0   // Permute: [1, 0, 3, 2]
    VPMAXSD Y0, Y5, Y5
    VPSHUFD $0x1B, Y5, Y0  // Shuffle: [3, 2, 1, 0, 7, 6, 5, 4]
    VPMAXSD Y0, Y5, Y5
    VMOVD X5, DX           // DX = final max (lane 0 of Y5)
    JMP tail_loop

tail_loop_reset:
    // Prepare for scalar tail loop
    XORQ BX, BX            // BX = index, reset to 0
    MOVL R13, AX           // AX = vals[0], initial min for tail (or full run if len <= 7)
    MOVL R13, DX           // DX = vals[0], initial max for tail (or full run if len <= 7)

tail_loop:
    // Process remaining elements (len % 8 or len <= 7)
    CMPQ BX, R9
    JAE tail_done          // Exit if BX >= len
    MOVL (R8)(BX*4), R12   // R12 = vals[i]
    CMPL R12, AX
    JGE min_done           // Signed comparison: if R12 >= AX, skip update
    MOVL R12, AX           // Update AX
min_done:
    CMPL R12, DX
    JLE max_done           // Signed comparison: if R12 <= DX, skip update
    MOVL R12, DX           // Update DX
max_done:
    TESTQ BX, BX
    JZ run_done            // Skip runs on first element (no previous)
    CMPL R12, R13          // Compare current with previous for runs
    JE run_done            // If equal, no new run
    INCQ SI                // Increment num_runs for transition
run_done:
    TESTB R11B, R11B
    JZ delta_done          // Skip delta if hasDelta = 0
    TESTQ BX, BX           // Skip delta on first element (no previous)
    JZ delta_done
    MOVL R12, R15          // R15 = vals[i]
    SUBL R13, R15          // R15 = vals[i] - last_prev (current - previous)
    XORB R11B, R11B
    CMPL R15, R10          // Compare with ctx->Delta
    SETEQ R11B
delta_done:
    INCQ BX
    MOVL R12, R13          // Update last_prev = vals[i]
    JMP tail_loop

tail_done:
    // Finalize context
    TESTB R11B, R11B
    JNZ delta_ok           // Keep Delta if hasDelta = 1
    XORQ R10, R10          // Clear Delta if hasDelta = 0
delta_ok:
    MOVL AX, (DI)          // ctx->Min (offset 0)
    MOVL DX, 4(DI)         // ctx->Max (offset 4)
    MOVL R10, 8(DI)        // ctx->Delta (offset 8)
    MOVL SI, 12(DI)        // ctx->NumRuns (offset 12)

    // Clean up AVX state
    VZEROUPPER             // Clear upper bits of YMM registers
    RET                    // Return to caller
