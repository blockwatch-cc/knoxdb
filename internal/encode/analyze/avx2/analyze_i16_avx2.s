// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
// go:build amd64
//  +build amd64

#include "textflag.h"

TEXT ·analyze_i16_avx2(SB), NOSPLIT, $0-24
    // Load arguments from stack (Go calling convention)
    MOVQ vals+0(FP), R8    // R8 = pointer to vals array (int16_t*)
    MOVQ ctx+8(FP), DI     // DI = pointer to ctx struct (I16Context*)
    MOVQ len+16(FP), R9    // R9 = length of vals array

    // Initialize context from input
    MOVW 4(DI), R10        // R10 = ctx->Delta (offset 8 in I16Context, 32-bit but holds 16-bit value)
    CMPQ R9, $1            // Compare len with 1
    SETNE AL               // AL = 1 if len != 1
    TESTQ R10, R10         // Check if Delta != 0
    SETNE R11B             // R11B = 1 if Delta != 0
    ANDB AL, R11B          // R11B = hasDelta (Delta != 0 && len > 1)

    // Initialize vector registers and scalars
    MOVW (R8), R13         // R13 = vals[0], initial last_prev (16-bit, zero-extended to 64-bit)
    VMOVD R13, X7          // X7 = vals[0] (32-bit, lower 16 bits valid)
    VPBROADCASTW X7, Y4    // Y4 = min_vec, broadcast vals[0] to all 16 lanes (16-bit)
    VMOVDQA Y4, Y5         // Y5 = max_vec, copy min_vec
    VMOVD R10, X7          // X7 = Delta (32-bit, lower 16 bits valid)
    VPBROADCASTW X7, Y7    // Y7 = delta_vec, broadcast Delta to all 16 lanes
    MOVQ $1, SI            // SI = num_runs, initialized to 1 (first run)
    VPXOR Y6, Y6, Y6       // Y6 = prev_vec, zeroed initially (256-bit)

    // Check if len <= 15 (vector loop threshold: 16 elements per iteration)
    CMPQ R9, $15
    JBE tail_loop_reset    // Jump to tail if len <= 15

    // Set up vector loop bounds
    LEAQ -15(R9), R12      // R12 = len - 15 (last index for full 16-element blocks)
    XORQ BX, BX            // BX = index i, start at 0

    // First iteration
first_loop:
    VMOVDQU (R8)(BX*2), Y1      // Load first 16 shorts
    VPMINSW Y1, Y4, Y4          // Update min_vec
    VPMAXSW Y1, Y5, Y5          // Update max_vec

    // Create shifted vector
    VPERM2I128 $0x3, Y6, Y1, Y2 // Permute with zeroed Y6
    VPALIGNR $14, Y2, Y1, Y3    // Shift for runs/delta (14 bytes = 7 lanes)
    VMOVDQA Y1, Y6              // Y6 = curr_vec

    // Count runs
    VPCMPEQW Y1, Y3, Y8         // Runs comparison
    VMOVDQA X8, X9              // Extract lower 128 bits
    VEXTRACTI128 $1, Y8, X8     // Extract upper 128 bits
    VPACKSSWB X8, X9, X9        // Pack to 8-bit
    VPMOVMSKB X9, AX            // Extract mask
    NOTL AX                     // Invert
    ANDL $0xFFFE, AX            // Mask bit 0 (16-bit lanes)
    POPCNTL AX, AX              // Count runs
    ADDQ AX, SI                 // Add to num_runs

    // Delta check
    TESTB R11B, R11B            // hasDelta?
    JZ next_iter
    VPSUBW Y3, Y1, Y1           // Differences
    VPCMPEQW Y7, Y1, Y1         // Compare with delta_vec
    VMOVDQA X1, X9              // Extract lower 128 bits
    VEXTRACTI128 $1, Y1, X1     // Extract upper 128 bits
    VPACKSSWB X1, X9, X1        // Pack to 8-bit
    VPMOVMSKB X1, DX            // Extract delta mask
    ANDL $0xFFFE, DX            // Mask bit 0
    CMPL DX, $0xFFFE            // Check against expected (masked)
    SETEQ R11B                  // Set hasDelta if equal
    JMP next_iter

vector_loop:
    // Load 16x16-bit elements into curr_vec (256-bit YMM register)
    VMOVDQU (R8)(BX*2), Y1 // Y1 = curr_vec = vals[i:i+16] (2 bytes per element)
    VPMINSW Y1, Y4, Y4     // Y4 = min(Y4, Y1), signed 16-bit min across 16 lanes
    VPMAXSW Y1, Y5, Y5     // Y5 = max(Y5, Y1), signed 16-bit max across 16 lanes

    // Create shifted vector using previous block's last element
    VPERM2I128 $0x3, Y6, Y1, Y2 // Y2 = [Y6[127:0], Y1[255:128]] (high Y6, low Y1)
    VPALIGNR $14, Y2, Y1, Y3 // Y3 = shifted, align right by 14 bytes (7x16-bit), last_prev in lane 0
    VMOVDQA Y1, Y6         // Y6 = prev_vec, save current block

    // Count runs: compare curr_vec with shifted
    VPCMPEQW Y1, Y3, Y8    // Y8 = equality mask (0xFFFF for equal)
    VMOVDQA X8, X9          // X9 = extract Y8 lower 128 bits
    VEXTRACTI128 $1, Y8, X8 // X8 = upper 128 bits
    VPACKSSWB X8, X9, X9   // X9 = packed 16-bit to 8-bit preserving order
    VPMOVMSKB X9, AX       // AX = 16-bit mask (1 = equal, 0 = transition)
    NOTL AX                // Invert: 1 = transition
    ANDL $0xFFFF, AX       // Mask to 16 bits
    POPCNTL AX, AX         // Count transitions
    ADDQ AX, SI            // Add to num_runs

    // Delta check: verify consistent differences within block
    TESTB R11B, R11B       // Check hasDelta
    JZ next_iter           // Skip if no delta
    VPSUBW Y3, Y1, Y1      // Y1 = curr_vec - shifted (signed 16-bit differences)
    VPCMPEQW Y7, Y1, Y1    // Y1 = equality mask with delta_vec
    VMOVDQA X1, X9         // X9 = extract lower 128 bits from Y1
    VEXTRACTI128 $1, Y1, X1 // X1 = extract upper 128 bits from Y1
    VPACKSSWB X1, X9, X1   // Pack to 8-bit preserving order
    VPMOVMSKB X1, DX       // DX = 16-bit mask of delta matches
    CMPL DX, $0xFFFF       // Check against full mask
    SETEQ R11B             // Set hasDelta if equal

next_iter:
    MOVL 30(R8)(BX*2), R13 // R13 = vals[i + 15], last element (2-byte offset)
    ADDQ $16, BX           // Increment index by 16
    CMPQ BX, R12           // Check if more full blocks remain
    JB vector_loop         // Loop if BX < len - 15

    // Reduce min_vec to scalar
    VPSHUFD $0xB1, Y4, Y1  // Shuffle: [2, 3, 0, 1, 6, 7, 4, 5]
    VPMINSW Y1, Y4, Y4     // Pairwise min
    VPERMQ $0x4E, Y4, Y1   // Permute: [1, 0, 3, 2]
    VPMINSW Y1, Y4, Y4
    VPSHUFD $0x1B, Y4, Y1  // Shuffle: [3, 2, 1, 0, 7, 6, 5, 4]
    VPMINSW Y1, Y4, Y4
    VPSHUFD $0x0E, Y4, Y1  // Shuffle: [0, 1, 2, 3, 4, 5, 6, 7]
    VPMINSW Y1, Y4, Y4
    VMOVD X4, AX           // AX = final min (lower 32 bits of Y4)

    // Reduce max_vec to scalar
    VPSHUFD $0xB1, Y5, Y0  // Shuffle: [2, 3, 0, 1, 6, 7, 4, 5]
    VPMAXSW Y0, Y5, Y5     // Pairwise max
    VPERMQ $0x4E, Y5, Y0   // Permute: [1, 0, 3, 2]
    VPMAXSW Y0, Y5, Y5
    VPSHUFD $0x1B, Y5, Y0  // Shuffle: [3, 2, 1, 0, 7, 6, 5, 4]
    VPMAXSW Y0, Y5, Y5
    VPSHUFD $0x0E, Y5, Y0  // Shuffle: [0, 1, 2, 3, 4, 5, 6, 7]
    VPMAXSW Y0, Y5, Y5
    VMOVD X5, DX           // DX = final max (lower 32 bits of Y5)
    JMP tail_loop

tail_loop_reset:
    // Prepare for scalar tail loop
    XORQ BX, BX            // BX = index, reset to 0
    MOVW R13, AX           // AX = R13 (vals[0]), initial min
    MOVW R13, DX           // DX = R13 (vals[0]), initial max

tail_loop:
    CMPQ BX, R9
    JAE tail_done          // Exit if BX >= len
    MOVW (R8)(BX*2), R12   // R12 = vals[i] (16-bit)
    CMPW R12, AX
    JGE min_done           // Signed comparison: if R12 >= AX
    MOVW R12, AX           // Update AX
min_done:
    CMPW R12, DX
    JLE max_done           // Signed comparison: if R12 <= DX
    MOVW R12, DX           // Update DX
max_done:
    TESTQ BX, BX
    JZ run_done            // Skip runs on first element
    CMPW R12, R13
    JE run_done            // If equal, no new run
    INCQ SI                // Increment num_runs
run_done:
    TESTB R11B, R11B
    JZ delta_done          // Skip delta if hasDelta = 0
    TESTQ BX, BX
    JZ delta_done          // Skip delta on first element
    MOVW R12, R15          // R15 = vals[i]
    SUBW R13, R15          // R15 = vals[i] - last_prev
    XORB R11B, R11B        // Reset hasDelta
    CMPW R15, R10          // Compare with Delta (lower 16 bits)
    SETEQ R11B             // hasDelta = 1 if equal
delta_done:
    INCQ BX
    MOVW R12, R13          // Update last_prev = vals[i]
    JMP tail_loop

tail_done:
    TESTB R11B, R11B
    JNZ delta_ok
    XORQ R10, R10
delta_ok:
    MOVW AX, (DI)          // ctx->Min (offset 0, 16-bit)
    MOVW DX, 2(DI)         // ctx->Max (offset 2, 16-bit)
    MOVW R10, 4(DI)        // ctx->Delta (offset 4, 16-bit but 16-bit value)
    MOVL SI, 8(DI)         // ctx->NumRuns (offset 8, 16-bit)

    VZEROUPPER
    RET
