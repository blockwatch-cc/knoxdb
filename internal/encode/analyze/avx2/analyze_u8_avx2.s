// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
// go:build amd64
//  +build amd64

#include "textflag.h"
#include "constants.h"

TEXT ·analyze_u8_avx2(SB), NOSPLIT, $0-24
    // Load arguments (Go calling convention)
    MOVQ vals+0(FP), R8    // R8 = pointer to vals array (uint8_t*)
    MOVQ ctx+8(FP), DI     // DI = pointer to ctx struct (Context[uint8])
    MOVQ len+16(FP), R9    // R9 = length of vals array

    // Initialize hasDelta
    MOVB 2(DI), R10        // R10 = ctx->Delta (offset 2, 8-bit value, zero-extended)
    CMPQ R9, $1            // Compare len with 1
    SETNE AL               // AL = 1 if len != 1
    TESTB R10, R10         // Check if Delta != 0
    SETNE R11B             // R11B = 1 if Delta != 0
    ANDB AL, R11B          // R11B = hasDelta (Delta != 0 && len > 1)

    // Initialize vector registers and scalars
    MOVB (R8), R13         // R13 = vals[0], initial last_prev (8-bit, zero-extended)
    VMOVD R13, X7          // X7 = vals[0] (32-bit, lower 8 bits valid)
    VPBROADCASTB X7, Y4    // Y4 = min_vec, broadcast vals[0] to all 32 lanes
    VMOVDQA Y4, Y5         // Y5 = max_vec, copy min_vec
    VMOVD R10, X7          // X7 = Delta (32-bit, lower 8 bits valid)
    VPBROADCASTB X7, Y7    // Y7 = delta_vec, broadcast Delta to all 32 lanes
    MOVQ $1, SI            // SI = num_runs, initialized to 1
    VPXOR Y6, Y6, Y6       // Y6 = prev_vec, zeroed initially

    // Check vector loop threshold (32 elements = 256 bits)
    CMPQ R9, $31
    JBE tail_loop_reset    // Jump to tail if len <= 31

    // Set up vector loop bounds
    LEAQ -31(R9), R12      // R12 = len - 31 (last index for full 32-element blocks)
    XORQ BX, BX            // BX = index i, start at 0

    // First iteration
first_loop:
    // Load 32x8-bit elements
    VMOVDQU (R8)(BX*1), Y1      // Load first 32 bytes
    VPMINUB Y1, Y4, Y4          // Update min_vec
    VPMAXUB Y1, Y5, Y5          // Update max_vec

    // Create shifted vector
    VPERM2I128 $0x3, Y6, Y1, Y2 // Permute with zeroed Y6
    VPALIGNR $15, Y2, Y1, Y3    // Shift for runs/delta
    VMOVDQA Y1, Y6              // Y6 = curr_vec

    // Count runs
    VPCMPEQB Y1, Y3, Y8         // Runs comparison
    VPMOVMSKB Y8, AX            // Extract mask
    NOTL AX                     // Invert
    ANDL $0xFFFFFFFE, AX        // Mask bit 0 (no prev for vals[0])
    POPCNTL AX, AX              // Count runs
    ADDQ AX, SI                 // Add to num_runs

    // Delta check
    TESTB R11B, R11B            // hasDelta?
    JZ next_iter
    VPSUBB Y3, Y1, Y1           // Differences
    VPCMPEQB Y7, Y1, Y1         // Compare with delta_vec
    VPMOVMSKB Y1, DX            // Extract delta mask
    ANDL $0xFFFFFFFE, DX        // Mask bit 0
    CMPL DX, $0xFFFFFFFE        // Check against expected (masked)
    SETEQ R11B                  // Set hasDelta if equal
    JMP next_iter

vector_loop:
    // Load 32x8-bit elements
    VMOVDQU (R8)(BX*1), Y1 // Y1 = curr_vec = vals[i:i+32] (1 byte per element)
    VPMINUB Y1, Y4, Y4     // Y4 = min(Y4, Y1), unsigned 8-bit min across 32 lanes
    VPMAXUB Y1, Y5, Y5     // Y5 = max(Y5, Y1), unsigned 8-bit max across 32 lanes

    // Create shifted vector
    VPERM2I128 $0x3, Y6, Y1, Y2 // Y2 = [Y6[127:0], Y1[255:128]]
    VPALIGNR $15, Y2, Y1, Y3 // Y3 = shifted, align right by 15 bytes
    VMOVDQA Y1, Y6         // Y6 = prev_vec, save current block

    // Count runs
    VPCMPEQB Y1, Y3, Y8    // Y8 = equality mask (0xFF per equal byte)
    VPMOVMSKB Y8, AX       // AX = 32-bit mask (1 = equal, 0 = transition)
    NOTL AX                // Invert: 1 = transition
    POPCNTL AX, AX         // Count transitions (32-bit)
    ADDQ AX, SI            // Add to num_runs

    // Delta check
    TESTB R11B, R11B       // Check hasDelta
    JZ next_iter           // Skip if no delta
    VPSUBB Y3, Y1, Y1      // Y1 = curr_vec - shifted (signed 8-bit differences)
    VPCMPEQB Y7, Y1, Y1    // Y1 = equality mask with delta_vec
    VPMOVMSKB Y1, DX       // DX = 32-bit mask of delta matches
    CMPL DX, $0xFFFFFFFF        // Check against full mask
    SETEQ R11B                  // Set hasDelta if equal

next_iter:
    MOVB 31(R8)(BX*1), R13 // R13 = vals[i + 31], last element (1-byte offset)
    ADDQ $32, BX           // Increment index by 32
    CMPQ BX, R12           // Check if more full blocks remain
    JB vector_loop         // Loop if BX < len - 31

    // load shuffle masks
    VMOVDQA shuffle_mask_16<>+0(SB), Y2 // Load 16bit shuffle mask into Y2
    VMOVDQA shuffle_mask_8<>+0(SB), Y3 // Load 8bit shuffle mask into Y3

    // Reduce min_vec to scalar
    VPSHUFD $0xB1, Y4, Y1  // Shuffle: [2, 3, 0, 1, ...]
    VPMINUB Y1, Y4, Y4     // Pairwise min
    VPERMQ $0x4E, Y4, Y1   // Permute: [1, 0, 3, 2]
    VPMINUB Y1, Y4, Y4
    VPSHUFD $0x1B, Y4, Y1  // Shuffle: [3, 2, 1, 0, ...]
    VPMINUB Y1, Y4, Y4
    VPSHUFB Y2, Y4, Y1     // Shuffle 16-bit lanes: [A, B, ...] -> [B, A, ...]
    VPMINUB Y1, Y4, Y4
    VPSHUFB Y3, Y4, Y1     // Shuffle 8-bit lanes: [A, B, ...] -> [B, A, ...]
    VPMINUB Y1, Y4, Y4
    VMOVD X4, AX           // AX = final min (lower 32 bits, 8-bit valid)

    // Reduce max_vec to scalar
    VPSHUFD $0xB1, Y5, Y0  // Shuffle: [2, 3, 0, 1, ...]
    VPMAXUB Y0, Y5, Y5     // Pairwise max
    VPERMQ $0x4E, Y5, Y0   // Permute: [1, 0, 3, 2]
    VPMAXUB Y0, Y5, Y5
    VPSHUFD $0x1B, Y5, Y0  // Shuffle: [3, 2, 1, 0, ...]
    VPMAXUB Y0, Y5, Y5
    VPSHUFB Y2, Y5, Y0     // Shuffle 16-bit lanes: [A, B, ...] -> [B, A, ...]
    VPMAXUB Y0, Y5, Y5
    VPSHUFB Y3, Y5, Y0     // Shuffle 8-bit lanes: [A, B, ...] -> [B, A, ...]
    VPMAXUB Y0, Y5, Y5
    VMOVD X5, DX           // DX = final max (lower 32 bits, 8-bit valid)

    JMP tail_loop

tail_loop_reset:
    XORQ BX, BX            // BX = index, reset to 0
    MOVB R13, AX           // AX = R13 (vals[0]), initial min
    MOVB R13, DX           // DX = R13 (vals[0]), initial max

tail_loop:
    CMPQ BX, R9
    JAE tail_done          // Exit if BX >= len
    MOVB (R8)(BX*1), R12   // R12 = vals[i] (8-bit)
    CMPB R12, AX
    JAE min_done           // Unsigned comparison: if R12 >= AX
    MOVB R12, AX           // Update AX
min_done:
    CMPB R12, DX
    JBE max_done           // Unsigned comparison: if R12 <= DX
    MOVB R12, DX           // Update DX
max_done:
    TESTQ BX, BX
    JZ run_done            // Skip runs on first element
    CMPB R12, R13
    JE run_done            // If equal, no new run
    INCQ SI                // Increment num_runs
run_done:
    TESTB R11B, R11B
    JZ delta_done          // Skip delta if hasDelta = 0
    TESTQ BX, BX
    JZ delta_done          // Skip delta on first element
    MOVB R12, R15          // R15 = vals[i]
    SUBB R13, R15          // R15 = vals[i] - last_prev (signed, per C code)
    XORB R11B, R11B        // Reset hasDelta
    CMPB R15, R10          // Compare with Delta
    SETEQ R11B             // hasDelta = 1 if equal
delta_done:
    INCQ BX
    MOVB R12, R13          // Update last_prev = vals[i]
    JMP tail_loop

tail_done:
    TESTB R11B, R11B
    JNZ delta_ok
    XORQ R10, R10
delta_ok:
    MOVB AX, (DI)          // ctx->Min (offset 0, 8-bit)
    MOVB DX, 1(DI)         // ctx->Max (offset 1, 8-bit)
    MOVB R10, 2(DI)        // ctx->Delta (offset 2, 8-bit)
    MOVL SI, 4(DI)         // ctx->NumRuns (offset 8, 32-bit)

    VZEROUPPER
    RET
