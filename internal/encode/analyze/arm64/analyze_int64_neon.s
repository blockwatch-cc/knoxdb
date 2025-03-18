// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build arm64
// +build arm64

#include "textflag.h"

TEXT ·analyze_i64_neon(SB), NOSPLIT, $0-32
    MOVD vals+0(FP), R0    // Slice pointer
    MOVD len+8(FP), R1     // Length
    MOVD ret+24(FP), R2    // Return pointer to Context[int64]

    // Initialize Context
    MOVD (R0), R3          // c.Min = vals[0]
    MOVD R3, (R2)          // Store at ret+0 (Min)
    MOVD R3, 8(R2)         // c.Max at ret+8
    MOVD 8(R0), R4         // vals[1]
    SUB R4, R3, R5         // c.Delta = vals[1] - vals[0]
    MOVD R5, 16(R2)        // Store at ret+16 (Delta)
    MOVW $1, R6            // c.NumRuns = 1 (int, 32-bit)
    MOVW R6, 24(R2)        // Store at ret+24 (NumRuns)
    MOVD R3, R7            // prev = vals[0]

    MOVD $1, R10           // Constant 1
    MOVD $2, R11           // Constant 2
    SUB R1, R1, R10        // Adjust length for initial element

loop:
    SUB R1, R1, R11        // Process 2 elements per iteration
    CMP R1, ZR             // Compare with zero register (ZR = 0)
    BLT remainder          // If less than 0, jump to remainder
    MOVD R0, R12           // Save current pointer
    ADD $16, R0            // Pre-increment R0 by 16 (2 int64s)
    MOVD (R12), R13        // Load v1
    MOVD 8(R12), R14       // Load v2

    // Update min/max
    CMP R13, R3            // Compare v1 with min
    CSEL LT, R3, R13, R3   // min = v1 < min ? v1 : min
    CMP R14, R3            // Compare v2 with min
    CSEL LT, R3, R14, R3   // min = v2 < min ? v2 : min

    CMP R13, R4            // Compare v1 with max
    CSEL GT, R4, R13, R4   // max = v1 > max ? v1 : max
    CMP R14, R4            // Compare v2 with max
    CSEL GT, R4, R14, R4   // max = v2 > max ? v2 : max

    // Run counting
    CMP R7, R13            // prev != v1
    BNE inc_runs1
cont_runs1:
    CMP R13, R14           // v1 != v2
    BNE inc_runs2
cont_runs2:
    // Delta check
    CBZ R5, skip_delta     // Skip if c.Delta == 0
    SUB R13, R7, R15       // v1 - prev
    CMP R5, R15
    BNE zero_delta
    SUB R14, R13, R15      // v2 - v1
    CMP R5, R15
    BNE zero_delta
skip_delta:
    MOVD R14, R7           // Update prev = v2
    B loop

inc_runs1:
    ADD R6, R6, R10        // NumRuns += 1
    B cont_runs1

inc_runs2:
    ADD R6, R6, R10        // NumRuns += 1
    B cont_runs2

zero_delta:
    MOVD $0, R5            // c.Delta = 0
    MOVD R14, R7           // Update prev = v2
    B loop

remainder:
    ADD R1, R1, R11        // Restore remaining count
    CBZ R1, done           // If no remainder, finish
scalar_loop:
    MOVD R0, R12           // Save current pointer
    ADD $8, R0             // Pre-increment R0 by 8 (1 int64)
    MOVD (R12), R8         // Load next int64
    CMP R8, R3             // Update min
    CSEL LT, R3, R8, R3    // min = v < min ? v : min
    CMP R8, R4             // Update max
    CSEL GT, R4, R8, R4    // max = v > max ? v : max
    CMP R7, R8
    BNE inc_runs_scalar
cont_runs_scalar:
    CBZ R5, skip_scalar_delta
    SUB R8, R7, R9         // v - prev
    CMP R5, R9
    BNE zero_scalar_delta
skip_scalar_delta:
    MOVD R8, R7            // Update prev
    SUB R1, R1, R10        // Decrement count
    BNE scalar_loop
    B done

inc_runs_scalar:
    ADD R6, R6, R10        // NumRuns += 1
    B cont_runs_scalar

zero_scalar_delta:
    MOVD $0, R5
    MOVD R8, R7
    SUB R1, R1, R10        // Decrement count
    BNE scalar_loop

done:
    MOVD R3, (R2)          // c.Min
    MOVD R4, 8(R2)         // c.Max
    MOVD R5, 16(R2)        // c.Delta
    MOVW R6, 24(R2)        // c.NumRuns
    RET
