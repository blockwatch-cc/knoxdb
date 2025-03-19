// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build arm64
// +build arm64

#include "textflag.h"

// scalar code working
TEXT ·analyze_i64_neon(SB), NOSPLIT, $0-32
    MOVD vals+0(FP), R0    // Slice pointer
    MOVD len+8(FP), R1     // Length
    MOVD ret+24(FP), R2    // Return pointer to Context[int64]

    // TODO: if len == 0 return

    // Load initial values (no writes to ctx yet)
    MOVD (R0), R3          // Initial min = vals[0]
    MOVD R3, R4            // Initial max = vals[0]
    MOVD 16(R2), R5        // Load delta from ctx.Delta (pre-set in Go)
    MOVD R3, R7            // prev = vals[0]
    MOVW $1, R6            // numRuns = 1
    EOR R19, R19, R19      // Clear R19 (hasDelta = 0)
    CMP R5, ZR             // Compare delta with 0
    CSET NE, R19           // hasDelta = 1 if delta != 0, else 0


    ADD $8, R0, R0        // start at vals[1:]
    SUB $1, R1, R1        // Adjust length for initial element (i = 1)
scalar_loop:
    CMP ZR, R1             // Check if any elements remain
    BEQ finalize_delta     // If none, finish
    MOVD (R0), R8          // Load next int64
    ADD $8, R0, R0         // Pre-increment R0 by 8 (1 int64)

    // Scalar updates to min/max using R3 and R4
    CMP R8, R3
    CSEL LT, R3, R8, R3    // min = v < min ? v : min
    CMP R8, R4
    CSEL GT, R4, R8, R4    // max = v > max ? v : max

    CMP R7, R8
    BNE inc_runs_scalar

cont_runs_scalar:
    MOVD R8, R7            // Update prev
    SUB $1, R1, R1        // Decrement count
    B scalar_loop

inc_runs_scalar:
    ADD $1, R6, R6        // NumRuns += 1
    CBZ R19, cont_runs_scalar
    SUB R8, R7, R9         // v - prev
    CMP R5, R9
    CSEL NE, R19, ZR, R19  // hasDelta &= delta == v-prev
    MOVD R8, R7            // Update prev
    SUB $1, R1, R1        // Decrement count
    B scalar_loop

finalize_delta:
    // Finalize results
    CMP R19, ZR            // if !hasDelta
    BEQ reset_delta

store_results:
    MOVD R3, (R2)          // c.Min
    MOVD R4, 8(R2)         // c.Max
    MOVD R5, 16(R2)        // c.Delta
    MOVW R6, 24(R2)        // c.NumRuns
    RET

reset_delta:
    MOVD $0, R5            // delta = 0
    B store_results


// Neon does not support min/max on i64 (all opcodes crash)
// M1 does not support SVE either.
//TEXT ·analyze_i64_neon(SB), NOSPLIT, $0-32
//    MOVD vals+0(FP), R0    // Slice pointer
//    MOVD len+8(FP), R1     // Length
//    MOVD ret+24(FP), R2    // Return pointer to Context[int64]
//
//    // Load initial values (no writes to ctx yet)
//    MOVD (R0), R3          // Initial min = vals[0]
//    MOVD R3, R4            // Initial max = vals[0]
//    MOVD 16(R2), R5        // Load delta from ctx.Delta (pre-set in Go)
//    MOVD R3, R7            // prev = vals[0]
//    MOVW $1, R6            // numRuns = 1
//    EOR R19, R19, R19      // Clear R19 (hasDelta = 0)
//    CMP R5, ZR             // Compare delta with 0
//    CSET NE, R19           // hasDelta = 1 if delta != 0, else 0
//
//
//    ADD $8, R0, R0        // start at vals[1:]
//    SUB $1, R1, R1        // Adjust length for initial element (i = 1)

////    MOVD $4, R11           // Constant 4 for unrolling
//
//    // Check if len < 4
////    CMP R11, R1
////    BLT scalar_loop
////
////    // Initialize NEON min/max vectors
////    VDUP R3, V2.D2         // V2 = [min, min] (initial min across lanes)
////    VDUP R3, V3.D2         // V3 = [max, max] (initial max across lanes)
////
////    // Unrolled NEON loop (4 elements)
////neon_loop:
////    SUB $4, R1, R1         // Process 4 elements per iteration
////    CMP ZR, R1             // Compare with zero
////    BLT neon_remainder     // If less than 0, handle remainder
////
////    // Load 4 int64s into two NEON registers (2 per register)
////    VLD1 (R0), [V0.D2]     // v0, v1
////    ADD  $16, R0, R0       // Pre-increment R0 by 16 (2 int64s)
////    VLD1 (R0), [V1.D2]     // v2, v3
////    ADD  $16, R0, R0       // Pre-increment R0 by 16 (2 int64s)
////
////    // Aggregate min/max across iterations (encoded as WORD)
////    // NEON (grok?) crashes
////    // WORD $0x6e806402       // VMINQ V2.2D, V2.2D, V0.2D (min V2, V0 → V2)
////    // WORD $0x6e806c22       // VMINQ V2.2D, V2.2D, V1.2D (min V2, V1 → V2)
////    // WORD $0x6e80ac43       // VMAXQ V3.2D, V3.2D, V0.2D (max V3, V0 → V3)
////    // WORD $0x6e80b463       // VMAXQ V3.2D, V3.2D, V1.2D (max V3, V1 → V3)
////
////    // reverse order, crashes
////    // WORD $0x0264806e       // VMINQ V2.2D, V2.2D, V0.2D (min V2, V0 → V2)
////    // WORD $0x226c806e       // VMINQ V2.2D, V2.2D, V1.2D (min V2, V1 → V2)
////    // WORD $0x43ac806e       // VMAXQ V3.2D, V3.2D, V0.2D (max V3, V0 → V3)
////    // WORD $0x63b4806e       // VMAXQ V3.2D, V3.2D, V1.2D (max V3, V1 → V3)
////
////    // SVE from manual (not sure what's in predicate register P0, not supported on M1)
////    // WORD $0x04CA0002
////    // WORD $0x04CA0022
////    // WORD $0x04C80003
////    // WORD $0x04C80023
////
////    // arm manual C7.2.287, C7.2.290 SMIN/SMAX using reserved size 11, crashes
////    // WORD $0x4ee26c02
////    // WORD $0x4ee26c22
////    // WORD $0x4ee36403
////    // WORD $0x4ee36423
////
////
////    // Run counting and delta checking (scalar)
////    VMOV V0.D[0], R13      // v1
////    VMOV V0.D[1], R14      // v2
////    VMOV V1.D[0], R15      // v3
////    VMOV V1.D[1], R16      // v4
////
////    CMP R7, R13            // v0 != v1
////    BNE inc_runs1
////cont_runs1:
////    CMP R13, R14           // v1 != v2
////    BNE inc_runs2
////cont_runs2:
////    CMP R14, R15           // v2 != v3
////    BNE inc_runs3
////cont_runs3:
////    CMP R15, R16           // v3 != v4
////    BNE inc_runs4
////cont_runs4:
////    // Delta checks
////    CBZ R19, skip_deltas   // Skip if hasDelta == false
////    SUB R13, R8, R17       // v1 - v0
////    CMP R5, R17
////    CSEL NE, R19, ZR, R19  // hasDelta &= delta == v1-v0
////    SUB R14, R13, R17      // v2 - v1
////    CMP R5, R17
////    CSEL NE, R19, ZR, R19  // hasDelta &= delta == v2-v1
////    SUB R15, R14, R17      // v3 - v2
////    CMP R5, R17
////    CSEL NE, R19, ZR, R19  // hasDelta &= delta == v3-v2
////    SUB R16, R15, R17      // v4 - v3
////    CMP R5, R17
////    CSEL NE, R19, ZR, R19  // hasDelta &= delta == v4-v3
////skip_deltas:
////    MOVD R16, R7           // Update prev = v4
////    B neon_loop
////
////inc_runs1:
////    ADD $1, R6, R6         // NumRuns += 1
////    SUB R13, R8, R17       // v1 - v0
////    CMP R5, R17
////    CSEL NE, R19, ZR, R19  // hasDelta &= delta == v1-v0
////    B cont_runs1
////
////inc_runs2:
////    ADD $1, R6, R6         // NumRuns += 1
////    SUB R14, R13, R17      // v2 - v1
////    CMP R5, R17
////    CSEL NE, R19, ZR, R19  // hasDelta &= delta == v2-v1
////    B cont_runs2
////
////inc_runs3:
////    ADD $1, R6, R6         // NumRuns += 1
////    SUB R15, R14, R17      // v3 - v2
////    CMP R5, R17
////    CSEL NE, R19, ZR, R19  // hasDelta &= delta == v3-v2
////    B cont_runs3
////
////inc_runs4:
////    ADD $1, R6, R6         // NumRuns += 1
////    SUB R16, R15, R17      // v4 - v3
////    CMP R5, R17
////    CSEL NE, R19, ZR, R19  // hasDelta &= delta == v4-v3
////    B cont_runs4
////
////neon_remainder:
////    ADD $4, R1, R1         // Restore remaining count
////
////    // Reduce NEON vectors to scalars before scalar loop
////    VMOV V2.D[0], R13      // Min from V2 lane 0
////    VMOV V2.D[1], R14      // Min from V2 lane 1
////    CMP R13, R14
////    CSEL LT, R3, R13, R14  // Final min in R3
////    VMOV V3.D[0], R15      // Max from V3 lane 0
////    VMOV V3.D[1], R16      // Max from V3 lane 1
////    CMP R15, R16
////    CSEL GT, R4, R15, R16  // Final max in R4

// scalar_loop:
//     CMP ZR, R1             // Check if any elements remain
//     BEQ finalize_delta     // If none, finish
//     MOVD (R0), R8          // Load next int64
//     ADD $8, R0, R0         // Pre-increment R0 by 8 (1 int64)
//
//     // Scalar updates to min/max using R3 and R4
//     CMP R8, R3
//     CSEL LT, R3, R8, R3    // min = v < min ? v : min
//     CMP R8, R4
//     CSEL GT, R4, R8, R4    // max = v > max ? v : max
//
//     CMP R7, R8
//     BNE inc_runs_scalar
//
// cont_runs_scalar:
//     MOVD R8, R7            // Update prev
//     SUB $1, R1, R1        // Decrement count
//     B scalar_loop
//
// inc_runs_scalar:
//     ADD $1, R6, R6        // NumRuns += 1
//     CBZ R19, cont_runs_scalar
//     SUB R8, R7, R9         // v - prev
//     CMP R5, R9
//     CSEL NE, R19, ZR, R19  // hasDelta &= delta == v-prev
//     MOVD R8, R7            // Update prev
//     SUB $1, R1, R1        // Decrement count
//     B scalar_loop
//
// finalize_delta:
//     // Finalize results
//     CMP R19, ZR            // if !hasDelta
//     BEQ reset_delta
//
// store_results:
//     MOVD R3, (R2)          // c.Min
//     MOVD R4, 8(R2)         // c.Max
//     MOVD R5, 16(R2)        // c.Delta
//     MOVW R6, 24(R2)        // c.NumRuns
//     RET
//
// reset_delta:
//     MOVD $0, R5            // delta = 0
//     B store_results
