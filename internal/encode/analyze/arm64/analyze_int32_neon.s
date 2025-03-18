// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//+build ignore

TEXT ·AnalyzeInt32Neon(SB), NOSPLIT, $0-32
    MOVD vals+0(FP), R0    // Slice pointer
    MOVD len+8(FP), R1     // Length
    MOVD ret+24(FP), R2    // Return pointer

    // Initialize
    LDR W3, [R0]           // c.Min = vals[0]
    STR W3, [R2]
    STR W3, [R2, #4]       // c.Max
    LDR W4, [R0, #4]       // vals[1]
    SUB W5, W4, W3         // c.Delta
    STR W5, [R2, #8]
    MOV W6, #1             // c.NumRuns
    STR W6, [R2, #12]
    MOV W7, W3             // prev

    // NEON setup
    DUP V1.4S, W3          // Min vector (broadcast c.Min)
    DUP V2.4S, W4          // Max vector (broadcast c.Max)
    SUBS R1, R1, #1        // Adjust for initial element

loop:
    SUBS R1, R1, #4
    BLT remainder
    LD1 {V0.4S}, [R0], #16 // Load 4 int32s (v1-v4)

    // Min/Max with NEON
    UMIN V1.4S, V1.4S, V0.4S  // Update min vector
    UMAX V2.4S, V2.4S, V0.4S  // Update max vector

    // Run counting (scalar for simplicity, NEON possible)
    CMP W7, V0.S[0]        // prev != v1
    CINC W6, W6, NE
    CMP V0.S[0], V0.S[1]   // v1 != v2
    CINC W6, W6, NE
    CMP V0.S[1], V0.S[2]   // v2 != v3
    CINC W6, W6, NE
    CMP V0.S[2], V0.S[3]   // v3 != v4
    CINC W6, W6, NE

    // Delta check (scalar, NEON possible)
    CBZ W5, skip_delta
    SUB W8, V0.S[0], W7    // v1-prev
    CMP W5, W8
    BNE zero_delta
    SUB W8, V0.S[1], V0.S[0]
    CMP W5, W8
    BNE zero_delta
    SUB W8, V0.S[2], V0.S[1]
    CMP W5, W8
    BNE zero_delta
    SUB W8, V0.S[3], V0.S[2]
    CMP W5, W8
    BNE zero_delta
skip_delta:
    MOV W7, V0.S[3]        // Update prev
    B loop

zero_delta:
    MOV W5, #0
    MOV W7, V0.S[3]
    B loop

remainder:
    ADD R1, R1, #4         // Restore remaining count
    CBZ R1, done
scalar_loop:
    LDR W8, [R0], #4
    CMP W8, W3
    CSEL W3, W8, W3, LT    // Update min
    CMP W8, W4
    CSEL W4, W8, W4, GT    // Update max
    CMP W7, W8
    CINC W6, W6, NE        // NumRuns++
    CBZ W5, skip_scalar_delta
    SUB W9, W8, W7
    CMP W5, W9
    BNE zero_scalar_delta
skip_scalar_delta:
    MOV W7, W8
    SUBS R1, R1, #1
    BNE scalar_loop
    B done

zero_scalar_delta:
    MOV W5, #0
    MOV W7, W8
    SUBS R1, R1, #1
    BNE scalar_loop

done:
    // Store results
    UMINV S3, V1.4S        // Reduce min vector to scalar
    UMAXV S4, V2.4S        // Reduce max vector to scalar
    STR W3, [R2]           // c.Min
    STR W4, [R2, #4]       // c.Max
    STR W5, [R2, #8]       // c.Delta
    STR W6, [R2, #12]      // c.NumRuns
    RET