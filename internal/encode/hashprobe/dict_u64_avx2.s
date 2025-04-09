// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
// go:build amd64
//  +build amd64

#include "textflag.h"
#include "constants.h"

// func ht_build64(vals, dict, ht_keys *uint64, ht_values *uint16, len uint32, dict_size *uint32)
TEXT ·ht_build64(SB), NOSPLIT, $0-48
    // Move arguments to named registers for clarity and correctness
    MOVQ vals+0(FP), DI           // DI = vals
    MOVQ dict+8(FP), SI           // SI = dict
    MOVQ ht_keys+16(FP), DX       // DX = ht_keys
    MOVQ ht_values+24(FP), CX     // CX = ht_values
    MOVL len+32(FP), R8           // R8 = len (uint32)
    MOVQ dict_size+40(FP), R9     // R9 = dict_size

    // Constants
    MOVQ hash_const<>+0(SB), R15  // HASH_CONST (scalar, preserved)
    VPBROADCASTQ hash_const<>+0(SB), Y11 // Y11 = [HASH_CONST, ...] (for VPMULLD)
    MOVQ $0xFFFF, R11             // HASH_MASK
    VPBROADCASTW hash_mask_16<>+0(SB), Y0 // Y0 = [HASH_MASK, ...] (16x uint16)
    MOVQ $65536, R12              // HASH_TABLE_SIZE (1 << 16)

    // Initialize ht_values with 0xFFFF (SIMD)
    XORQ AX, AX                   // i = 0
init_loop:
    CMPQ AX, R12
    JGE init_done
    VMOVDQU Y0, (CX)(AX*2)        // Store 32 bytes (16 uint16_t) to ht_values[i]
    ADDQ $16, AX                  // i += 16 (uint16_t elements)
    JMP init_loop
init_done:

    // Deduplicate
    XORQ BX, BX                   // i = 0
    CMPQ R8, $3                   // vectors < len 3
    JBE  tail_start
    LEAQ -3(R8), R14              // R14 = len - 3

main_loop:
    // Load 4x uint64_t
    VMOVDQU (DI)(BX*8), Y1        // kvec = vals[i:i+4]
    VPMULLD Y1, Y11, Y2           // hvec = kvec * HASH_CONST (32-bit mul)

    // Extract and probe hashes, find the next free slot
    VMOVDQU Y2, -32(SP)           // h_vals[4] on stack (32 bytes)

    // Probe h0
    MOVQ -32(SP), AX              // h_vals[0]
    ANDQ R11, AX                  // h0 = h_vals[0] & HASH_MASK
    XORQ BP, BP                   // p0 = 0
probe_h0:
    CMPW (CX)(AX*2), R11          // ht_values[h0] == HASH_MASK?
    JE h0_empty
    MOVQ (DX)(AX*8), R10          // Load ht_keys[h0]
    CMPQ R10, (DI)(BX*8)          // ht_keys[h0] == vals[i]?
    JE h0_done
    INCQ BP                       // p0++
    MOVQ BP, R13                  // p0
    IMULQ R13, R13                // p0 * p0
    ADDQ R13, AX                  // h0 + p0 * p0
    ANDQ R11, AX                  // h0 & HASH_MASK
    JMP probe_h0
h0_empty:
    MOVQ (DI)(BX*8), R10          // Load vals[i]
    MOVQ R10, (DX)(AX*8)          // ht_keys[h0] = vals[i]
    MOVW $0, (CX)(AX*2)           // ht_values[h0] = 0
h0_done:

    // Probe h1
    MOVQ -24(SP), AX              // h_vals[1]
    ANDQ R11, AX                  // h1
    XORQ BP, BP                   // p1 = 0
probe_h1:
    CMPW (CX)(AX*2), R11
    JE h1_empty
    MOVQ (DX)(AX*8), R10
    CMPQ R10, 8(DI)(BX*8)
    JE h1_done
    INCQ BP
    MOVQ BP, R13
    IMULQ R13, R13
    ADDQ R13, AX
    ANDQ R11, AX
    JMP probe_h1
h1_empty:
    MOVQ 8(DI)(BX*8), R10
    MOVQ R10, (DX)(AX*8)
    MOVW $0, (CX)(AX*2)
h1_done:

    // Probe h2
    MOVQ -16(SP), AX              // h_vals[2]
    ANDQ R11, AX                  // h2
    XORQ BP, BP
probe_h2:
    CMPW (CX)(AX*2), R11
    JE h2_empty
    MOVQ (DX)(AX*8), R10
    CMPQ R10, 16(DI)(BX*8)
    JE h2_done
    INCQ BP
    MOVQ BP, R13
    IMULQ R13, R13
    ADDQ R13, AX
    ANDQ R11, AX
    JMP probe_h2
h2_empty:
    MOVQ 16(DI)(BX*8), R10
    MOVQ R10, (DX)(AX*8)
    MOVW $0, (CX)(AX*2)
h2_done:

    // Probe h3
    MOVQ -8(SP), AX               // h_vals[3]
    ANDQ R11, AX                  // h3
    XORQ BP, BP
probe_h3:
    CMPW (CX)(AX*2), R11
    JE h3_empty
    MOVQ (DX)(AX*8), R10
    CMPQ R10, 24(DI)(BX*8)
    JE h3_done
    INCQ BP
    MOVQ BP, R13
    IMULQ R13, R13
    ADDQ R13, AX
    ANDQ R11, AX
    JMP probe_h3
h3_empty:
    MOVQ 24(DI)(BX*8), R10
    MOVQ R10, (DX)(AX*8)
    MOVW $0, (CX)(AX*2)
h3_done:
    ADDQ $4, BX                   // i += 4
    CMPQ BX, R14
    JB main_loop

tail_start:
    CMPQ BX, R8
    JGE extract_start
    MOVQ (DI)(BX*8), AX           // val = vals[i]
    MOVQ AX, R10                  // Save val for comparison
    IMULQ R15, AX                 // val * HASH_CONST (R15 = HASH_CONST, result in RAX)
    ANDQ R11, AX                  // h = val & HASH_MASK
    XORQ BP, BP                   // p = 0
tail_probe:
    CMPW (CX)(AX*2), R11          // ht_values[h] == HASH_MASK?
    JE tail_empty
    MOVQ (DX)(AX*8), R13          // Load ht_keys[h]
    CMPQ R13, R10                 // ht_keys[h] == val?
    JE tail_done
    INCQ BP
    MOVQ BP, R13
    IMULQ R13, R13                // p * p
    ADDQ R13, AX                  // h + p * p
    ANDQ R11, AX                  // h & HASH_MASK
    JMP tail_probe
tail_empty:
    MOVQ R10, (DX)(AX*8)          // ht_keys[h] = val
    MOVW $0, (CX)(AX*2)           // ht_values[h] = 0
tail_done:
    INCQ BX
    JMP tail_start

extract_start:
    XORQ BX, BX                   // i = 0
    XORQ AX, AX                   // n = 0
    MOVQ CX, BP                   // Save CX (ht_values) to BP
extract_loop:
    CMPQ BX, R12                  // R12 = 65536
    JGE extract_done
    MOVQ BP, CX                   // Restore CX (ht_values) from BP
    VMOVDQU (CX)(BX*2), Y1        // v = ht_values[i:i+16]
    VPCMPEQW Y1, Y0, Y2           // cmp = v == HASH_MASK
    VPMOVMSKB Y2, R10             // mask = movemask(cmp)
    MOVQ R10, R13                 // Save original mask in R13
    CMPL R13, $0xFFFFFFFF
    JE extract_skip
extract_inner:
    NOTL R13                      // Invert mask for TZCNTQ
    TZCNTL R13, R14               // j = ctz(~mask)
    SHRQ $1, R14                  // j >>= 1
    MOVQ BX, R15                  // i
    ADDQ R14, R15                 // i + j
    MOVQ (DX)(R15*8), R13         // Load ht_keys[i + j]
    MOVQ R13, (SI)(AX*8)          // dict[n] = ht_keys[i + j]
    INCQ AX                       // n++
    MOVQ R14, R13                 // Move j to R13
    SHLQ $1, R13                  // j * 2
    MOVQ $3, R14                  // Base value 3 (R14 free after j)
    MOVB R13B, CL                 // Move j * 2 to CL for shift
    SHLQ CL, R14                  // 3 << (j * 2)
    ORQ R14, R10                  // Update original mask: mask |= 3 << (j * 2)
    MOVQ R10, R13                 // Refresh R13 with updated mask
    CMPL R10, $0xFFFFFFFF         // Check original mask
    JNE extract_inner
extract_skip:
    ADDQ $16, BX                  // i += 16
    JMP extract_loop
extract_done:
    MOVL AX, (R9)                 // *dict_size = n (uint32)
    VZEROUPPER                    // Reset upper YMM bits
    RET


// func ht_encode64(vals, ht_keys *uint64, ht_values, codes *uint16, len uint32)
TEXT ·ht_encode64(SB), NOSPLIT, $0-40
    // Move arguments to named registers for clarity and correctness
    MOVQ vals+0(FP), DI           // DI = vals
    MOVQ ht_keys+8(FP), SI        // SI = ht_keys
    MOVQ ht_values+16(FP), DX     // DX = ht_values
    MOVQ codes+24(FP), CX         // CX = codes
    MOVL len+32(FP), R8           // R8 = len (uint32)

    // Constants
    MOVQ hash_const<>+0(SB), R15  // HASH_CONST
    VPBROADCASTQ hash_const<>+0(SB), Y11 // Y11 = [HASH_CONST, ...] (for VPMULLD)
    MOVQ $0xFFFF, R11             // HASH_MASK

    // Main loop
    XORQ BX, BX                   // i = 0
    CMPQ R8, $3                   // vectors < len 3
    JBE  tail_start
    LEAQ -3(R8), R14              // R14 = len - 3

main_loop:
    // Load and hash 4x uint64_t
    VMOVDQU (DI)(BX*8), Y1        // kvec = vals[i:i+4]
    VPMULLD Y1, Y11, Y2           // hvec = kvec * HASH_CONST
    VMOVDQU Y2, -32(SP)           // h_vals[4] on stack

    // Extract and probe hashes

    // Probe h0
    MOVQ -32(SP), AX              // h_vals[0]
    ANDQ R11, AX                  // h0 = h_vals[0] & HASH_MASK
    XORQ BP, BP                   // p0 = 0
probe_h0:
    MOVQ (SI)(AX*8), R12          // Load ht_keys[h0]
    CMPQ R12, (DI)(BX*8)          // ht_keys[h0] == vals[i]?
    JE h0_done
    INCQ BP                       // p0++
    MOVQ BP, R12                  // p0
    IMULQ R12, R12                // p0 * p0
    ADDQ R12, AX                  // h0 + p0 * p0
    ANDQ R11, AX                  // h0 & HASH_MASK
    JMP probe_h0
h0_done:
    MOVW (DX)(AX*2), R12          // Load ht_values[h0]
    MOVW R12, (CX)(BX*2)          // codes[i] = ht_values[h0]

    // Probe h1
    MOVQ -24(SP), AX              // h_vals[1]
    ANDQ R11, AX                  // h1
    XORQ BP, BP
probe_h1:
    MOVQ (SI)(AX*8), R12
    CMPQ R12, 8(DI)(BX*8)
    JE h1_done
    INCQ BP
    MOVQ BP, R12
    IMULQ R12, R12
    ADDQ R12, AX
    ANDQ R11, AX
    JMP probe_h1
h1_done:
    MOVW (DX)(AX*2), R12
    MOVW R12, 2(CX)(BX*2)         // codes[i+1]

    // Probe h2
    MOVQ -16(SP), AX              // h_vals[2]
    ANDQ R11, AX                  // h2
    XORQ BP, BP
probe_h2:
    MOVQ (SI)(AX*8), R12
    CMPQ R12, 16(DI)(BX*8)
    JE h2_done
    INCQ BP
    MOVQ BP, R12
    IMULQ R12, R12
    ADDQ R12, AX
    ANDQ R11, AX
    JMP probe_h2
h2_done:
    MOVW (DX)(AX*2), R12
    MOVW R12, 4(CX)(BX*2)        // codes[i+2]

    // Probe h3
    MOVQ -8(SP), AX              // h_vals[3]
    ANDQ R11, AX                 // h3
    XORQ BP, BP
probe_h3:
    MOVQ (SI)(AX*8), R12
    CMPQ R12, 24(DI)(BX*8)
    JE h3_done
    INCQ BP
    MOVQ BP, R12
    IMULQ R12, R12
    ADDQ R12, AX
    ANDQ R11, AX
    JMP probe_h3
h3_done:
    MOVW (DX)(AX*2), R12
    MOVW R12, 6(CX)(BX*2)         // codes[i+3]

    ADDQ $4, BX                   // i += 4
    CMPQ BX, R14
    JB main_loop

tail_start:
    CMPQ BX, R8
    JGE done
    MOVQ (DI)(BX*8), AX           // val = vals[i]
    MOVQ AX, R10                  // Save val for comparison
    IMULQ R15, AX                 // val * HASH_CONST (R15 = HASH_CONST)
    ANDQ R11, AX                  // h = val & HASH_MASK
    XORQ BP, BP                   // p = 0
tail_probe:
    MOVQ (SI)(AX*8), R12          // Load ht_keys[h]
    CMPQ R12, (DI)(BX*8)
    JE tail_done
    INCQ BP
    MOVQ BP, R12
    IMULQ R12, R12
    ADDQ R12, AX
    ANDQ R11, AX
    JMP tail_probe
tail_done:
    MOVW (DX)(AX*2), R12
    MOVW R12, (CX)(BX*2)          // codes[i] = ht_values[h]
    INCQ BX
    JMP tail_start

done:
    VZEROUPPER                    // Reset upper YMM bits
    RET
