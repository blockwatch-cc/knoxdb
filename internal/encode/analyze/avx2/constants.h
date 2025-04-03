// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// Control vector for VPERMD: defines a left rotation shuffle mask for 8x32-bit lanes.
// In C: _mm256_set_epi32(6, 5, 4, 3, 2, 1, 0, 7) sets lanes [7, 6, 5, 4, 3, 2, 1, 0] logically.
// In Go: Little-endian memory layout reverses this to achieve the same effect in YMM register.
// Memory order [7, 0, 1, 2, 3, 4, 5, 6] loads as [6, 5, 4, 3, 2, 1, 0, 7] in Y6 (lanes [7, 6, 5, 4, 3, 2, 1, 0]).
DATA shift_control<>+0(SB)/4, $7
DATA shift_control<>+4(SB)/4, $0
DATA shift_control<>+8(SB)/4, $1
DATA shift_control<>+12(SB)/4, $2
DATA shift_control<>+16(SB)/4, $3
DATA shift_control<>+20(SB)/4, $4
DATA shift_control<>+24(SB)/4, $5
DATA shift_control<>+28(SB)/4, $6
GLOBL shift_control<>(SB), (NOPTR+RODATA), $32

// a custom shuffle mask for 16bit shuffles during min/max aggregation
DATA shuffle_mask_16<>+0(SB)/8, $0x0504070601000302  // [2, 3, 0, 1, 6, 7, 4, 5]
DATA shuffle_mask_16<>+8(SB)/8, $0x0d0c0f0e09080b0a  // [10, 11, 8, 9, 14, 15, 12, 13]
DATA shuffle_mask_16<>+16(SB)/8, $0x0504070601000302 // [2, 3, 0, 1, 6, 7, 4, 5]
DATA shuffle_mask_16<>+24(SB)/8, $0x0d0c0f0e09080b0a // [10, 11, 8, 9, 14, 15, 12, 13]
GLOBL shuffle_mask_16<>(SB), RODATA, $32

// a custom shuffle mask for 8bit shuffles during min/max aggregation
DATA shuffle_mask_8<>+0(SB)/8, $0x0607040502030001  // [1, 0, 3, 2, 5, 4, 7, 6]
DATA shuffle_mask_8<>+8(SB)/8, $0x0e0f0c0d0a0b0809  // [9, 8, 11, 10, 13, 12, 15, 14]
DATA shuffle_mask_8<>+16(SB)/8, $0x0607040502030001 // [1, 0, 3, 2, 5, 4, 7, 6]
DATA shuffle_mask_8<>+24(SB)/8, $0x0e0f0c0d0a0b0809 // [9, 8, 11, 10, 13, 12, 15, 14]
GLOBL shuffle_mask_8<>(SB), RODATA|NOPTR, $32