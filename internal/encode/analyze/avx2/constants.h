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
