// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// Data section
DATA hash_const<>+0(SB)/8, $0x9e3779b97f4a7c15 // HASH_CONST
GLOBL hash_const<>(SB), RODATA, $8

DATA hash_mask_16<>+0(SB)/2, $0xFFFF              // HASH_MASK
GLOBL hash_mask_16<>(SB), RODATA, $2

// DATA hash_mask_32<>+0(SB)/4, $0x0000FFFF // 32-bit mask
// GLOBL hash_mask_32<>(SB), RODATA, $4

// DATA hash_mask_64<>+0(SB)/8, $0x000000000000FFFF // 64-bit mask
// GLOBL hash_mask_64<>(SB), RODATA, $8