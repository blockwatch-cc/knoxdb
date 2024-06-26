// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// counter vector for calculating the bitmask for non fullfilled registers
// used by all compare algorithms
DATA countup64<>+0x00(SB)/8, $(1)
DATA countup64<>+0x08(SB)/8, $(2)
DATA countup64<>+0x10(SB)/8, $(3)
DATA countup64<>+0x18(SB)/8, $(4)
DATA countup64<>+0x20(SB)/8, $(5)
DATA countup64<>+0x28(SB)/8, $(6)
DATA countup64<>+0x30(SB)/8, $(7)
DATA countup64<>+0x38(SB)/8, $(8)
GLOBL countup64<>(SB), (RODATA+NOPTR), $64
