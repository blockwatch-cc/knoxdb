// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// const_0x55 is the static input for POPCOUNT(VAL) sub-algorithm
DATA const_0x55<>+0x00(SB)/1, $(0x55)
GLOBL const_0x55<>(SB), (RODATA+NOPTR), $1

// const_0x33 is the static input for POPCOUNT(VAL) sub-algorithm
DATA const_0x33<>+0x00(SB)/1, $(0x33)
GLOBL const_0x33<>(SB), (RODATA+NOPTR), $1

// const_0x0f is the static input for POPCOUNT(VAL) sub-algorithm
DATA const_0x0f<>+0x00(SB)/1, $(0x0f)
GLOBL const_0x0f<>(SB), (RODATA+NOPTR), $1

// shuffle64 is the VPERMQ input required to permute bytes in each input word
// used by all 64 bit compare algorithms
DATA shuffle64<>+0x00(SB)/8, $(7)
DATA shuffle64<>+0x08(SB)/8, $(6)
DATA shuffle64<>+0x10(SB)/8, $(5)
DATA shuffle64<>+0x18(SB)/8, $(4)
DATA shuffle64<>+0x20(SB)/8, $(3)
DATA shuffle64<>+0x28(SB)/8, $(2)
DATA shuffle64<>+0x30(SB)/8, $(1)
DATA shuffle64<>+0x38(SB)/8, $(0)
GLOBL shuffle64<>(SB), (RODATA+NOPTR), $64

// counter vector for calculating the bitmask for non fullfilled registers
// used by all compare algorithms
DATA countup64<>+0x00(SB)/8, $(8)
DATA countup64<>+0x08(SB)/8, $(7)
DATA countup64<>+0x10(SB)/8, $(6)
DATA countup64<>+0x18(SB)/8, $(5)
DATA countup64<>+0x20(SB)/8, $(4)
DATA countup64<>+0x28(SB)/8, $(3)
DATA countup64<>+0x30(SB)/8, $(2)
DATA countup64<>+0x38(SB)/8, $(1)
GLOBL countup64<>(SB), (RODATA+NOPTR), $64
