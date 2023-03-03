// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// crosslane is the VPERMD input required to move data between AVX2 lanes
// used by all compare algorithms
DATA crosslane<>+0x00(SB)/4, $(6)
DATA crosslane<>+0x04(SB)/4, $(7)
DATA crosslane<>+0x08(SB)/4, $(2)
DATA crosslane<>+0x0c(SB)/4, $(3)
DATA crosslane<>+0x10(SB)/4, $(4)
DATA crosslane<>+0x14(SB)/4, $(5)
DATA crosslane<>+0x18(SB)/4, $(0)
DATA crosslane<>+0x1c(SB)/4, $(1)
GLOBL crosslane<>(SB), (RODATA+NOPTR), $32

// VPSHUFB input required to spread bytes in each YMM register
// used by all 64 bit compare algorithms
DATA shuffle64<>+0x00(SB)/1, $(14)
DATA shuffle64<>+0x01(SB)/1, $(15)
DATA shuffle64<>+0x02(SB)/1, $(6)
DATA shuffle64<>+0x03(SB)/1, $(7)
DATA shuffle64<>+0x04(SB)/1, $(12)
DATA shuffle64<>+0x05(SB)/1, $(13)
DATA shuffle64<>+0x06(SB)/1, $(4)
DATA shuffle64<>+0x07(SB)/1, $(5)
DATA shuffle64<>+0x08(SB)/1, $(10)
DATA shuffle64<>+0x09(SB)/1, $(11)
DATA shuffle64<>+0x0a(SB)/1, $(2)
DATA shuffle64<>+0x0b(SB)/1, $(3)
DATA shuffle64<>+0x0c(SB)/1, $(8)
DATA shuffle64<>+0x0d(SB)/1, $(9)
DATA shuffle64<>+0x0e(SB)/1, $(0)
DATA shuffle64<>+0x0f(SB)/1, $(1)
DATA shuffle64<>+0x10(SB)/1, $(14)
DATA shuffle64<>+0x11(SB)/1, $(15)
DATA shuffle64<>+0x12(SB)/1, $(6)
DATA shuffle64<>+0x13(SB)/1, $(7)
DATA shuffle64<>+0x14(SB)/1, $(12)
DATA shuffle64<>+0x15(SB)/1, $(13)
DATA shuffle64<>+0x16(SB)/1, $(4)
DATA shuffle64<>+0x17(SB)/1, $(5)
DATA shuffle64<>+0x18(SB)/1, $(10)
DATA shuffle64<>+0x19(SB)/1, $(11)
DATA shuffle64<>+0x1a(SB)/1, $(2)
DATA shuffle64<>+0x1b(SB)/1, $(3)
DATA shuffle64<>+0x1c(SB)/1, $(8)
DATA shuffle64<>+0x1d(SB)/1, $(9)
DATA shuffle64<>+0x1e(SB)/1, $(0)
DATA shuffle64<>+0x1f(SB)/1, $(1)
GLOBL shuffle64<>(SB), (RODATA+NOPTR), $32
