// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// const_0x55 is the static input for POPCOUNT(VAL) sub-algorithm
DATA const_0x55<>+0x00(SB)/1, $(0x55)
GLOBL const_0x55<>(SB), (RODATA+NOPTR), $1

// const_0x33 is the static input for POPCOUNT(VAL) sub-algorithm
DATA const_0x33<>+0x00(SB)/1, $(0x33)
GLOBL const_0x33<>(SB), (RODATA+NOPTR), $1

// const_0x0f is the static input for POPCOUNT(VAL) sub-algorithm
DATA const_0x0f<>+0x00(SB)/1, $(0x0f)
GLOBL const_0x0f<>(SB), (RODATA+NOPTR), $1

DATA const_64<>+0x00(SB)/4, $(64)
GLOBL const_64<>(SB), (RODATA+NOPTR), $4

DATA const_128<>+0x00(SB)/4, $(128)
GLOBL const_128<>(SB), (RODATA+NOPTR), $4

DATA const_2048<>+0x00(SB)/4, $(2048)
GLOBL const_2048<>(SB), (RODATA+NOPTR), $4

DATA const_8<>+0x00(SB)/4, $(8)
GLOBL const_8<>(SB), (RODATA+NOPTR), $4


// shuffle128 is the VPSHUFB input required to spread bytes in each word
// used by all compare algorithms
DATA shuffle128<>+0x00(SB)/1, $(14)
DATA shuffle128<>+0x01(SB)/1, $(6)
DATA shuffle128<>+0x02(SB)/1, $(15)
DATA shuffle128<>+0x03(SB)/1, $(7)
DATA shuffle128<>+0x04(SB)/1, $(12)
DATA shuffle128<>+0x05(SB)/1, $(4)
DATA shuffle128<>+0x06(SB)/1, $(13)
DATA shuffle128<>+0x07(SB)/1, $(5)
DATA shuffle128<>+0x08(SB)/1, $(10)
DATA shuffle128<>+0x09(SB)/1, $(2)
DATA shuffle128<>+0x0a(SB)/1, $(11)
DATA shuffle128<>+0x0b(SB)/1, $(3)
DATA shuffle128<>+0x0c(SB)/1, $(8)
DATA shuffle128<>+0x0d(SB)/1, $(0)
DATA shuffle128<>+0x0e(SB)/1, $(9)
DATA shuffle128<>+0x0f(SB)/1, $(1)
DATA shuffle128<>+0x10(SB)/1, $(14)
DATA shuffle128<>+0x11(SB)/1, $(6)
DATA shuffle128<>+0x12(SB)/1, $(15)
DATA shuffle128<>+0x13(SB)/1, $(7)
DATA shuffle128<>+0x14(SB)/1, $(12)
DATA shuffle128<>+0x15(SB)/1, $(4)
DATA shuffle128<>+0x16(SB)/1, $(13)
DATA shuffle128<>+0x17(SB)/1, $(5)
DATA shuffle128<>+0x18(SB)/1, $(10)
DATA shuffle128<>+0x19(SB)/1, $(2)
DATA shuffle128<>+0x1a(SB)/1, $(11)
DATA shuffle128<>+0x1b(SB)/1, $(3)
DATA shuffle128<>+0x1c(SB)/1, $(8)
DATA shuffle128<>+0x1d(SB)/1, $(0)
DATA shuffle128<>+0x1e(SB)/1, $(9)
DATA shuffle128<>+0x1f(SB)/1, $(1)
GLOBL shuffle128<>(SB), (RODATA+NOPTR), $32

// look up table for reverting nibbles
// used by bitset revert algorithm
DATA LUT_reverse<>+0x00(SB)/1, $(0)
DATA LUT_reverse<>+0x01(SB)/1, $(8)
DATA LUT_reverse<>+0x02(SB)/1, $(4)
DATA LUT_reverse<>+0x03(SB)/1, $(12)
DATA LUT_reverse<>+0x04(SB)/1, $(2)
DATA LUT_reverse<>+0x05(SB)/1, $(10)
DATA LUT_reverse<>+0x06(SB)/1, $(6)
DATA LUT_reverse<>+0x07(SB)/1, $(14)
DATA LUT_reverse<>+0x08(SB)/1, $(1)
DATA LUT_reverse<>+0x09(SB)/1, $(9)
DATA LUT_reverse<>+0x0a(SB)/1, $(5)
DATA LUT_reverse<>+0x0b(SB)/1, $(13)
DATA LUT_reverse<>+0x0c(SB)/1, $(3)
DATA LUT_reverse<>+0x0d(SB)/1, $(11)
DATA LUT_reverse<>+0x0e(SB)/1, $(7)
DATA LUT_reverse<>+0x0f(SB)/1, $(15)
DATA LUT_reverse<>+0x10(SB)/1, $(0)
DATA LUT_reverse<>+0x11(SB)/1, $(8)
DATA LUT_reverse<>+0x12(SB)/1, $(4)
DATA LUT_reverse<>+0x13(SB)/1, $(12)
DATA LUT_reverse<>+0x14(SB)/1, $(2)
DATA LUT_reverse<>+0x15(SB)/1, $(10)
DATA LUT_reverse<>+0x16(SB)/1, $(6)
DATA LUT_reverse<>+0x17(SB)/1, $(14)
DATA LUT_reverse<>+0x18(SB)/1, $(1)
DATA LUT_reverse<>+0x19(SB)/1, $(9)
DATA LUT_reverse<>+0x1a(SB)/1, $(5)
DATA LUT_reverse<>+0x1b(SB)/1, $(13)
DATA LUT_reverse<>+0x1c(SB)/1, $(3)
DATA LUT_reverse<>+0x1d(SB)/1, $(11)
DATA LUT_reverse<>+0x1e(SB)/1, $(7)
DATA LUT_reverse<>+0x1f(SB)/1, $(15)
GLOBL LUT_reverse<>(SB), (RODATA+NOPTR), $32

// VPSHUFB input required to revert bytes within qwords
// used by bitset revert algorithm
DATA shuf_reverse<>+0x00(SB)/1, $(15)
DATA shuf_reverse<>+0x01(SB)/1, $(14)
DATA shuf_reverse<>+0x02(SB)/1, $(13)
DATA shuf_reverse<>+0x03(SB)/1, $(12)
DATA shuf_reverse<>+0x04(SB)/1, $(11)
DATA shuf_reverse<>+0x05(SB)/1, $(10)
DATA shuf_reverse<>+0x06(SB)/1, $(9)
DATA shuf_reverse<>+0x07(SB)/1, $(8)
DATA shuf_reverse<>+0x08(SB)/1, $(7)
DATA shuf_reverse<>+0x09(SB)/1, $(6)
DATA shuf_reverse<>+0x0a(SB)/1, $(5)
DATA shuf_reverse<>+0x0b(SB)/1, $(4)
DATA shuf_reverse<>+0x0c(SB)/1, $(3)
DATA shuf_reverse<>+0x0d(SB)/1, $(2)
DATA shuf_reverse<>+0x0e(SB)/1, $(1)
DATA shuf_reverse<>+0x0f(SB)/1, $(0)
DATA shuf_reverse<>+0x10(SB)/1, $(15)
DATA shuf_reverse<>+0x11(SB)/1, $(14)
DATA shuf_reverse<>+0x12(SB)/1, $(13)
DATA shuf_reverse<>+0x13(SB)/1, $(12)
DATA shuf_reverse<>+0x14(SB)/1, $(11)
DATA shuf_reverse<>+0x15(SB)/1, $(10)
DATA shuf_reverse<>+0x16(SB)/1, $(9)
DATA shuf_reverse<>+0x17(SB)/1, $(8)
DATA shuf_reverse<>+0x18(SB)/1, $(7)
DATA shuf_reverse<>+0x19(SB)/1, $(6)
DATA shuf_reverse<>+0x1a(SB)/1, $(5)
DATA shuf_reverse<>+0x1b(SB)/1, $(4)
DATA shuf_reverse<>+0x1c(SB)/1, $(3)
DATA shuf_reverse<>+0x1d(SB)/1, $(2)
DATA shuf_reverse<>+0x1e(SB)/1, $(1)
DATA shuf_reverse<>+0x1f(SB)/1, $(0)
GLOBL shuf_reverse<>(SB), (RODATA+NOPTR), $32

// VPERMD input required to revert 128 bit lanes within YMM-register
// used by bitset revert algorithm
DATA perm_reverse<>+0x00(SB)/4, $(4)
DATA perm_reverse<>+0x04(SB)/4, $(5)
DATA perm_reverse<>+0x08(SB)/4, $(6)
DATA perm_reverse<>+0x0c(SB)/4, $(7)
DATA perm_reverse<>+0x10(SB)/4, $(0)
DATA perm_reverse<>+0x14(SB)/4, $(1)
DATA perm_reverse<>+0x18(SB)/4, $(2)
DATA perm_reverse<>+0x1c(SB)/4, $(3)
GLOBL perm_reverse<>(SB), (RODATA+NOPTR), $32

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
