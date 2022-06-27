// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

DATA countdown<>+0x00(SB)/8, $(0x00)
DATA countdown<>+0x08(SB)/8, $(0x01)
DATA countdown<>+0x10(SB)/8, $(0x02)
DATA countdown<>+0x18(SB)/8, $(0x03)
GLOBL countdown<>(SB), (RODATA+NOPTR), $32

DATA sel_mask64<>+0x0(SB)/8, $(0x00000000000000ff)
GLOBL sel_mask64<>(SB), (RODATA+NOPTR), $8

DATA sel_mask32<>+0x0(SB)/4, $(0x000000ff)
GLOBL sel_mask32<>(SB), (RODATA+NOPTR), $4

DATA sel_LUT<>+0x00(SB)/1, $(240)
DATA sel_LUT<>+0x01(SB)/1, $(120)
DATA sel_LUT<>+0x02(SB)/1, $(60)
DATA sel_LUT<>+0x03(SB)/1, $(30)
DATA sel_LUT<>+0x04(SB)/1, $(20)
DATA sel_LUT<>+0x05(SB)/1, $(15)
DATA sel_LUT<>+0x06(SB)/1, $(12)
DATA sel_LUT<>+0x07(SB)/1, $(10)
DATA sel_LUT<>+0x08(SB)/1, $(8)
DATA sel_LUT<>+0x09(SB)/1, $(7)
DATA sel_LUT<>+0x0a(SB)/1, $(6)
DATA sel_LUT<>+0x0b(SB)/1, $(5)
DATA sel_LUT<>+0x0c(SB)/1, $(4)
DATA sel_LUT<>+0x0d(SB)/1, $(3)
DATA sel_LUT<>+0x0e(SB)/1, $(2)
DATA sel_LUT<>+0x0f(SB)/1, $(1)
DATA sel_LUT<>+0x10(SB)/1, $(240)
DATA sel_LUT<>+0x11(SB)/1, $(120)
DATA sel_LUT<>+0x12(SB)/1, $(60)
DATA sel_LUT<>+0x13(SB)/1, $(30)
DATA sel_LUT<>+0x14(SB)/1, $(20)
DATA sel_LUT<>+0x15(SB)/1, $(15)
DATA sel_LUT<>+0x16(SB)/1, $(12)
DATA sel_LUT<>+0x17(SB)/1, $(10)
DATA sel_LUT<>+0x18(SB)/1, $(8)
DATA sel_LUT<>+0x19(SB)/1, $(7)
DATA sel_LUT<>+0x1a(SB)/1, $(6)
DATA sel_LUT<>+0x1b(SB)/1, $(5)
DATA sel_LUT<>+0x1c(SB)/1, $(4)
DATA sel_LUT<>+0x1d(SB)/1, $(3)
DATA sel_LUT<>+0x1e(SB)/1, $(2)
DATA sel_LUT<>+0x1f(SB)/1, $(1)
GLOBL sel_LUT<>(SB), (RODATA+NOPTR), $32
