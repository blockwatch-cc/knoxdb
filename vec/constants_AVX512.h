// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// perm64 is the VPERMQ input required to permute bytes in each input word
// used by all 64 bit compare algorithms
DATA perm64<>+0x00(SB)/8, $(7)
DATA perm64<>+0x08(SB)/8, $(6)
DATA perm64<>+0x10(SB)/8, $(5)
DATA perm64<>+0x18(SB)/8, $(4)
DATA perm64<>+0x20(SB)/8, $(3)
DATA perm64<>+0x28(SB)/8, $(2)
DATA perm64<>+0x30(SB)/8, $(1)
DATA perm64<>+0x38(SB)/8, $(0)
GLOBL perm64<>(SB), (RODATA+NOPTR), $64

// perm32 is the VPERMD input required to permute bytes in each input word
// used by all 32 bit compare algorithms
DATA perm32<>+0x00(SB)/4, $(7)
DATA perm32<>+0x04(SB)/4, $(6)
DATA perm32<>+0x08(SB)/4, $(5)
DATA perm32<>+0x0C(SB)/4, $(4)
DATA perm32<>+0x10(SB)/4, $(3)
DATA perm32<>+0x14(SB)/4, $(2)
DATA perm32<>+0x18(SB)/4, $(1)
DATA perm32<>+0x1C(SB)/4, $(0)
DATA perm32<>+0x20(SB)/4, $(15)
DATA perm32<>+0x24(SB)/4, $(14)
DATA perm32<>+0x28(SB)/4, $(13)
DATA perm32<>+0x2C(SB)/4, $(12)
DATA perm32<>+0x30(SB)/4, $(11)
DATA perm32<>+0x34(SB)/4, $(10)
DATA perm32<>+0x38(SB)/4, $(9)
DATA perm32<>+0x3C(SB)/4, $(8)
GLOBL perm32<>(SB), (RODATA+NOPTR), $64

// perm16 is the VPERMD input required to permute bytes in each input word
// used by all 32 bit compare algorithms
DATA perm16<>+0x00(SB)/2, $(7)
DATA perm16<>+0x02(SB)/2, $(6)
DATA perm16<>+0x04(SB)/2, $(5)
DATA perm16<>+0x06(SB)/2, $(4)
DATA perm16<>+0x08(SB)/2, $(3)
DATA perm16<>+0x0A(SB)/2, $(2)
DATA perm16<>+0x0C(SB)/2, $(1)
DATA perm16<>+0x0E(SB)/2, $(0)
DATA perm16<>+0x10(SB)/2, $(15)
DATA perm16<>+0x12(SB)/2, $(14)
DATA perm16<>+0x14(SB)/2, $(13)
DATA perm16<>+0x16(SB)/2, $(12)
DATA perm16<>+0x18(SB)/2, $(11)
DATA perm16<>+0x1A(SB)/2, $(10)
DATA perm16<>+0x1C(SB)/2, $(9)
DATA perm16<>+0x1E(SB)/2, $(8)
DATA perm16<>+0x20(SB)/2, $(23)
DATA perm16<>+0x22(SB)/2, $(22)
DATA perm16<>+0x24(SB)/2, $(21)
DATA perm16<>+0x26(SB)/2, $(20)
DATA perm16<>+0x28(SB)/2, $(19)
DATA perm16<>+0x2A(SB)/2, $(18)
DATA perm16<>+0x2C(SB)/2, $(17)
DATA perm16<>+0x2E(SB)/2, $(16)
DATA perm16<>+0x30(SB)/2, $(31)
DATA perm16<>+0x32(SB)/2, $(30)
DATA perm16<>+0x34(SB)/2, $(29)
DATA perm16<>+0x36(SB)/2, $(28)
DATA perm16<>+0x38(SB)/2, $(27)
DATA perm16<>+0x3A(SB)/2, $(26)
DATA perm16<>+0x3C(SB)/2, $(25)
DATA perm16<>+0x3E(SB)/2, $(24)
GLOBL perm16<>(SB), (RODATA+NOPTR), $64

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

// counter vector for calculating the bitmask for non fullfilled registers
// used by all compare algorithms
/*DATA countup32<>+0x00(SB)/4, $(8)
DATA countup32<>+0x04(SB)/4,  $(7)
DATA countup32<>+0x08(SB)/4, $(6)
DATA countup32<>+0x0C(SB)/4, $(5)
DATA countup32<>+0x10(SB)/4, $(4)
DATA countup32<>+0x14(SB)/4, $(3)
DATA countup32<>+0x18(SB)/4, $(2)
DATA countup32<>+0x1C(SB)/4, $(1)
GLOBL countup32<>(SB), (RODATA+NOPTR), $32
*/
