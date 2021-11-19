// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

DATA PRIME32_1<>+0x00(SB)/4, $(2654435761)
GLOBL PRIME32_1<>(SB), (RODATA+NOPTR), $4

DATA PRIME32_2<>+0x00(SB)/4, $(2246822519)
GLOBL PRIME32_2<>(SB), (RODATA+NOPTR), $4

DATA PRIME32_3<>+0x00(SB)/4, $(3266489917)
GLOBL PRIME32_3<>(SB), (RODATA+NOPTR), $4

DATA PRIME32_4<>+0x00(SB)/4, $(668265263)
GLOBL PRIME32_4<>(SB), (RODATA+NOPTR), $4

DATA PRIME32_5<>+0x00(SB)/4, $(374761393)
GLOBL PRIME32_5<>(SB), (RODATA+NOPTR), $4

DATA PRIME64_1<>+0x00(SB)/8, $(11400714785074694791)
GLOBL PRIME64_1<>(SB), (RODATA+NOPTR), $8

DATA PRIME64_2<>+0x00(SB)/8, $(14029467366897019727)
GLOBL PRIME64_2<>(SB), (RODATA+NOPTR), $8

DATA PRIME64_3<>+0x00(SB)/8, $(1609587929392839161)
GLOBL PRIME64_3<>(SB), (RODATA+NOPTR), $8

DATA PRIME64_4<>+0x00(SB)/8, $(9650029242287828579)
GLOBL PRIME64_4<>(SB), (RODATA+NOPTR), $8

DATA PRIME64_5<>+0x00(SB)/8, $(2870177450012600261)
GLOBL PRIME64_5<>(SB), (RODATA+NOPTR), $8
    
DATA constU32_4<>+0x00(SB)/4, $(4)
GLOBL constU32_4<>(SB), (RODATA+NOPTR), $4

DATA constU32_8<>+0x00(SB)/4, $(8)
GLOBL constU32_8<>(SB), (RODATA+NOPTR), $4

DATA constU64_4<>+0x00(SB)/8, $(4)
GLOBL constU64_4<>(SB), (RODATA+NOPTR), $8

DATA constU64_8<>+0x00(SB)/8, $(8)
GLOBL constU64_8<>(SB), (RODATA+NOPTR), $8

DATA maskHighD<>+0x00(SB)/8, $(0xffffffff00000000)
GLOBL maskHighD<>(SB), (RODATA+NOPTR), $8

DATA maskLowD<>+0x00(SB)/8, $(0x00000000ffffffff)
GLOBL maskLowD<>(SB), (RODATA+NOPTR), $8

DATA key64_008<>+0x00(SB)/8, $(0x1cad21f72c81017c)
GLOBL key64_008<>(SB), (RODATA+NOPTR), $8

DATA key64_016<>+0x00(SB)/8, $(0xdb979083e96dd4de)
GLOBL key64_016<>(SB), (RODATA+NOPTR), $8

DATA con64_1<>+0x00(SB)/8, $(0x9fb21c651e98df25)
GLOBL con64_1<>(SB), (RODATA+NOPTR), $8

// exp32_64 is the VPERMD input required to expand 4 32 bit values in lower part of YMM register 
// to 4 64 bit values in YMM register. Upper part of input register has to be zero
DATA exp32_64<>+0x00(SB)/4, $(0)
DATA exp32_64<>+0x04(SB)/4, $(4)
DATA exp32_64<>+0x08(SB)/4, $(1)
DATA exp32_64<>+0x0c(SB)/4, $(4)
DATA exp32_64<>+0x10(SB)/4, $(2)
DATA exp32_64<>+0x14(SB)/4, $(4)
DATA exp32_64<>+0x18(SB)/4, $(3)
DATA exp32_64<>+0x1c(SB)/4, $(4)
GLOBL exp32_64<>(SB), (RODATA+NOPTR), $32

// dbl32_64 is the VPERMD input required to expand 4 32 bit values in lower part of YMM register 
// to 4 64 bit values in YMM register. lower and upper part of 64 bit values contain the 32 bit input
DATA dbl32_64<>+0x00(SB)/4, $(0)
DATA dbl32_64<>+0x04(SB)/4, $(0)
DATA dbl32_64<>+0x08(SB)/4, $(1)
DATA dbl32_64<>+0x0c(SB)/4, $(1)
DATA dbl32_64<>+0x10(SB)/4, $(2)
DATA dbl32_64<>+0x14(SB)/4, $(2)
DATA dbl32_64<>+0x18(SB)/4, $(3)
DATA dbl32_64<>+0x1c(SB)/4, $(3)
DATA dbl32_64<>+0x20(SB)/4, $(4)
DATA dbl32_64<>+0x24(SB)/4, $(4)
DATA dbl32_64<>+0x28(SB)/4, $(5)
DATA dbl32_64<>+0x2c(SB)/4, $(5)
DATA dbl32_64<>+0x30(SB)/4, $(6)
DATA dbl32_64<>+0x34(SB)/4, $(6)
DATA dbl32_64<>+0x38(SB)/4, $(7)
DATA dbl32_64<>+0x3c(SB)/4, $(7)
GLOBL dbl32_64<>(SB), (RODATA+NOPTR), $64

// perm is the VPERMD input required to deinterleave the results
DATA perm<>+0x00(SB)/4, $(0)
DATA perm<>+0x04(SB)/4, $(2)
DATA perm<>+0x08(SB)/4, $(4)
DATA perm<>+0x0c(SB)/4, $(6)
DATA perm<>+0x10(SB)/4, $(1)
DATA perm<>+0x14(SB)/4, $(3)
DATA perm<>+0x18(SB)/4, $(5)
DATA perm<>+0x1c(SB)/4, $(7)
GLOBL perm<>(SB), (RODATA+NOPTR), $32

// perm512 is the VPERMD input required to deinterleave the results
DATA perm512<>+0x00(SB)/4, $(0)
DATA perm512<>+0x04(SB)/4, $(2)
DATA perm512<>+0x08(SB)/4, $(4)
DATA perm512<>+0x0c(SB)/4, $(6)
DATA perm512<>+0x10(SB)/4, $(8)
DATA perm512<>+0x14(SB)/4, $(10)
DATA perm512<>+0x18(SB)/4, $(12)
DATA perm512<>+0x1c(SB)/4, $(14)
DATA perm512<>+0x20(SB)/4, $(1)
DATA perm512<>+0x24(SB)/4, $(3)
DATA perm512<>+0x28(SB)/4, $(5)
DATA perm512<>+0x2c(SB)/4, $(7)
DATA perm512<>+0x30(SB)/4, $(9)
DATA perm512<>+0x34(SB)/4, $(11)
DATA perm512<>+0x38(SB)/4, $(13)
DATA perm512<>+0x3c(SB)/4, $(15)
GLOBL perm512<>(SB), (RODATA+NOPTR), $64

// buffers for bit positions and bitmasks
GLOBL buf_pos<>(SB), (NOPTR), $32
GLOBL buf_val<>(SB), (NOPTR), $32
