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

DATA constU32_1<>+0x00(SB)/4, $(1)
GLOBL constU32_1<>(SB), (RODATA+NOPTR), $4

DATA constU32_4<>+0x00(SB)/4, $(4)
GLOBL constU32_4<>(SB), (RODATA+NOPTR), $4

DATA constU32_8<>+0x00(SB)/4, $(8)
GLOBL constU32_8<>(SB), (RODATA+NOPTR), $4

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
GLOBL buf_pos<>(SB), (NOPTR), $64
GLOBL buf_val<>(SB), (NOPTR), $64
