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
    
DATA constU32_4<>+0x00(SB)/4, $(4)
GLOBL constU32_4<>(SB), (RODATA+NOPTR), $4

DATA constU32_8<>+0x00(SB)/4, $(8)
GLOBL constU32_8<>(SB), (RODATA+NOPTR), $4

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

// buffers for bit positions and bitmasks
//DATA buf_bpos<>+0x00(SB)/8, $(0)
GLOBL buf_bpos<>(SB), (NOPTR), $128
//DATA perm512<>+0x00(SB)/4, $(0)
GLOBL buf_mask<>(SB), (NOPTR), $128
