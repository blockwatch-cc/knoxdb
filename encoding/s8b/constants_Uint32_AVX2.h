// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

#define mask1 $(0xffffffff)

DATA mask2<>+0x00(SB)/8, $(0x3fffffff3fffffff)
GLOBL mask2<>(SB), (RODATA+NOPTR), $8

DATA mask3<>+0x00(SB)/4, $(0xfffff)
DATA mask3<>+0x04(SB)/4, $(0xfffff)
DATA mask3<>+0x08(SB)/4, $(0xfffff)
DATA mask3<>+0x0c(SB)/4, $(0xfffff)
GLOBL mask3<>(SB), (RODATA+NOPTR), $16

DATA mask4<>+0x00(SB)/4, $(0x7fff)
DATA mask4<>+0x04(SB)/4, $(0x7fff)
DATA mask4<>+0x08(SB)/4, $(0x7fff)
DATA mask4<>+0x0c(SB)/4, $(0x7fff)
GLOBL mask4<>(SB), (RODATA+NOPTR), $16

DATA mask5<>+0x00(SB)/4, $(0xfff)
DATA mask5<>+0x04(SB)/4, $(0xfff)
DATA mask5<>+0x08(SB)/4, $(0xfff)
DATA mask5<>+0x0c(SB)/4, $(0xfff)
DATA mask5<>+0x10(SB)/4, $(0xfff)
DATA mask5<>+0x14(SB)/4, $(0xfff)
DATA mask5<>+0x18(SB)/4, $(0xfff)
DATA mask5<>+0x1c(SB)/4, $(0xfff)
GLOBL mask5<>(SB), (RODATA+NOPTR), $32

DATA mask6<>+0x00(SB)/4, $(0x3ff)
DATA mask6<>+0x04(SB)/4, $(0x3ff)
DATA mask6<>+0x08(SB)/4, $(0x3ff)
DATA mask6<>+0x0c(SB)/4, $(0x3ff)
DATA mask6<>+0x10(SB)/4, $(0x3ff)
DATA mask6<>+0x14(SB)/4, $(0x3ff)
DATA mask6<>+0x18(SB)/4, $(0x3ff)
DATA mask6<>+0x1c(SB)/4, $(0x3ff)
GLOBL mask6<>(SB), (RODATA+NOPTR), $32

DATA mask7<>+0x00(SB)/4, $(0xff)
DATA mask7<>+0x04(SB)/4, $(0xff)
DATA mask7<>+0x08(SB)/4, $(0xff)
DATA mask7<>+0x0c(SB)/4, $(0xff)
DATA mask7<>+0x10(SB)/4, $(0xff)
DATA mask7<>+0x14(SB)/4, $(0xff)
DATA mask7<>+0x18(SB)/4, $(0xff)
DATA mask7<>+0x1c(SB)/4, $(0xff)
GLOBL mask7<>(SB), (RODATA+NOPTR), $32

DATA mask8<>+0x00(SB)/4, $(0x7f)
DATA mask8<>+0x04(SB)/4, $(0x7f)
DATA mask8<>+0x08(SB)/4, $(0x7f)
DATA mask8<>+0x0c(SB)/4, $(0x7f)
DATA mask8<>+0x10(SB)/4, $(0x7f)
DATA mask8<>+0x14(SB)/4, $(0x7f)
DATA mask8<>+0x18(SB)/4, $(0x7f)
DATA mask8<>+0x1c(SB)/4, $(0x7f)
GLOBL mask8<>(SB), (RODATA+NOPTR), $32

DATA mask10<>+0x00(SB)/4, $(0x3f)
GLOBL mask10<>(SB), (RODATA+NOPTR), $4

DATA mask12<>+0x00(SB)/4, $(0x1f)
GLOBL mask12<>(SB), (RODATA+NOPTR), $4

DATA mask15<>+0x00(SB)/4, $(0xf)
GLOBL mask15<>(SB), (RODATA+NOPTR), $4

DATA mask20<>+0x00(SB)/4, $(0x7)
GLOBL mask20<>(SB), (RODATA+NOPTR), $4

DATA mask30<>+0x00(SB)/4, $(0x3)
GLOBL mask30<>(SB), (RODATA+NOPTR), $4

DATA mask60<>+0x00(SB)/8, $(0x1)
GLOBL mask60<>(SB), (RODATA+NOPTR), $8

DATA shift2<>+0x00(SB)/8, $(0)
DATA shift2<>+0x08(SB)/8, $(30)
GLOBL shift2<>(SB), (RODATA+NOPTR), $16

DATA shift3<>+0x00(SB)/8, $(0)
DATA shift3<>+0x08(SB)/8, $(40)
DATA shift3<>+0x10(SB)/8, $(20)
DATA shift3<>+0x18(SB)/8, $(0)
GLOBL shift3<>(SB), (RODATA+NOPTR), $32

DATA shift4<>+0x00(SB)/8, $(0)
DATA shift4<>+0x08(SB)/8, $(30)
DATA shift4<>+0x10(SB)/8, $(15)
DATA shift4<>+0x18(SB)/8, $(45)
GLOBL shift4<>(SB), (RODATA+NOPTR), $32

DATA shift5<>+0x00(SB)/8, $(0)
DATA shift5<>+0x08(SB)/8, $(24)
DATA shift5<>+0x10(SB)/8, $(48)
DATA shift5<>+0x18(SB)/8, $(0)
DATA shift5<>+0x20(SB)/8, $(12)
DATA shift5<>+0x28(SB)/8, $(36)
DATA shift5<>+0x30(SB)/8, $(0)
DATA shift5<>+0x38(SB)/8, $(0)
GLOBL shift5<>(SB), (RODATA+NOPTR), $64

DATA shift6<>+0x00(SB)/8, $(0)
DATA shift6<>+0x08(SB)/8, $(20)
DATA shift6<>+0x10(SB)/8, $(40)
DATA shift6<>+0x18(SB)/8, $(0)
DATA shift6<>+0x20(SB)/8, $(10)
DATA shift6<>+0x28(SB)/8, $(30)
DATA shift6<>+0x30(SB)/8, $(50)
DATA shift6<>+0x38(SB)/8, $(0)
GLOBL shift6<>(SB), (RODATA+NOPTR), $64

DATA shift7<>+0x00(SB)/4, $(0)
DATA shift7<>+0x04(SB)/4, $(8)
DATA shift7<>+0x08(SB)/4, $(16)
DATA shift7<>+0x0c(SB)/4, $(24)
DATA shift7<>+0x10(SB)/4, $(0)
DATA shift7<>+0x14(SB)/4, $(8)
DATA shift7<>+0x18(SB)/4, $(16)
DATA shift7<>+0x1c(SB)/4, $(0)
GLOBL shift7<>(SB), (RODATA+NOPTR), $32

DATA shift8<>+0x00(SB)/8, $(0)
DATA shift8<>+0x08(SB)/8, $(14)
DATA shift8<>+0x10(SB)/8, $(28)
DATA shift8<>+0x18(SB)/8, $(42)
DATA shift8<>+0x20(SB)/8, $(7)
DATA shift8<>+0x28(SB)/8, $(21)
DATA shift8<>+0x30(SB)/8, $(35)
DATA shift8<>+0x38(SB)/8, $(49)
GLOBL shift8<>(SB), (RODATA+NOPTR), $64

DATA shift10<>+0x00(SB)/8, $(0)
DATA shift10<>+0x08(SB)/8, $(12)
DATA shift10<>+0x10(SB)/8, $(24)
DATA shift10<>+0x18(SB)/8, $(36)
DATA shift10<>+0x20(SB)/8, $(6)
DATA shift10<>+0x28(SB)/8, $(18)
DATA shift10<>+0x30(SB)/8, $(30)
DATA shift10<>+0x38(SB)/8, $(42)
DATA shift10<>+0x40(SB)/4, $(16)
DATA shift10<>+0x44(SB)/4, $(22)
DATA shift10<>+0x48(SB)/8, $(0)
DATA shift10<>+0x50(SB)/8, $(0)
DATA shift10<>+0x58(SB)/8, $(0)
GLOBL shift10<>(SB), (RODATA+NOPTR), $96

DATA shift12<>+0x00(SB)/8, $(0)
DATA shift12<>+0x08(SB)/8, $(10)
DATA shift12<>+0x10(SB)/8, $(20)
DATA shift12<>+0x18(SB)/8, $(30)
DATA shift12<>+0x20(SB)/8, $(5)
DATA shift12<>+0x28(SB)/8, $(15)
DATA shift12<>+0x30(SB)/8, $(25)
DATA shift12<>+0x38(SB)/8, $(35)
DATA shift12<>+0x40(SB)/8, $(40)
DATA shift12<>+0x48(SB)/8, $(50)
DATA shift12<>+0x50(SB)/8, $(45)
DATA shift12<>+0x58(SB)/8, $(55)
GLOBL shift12<>(SB), (RODATA+NOPTR), $96

DATA shift15<>+0x00(SB)/4, $(0)
DATA shift15<>+0x04(SB)/4, $(4)
DATA shift15<>+0x08(SB)/4, $(8)
DATA shift15<>+0x0c(SB)/4, $(12)
DATA shift15<>+0x10(SB)/4, $(16)
DATA shift15<>+0x14(SB)/4, $(20)
DATA shift15<>+0x18(SB)/4, $(24)
DATA shift15<>+0x1c(SB)/4, $(28)
DATA shift15<>+0x20(SB)/4, $(0)
DATA shift15<>+0x24(SB)/4, $(4)
DATA shift15<>+0x28(SB)/4, $(8)
DATA shift15<>+0x2c(SB)/4, $(12)
DATA shift15<>+0x30(SB)/4, $(16)
DATA shift15<>+0x34(SB)/4, $(20)
DATA shift15<>+0x38(SB)/4, $(24)
DATA shift15<>+0x3c(SB)/4, $(28)
GLOBL shift15<>(SB), (RODATA+NOPTR), $64

DATA shift20<>+0x00(SB)/4, $(0)
DATA shift20<>+0x04(SB)/4, $(3)
DATA shift20<>+0x08(SB)/4, $(6)
DATA shift20<>+0x0c(SB)/4, $(9)
DATA shift20<>+0x10(SB)/4, $(12)
DATA shift20<>+0x14(SB)/4, $(15)
DATA shift20<>+0x18(SB)/4, $(18)
DATA shift20<>+0x1c(SB)/4, $(21)
DATA shift20<>+0x20(SB)/8, $(24)
DATA shift20<>+0x28(SB)/8, $(30)
DATA shift20<>+0x30(SB)/8, $(36)
DATA shift20<>+0x38(SB)/8, $(42)
DATA shift20<>+0x40(SB)/8, $(27)
DATA shift20<>+0x48(SB)/8, $(33)
DATA shift20<>+0x50(SB)/8, $(39)
DATA shift20<>+0x58(SB)/8, $(45)
DATA shift20<>+0x60(SB)/4, $(16)
DATA shift20<>+0x64(SB)/4, $(19)
DATA shift20<>+0x68(SB)/4, $(22)
DATA shift20<>+0x6c(SB)/4, $(25)
GLOBL shift20<>(SB), (RODATA+NOPTR), $112

DATA shift30<>+0x00(SB)/4, $(0)
DATA shift30<>+0x04(SB)/4, $(2)
DATA shift30<>+0x08(SB)/4, $(4)
DATA shift30<>+0x0c(SB)/4, $(6)
DATA shift30<>+0x10(SB)/4, $(8)
DATA shift30<>+0x14(SB)/4, $(10)
DATA shift30<>+0x18(SB)/4, $(12)
DATA shift30<>+0x1c(SB)/4, $(14)
DATA shift30<>+0x20(SB)/4, $(16)
DATA shift30<>+0x24(SB)/4, $(18)
DATA shift30<>+0x28(SB)/4, $(20)
DATA shift30<>+0x2c(SB)/4, $(22)
DATA shift30<>+0x30(SB)/4, $(24)
DATA shift30<>+0x34(SB)/4, $(26)
DATA shift30<>+0x38(SB)/4, $(28)
DATA shift30<>+0x3c(SB)/4, $(30)
GLOBL shift30<>(SB), (RODATA+NOPTR), $64

DATA shift60<>+0x00(SB)/4, $(0)
DATA shift60<>+0x04(SB)/4, $(1)
DATA shift60<>+0x08(SB)/4, $(2)
DATA shift60<>+0x0c(SB)/4, $(3)
DATA shift60<>+0x10(SB)/4, $(4)
DATA shift60<>+0x14(SB)/4, $(5)
DATA shift60<>+0x18(SB)/4, $(6)
DATA shift60<>+0x1c(SB)/4, $(7)
DATA shift60<>+0x20(SB)/4, $(8)
DATA shift60<>+0x24(SB)/4, $(9)
DATA shift60<>+0x28(SB)/4, $(10)
DATA shift60<>+0x2c(SB)/4, $(11)
DATA shift60<>+0x30(SB)/4, $(12)
DATA shift60<>+0x34(SB)/4, $(13)
DATA shift60<>+0x38(SB)/4, $(14)
DATA shift60<>+0x3c(SB)/4, $(15)
DATA shift60<>+0x40(SB)/4, $(16)
DATA shift60<>+0x44(SB)/4, $(17)
DATA shift60<>+0x48(SB)/4, $(18)
DATA shift60<>+0x4c(SB)/4, $(19)
DATA shift60<>+0x50(SB)/4, $(20)
DATA shift60<>+0x54(SB)/4, $(21)
DATA shift60<>+0x58(SB)/4, $(22)
DATA shift60<>+0x5c(SB)/4, $(23)
DATA shift60<>+0x60(SB)/4, $(24)
DATA shift60<>+0x64(SB)/4, $(25)
DATA shift60<>+0x68(SB)/4, $(26)
DATA shift60<>+0x6c(SB)/4, $(27)
DATA shift60<>+0x70(SB)/4, $(28)
DATA shift60<>+0x74(SB)/4, $(29)
DATA shift60<>+0x78(SB)/4, $(30)
DATA shift60<>+0x7c(SB)/4, $(31)
GLOBL shift60<>(SB), (RODATA+NOPTR), $128

DATA write3mask<>+0x00(SB)/4, $(0xffffffff)
DATA write3mask<>+0x04(SB)/4, $(0xffffffff)
DATA write3mask<>+0x08(SB)/4, $(0xffffffff)
DATA write3mask<>+0x0c(SB)/4, $(0)
DATA write3mask<>+0x10(SB)/4, $(0)
DATA write3mask<>+0x14(SB)/4, $(0)
DATA write3mask<>+0x18(SB)/4, $(0)
DATA write3mask<>+0x1c(SB)/4, $(0)
GLOBL write3mask<>(SB), (RODATA+NOPTR), $32

DATA write5mask<>+0x00(SB)/4, $(0xffffffff)
DATA write5mask<>+0x04(SB)/4, $(0xffffffff)
DATA write5mask<>+0x08(SB)/4, $(0xffffffff)
DATA write5mask<>+0x0c(SB)/4, $(0xffffffff)
DATA write5mask<>+0x10(SB)/4, $(0xffffffff)
DATA write5mask<>+0x14(SB)/4, $(0)
DATA write5mask<>+0x18(SB)/4, $(0)
DATA write5mask<>+0x1c(SB)/4, $(0)
GLOBL write5mask<>(SB), (RODATA+NOPTR), $32

DATA write6mask<>+0x00(SB)/4, $(0xffffffff)
DATA write6mask<>+0x04(SB)/4, $(0xffffffff)
DATA write6mask<>+0x08(SB)/4, $(0xffffffff)
DATA write6mask<>+0x0c(SB)/4, $(0xffffffff)
DATA write6mask<>+0x10(SB)/4, $(0xffffffff)
DATA write6mask<>+0x14(SB)/4, $(0xffffffff)
DATA write6mask<>+0x18(SB)/4, $(0)
DATA write6mask<>+0x1c(SB)/4, $(0)
GLOBL write6mask<>(SB), (RODATA+NOPTR), $32

DATA write7mask<>+0x00(SB)/4, $(0xffffffff)
DATA write7mask<>+0x04(SB)/4, $(0xffffffff)
DATA write7mask<>+0x08(SB)/4, $(0xffffffff)
DATA write7mask<>+0x0c(SB)/4, $(0xffffffff)
DATA write7mask<>+0x10(SB)/4, $(0xffffffff)
DATA write7mask<>+0x14(SB)/4, $(0xffffffff)
DATA write7mask<>+0x18(SB)/4, $(0xffffffff)
DATA write7mask<>+0x1c(SB)/4, $(0)
GLOBL write7mask<>(SB), (RODATA+NOPTR), $32

GLOBL funcTableUint32AVX2<>(SB), (NOPTR), $128
