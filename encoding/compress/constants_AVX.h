// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

#define mask1 $(0xfffffffffffffff)

DATA const8_01<>+0x00(SB)/1, $(1)
GLOBL const8_01<>(SB), (RODATA+NOPTR), $1

DATA const8_7f<>+0x00(SB)/1, $(0x7f)
GLOBL const8_7f<>(SB), (RODATA+NOPTR), $1

DATA mask2<>+0x00(SB)/8, $(0x3fffffff)
GLOBL mask2<>(SB), (RODATA+NOPTR), $8

DATA mask3<>+0x00(SB)/8, $(0xfffff)
GLOBL mask3<>(SB), (RODATA+NOPTR), $8

DATA mask4<>+0x00(SB)/8, $(0x7fff)
GLOBL mask4<>(SB), (RODATA+NOPTR), $8

DATA mask5<>+0x00(SB)/8, $(0xfff)
GLOBL mask5<>(SB), (RODATA+NOPTR), $8

DATA mask6<>+0x00(SB)/8, $(0x3ff)
GLOBL mask6<>(SB), (RODATA+NOPTR), $8

DATA mask7<>+0x00(SB)/8, $(0xff)
GLOBL mask7<>(SB), (RODATA+NOPTR), $8

DATA mask8<>+0x00(SB)/8, $(0x7f)
GLOBL mask8<>(SB), (RODATA+NOPTR), $8

DATA mask10<>+0x00(SB)/8, $(0x3f)
GLOBL mask10<>(SB), (RODATA+NOPTR), $8

DATA mask12<>+0x00(SB)/8, $(0x1f)
GLOBL mask12<>(SB), (RODATA+NOPTR), $8

DATA mask15<>+0x00(SB)/8, $(0xf)
GLOBL mask15<>(SB), (RODATA+NOPTR), $8

DATA mask20<>+0x00(SB)/8, $(0x7)
GLOBL mask20<>(SB), (RODATA+NOPTR), $8

DATA mask30<>+0x00(SB)/8, $(0x3)
GLOBL mask30<>(SB), (RODATA+NOPTR), $8

DATA mask60<>+0x00(SB)/8, $(0x1)
GLOBL mask60<>(SB), (RODATA+NOPTR), $8

DATA shift2<>+0x00(SB)/8, $(0)
DATA shift2<>+0x08(SB)/8, $(30)
GLOBL shift2<>(SB), (RODATA+NOPTR), $16

DATA shift3<>+0x00(SB)/8, $(0)
DATA shift3<>+0x08(SB)/8, $(20)
DATA shift3<>+0x10(SB)/8, $(40)
DATA shift3<>+0x18(SB)/8, $(0)
GLOBL shift3<>(SB), (RODATA+NOPTR), $32

DATA shift4<>+0x00(SB)/8, $(0)
DATA shift4<>+0x08(SB)/8, $(15)
DATA shift4<>+0x10(SB)/8, $(30)
DATA shift4<>+0x18(SB)/8, $(45)
GLOBL shift4<>(SB), (RODATA+NOPTR), $32

DATA shift5<>+0x00(SB)/8, $(0)
DATA shift5<>+0x08(SB)/8, $(12)
DATA shift5<>+0x10(SB)/8, $(24)
DATA shift5<>+0x18(SB)/8, $(36)
DATA shift5<>+0x20(SB)/8, $(48)
DATA shift5<>+0x28(SB)/8, $(0)
DATA shift5<>+0x30(SB)/8, $(0)
DATA shift5<>+0x38(SB)/8, $(0)
GLOBL shift5<>(SB), (RODATA+NOPTR), $64

DATA shift6<>+0x00(SB)/8, $(0)
DATA shift6<>+0x08(SB)/8, $(10)
DATA shift6<>+0x10(SB)/8, $(20)
DATA shift6<>+0x18(SB)/8, $(30)
DATA shift6<>+0x20(SB)/8, $(40)
DATA shift6<>+0x28(SB)/8, $(50)
DATA shift6<>+0x30(SB)/8, $(0)
DATA shift6<>+0x38(SB)/8, $(0)
GLOBL shift6<>(SB), (RODATA+NOPTR), $64

DATA shift7<>+0x00(SB)/8, $(0)
DATA shift7<>+0x08(SB)/8, $(8)
DATA shift7<>+0x10(SB)/8, $(16)
DATA shift7<>+0x18(SB)/8, $(24)
DATA shift7<>+0x20(SB)/8, $(32)
DATA shift7<>+0x28(SB)/8, $(40)
DATA shift7<>+0x30(SB)/8, $(48)
DATA shift7<>+0x38(SB)/8, $(0)
GLOBL shift7<>(SB), (RODATA+NOPTR), $64

DATA shift8<>+0x00(SB)/8, $(0)
DATA shift8<>+0x08(SB)/8, $(7)
DATA shift8<>+0x10(SB)/8, $(14)
DATA shift8<>+0x18(SB)/8, $(21)
DATA shift8<>+0x20(SB)/8, $(28)
DATA shift8<>+0x28(SB)/8, $(35)
DATA shift8<>+0x30(SB)/8, $(42)
DATA shift8<>+0x38(SB)/8, $(49)
GLOBL shift8<>(SB), (RODATA+NOPTR), $64

DATA shift10<>+0x00(SB)/8, $(0)
DATA shift10<>+0x08(SB)/8, $(6)
DATA shift10<>+0x10(SB)/8, $(12)
DATA shift10<>+0x18(SB)/8, $(18)
DATA shift10<>+0x20(SB)/8, $(24)
DATA shift10<>+0x28(SB)/8, $(30)
DATA shift10<>+0x30(SB)/8, $(36)
DATA shift10<>+0x38(SB)/8, $(42)
DATA shift10<>+0x40(SB)/8, $(48)
DATA shift10<>+0x48(SB)/8, $(54)
DATA shift10<>+0x50(SB)/8, $(0)
DATA shift10<>+0x58(SB)/8, $(0)
GLOBL shift10<>(SB), (RODATA+NOPTR), $96

DATA shift12<>+0x00(SB)/8, $(0)
DATA shift12<>+0x08(SB)/8, $(5)
DATA shift12<>+0x10(SB)/8, $(10)
DATA shift12<>+0x18(SB)/8, $(15)
DATA shift12<>+0x20(SB)/8, $(20)
DATA shift12<>+0x28(SB)/8, $(25)
DATA shift12<>+0x30(SB)/8, $(30)
DATA shift12<>+0x38(SB)/8, $(35)
DATA shift12<>+0x40(SB)/8, $(40)
DATA shift12<>+0x48(SB)/8, $(45)
DATA shift12<>+0x50(SB)/8, $(50)
DATA shift12<>+0x58(SB)/8, $(55)
GLOBL shift12<>(SB), (RODATA+NOPTR), $96

DATA shift15<>+0x00(SB)/8, $(0)
DATA shift15<>+0x08(SB)/8, $(4)
DATA shift15<>+0x10(SB)/8, $(8)
DATA shift15<>+0x18(SB)/8, $(12)
DATA shift15<>+0x20(SB)/8, $(16)
DATA shift15<>+0x28(SB)/8, $(20)
DATA shift15<>+0x30(SB)/8, $(24)
DATA shift15<>+0x38(SB)/8, $(28)
DATA shift15<>+0x40(SB)/8, $(32)
DATA shift15<>+0x48(SB)/8, $(36)
DATA shift15<>+0x50(SB)/8, $(40)
DATA shift15<>+0x58(SB)/8, $(44)
DATA shift15<>+0x60(SB)/8, $(48)
DATA shift15<>+0x68(SB)/8, $(52)
DATA shift15<>+0x70(SB)/8, $(56)
DATA shift15<>+0x78(SB)/8, $(0)
GLOBL shift15<>(SB), (RODATA+NOPTR), $128

DATA shift20<>+0x00(SB)/8, $(0)
DATA shift20<>+0x08(SB)/8, $(3)
DATA shift20<>+0x10(SB)/8, $(6)
DATA shift20<>+0x18(SB)/8, $(9)
DATA shift20<>+0x20(SB)/8, $(12)
DATA shift20<>+0x28(SB)/8, $(15)
DATA shift20<>+0x30(SB)/8, $(18)
DATA shift20<>+0x38(SB)/8, $(21)
DATA shift20<>+0x40(SB)/8, $(24)
DATA shift20<>+0x48(SB)/8, $(27)
DATA shift20<>+0x50(SB)/8, $(30)
DATA shift20<>+0x58(SB)/8, $(33)
DATA shift20<>+0x60(SB)/8, $(36)
DATA shift20<>+0x68(SB)/8, $(39)
DATA shift20<>+0x70(SB)/8, $(42)
DATA shift20<>+0x78(SB)/8, $(45)
DATA shift20<>+0x80(SB)/8, $(48)
DATA shift20<>+0x88(SB)/8, $(51)
DATA shift20<>+0x90(SB)/8, $(54)
DATA shift20<>+0x98(SB)/8, $(57)
GLOBL shift20<>(SB), (RODATA+NOPTR), $160

DATA shift30<>+0x00(SB)/8, $(0)
DATA shift30<>+0x08(SB)/8, $(2)
DATA shift30<>+0x10(SB)/8, $(4)
DATA shift30<>+0x18(SB)/8, $(6)
DATA shift30<>+0x20(SB)/8, $(8)
DATA shift30<>+0x28(SB)/8, $(10)
DATA shift30<>+0x30(SB)/8, $(12)
DATA shift30<>+0x38(SB)/8, $(14)
DATA shift30<>+0x40(SB)/8, $(16)
DATA shift30<>+0x48(SB)/8, $(18)
DATA shift30<>+0x50(SB)/8, $(20)
DATA shift30<>+0x58(SB)/8, $(22)
DATA shift30<>+0x60(SB)/8, $(24)
DATA shift30<>+0x68(SB)/8, $(26)
DATA shift30<>+0x70(SB)/8, $(28)
DATA shift30<>+0x78(SB)/8, $(30)
DATA shift30<>+0x80(SB)/8, $(32)
DATA shift30<>+0x88(SB)/8, $(34)
DATA shift30<>+0x90(SB)/8, $(36)
DATA shift30<>+0x98(SB)/8, $(38)
DATA shift30<>+0xa0(SB)/8, $(40)
DATA shift30<>+0xa8(SB)/8, $(42)
DATA shift30<>+0xb0(SB)/8, $(44)
DATA shift30<>+0xb8(SB)/8, $(46)
DATA shift30<>+0xc0(SB)/8, $(48)
DATA shift30<>+0xc8(SB)/8, $(50)
DATA shift30<>+0xd0(SB)/8, $(52)
DATA shift30<>+0xd8(SB)/8, $(54)
DATA shift30<>+0xe0(SB)/8, $(56)
DATA shift30<>+0xe8(SB)/8, $(58)
DATA shift30<>+0xf0(SB)/8, $(0)
DATA shift30<>+0xf8(SB)/8, $(0)
GLOBL shift30<>(SB), (RODATA+NOPTR), $256

DATA shift60<>+0x000(SB)/8, $(0)
DATA shift60<>+0x008(SB)/8, $(1)
DATA shift60<>+0x010(SB)/8, $(2)
DATA shift60<>+0x018(SB)/8, $(3)
DATA shift60<>+0x020(SB)/8, $(4)
DATA shift60<>+0x028(SB)/8, $(5)
DATA shift60<>+0x030(SB)/8, $(6)
DATA shift60<>+0x038(SB)/8, $(7)
DATA shift60<>+0x040(SB)/8, $(8)
DATA shift60<>+0x048(SB)/8, $(9)
DATA shift60<>+0x050(SB)/8, $(10)
DATA shift60<>+0x058(SB)/8, $(11)
DATA shift60<>+0x060(SB)/8, $(12)
DATA shift60<>+0x068(SB)/8, $(13)
DATA shift60<>+0x070(SB)/8, $(14)
DATA shift60<>+0x078(SB)/8, $(15)
DATA shift60<>+0x080(SB)/8, $(16)
DATA shift60<>+0x088(SB)/8, $(17)
DATA shift60<>+0x090(SB)/8, $(18)
DATA shift60<>+0x098(SB)/8, $(19)
DATA shift60<>+0x0a0(SB)/8, $(20)
DATA shift60<>+0x0a8(SB)/8, $(21)
DATA shift60<>+0x0b0(SB)/8, $(22)
DATA shift60<>+0x0b8(SB)/8, $(23)
DATA shift60<>+0x0c0(SB)/8, $(24)
DATA shift60<>+0x0c8(SB)/8, $(25)
DATA shift60<>+0x0d0(SB)/8, $(26)
DATA shift60<>+0x0d8(SB)/8, $(27)
DATA shift60<>+0x0e0(SB)/8, $(28)
DATA shift60<>+0x0e8(SB)/8, $(29)
DATA shift60<>+0x0f0(SB)/8, $(30)
DATA shift60<>+0x0f8(SB)/8, $(31)
DATA shift60<>+0x100(SB)/8, $(32)
DATA shift60<>+0x108(SB)/8, $(33)
DATA shift60<>+0x110(SB)/8, $(34)
DATA shift60<>+0x118(SB)/8, $(35)
DATA shift60<>+0x120(SB)/8, $(36)
DATA shift60<>+0x128(SB)/8, $(37)
DATA shift60<>+0x130(SB)/8, $(38)
DATA shift60<>+0x138(SB)/8, $(39)
DATA shift60<>+0x140(SB)/8, $(40)
DATA shift60<>+0x148(SB)/8, $(41)
DATA shift60<>+0x150(SB)/8, $(42)
DATA shift60<>+0x158(SB)/8, $(43)
DATA shift60<>+0x160(SB)/8, $(44)
DATA shift60<>+0x168(SB)/8, $(45)
DATA shift60<>+0x170(SB)/8, $(46)
DATA shift60<>+0x178(SB)/8, $(47)
DATA shift60<>+0x180(SB)/8, $(48)
DATA shift60<>+0x188(SB)/8, $(49)
DATA shift60<>+0x190(SB)/8, $(50)
DATA shift60<>+0x198(SB)/8, $(51)
DATA shift60<>+0x1a0(SB)/8, $(52)
DATA shift60<>+0x1a8(SB)/8, $(53)
DATA shift60<>+0x1b0(SB)/8, $(54)
DATA shift60<>+0x1b8(SB)/8, $(55)
DATA shift60<>+0x1c0(SB)/8, $(56)
DATA shift60<>+0x1c8(SB)/8, $(57)
DATA shift60<>+0x1d0(SB)/8, $(58)
DATA shift60<>+0x1d8(SB)/8, $(59)
GLOBL shift60<>(SB), (RODATA+NOPTR), $480

DATA write3mask<>+0x00(SB)/8, $(0xffffffffffffffff)
DATA write3mask<>+0x08(SB)/8, $(0xffffffffffffffff)
DATA write3mask<>+0x10(SB)/8, $(0xffffffffffffffff)
DATA write3mask<>+0x18(SB)/8, $(0)
GLOBL write3mask<>(SB), (RODATA+NOPTR), $32

GLOBL funcTableJmp<>(SB), (NOPTR), $128

GLOBL funcTableCall<>(SB), (NOPTR), $128

GLOBL funcTableOpt<>(SB), (NOPTR), $128

// VPSHUFB input required to spread bytes in each YMM register
DATA shuffle16<>+0x00(SB)/1, $(0xe)
DATA shuffle16<>+0x01(SB)/1, $(0xf)
GLOBL shuffle16<>(SB), (RODATA+NOPTR), $2

// VPSHUFB input required to spread bytes in each YMM register
DATA shuffle16_1<>+0x00(SB)/1, $(0x6)
DATA shuffle16_1<>+0x01(SB)/1, $(0x7)
GLOBL shuffle16_1<>(SB), (RODATA+NOPTR), $2

// VPSHUFB input required to spread bytes in each YMM register
DATA shuffle8<>+0x00(SB)/1, $(15)
GLOBL shuffle8<>(SB), (RODATA+NOPTR), $1

// VPSHUFB input required to spread bytes in each YMM register
DATA shuffle83<>+0x00(SB)/1, $(7)
GLOBL shuffle83<>(SB), (RODATA+NOPTR), $1

// VPSHUFB input required to spread bytes in each YMM register
DATA shuffle81<>+0x00(SB)/1, $(0xff)
DATA shuffle81<>+0x01(SB)/1, $(0xff)
DATA shuffle81<>+0x02(SB)/1, $(0xff)
DATA shuffle81<>+0x03(SB)/1, $(0xff)
DATA shuffle81<>+0x04(SB)/1, $(3)
DATA shuffle81<>+0x05(SB)/1, $(3)
DATA shuffle81<>+0x06(SB)/1, $(3)
DATA shuffle81<>+0x07(SB)/1, $(3)
DATA shuffle81<>+0x08(SB)/1, $(0xff)
DATA shuffle81<>+0x09(SB)/1, $(0xff)
DATA shuffle81<>+0x0a(SB)/1, $(0xff)
DATA shuffle81<>+0x0b(SB)/1, $(0xff)
DATA shuffle81<>+0x0c(SB)/1, $(11)
DATA shuffle81<>+0x0d(SB)/1, $(11)
DATA shuffle81<>+0x0e(SB)/1, $(11)
DATA shuffle81<>+0x0f(SB)/1, $(11)
DATA shuffle81<>+0x10(SB)/1, $(0xff)
DATA shuffle81<>+0x11(SB)/1, $(0xff)
DATA shuffle81<>+0x12(SB)/1, $(0xff)
DATA shuffle81<>+0x13(SB)/1, $(0xff)
DATA shuffle81<>+0x14(SB)/1, $(3)
DATA shuffle81<>+0x15(SB)/1, $(3)
DATA shuffle81<>+0x16(SB)/1, $(3)
DATA shuffle81<>+0x17(SB)/1, $(3)
DATA shuffle81<>+0x18(SB)/1, $(0xff)
DATA shuffle81<>+0x19(SB)/1, $(0xff)
DATA shuffle81<>+0x1a(SB)/1, $(0xff)
DATA shuffle81<>+0x1b(SB)/1, $(0xff)
DATA shuffle81<>+0x1c(SB)/1, $(11)
DATA shuffle81<>+0x1d(SB)/1, $(11)
DATA shuffle81<>+0x1e(SB)/1, $(11)
DATA shuffle81<>+0x1f(SB)/1, $(11)
GLOBL shuffle81<>(SB), (RODATA+NOPTR), $32

// VPSHUFB input required to spread bytes in each YMM register
DATA shuffle82<>+0x00(SB)/1, $(0xff)
DATA shuffle82<>+0x01(SB)/1, $(0xff)
DATA shuffle82<>+0x02(SB)/1, $(0xff)
DATA shuffle82<>+0x03(SB)/1, $(0xff)
DATA shuffle82<>+0x04(SB)/1, $(0xff)
DATA shuffle82<>+0x05(SB)/1, $(0xff)
DATA shuffle82<>+0x06(SB)/1, $(0xff)
DATA shuffle82<>+0x07(SB)/1, $(0xff)
DATA shuffle82<>+0x08(SB)/1, $(7)
DATA shuffle82<>+0x09(SB)/1, $(7)
DATA shuffle82<>+0x0a(SB)/1, $(7)
DATA shuffle82<>+0x0b(SB)/1, $(7)
DATA shuffle82<>+0x0c(SB)/1, $(7)
DATA shuffle82<>+0x0d(SB)/1, $(7)
DATA shuffle82<>+0x0e(SB)/1, $(7)
DATA shuffle82<>+0x0f(SB)/1, $(7)
DATA shuffle82<>+0x10(SB)/1, $(0xff)
DATA shuffle82<>+0x11(SB)/1, $(0xff)
DATA shuffle82<>+0x12(SB)/1, $(0xff)
DATA shuffle82<>+0x13(SB)/1, $(0xff)
DATA shuffle82<>+0x14(SB)/1, $(0xff)
DATA shuffle82<>+0x15(SB)/1, $(0xff)
DATA shuffle82<>+0x16(SB)/1, $(0xff)
DATA shuffle82<>+0x17(SB)/1, $(0xff)
DATA shuffle82<>+0x18(SB)/1, $(7)
DATA shuffle82<>+0x19(SB)/1, $(7)
DATA shuffle82<>+0x1a(SB)/1, $(7)
DATA shuffle82<>+0x1b(SB)/1, $(7)
DATA shuffle82<>+0x1c(SB)/1, $(7)
DATA shuffle82<>+0x1d(SB)/1, $(7)
DATA shuffle82<>+0x1e(SB)/1, $(7)
DATA shuffle82<>+0x1f(SB)/1, $(7)
GLOBL shuffle82<>(SB), (RODATA+NOPTR), $32

// VPSHUFB input required to spread bytes in each YMM register
DATA shuffle8e<>+0x00(SB)/1, $(3)
DATA shuffle8e<>+0x01(SB)/1, $(3)
DATA shuffle8e<>+0x02(SB)/1, $(3)
DATA shuffle8e<>+0x03(SB)/1, $(3)
DATA shuffle8e<>+0x04(SB)/1, $(7)
DATA shuffle8e<>+0x05(SB)/1, $(7)
DATA shuffle8e<>+0x06(SB)/1, $(7)
DATA shuffle8e<>+0x07(SB)/1, $(7)
DATA shuffle8e<>+0x08(SB)/1, $(11)
DATA shuffle8e<>+0x09(SB)/1, $(11)
DATA shuffle8e<>+0x0a(SB)/1, $(11)
DATA shuffle8e<>+0x0b(SB)/1, $(11)
DATA shuffle8e<>+0x0c(SB)/1, $(15)
DATA shuffle8e<>+0x0d(SB)/1, $(15)
DATA shuffle8e<>+0x0e(SB)/1, $(15)
DATA shuffle8e<>+0x0f(SB)/1, $(15)
GLOBL shuffle8e<>(SB), (RODATA+NOPTR), $16
