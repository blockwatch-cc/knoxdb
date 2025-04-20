// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

#include "textflag.h"
#include "constants_uint8_AVX2.h"

// allow buffer overflows due to writung full vector even its not full
// caller hast to care about
#define ALLOW_BO

TEXT ·initUint8AVX2(SB), NOSPLIT, $0-0
        LEAQ            ·unpackZerosUint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>(SB)
        LEAQ            ·unpackOnesUint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>+8(SB)
        LEAQ            ·unpack60Uint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>+16(SB)
        LEAQ            ·unpack30Uint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>+24(SB)
        LEAQ            ·unpack20Uint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>+32(SB)
        LEAQ            ·unpack15Uint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>+40(SB)
        LEAQ            ·unpack12Uint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>+48(SB)
        LEAQ            ·unpack10Uint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>+56(SB)
        LEAQ            ·unpack8Uint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>+64(SB)
        LEAQ            ·unpack7Uint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>+72(SB)
        LEAQ            ·unpack6Uint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>+80(SB)
        LEAQ            ·unpack5Uint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>+88(SB)
        LEAQ            ·unpack4Uint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>+96(SB)
        LEAQ            ·unpack3Uint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>+104(SB)
        LEAQ            ·unpack2Uint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>+112(SB)
        LEAQ            ·unpack1Uint8AVX2(SB), DX
        MOVQ            DX, funcTableUint8AVX2<>+120(SB)

        RET

// func decodeUint8AVX2Core(dst []uint16, src []byte) (value int)
TEXT ·decodeUint8AVX2Core(SB), NOSPLIT, $0-56
        MOVQ            dst_base(FP), DI
        MOVQ            src_base+24(FP), SI
        MOVQ            src_len+32(FP), BX
        SHRQ            $3, BX
        MOVQ            DI, R15                     // save DI

    	CMPQ	        BX, $0
	JNE		start
        JMP             ·decodeUint8AVX2Exit(SB)
start:
        LEAQ            funcTableUint8AVX2<>(SB), R14    // base of function pointer table
        VMOVDQU         blendBytes<>(SB), Y13

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX
        JMP             AX

TEXT ·decodeUint8AVX2Exit(SB), NOSPLIT, $0-0
        VZEROUPPER
        SUBQ            R15, DI
        MOVQ            DI, value+48(FP)
        RET

// func unpack1AVX2()
TEXT ·unpack1Uint8AVX2(SB), NOSPLIT, $0-0
        MOVQ            mask1, R8
        ANDQ            (SI), R8            
        MOVB            R8, (DI)

        ADDQ            $1, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)

// func unpack2AVX2()
TEXT ·unpack2Uint8AVX2(SB), NOSPLIT, $0-0
        VMOVQ           (SI), X0
        VPSRLQ          $22, X0, X1
        VPBLENDVB       X13, X1, X0, X0

        VPEXTRW         $0, X0, (DI)

        ADDQ            $2, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)

// func unpack3AVX2()
TEXT ·unpack3Uint8AVX2(SB), NOSPLIT, $0-0
        VMOVQ           (SI), X0

        VPSRLQ          $12, X0, X1
        VPSRLQ          $24, X0, X2

        VPBLENDVB       X13, X1, X0, X0

        VPEXTRW         $0, X0, (DI)
        VPEXTRB         $2, X2, 2(DI)

        ADDQ            $3, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)

// func unpack4AVX2()
TEXT ·unpack4Uint8AVX2(SB), NOSPLIT, $0-0
        VMOVQ           (SI), X0

        VPSRLQ          $7, X0, X1
        VPSRLQ          $14, X0, X3
        VPSRLQ          $21, X0, X2

        VPBLENDVB       X13, X1, X0, X0
        VPBLENDVB       X13, X2, X3, X2
        VPBLENDW        $(0x02), X2, X0, X0

        VMOVD           X0, (DI)

        ADDQ            $4, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)

// func unpack5AVX2()
TEXT ·unpack5Uint8AVX2(SB), NOSPLIT, $0-0
        VMOVQ           (SI), X0
        VPBROADCASTW    mask5<>(SB), X15

        VPSRLQ          $4, X0, X1
        VPSRLQ          $8, X0, X3
        VPSRLQ          $12, X0, X2

        VPBLENDVB       X13, X1, X0, X1
        VPBLENDVB       X13, X2, X3, X2
        VPBLENDW        $(0x02), X2, X1, X1

        VMOVD           X1, (DI)

        VPEXTRB         $6, X0, 4(DI)

        ADDQ            $5, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)

// func unpack6AVX2()
TEXT ·unpack6Uint8AVX2(SB), NOSPLIT, $0-0
        VMOVQ           (SI), X0

        VPSRLQ          $2, X0, X1
        VPSRLQ          $4, X0, X3
        VPSRLQ          $6, X0, X2

        VPBLENDVB       X13, X1, X0, X1
        VPBLENDVB       X13, X2, X3, X2
        VPBLENDW        $(0x02), X2, X1, X1

        VMOVD           X1, (DI)

        VPSRLQ          $42, X0, X1
        VPSRLQ          $40, X0, X0
        VPBLENDVB       X13, X1, X0, X0

        VPEXTRW         $0, X0, 4(DI)

        ADDQ            $6, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)

// func unpack7AVX2()
TEXT ·unpack7Uint8AVX2(SB), NOSPLIT, $0-0
#ifdef ALLOW_BO
        MOVQ            (SI),AX
        MOVQ            AX, (DI)
#else
        VMOVQ           (SI), X0
        VPEXTRD         $0, X0, 0(DI)
        VPEXTRW         $2, X0, 4(DI)
        VPEXTRB         $6, X0, 6(DI)
#endif

        ADDQ            $7, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)

// func unpack8AVX2()
TEXT ·unpack8Uint8AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), X0

        VPSRLVQ         shift8<>+0x00(SB), X0, X3
        VPSRLVQ         shift8<>+0x10(SB), X0, X2
        VPSRLVQ         shift8<>+0x20(SB), X0, X1
        VPSRLVQ         shift8<>+0x30(SB), X0, X0

        VPSLLQ          $8, X2, X2
        VPSLLQ          $16, X1, X1
        VPSLLQ          $24, X0, X0

        VPBLENDVB       X13, X2, X3, X3
        VPBLENDVB       X13, X0, X1, X1
        VPBLENDW        $(0xaa), X1, X3, X3
        VPSHUFD         $8, X3, X3
        VPAND           mask8<>(SB), X3, X3

        VMOVQ           X3, (DI)

        ADDQ            $8, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)

// func unpack10AVX2()
TEXT ·unpack10Uint8AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
        VMOVDQU         permDWord10<>(SB), Y5

        VPSRLVQ         shift10<>+0x00(SB), Y0, Y3
        VPSRLVQ         shift10<>+0x20(SB), Y0, Y2
        VPSRLVQ         shift10<>+0x40(SB), Y0, Y1
        VPSRLVQ         shift10<>+0x60(SB), Y0, Y0

        VPSLLQ          $8, Y2, Y2
        VPSLLQ          $16, Y1, Y1
        VPSLLQ          $24, Y0, Y0

        VPBLENDVB       Y13, Y2, Y3, Y3
        VPBLENDVB       Y13, Y0, Y1, Y1
        VPBLENDW        $(0xaa), Y1, Y3, Y3
        VPERMD          Y3, Y5, Y3
        VPAND           mask10<>(SB), X3, X3

#ifdef ALLOW_BO
        VMOVDQU         X3, (DI)
#else
        VMOVQ           X3, (DI)
        VPEXTRW         $4, X3, 8(DI)
#endif

        ADDQ            $10, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)

// func unpack12AVX2()
TEXT ·unpack12Uint8AVX2(SB), NOSPLIT, $0-0
#ifndef ALLOW_BO
        VMOVDQU         write12mask<>(SB), Y14
#endif
        VMOVDQU         permDWord<>(SB), Y15
        VPBROADCASTQ    (SI), Y0

        VPSRLVQ         shift12<>+0x00(SB), Y0, Y3
        VPSRLVQ         shift12<>+0x20(SB), Y0, Y2
        VPSRLVQ         shift12<>+0x40(SB), Y0, Y1
        VPSRLVQ         shift12<>+0x60(SB), Y0, Y0

        VPSLLQ          $8, Y2, Y2
        VPSLLQ          $16, Y1, Y1
        VPSLLQ          $24, Y0, Y0

        VPBLENDVB       Y13, Y2, Y3, Y3
        VPBLENDVB       Y13, Y0, Y1, Y1
        VPBLENDW        $(0xaa), Y1, Y3, Y3

        VPERMD          Y3, Y15, Y3

        VPAND           mask12<>(SB), X3, X3

#ifdef ALLOW_BO
        VMOVDQU         X3, (DI)
#else
        VPMASKMOVD      X3, X14, (DI)
#endif

        ADDQ            $12, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)

// func unpack15AVX2()
TEXT ·unpack15Uint8AVX2(SB), NOSPLIT, $0-0
#ifndef ALLOW_BO
        VMOVDQU         write12mask<>(SB), Y14
#endif
        VMOVDQU         permDWord<>(SB), Y15
        VPBROADCASTQ    (SI), Y0

        VPSRLVQ         shift15<>+0x00(SB), Y0, Y3
        VPSRLVQ         shift15<>+0x20(SB), Y0, Y2
        VPSRLVQ         shift15<>+0x40(SB), Y0, Y1
        VPSRLVQ         shift15<>+0x60(SB), Y0, Y0

        VPSLLQ          $8, Y2, Y2
        VPSLLQ          $16, Y1, Y1
        VPSLLQ          $24, Y0, Y0

        VPBLENDVB       Y13, Y2, Y3, Y3
        VPBLENDVB       Y13, Y0, Y1, Y1
        VPBLENDW        $(0xaa), Y1, Y3, Y3

        VPERMD          Y3, Y15, Y3

        VPAND           mask15<>(SB), X3, X3

#ifdef ALLOW_BO
        VMOVDQU         X3, (DI)
#else
        VPMASKMOVD      X3, X14, (DI)
        VPEXTRW         $6, X3, 12(DI)
        VPEXTRB         $14, X3, 14(DI)
#endif

        ADDQ            $15, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)

// func unpack20AVX2()
TEXT ·unpack20Uint8AVX2(SB), NOSPLIT, $0-0
        VMOVDQU         permDWord<>(SB), Y14
        VPBROADCASTB    mask20<>(SB), X15
        VPBROADCASTQ    (SI), Y0

        VPSRLVQ         shift20<>+0x00(SB), Y0, Y3
        VPSRLVQ         shift20<>+0x20(SB), Y0, Y2
        VPSRLVQ         shift20<>+0x40(SB), Y0, Y1
        VPSRLVQ         shift20<>+0x60(SB), Y0, Y0

        VPSLLQ          $8, Y2, Y2
        VPSLLQ          $16, Y1, Y1
        VPSLLQ          $24, Y0, Y0

        VPBLENDVB       Y13, Y2, Y3, Y3
        VPBLENDVB       Y13, Y0, Y1, Y1
        VPBLENDW        $(0xaa), Y1, Y3, Y3
        VPERMD          Y3, Y14, Y3
        VPAND           X15, X3, X3

        VMOVDQU         X3, (DI)

        VPBROADCASTD    4(SI), X0

        VPSRLVD         shift20<>+0x80(SB), X0, X3

        VPSHUFB         shuf4Bytes<>(SB), X3, X3
        VPAND           X15, X3, X3

        VMOVD           X3, 16(DI)

        ADDQ            $20, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)

// func unpack30AVX2()
TEXT ·unpack30Uint8AVX2(SB), NOSPLIT, $0-0
#ifndef ALLOW_BO
        VMOVDQU         write12mask<>(SB), X14
#endif
        VPBROADCASTD    (SI), X0
        VPBROADCASTB    mask30<>(SB), X15

        VPSRLVD         shift30<>+0x00(SB), X0, X3
        VPSRLVD         shift30<>+0x10(SB), X0, X2
        VPSRLVD         shift30<>+0x20(SB), X0, X1
        VPSRLVD         shift30<>+0x30(SB), X0, X0

        VPSLLD          $8, X2, X2
        VPSLLD          $16, X1, X1
        VPSLLD          $24, X0, X0
        VPBLENDVB       X13, X2, X3, X2
        VPBLENDVB       X13, X0, X1, X0
        VPBLENDW        $(0xaa), X0, X2, X0
        VPAND           X0, X15, X0

        VMOVDQU         X0, 0(DI)

        VPBROADCASTD    4(SI), X0

        VPSRLVD         shift30<>+0x00(SB), X0, X3
        VPSRLVD         shift30<>+0x10(SB), X0, X2
        VPSRLVD         shift30<>+0x20(SB), X0, X1
        VPSRLVD         shift30<>+0x30(SB), X0, X0

        VPSLLD          $8, X2, X2
        VPSLLD          $16, X1, X1
        VPSLLD          $24, X0, X0
        VPBLENDVB       X13, X2, X3, X2
        VPBLENDVB       X13, X0, X1, X0
        VPBLENDW        $(0xaa), X0, X2, X0
        VPAND           X0, X15, X0

#ifdef ALLOW_BO
        VMOVDQU         X0, 16(DI)
#else
        VPMASKMOVD      X0, X14, 16(DI)
        VPEXTRW         $6, X0, 28(DI)
#endif

        ADDQ            $30, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)

// func unpack60AVX2()
TEXT ·unpack60Uint8AVX2(SB), NOSPLIT, $0-0
        VMOVDQU         write28mask<>(SB), Y14

        VPBROADCASTD    (SI), Y0
        VPBROADCASTB    mask60<>(SB), Y15

        VPSRLVD         shift60<>+0x00(SB), Y0, Y3
        VPSRLVD         shift60<>+0x20(SB), Y0, Y2
        VPSRLVD         shift60<>+0x40(SB), Y0, Y1
        VPSRLVD         shift60<>+0x60(SB), Y0, Y0

        VPSLLD          $8, Y2, Y2
        VPSLLD          $16, Y1, Y1
        VPSLLD          $24, Y0, Y0
        VPBLENDVB       Y13, Y2, Y3, Y2
        VPBLENDVB       Y13, Y0, Y1, Y0
        VPBLENDW        $(0xaa), Y0, Y2, Y0
        VPAND           Y0, Y15, Y0

        VMOVDQU         Y0, 0(DI)

        VPBROADCASTD    4(SI), Y0

        VPSRLVD         shift60<>+0x00(SB), Y0, Y3
        VPSRLVD         shift60<>+0x20(SB), Y0, Y2
        VPSRLVD         shift60<>+0x40(SB), Y0, Y1
        VPSRLVD         shift60<>+0x60(SB), Y0, Y0

        VPSLLD          $8, Y2, Y2
        VPSLLD          $16, Y1, Y1
        VPSLLD          $24, Y0, Y0
        VPBLENDVB       Y13, Y2, Y3, Y2
        VPBLENDVB       Y13, Y0, Y1, Y0
        VPBLENDW        $(0xaa), Y0, Y2, Y0
        VPAND           Y0, Y15, Y0

        VPMASKMOVD      Y0, Y14, 32(DI)

        ADDQ            $60, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)

// func unpackOnesAVX2()
TEXT ·unpackOnesUint8AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTB    const1<>(SB), Y0

        VMOVDQU         Y0, (DI)
        VMOVDQU         Y0, 32(DI)
        VMOVDQU         Y0, 64(DI)
        VMOVDQU         Y0, 96(DI)

        ADDQ            $128, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)

// func unpackZerosAVX2()
TEXT ·unpackZerosUint8AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTB    const0<>(SB), Y0

        VMOVDQU         Y0, (DI)
        VMOVDQU         Y0, 32(DI)
        VMOVDQU         Y0, 64(DI)
        VMOVDQU         Y0, 96(DI)

        ADDQ            $128, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint8AVX2Exit(SB)
