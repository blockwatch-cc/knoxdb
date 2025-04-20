// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

#include "textflag.h"
#include "constants_uint16_AVX2.h"

// allow buffer overflows due to writung full vector even its not full
// caller has to care about
#define ALLOW_BO

TEXT ·initUint16AVX2(SB), NOSPLIT, $0-0
        LEAQ            ·unpackZerosUint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>(SB)
        LEAQ            ·unpackOnesUint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>+8(SB)
        LEAQ            ·unpack60Uint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>+16(SB)
        LEAQ            ·unpack30Uint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>+24(SB)
        LEAQ            ·unpack20Uint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>+32(SB)
        LEAQ            ·unpack15Uint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>+40(SB)
        LEAQ            ·unpack12Uint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>+48(SB)
        LEAQ            ·unpack10Uint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>+56(SB)
        LEAQ            ·unpack8Uint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>+64(SB)
        LEAQ            ·unpack7Uint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>+72(SB)
        LEAQ            ·unpack6Uint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>+80(SB)
        LEAQ            ·unpack5Uint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>+88(SB)
        LEAQ            ·unpack4Uint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>+96(SB)
        LEAQ            ·unpack3Uint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>+104(SB)
        LEAQ            ·unpack2Uint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>+112(SB)
        LEAQ            ·unpack1Uint16AVX2(SB), DX
        MOVQ            DX, funcTableUint16AVX2<>+120(SB)

        RET

// func decodeUint16AVX2Core(dst []uint16, src []byte) (value int)
TEXT ·decodeUint16AVX2Core(SB), NOSPLIT, $0-56
        MOVQ            dst_base(FP), DI
        MOVQ            src_base+24(FP), SI
        MOVQ            src_len+32(FP), BX
        SHRQ            $3, BX
        MOVQ            DI, R15                     // save DI

    	CMPQ	        BX, $0
	JNE		start
        JMP             ·decodeUint16AVX2Exit(SB)
start:
        LEAQ            funcTableUint16AVX2<>(SB), R14    // base of function pointer table

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX
        JMP             AX

TEXT ·decodeUint16AVX2Exit(SB), NOSPLIT, $0-0
        VZEROUPPER
        SUBQ            R15, DI
        SHRQ            $1, DI
        MOVQ            DI, value+48(FP)
        RET

// func unpack1AVX2()
TEXT ·unpack1Uint16AVX2(SB), NOSPLIT, $0-0
        MOVQ            mask1, R8
        ANDQ            (SI), R8            
        MOVW            R8, (DI)

        ADDQ            $2, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint16AVX2Exit(SB)

// func unpack2AVX2()
TEXT ·unpack2Uint16AVX2(SB), NOSPLIT, $0-0
        VMOVQ           (SI), X0
        VPSRLQ          $14, X0, X1
        VPBLENDW        $(0x11), X0, X1, X0

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
        JMP ·decodeUint16AVX2Exit(SB)

// func unpack3AVX2()
TEXT ·unpack3Uint16AVX2(SB), NOSPLIT, $0-0
        VMOVQ           (SI), X0

        VPSRLQ          $4, X0, X1
        VPSRLQ          $8, X0, X2

        VPBLENDW        $(0xaa), X1, X0, X0
        VPBLENDW        $(0xcc), X2, X0, X0

#ifdef ALLOW_BO
        VMOVQ           X0, (DI)
#else
        VMOVD           X0, (DI)
        VPEXTRW         $2, X0, 4(DI)
#endif

        ADDQ            $6, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint16AVX2Exit(SB)

// func unpack4AVX2()
TEXT ·unpack4Uint16AVX2(SB), NOSPLIT, $0-0
        VMOVQ           (SI), X0

        VPSLLQ          $1, X0, X1
        VPSLLQ          $2, X0, X3
        VPSLLQ          $3, X0, X2

        VPBLENDW        $(0xaa), X1, X0, X0
        VPBLENDW        $(0xaa), X2, X3, X2
        VPBLENDW        $(0xcc), X2, X0, X0
        VPAND           mask4<>(SB), X0, X0

        VMOVQ           X0, (DI)

        ADDQ            $8, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint16AVX2Exit(SB)

// func unpack5AVX2()
TEXT ·unpack5Uint16AVX2(SB), NOSPLIT, $0-0
        VMOVQ           (SI), X0
        VPBROADCASTW    mask5<>(SB), X15

        VPSLLQ          $4, X0, X1
        VPSLLQ          $8, X0, X3
        VPSLLQ          $12, X0, X2

        VPBLENDW        $(0xaa), X1, X0, X1
        VPBLENDW        $(0xaa), X2, X3, X2
        VPBLENDW        $(0xcc), X2, X1, X1
        VPAND           X15, X1, X1

        VMOVQ           X1, (DI)

        VPAND           X15, X0, X0

        VPEXTRW         $3, X0, 8(DI)

        ADDQ            $10, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint16AVX2Exit(SB)

// func unpack6AVX2()
TEXT ·unpack6Uint16AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), X0

        VPSRLVQ         shift6<>+0x00(SB), X0, X3
        VPSRLVQ         shift6<>+0x10(SB), X0, X2
        VPSRLVQ         shift6<>+0x20(SB), X0, X1
        VPSRLVQ         shift6<>+0x30(SB), X0, X0

        VPSLLQ          $16, X2, X2
        VPSLLQ          $32, X1, X1
        VPSLLQ          $48, X0, X0

        VPBLENDW        $(0xaa), X2, X3, X3
        VPBLENDW        $(0xaa), X0, X1, X1
        VPBLENDW        $(0xcc), X1, X3, X3
        VPAND           mask6<>(SB), X3, X3

#ifdef ALLOW_BO
        VMOVDQU         X3, (DI)
#else
        VMOVQ           X3, (DI)
        VPEXTRD         $2, X3, 8(DI)
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
        JMP ·decodeUint16AVX2Exit(SB)

// func unpack7AVX2()
TEXT ·unpack7Uint16AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), X0

        VPSRLVQ         shift7<>+0x00(SB), X0, X3
        VPSRLVQ         shift7<>+0x10(SB), X0, X2
        VPSRLVQ         shift7<>+0x20(SB), X0, X1
        VPSRLVQ         shift7<>+0x30(SB), X0, X0

        VPSLLQ          $16, X2, X2
        VPSLLQ          $32, X1, X1
        VPSLLQ          $48, X0, X0

        VPBLENDW        $(0xaa), X2, X3, X3
        VPBLENDW        $(0xaa), X0, X1, X1
        VPBLENDW        $(0xcc), X1, X3, X3
        VPAND           mask7<>(SB), X3, X3

#ifdef ALLOW_BO
        VMOVDQU         X3, (DI)
#else
        VMOVQ           X3, (DI)
        VPEXTRD         $2, X3, 8(DI)
        VPEXTRW         $6, X3, 12(DI)
#endif

        ADDQ            $14, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint16AVX2Exit(SB)

// func unpack8AVX2()
TEXT ·unpack8Uint16AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), X0

        VPSRLVQ         shift8<>+0x00(SB), X0, X3
        VPSRLVQ         shift8<>+0x10(SB), X0, X2
        VPSRLVQ         shift8<>+0x20(SB), X0, X1
        VPSRLVQ         shift8<>+0x30(SB), X0, X0

        VPSLLQ          $16, X2, X2
        VPSLLQ          $32, X1, X1
        VPSLLQ          $48, X0, X0

        VPBLENDW        $(0xaa), X2, X3, X3
        VPBLENDW        $(0xaa), X0, X1, X1
        VPBLENDW        $(0xcc), X1, X3, X3
        VPAND           mask8<>(SB), X3, X3

        VMOVDQU         X3, (DI)

        ADDQ            $16, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint16AVX2Exit(SB)

// func unpack10AVX2()
TEXT ·unpack10Uint16AVX2(SB), NOSPLIT, $0-0
#ifndef ALLOW_BO
        VMOVDQU         write10mask<>(SB), Y14
#endif
        VPBROADCASTQ    (SI), Y0

        VPSRLVQ         shift10<>+0x00(SB), Y0, Y3
        VPSRLVQ         shift10<>+0x20(SB), Y0, Y2
        VPSRLVQ         shift10<>+0x40(SB), Y0, Y1
        VPSRLVQ         shift10<>+0x60(SB), Y0, Y0

        VPSLLQ          $16, Y2, Y2
        VPSLLQ          $32, Y1, Y1
        VPSLLQ          $48, Y0, Y0

        VPBLENDW        $(0xaa), Y2, Y3, Y3
        VPBLENDW        $(0xaa), Y0, Y1, Y1
        VPBLENDW        $(0xcc), Y1, Y3, Y3
        VPAND           mask10<>(SB), Y3, Y3

#ifdef ALLOW_BO
        VMOVDQU         Y3, (DI)
#else
        VPMASKMOVD      Y3, Y14, (DI)
#endif

        ADDQ            $20, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint16AVX2Exit(SB)

// func unpack12AVX2()
TEXT ·unpack12Uint16AVX2(SB), NOSPLIT, $0-0
#ifndef ALLOW_BO
        VMOVDQU         write12mask<>(SB), Y14
#endif
        VPBROADCASTQ    (SI), Y0

        VPSRLVQ         shift12<>+0x00(SB), Y0, Y3
        VPSRLVQ         shift12<>+0x20(SB), Y0, Y2
        VPSRLVQ         shift12<>+0x40(SB), Y0, Y1
        VPSRLVQ         shift12<>+0x60(SB), Y0, Y0

        VPSLLQ          $16, Y2, Y2
        VPSLLQ          $32, Y1, Y1
        VPSLLQ          $48, Y0, Y0

        VPBLENDW        $(0xaa), Y2, Y3, Y3
        VPBLENDW        $(0xaa), Y0, Y1, Y1
        VPBLENDW        $(0xcc), Y1, Y3, Y3
        VPAND           mask12<>(SB), Y3, Y3

#ifdef ALLOW_BO
        VMOVDQU         Y3, (DI)
#else
        VPMASKMOVD      Y3, Y14, (DI)
#endif

        ADDQ            $24, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint16AVX2Exit(SB)

// func unpack15AVX2()
TEXT ·unpack15Uint16AVX2(SB), NOSPLIT, $0-0
#ifndef ALLOW_BO
        VMOVDQU         write14mask<>(SB), Y14
#endif
        VPBROADCASTQ    (SI), Y0

        VPSRLVQ         shift15<>+0x00(SB), Y0, Y3
        VPSRLVQ         shift15<>+0x20(SB), Y0, Y2
        VPSRLVQ         shift15<>+0x40(SB), Y0, Y1
        VPSRLVQ         shift15<>+0x60(SB), Y0, Y0

        VPSLLQ          $16, Y2, Y2
        VPSLLQ          $32, Y1, Y1
        VPSLLQ          $48, Y0, Y0

        VPBLENDW        $(0xaa), Y2, Y3, Y3
        VPBLENDW        $(0xaa), Y0, Y1, Y1
        VPBLENDW        $(0xcc), Y1, Y3, Y3
        VPAND           mask15<>(SB), Y3, Y3

#ifdef ALLOW_BO
        VMOVDQU         Y3, (DI)
#else
        VPMASKMOVD      Y3, Y14, (DI)
        VPERMQ          $255, Y3, Y3
        VPEXTRW         $2, X3, 28(DI)
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
        JMP ·decodeUint16AVX2Exit(SB)

// func unpack20AVX2()
TEXT ·unpack20Uint16AVX2(SB), NOSPLIT, $0-0
        VMOVQ           (SI), X2
        VPBROADCASTD    X2, X0
        VPBROADCASTW    mask20<>(SB), X15

        VPSRLVD         shift20<>+0x00(SB), X0, X1
        VPSRLVD         shift20<>+0x10(SB), X0, X0
        VPSLLD          $16, X0, X0
        VPBLENDW        $(0xaa), X0, X1, X0

        VPAND           X0, X15, X0
        VMOVDQU         X0, (DI)

        VPSRLQ          $24, X2, X0
        VPSRLQ          $11, X2, X1
        VPSLLQ          $2, X2, X3
        VPSLLQ          $15, X2, X2

        VPBLENDW        $(0xaa), X1, X0, X0
        VPBLENDW        $(0xaa), X2, X3, X2
        VPBLENDW        $(0xcc), X2, X0, X0
        VPAND           X0, X15, X0

        VMOVQ           X0, 16(DI)

        VPBROADCASTD    4(SI), X0

        VPSRLVD         shift20<>+0x20(SB), X0, X1
        VPSRLVD         shift20<>+0x30(SB), X0, X0
        VPSLLD          $16, X0, X0
        VPBLENDW        $(0xaa), X0, X1, X0

        VPAND           X0, X15, X0
        VMOVDQU         X0, 24(DI)

        ADDQ            $40, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint16AVX2Exit(SB)

// func unpack30AVX2()
TEXT ·unpack30Uint16AVX2(SB), NOSPLIT, $0-0
#ifndef ALLOW_BO
        VMOVDQU         write14mask<>(SB), Y14
#endif
        VPBROADCASTD    (SI), Y0
        VPBROADCASTD    4(SI), Y2
        VPBROADCASTW    mask30<>(SB), Y15
        VMOVDQU         shift30<>+0x00(SB), Y4
        VMOVDQU         shift30<>+0x20(SB), Y5

        VPSRLVD         Y4, Y0, Y1
        VPSRLVD         Y5, Y0, Y0
        VPSRLVD         Y4, Y2, Y3
        VPSRLVD         Y5, Y2, Y2

        VPSLLD          $16, Y2, Y2
        VPBLENDW        $(0xaa), Y2, Y3, Y2
        VPSLLD          $16, Y0, Y0
        VPBLENDW        $(0xaa), Y0, Y1, Y0

        VPAND           Y2, Y15, Y2
        VPAND           Y0, Y15, Y0

        VMOVDQU         Y0, (DI)
#ifdef ALLOW_BO
        VMOVDQU         Y2, 32(DI)
#else
        VPMASKMOVD      Y2, Y14, 32(DI)
#endif

        ADDQ            $60, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint16AVX2Exit(SB)

// func unpack60AVX2()
TEXT ·unpack60Uint16AVX2(SB), NOSPLIT, $0-0
#ifndef ALLOW_BO
        VMOVDQU         write12mask<>(SB), Y14
#endif
        VPBROADCASTD    (SI), Y0
        VPBROADCASTW    mask60<>(SB), Y15

        VPSRLVD         shift60<>+0x00(SB), Y0, Y3
        VPSRLVD         shift60<>+0x20(SB), Y0, Y2
        VPSRLVD         shift60<>+0x40(SB), Y0, Y1
        VPSRLVD         shift60<>+0x60(SB), Y0, Y0

        VPSLLD          $16, Y2, Y2
        VPBLENDW        $(0xaa), Y2, Y3, Y2
        VPSLLD          $16, Y0, Y0
        VPBLENDW        $(0xaa), Y0, Y1, Y0

        VPAND           Y2, Y15, Y2
        VPAND           Y0, Y15, Y0

        VMOVDQU         Y2, 0(DI)
        VMOVDQU         Y0, 32(DI)

        VPBROADCASTD    4(SI), Y0

        VPSRLVD         shift60<>+0x00(SB), Y0, Y3
        VPSRLVD         shift60<>+0x20(SB), Y0, Y2
        VPSRLVD         shift60<>+0x40(SB), Y0, Y1
        VPSRLVD         shift60<>+0x60(SB), Y0, Y0

        VPSLLD          $16, Y2, Y2
        VPBLENDW        $(0xaa), Y2, Y3, Y2
        VPSLLD          $16, Y0, Y0
        VPBLENDW        $(0xaa), Y0, Y1, Y0

        VPAND           Y2, Y15, Y2
        VPAND           Y0, Y15, Y0

        VMOVDQU         Y2, 64(DI)
#ifdef ALLOW_BO
        VMOVDQU         Y0, 96(DI)
#else
        VPMASKMOVD      Y0, Y14, 96(DI)
#endif

        ADDQ            $120, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint16AVX2Exit(SB)

// func unpackOnesAVX2()
TEXT ·unpackOnesUint16AVX2(SB), NOSPLIT, $0-0
        VPCMPEQQ        Y0, Y0, Y0
        VPSRLW          $15, Y0, Y0             // Y0 = [1,1,...] 

        VMOVDQU         Y0, (DI)
        VMOVDQU         Y0, 32(DI)
        VMOVDQU         Y0, 64(DI)
        VMOVDQU         Y0, 96(DI)
        VMOVDQU         Y0, 128(DI)
        VMOVDQU         Y0, 160(DI)
        VMOVDQU         Y0, 192(DI)
        VMOVDQU         Y0, 224(DI)
    
        ADDQ            $256, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint16AVX2Exit(SB)

// func unpackZerosAVX2()
TEXT ·unpackZerosUint16AVX2(SB), NOSPLIT, $0-0
        VPXORQ          Y0, Y0, Y0             // Y0 = [0,0,...]

        VMOVDQU         Y0, (DI)
        VMOVDQU         Y0, 32(DI)
        VMOVDQU         Y0, 64(DI)
        VMOVDQU         Y0, 96(DI)
        VMOVDQU         Y0, 128(DI)
        VMOVDQU         Y0, 160(DI)
        VMOVDQU         Y0, 192(DI)
        VMOVDQU         Y0, 224(DI)

        ADDQ            $256, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint16AVX2Exit(SB)
