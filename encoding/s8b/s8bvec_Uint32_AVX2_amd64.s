// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

#include "textflag.h"
#include "constants_Uint32_AVX2.h"

// allow buffer overflows due to writung full vector even its not full
// caller hast to care about
#define ALLOW_BO

TEXT ·initUint32AVX2(SB), NOSPLIT, $0-0
        LEAQ            ·unpack240Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>(SB)
        LEAQ            ·unpack120Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>+8(SB)
        LEAQ            ·unpack60Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>+16(SB)
        LEAQ            ·unpack30Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>+24(SB)
        LEAQ            ·unpack20Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>+32(SB)
        LEAQ            ·unpack15Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>+40(SB)
        LEAQ            ·unpack12Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>+48(SB)
        LEAQ            ·unpack10Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>+56(SB)
        LEAQ            ·unpack8Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>+64(SB)
        LEAQ            ·unpack7Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>+72(SB)
        LEAQ            ·unpack6Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>+80(SB)
        LEAQ            ·unpack5Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>+88(SB)
        LEAQ            ·unpack4Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>+96(SB)
        LEAQ            ·unpack3Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>+104(SB)
        LEAQ            ·unpack2Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>+112(SB)
        LEAQ            ·unpack1Uint32AVX2(SB), DX
        MOVQ            DX, funcTableUint32AVX2<>+120(SB)

        RET

// func decodeAllUint32AVX2(dst []uint32, src []byte) (value int)
TEXT ·decodeAllUint32AVX2Core(SB), NOSPLIT, $0-56
        MOVQ            dst_base(FP), DI
        MOVQ            src_base+24(FP), SI
        MOVQ            src_len+32(FP), BX
        SHRQ            $3, BX
        MOVQ            DI, R15                     // save DI

    	CMPQ	        BX, $0
	JNE		start
        JMP             ·decodeAllUint32AVX2Exit(SB)
start:
        LEAQ            funcTableUint32AVX2<>(SB), R14    // base of function pointer table

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX
        JMP             AX

TEXT ·decodeAllUint32AVX2Exit(SB), NOSPLIT, $0-0
        VZEROUPPER
        SUBQ            R15, DI
        SHRQ            $2, DI
        MOVQ            DI, value+48(FP)
        RET

// func unpack1AVX2()
TEXT ·unpack1Uint32AVX2(SB), NOSPLIT, $0-0
//        VMOVQ           mask1<>(SB), X5
//        VPAND           (SI), X5, X0
//        VMOVQ           X0, (DI)

        MOVQ            mask1, R8
        ANDQ            (SI), R8            
        MOVL            R8, (DI)

        ADDQ            $4, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)

// func unpack2AVX2()
TEXT ·unpack2Uint32AVX2(SB), NOSPLIT, $0-0
        VMOVQ           (SI), X0
        VPSLLQ          $2, X0, X1
        VPBLENDW        $(0x33), X0, X1, X0
        VPAND           mask2<>(SB), X0, X0

        VMOVQ           X0, (DI)

        ADDQ            $8, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)

// func unpack3AVX2()
TEXT ·unpack3Uint32AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), X0

#ifndef ALLOW_BO
        VMOVDQU         write3mask<>(SB), X14
#endif

        VPSRLVQ         shift3<>(SB), X0, X1
        VPSRLVQ         shift3<>+0x10(SB), X0, X0

        VPSLLQ          $32, X0, X0
        VPBLENDW        $(0x33), X1, X0, X0
        VPAND           mask3<>(SB), X0, X0

#ifdef ALLOW_BO
        VMOVDQU         X0, (DI)
#else
        VPMASKMOVD      X0, X14, (DI)
#endif

        ADDQ            $12, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)

// func unpack4AVX2()
TEXT ·unpack4Uint32AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), X0
        VPSRLVQ         shift4<>(SB), X0, X1
        VPSRLVQ         shift4<>+0x10(SB), X0, X0
        VPSLLQ          $32, X0, X0
        VPBLENDW        $(0x33), X1, X0, X0
        VPAND           mask4<>(SB), X0, X0

        VMOVDQU         X0, (DI)

        ADDQ            $16, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)

// func unpack5AVX2()
TEXT ·unpack5Uint32AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0

#ifndef ALLOW_BO
        VMOVDQU         write5mask<>(SB), Y14
#endif

        VPSRLVQ         shift5<>(SB), Y0, Y1
        VPSRLVQ         shift5<>+0x20(SB), Y0, Y0

        VPSLLQ          $32, Y0, Y0
        VPBLENDW        $(0x33), Y1, Y0, Y0
        VPAND           mask5<>(SB), Y0, Y0

#ifdef ALLOW_BO
        VMOVDQU         Y0, (DI)
#else
        VPMASKMOVD      Y0, Y14, (DI)
#endif

        ADDQ            $20, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)

// func unpack6AVX2()
TEXT ·unpack6Uint32AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0

#ifndef ALLOW_BO
        VMOVDQU         write6mask<>(SB), Y14
#endif

        VPSRLVQ         shift6<>(SB), Y0, Y1
        VPSRLVQ         shift6<>+0x20(SB), Y0, Y0
        VPSLLQ          $32, Y0, Y0
        VPBLENDW        $(0x33), Y1, Y0, Y0
        VPAND           mask6<>(SB), Y0, Y0

#ifdef ALLOW_BO
        VMOVDQU         Y0, (DI)
#else
        VPMASKMOVD      Y0, Y14, (DI)
#endif

        ADDQ            $24, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)

// func unpack7AVX2()
TEXT ·unpack7Uint32AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTD    (SI), X0
        VPBROADCASTD    4(SI), X1
        VPERM2I128      $0x20, Y1, Y0, Y0

#ifndef ALLOW_BO
        VMOVDQU         write7mask<>(SB), Y14
#endif

        VPSRLVD         shift7<>(SB), Y0, Y0
        VPAND           mask7<>(SB), Y0, Y0

#ifdef ALLOW_BO
        VMOVDQU         Y0, (DI)
#else
        VPMASKMOVD      Y0, Y14, (DI)
#endif

        ADDQ            $28, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)

// func unpack8AVX2()
TEXT ·unpack8Uint32AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0

        VPSRLVQ         shift8<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift8<>+0x20(SB), Y0, Y0

        VPSLLQ          $32, Y0, Y0
        VPBLENDW        $(0xcc), Y0, Y1, Y0
        VPAND           mask8<>(SB), Y0, Y0

        VMOVDQU         Y0, (DI)

        ADDQ            $32, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)

// func unpack10AVX2()
TEXT ·unpack10Uint32AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTD    4(SI), X2
        VPBROADCASTD    mask10<>(SB), Y15

        VPSRLVQ         shift10<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift10<>+0x20(SB), Y0, Y0
        VPSRLVD         shift10<>+0x40(SB), X2, X2

        VPSLLQ          $32, Y0, Y0
        VPBLENDW        $(0xcc), Y0, Y1, Y0
        VPAND           Y0, Y15, Y0
        VPAND           X2, X15, X2

        VMOVDQU         Y0, (DI)
        VMOVQ           X2, 32(DI)
        ADDQ            $40, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)

// func unpack12AVX2()
TEXT ·unpack12Uint32AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTD    mask12<>(SB), Y15

        VPSRLVQ         shift12<>+0x00(SB), Y0, Y3
        VPSRLVQ         shift12<>+0x20(SB), Y0, Y2
        VPSRLVQ         shift12<>+0x40(SB), X0, X1
        VPSRLVQ         shift12<>+0x50(SB), X0, X0

        VPSLLQ          $32, Y2, Y2
        VPBLENDW        $(0xcc), Y2, Y3, Y2
        VPSLLQ          $32, X0, X0
        VPBLENDW        $(0xcc), X0, X1, X0
        VPAND           Y2, Y15, Y2
        VPAND           X0, X15, X0

        VMOVDQU         Y2, (DI)
        VMOVDQU         X0, 32(DI)

        ADDQ            $48, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)

// func unpack15AVX2()
TEXT ·unpack15Uint32AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTD    (SI), Y0
        VPBROADCASTD    4(SI), Y1
        VPBROADCASTD    mask15<>(SB), Y15
        VMOVDQU         shift15<>+0x00(SB), Y2

#ifndef ALLOW_BO
        VMOVDQU         write7mask<>(SB), Y14
#endif

        VPSRLVD         Y2, Y0, Y0
        VPSRLVD         Y2, Y1, Y1
        VPAND           Y0, Y15, Y0
        VPAND           Y1, Y15, Y1
        VMOVDQU         Y0, (DI)

#ifdef ALLOW_BO
        VMOVDQU         Y1, 32(DI)
#else
        VPMASKMOVD      Y1, Y14, 32(DI)
#endif

        ADDQ            $60, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)

// func unpack20AVX2()
TEXT ·unpack20Uint32AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y2
        VPBROADCASTD    X2, Y1
        VPBROADCASTD    4(SI), X0
        VPBROADCASTD    mask20<>(SB), Y15

        VPSRLVD         shift20<>+0x00(SB), Y1, Y1

        VPSRLVQ         shift20<>+0x20(SB), Y2, Y3
        VPSRLVQ         shift20<>+0x40(SB), Y2, Y2
        VPSLLQ          $32, Y2, Y2
        VPBLENDW        $(0xcc), Y2, Y3, Y2

        VPSRLVD         shift20<>+0x60(SB), X0, X0

        VPAND           Y1, Y15, Y1
        VPAND           Y2, Y15, Y2
        VPAND           X0, X15, X0

        VMOVDQU         Y1, (DI)
        VMOVDQU         Y2, 32(DI)
        VMOVDQU         X0, 64(DI)

        ADDQ            $80, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)

// func unpack30AVX2()
TEXT ·unpack30Uint32AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTD    (SI), Y0
        VPBROADCASTD    4(SI), Y2
        VPBROADCASTD    mask30<>(SB), Y15
#ifndef ALLOW_BO
        VMOVDQU         write6mask<>(SB), Y14
#endif
        VMOVDQU         shift30<>+0x00(SB), Y4
        VMOVDQU         shift30<>+0x20(SB), Y5

        VPSRLVD         Y4, Y0, Y1
        VPSRLVD         Y5, Y0, Y0
        VPSRLVD         Y4, Y2, Y3
        VPSRLVD         Y5, Y2, Y2
        VPAND           Y3, Y15, Y3
        VPAND           Y2, Y15, Y2
        VPAND           Y1, Y15, Y1
        VPAND           Y0, Y15, Y0

        VMOVDQU         Y1, (DI)
        VMOVDQU         Y0, 32(DI)
        VMOVDQU         Y3, 64(DI)
#ifdef ALLOW_BO
        VMOVDQU         Y2, 96(DI)
#else
        VPMASKMOVD      Y2, Y14, 96(DI)
#endif

        ADDQ            $120, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)

// func unpack60AVX2()
TEXT ·unpack60Uint32AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTD    (SI), Y0
        VPBROADCASTD    mask60<>(SB), Y15

        VPSRLVD         shift60<>+0x00(SB), Y0, Y3
        VPSRLVD         shift60<>+0x20(SB), Y0, Y2
        VPSRLVD         shift60<>+0x40(SB), Y0, Y1
        VPSRLVD         shift60<>+0x60(SB), Y0, Y0

        VPAND           Y3, Y15, Y3
        VPAND           Y2, Y15, Y2
        VPAND           Y1, Y15, Y1
        VPAND           Y0, Y15, Y0

        VMOVDQU         Y3, (DI)
        VMOVDQU         Y2, 32(DI)
        VMOVDQU         Y1, 64(DI)
        VMOVDQU         Y0, 96(DI)

        VPBROADCASTD    4(SI), Y0

        VPSRLVD         shift60<>+0x00(SB), Y0, Y3
        VPSRLVD         shift60<>+0x20(SB), Y0, Y2
        VPSRLVD         shift60<>+0x40(SB), Y0, Y1
        VPSRLVD         shift60<>+0x60(SB), Y0, Y0

        VPAND           Y3, Y15, Y3
        VPAND           Y2, Y15, Y2
        VPAND           Y1, Y15, Y1
        VPAND           Y0, Y15, Y0

        VMOVDQU         Y3, 128(DI)
        VMOVDQU         Y2, 160(DI)
        VMOVDQU         Y1, 192(DI)
        VMOVDQU         X0, 224(DI)

        ADDQ            $240, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)

// func unpack120AVX2()
TEXT ·unpack120Uint32AVX2(SB), NOSPLIT, $0-0
        VPCMPEQQ        Y0, Y0, Y0
        VPSRLD          $31, Y0, Y0             // Y0 = [1,1,...] 

        VMOVDQU         Y0, (DI)
        VMOVDQU         Y0, 32(DI)
        VMOVDQU         Y0, 64(DI)
        VMOVDQU         Y0, 96(DI)
        VMOVDQU         Y0, 128(DI)
        VMOVDQU         Y0, 160(DI)
        VMOVDQU         Y0, 192(DI)
        VMOVDQU         Y0, 224(DI)
        VMOVDQU         Y0, 256(DI)
        VMOVDQU         Y0, 288(DI)
        VMOVDQU         Y0, 320(DI)
        VMOVDQU         Y0, 352(DI)
        VMOVDQU         Y0, 384(DI)
        VMOVDQU         Y0, 416(DI)
        VMOVDQU         Y0, 448(DI)

        ADDQ            $480, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)

// func unpack240AVX2()
TEXT ·unpack240Uint32AVX2(SB), NOSPLIT, $0-0
        VPCMPEQQ        Y0, Y0, Y0
        VPSRLD          $31, Y0, Y0             // Y0 = [1,1,...] 

        VMOVDQU         Y0, (DI)
        VMOVDQU         Y0, 32(DI)
        VMOVDQU         Y0, 64(DI)
        VMOVDQU         Y0, 96(DI)
        VMOVDQU         Y0, 128(DI)
        VMOVDQU         Y0, 160(DI)
        VMOVDQU         Y0, 192(DI)
        VMOVDQU         Y0, 224(DI)
        VMOVDQU         Y0, 256(DI)
        VMOVDQU         Y0, 288(DI)
        VMOVDQU         Y0, 320(DI)
        VMOVDQU         Y0, 352(DI)
        VMOVDQU         Y0, 384(DI)
        VMOVDQU         Y0, 416(DI)
        VMOVDQU         Y0, 448(DI)
        VMOVDQU         Y0, 480(DI)
        VMOVDQU         Y0, 512(DI)
        VMOVDQU         Y0, 544(DI)
        VMOVDQU         Y0, 576(DI)
        VMOVDQU         Y0, 608(DI)
        VMOVDQU         Y0, 640(DI)
        VMOVDQU         Y0, 672(DI)
        VMOVDQU         Y0, 704(DI)
        VMOVDQU         Y0, 736(DI)
        VMOVDQU         Y0, 768(DI)
        VMOVDQU         Y0, 800(DI)
        VMOVDQU         Y0, 832(DI)
        VMOVDQU         Y0, 864(DI)
        VMOVDQU         Y0, 896(DI)
        VMOVDQU         Y0, 928(DI)

        ADDQ            $960, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllUint32AVX2Exit(SB)
