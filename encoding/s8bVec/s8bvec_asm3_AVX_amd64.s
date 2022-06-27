// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefanx@blockwatch.cc

#include "textflag.h"
#include "constants_AVX.h"

TEXT ·initAVX2Opt(SB), NOSPLIT, $0-0
        LEAQ            ·unpack240AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>(SB)
        LEAQ            ·unpack120AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>+8(SB)
        LEAQ            ·unpack60AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>+16(SB)
        LEAQ            ·unpack30AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>+24(SB)
        LEAQ            ·unpack20AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>+32(SB)
        LEAQ            ·unpack15AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>+40(SB)
        LEAQ            ·unpack12AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>+48(SB)
        LEAQ            ·unpack10AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>+56(SB)
        LEAQ            ·unpack8AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>+64(SB)
        LEAQ            ·unpack7AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>+72(SB)
        LEAQ            ·unpack6AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>+80(SB)
        LEAQ            ·unpack5AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>+88(SB)
        LEAQ            ·unpack4AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>+96(SB)
        LEAQ            ·unpack3AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>+104(SB)
        LEAQ            ·unpack2AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>+112(SB)
        LEAQ            ·unpack1AVX2Opt(SB), DX
        MOVQ            DX, funcTableOpt<>+120(SB)

        RET

// func decodeAllAVX2Opt(dst []uint64, src []byte) (value int)
TEXT ·decodeAllAVX2Opt(SB), NOSPLIT, $0-56
        MOVQ            dst_base(FP), DI
        MOVQ            src_base+24(FP), SI
        MOVQ            src_len+32(FP), BX
        SHRQ            $3, BX
        MOVQ            DI, R15                            // save DI

    	CMPQ	        BX, $0
	JNE		start
        JMP             ·decodeAllAVX2OptExit(SB)
start:
        LEAQ            funcTableOpt<>(SB), R14            // base of function pointer table
        VMOVDQU         write3mask<>(SB), Y15
        VPBROADCASTQ         mask60<>(SB), Y14
        VPBROADCASTQ         mask30<>(SB), Y13
        VPBROADCASTQ         mask20<>(SB), Y12
        VPBROADCASTQ         mask7<>(SB), Y11
        VPBROADCASTQ         mask6<>(SB), Y10
        VPBROADCASTQ         mask5<>(SB), Y9
        VPBROADCASTQ         mask4<>(SB), Y8
        VPBROADCASTQ         mask3<>(SB), Y7
        VPBROADCASTQ         mask2<>(SB), Y6
        VMOVDQU         mask1x<>(SB), Y5

        MOVQ            (SI), DX
        SHRQ            $60, DX                            // calc selector

        MOVQ            (R14)(DX*8), AX
        JMP             AX

TEXT ·decodeAllAVX2OptExit(SB), NOSPLIT, $0-0
        VZEROUPPER
        SUBQ            R15, DI
        SHRQ            $3, DI
        MOVQ            DI, ret+48(FP)
        RET

// func unpack1AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack1AVX2Opt(SB), NOSPLIT, $0-0
        VMOVQ           (SI), X0
//        VPAND           mask1x<>(SB), X0, X0
        VPAND           X0, X5, X0
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
        JMP ·decodeAllAVX2OptExit(SB)

// func unpack2AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack2AVX2Opt(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), X0
//        VPBROADCASTQ    mask2<>(SB), X15

        VPSRLVQ         shift2<>(SB), X0, X0
        VPAND           X0, X6, X0
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
        JMP ·decodeAllAVX2OptExit(SB)

// func unpack3AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack3AVX2Opt(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
//        VPBROADCASTQ    mask3<>(SB), Y15

//        VPSRLVQ         shift3x<>(SB), X0, X1
//        VPAND           X0, X15, X0
//        VPAND           X1, X15, X1

        VPSRLVQ         shift3<>(SB), Y0, Y0
        VPAND           Y0, Y7, Y0

//        VPERMQ          $0xaa, Y1, Y0
//        VMOVQ           X0, (DI)
//        VMOVDQU         X1, 8(DI)
//        VMOVDQU         X1, (DI)
//        VMOVQ           X0, 16(DI)
        VPMASKMOVQ      Y0, Y15, (DI)
        VMOVDQU         Y0, (DI)

        ADDQ            $24, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX
        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllAVX2OptExit(SB)

// func unpack4AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack4AVX2Opt(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
//        VPBROADCASTQ    mask4<>(SB), Y8

        VPSRLVQ         shift4<>(SB), Y0, Y0
        VPAND           Y0, Y8, Y0
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
        JMP ·decodeAllAVX2OptExit(SB)

// func unpack5AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack5AVX2Opt(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
//        VPBROADCASTQ    mask5<>(SB), Y15

        VPSRLVQ         shift5<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift5<>+0x20(SB), X0, X0
        VPAND           X0, X9, X0
        VPAND           Y1, Y9, Y1
        VMOVDQU         Y1, (DI)
        VMOVQ           X0, 32(DI)

        ADDQ            $40, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX
        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllAVX2OptExit(SB)

// func unpack6AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack6AVX2Opt(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
//        VPBROADCASTQ    mask6<>(SB), Y15

        VPSRLVQ         shift6<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift6<>+0x20(SB), X0, X0
        VPAND           X0, X10, X0
        VPAND           Y1, Y10, Y1
        VMOVDQU         Y1, (DI)
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
        JMP ·decodeAllAVX2OptExit(SB)

// func unpack7AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack7AVX2Opt(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
//        VPBROADCASTQ    mask7<>(SB), Y15

        VPSRLVQ         shift7<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift7<>+0x20(SB), Y0, Y0
        VPAND           Y0, Y11, Y0
        VPAND           Y1, Y11, Y1
        VMOVDQU         Y1, (DI)
        VPMASKMOVQ      Y0, Y15, 32(DI)
//        VMOVDQU         Y0, 32(DI)

        ADDQ            $56, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX
        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllAVX2OptExit(SB)

// func unpack8AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack8AVX2Opt(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask8<>(SB), Y4

        VPSRLVQ         shift8<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift8<>+0x20(SB), Y0, Y0
        VPAND           Y0, Y4, Y0
        VPAND           Y1, Y4, Y1
        VMOVDQU         Y1, (DI)
        VMOVDQU         Y0, 32(DI)

        ADDQ            $64, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX
        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllAVX2OptExit(SB)

// func unpack10AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack10AVX2Opt(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask10<>(SB), Y4

        VPSRLVQ         shift10<>+0x00(SB), Y0, Y2
        VPSRLVQ         shift10<>+0x20(SB), Y0, Y1
        VPSRLVQ         shift10<>+0x40(SB), X0, X0
        VPAND           X0, X4, X0
        VPAND           Y1, Y4, Y1
        VPAND           Y2, Y4, Y2
        VMOVDQU         Y2, (DI)
        VMOVDQU         Y1, 32(DI)
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
        JMP ·decodeAllAVX2OptExit(SB)

// func unpack12AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack12AVX2Opt(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask12<>(SB), Y4

        VPSRLVQ         shift12<>+0x00(SB), Y0, Y2
        VPSRLVQ         shift12<>+0x20(SB), Y0, Y1
        VPSRLVQ         shift12<>+0x40(SB), Y0, Y0
        VPAND           Y0, Y4, Y0
        VPAND           Y1, Y4, Y1
        VPAND           Y2, Y4, Y2
        VMOVDQU         Y2, (DI)
        VMOVDQU         Y1, 32(DI)
        VMOVDQU         Y0, 64(DI)

        ADDQ            $96, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX
        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllAVX2OptExit(SB)

// func unpack15AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack15AVX2Opt(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask15<>(SB), Y4

        VPSRLVQ         shift15<>+0x00(SB), Y0, Y3
        VPSRLVQ         shift15<>+0x20(SB), Y0, Y2
        VPSRLVQ         shift15<>+0x40(SB), Y0, Y1
        VPSRLVQ         shift15<>+0x60(SB), Y0, Y0
        VPAND           Y0, Y4, Y0
        VPAND           Y1, Y4, Y1
        VPAND           Y2, Y4, Y2
        VPAND           Y3, Y4, Y3
        VMOVDQU         Y3, (DI)
        VMOVDQU         Y2, 32(DI)
        VMOVDQU         Y1, 64(DI)
        VPMASKMOVQ      Y0, Y15, 96(DI)
//        VMOVDQU         Y0, 96(DI)

        ADDQ            $120, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX
        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllAVX2OptExit(SB)

// func unpack20AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack20AVX2Opt(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
//        VPBROADCASTQ    mask20<>(SB), Y15

        VPSRLVQ         shift20<>+0x00(SB), Y0, Y4
        VPSRLVQ         shift20<>+0x20(SB), Y0, Y3
        VPSRLVQ         shift20<>+0x40(SB), Y0, Y2
        VPSRLVQ         shift20<>+0x60(SB), Y0, Y1
        VPSRLVQ         shift20<>+0x80(SB), Y0, Y0
        VPAND           Y4, Y12, Y4
        VPAND           Y3, Y12, Y3
        VPAND           Y2, Y12, Y2
        VPAND           Y1, Y12, Y1
        VPAND           Y0, Y12, Y0
        VMOVDQU         Y4, (DI)
        VMOVDQU         Y3, 32(DI)
        VMOVDQU         Y2, 64(DI)
        VMOVDQU         Y1, 96(DI)
        VMOVDQU         Y0, 128(DI)

        ADDQ            $160, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX
        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllAVX2OptExit(SB)

// func unpack30AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack30AVX2Opt(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
        // VPBROADCASTQ    mask30<>(SB), Y15

        VPSRLVQ         shift30<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift30<>+0x20(SB), Y0, Y2
        VPSRLVQ         shift30<>+0x40(SB), Y0, Y3
        VPSRLVQ         shift30<>+0x60(SB), Y0, Y4
        VPAND           Y1, Y13, Y1
        VPAND           Y2, Y13, Y2
        VPAND           Y3, Y13, Y3
        VPAND           Y4, Y13, Y4
        VMOVDQU         Y1, (DI)
        VMOVDQU         Y2, 32(DI)
        VMOVDQU         Y3, 64(DI)
        VMOVDQU         Y4, 96(DI)

        VPSRLVQ         shift30<>+0x80(SB), Y0, Y3
        VPSRLVQ         shift30<>+0xa0(SB), Y0, Y2
        VPSRLVQ         shift30<>+0xc0(SB), Y0, Y1
        VPSRLVQ         shift30<>+0xe0(SB), X0, X0
        VPAND           Y3, Y13, Y3
        VPAND           Y2, Y13, Y2
        VPAND           Y1, Y13, Y1
        VPAND           X0, X13, X0
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
        JMP ·decodeAllAVX2OptExit(SB)

// func unpack60AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack60AVX2Opt(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
//        VPBROADCASTQ    mask60<>(SB), Y15

        VPSRLVQ         shift60<>+0x000(SB), Y0, Y1
        VPSRLVQ         shift60<>+0x020(SB), Y0, Y2
        VPSRLVQ         shift60<>+0x040(SB), Y0, Y3
        VPSRLVQ         shift60<>+0x060(SB), Y0, Y4
        VPAND           Y1, Y14, Y1
        VPAND           Y2, Y14, Y2
        VPAND           Y3, Y14, Y3
        VPAND           Y4, Y14, Y4
        VMOVDQU         Y1, (DI)
        VMOVDQU         Y2, 32(DI)
        VMOVDQU         Y3, 64(DI)
        VMOVDQU         Y4, 96(DI)

        VPSRLVQ         shift60<>+0x080(SB), Y0, Y1
        VPSRLVQ         shift60<>+0x0a0(SB), Y0, Y2
        VPSRLVQ         shift60<>+0x0c0(SB), Y0, Y3
        VPSRLVQ         shift60<>+0x0e0(SB), Y0, Y4
        VPAND           Y1, Y14, Y1
        VPAND           Y2, Y14, Y2
        VPAND           Y3, Y14, Y3
        VPAND           Y4, Y14, Y4
        VMOVDQU         Y1, 128(DI)
        VMOVDQU         Y2, 160(DI)
        VMOVDQU         Y3, 192(DI)
        VMOVDQU         Y4, 224(DI)

        VPSRLVQ         shift60<>+0x100(SB), Y0, Y1
        VPSRLVQ         shift60<>+0x120(SB), Y0, Y2
        VPSRLVQ         shift60<>+0x140(SB), Y0, Y3
        VPSRLVQ         shift60<>+0x160(SB), Y0, Y4
        VPAND           Y1, Y14, Y1
        VPAND           Y2, Y14, Y2
        VPAND           Y3, Y14, Y3
        VPAND           Y4, Y14, Y4
        VMOVDQU         Y1, 256(DI)
        VMOVDQU         Y2, 288(DI)
        VMOVDQU         Y3, 320(DI)
        VMOVDQU         Y4, 352(DI)

        VPSRLVQ         shift60<>+0x180(SB), Y0, Y2
        VPSRLVQ         shift60<>+0x1a0(SB), Y0, Y1
        VPSRLVQ         shift60<>+0x1c0(SB), Y0, Y0
        VPAND           Y2, Y14, Y2
        VPAND           Y1, Y14, Y1
        VPAND           Y0, Y14, Y0
        VMOVDQU         Y2, 384(DI)
        VMOVDQU         Y1, 416(DI)
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
        JMP ·decodeAllAVX2OptExit(SB)

// func unpack120AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack120AVX2Opt(SB), NOSPLIT, $0-0
        VPCMPEQQ        Y0, Y0, Y0
        VPSRLQ          $63, Y0, Y0             // Y0 = [1,1,...] 

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
        JMP ·decodeAllAVX2OptExit(SB)

// func unpack240AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack240AVX2Opt(SB), NOSPLIT, $0-0
        VPCMPEQQ        Y0, Y0, Y0
        VPSRLQ          $63, Y0, Y0             // Y0 = [1,1,...] 

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
        VMOVDQU         Y0, 960(DI)
        VMOVDQU         Y0, 992(DI)
        VMOVDQU         Y0, 1024(DI)
        VMOVDQU         Y0, 1056(DI)
        VMOVDQU         Y0, 1088(DI)
        VMOVDQU         Y0, 1120(DI)
        VMOVDQU         Y0, 1152(DI)
        VMOVDQU         Y0, 1184(DI)
        VMOVDQU         Y0, 1216(DI)
        VMOVDQU         Y0, 1248(DI)
        VMOVDQU         Y0, 1280(DI)
        VMOVDQU         Y0, 1312(DI)
        VMOVDQU         Y0, 1344(DI)
        VMOVDQU         Y0, 1376(DI)
        VMOVDQU         Y0, 1408(DI)
        VMOVDQU         Y0, 1440(DI)
        VMOVDQU         Y0, 1472(DI)
        VMOVDQU         Y0, 1504(DI)
        VMOVDQU         Y0, 1536(DI)
        VMOVDQU         Y0, 1568(DI)
        VMOVDQU         Y0, 1600(DI)
        VMOVDQU         Y0, 1632(DI)
        VMOVDQU         Y0, 1664(DI)
        VMOVDQU         Y0, 1696(DI)
        VMOVDQU         Y0, 1728(DI)
        VMOVDQU         Y0, 1760(DI)
        VMOVDQU         Y0, 1792(DI)
        VMOVDQU         Y0, 1824(DI)
        VMOVDQU         Y0, 1856(DI)
        VMOVDQU         Y0, 1888(DI)

        ADDQ            $1920, DI

        MOVQ            8(SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX
        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             AX
exit:
        JMP ·decodeAllAVX2OptExit(SB)
