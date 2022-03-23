// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefanx@blockwatch.cc

#include "textflag.h"
#include "constants_AVX.h"

TEXT ·initAVX2Call(SB), NOSPLIT, $0-0
        LEAQ            ·unpack240AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>(SB)
        LEAQ            ·unpack120AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>+8(SB)
        LEAQ            ·unpack60AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>+16(SB)
        LEAQ            ·unpack30AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>+24(SB)
        LEAQ            ·unpack20AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>+32(SB)
        LEAQ            ·unpack15AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>+40(SB)
        LEAQ            ·unpack12AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>+48(SB)
        LEAQ            ·unpack10AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>+56(SB)
        LEAQ            ·unpack8AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>+64(SB)
        LEAQ            ·unpack7AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>+72(SB)
        LEAQ            ·unpack6AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>+80(SB)
        LEAQ            ·unpack5AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>+88(SB)
        LEAQ            ·unpack4AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>+96(SB)
        LEAQ            ·unpack3AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>+104(SB)
        LEAQ            ·unpack2AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>+112(SB)
        LEAQ            ·unpack1AVX2Call(SB), DX
        MOVQ            DX, funcTableCall<>+120(SB)

        RET

// func decodeAllAVX2Call(dst, src []uint64) (value int)
TEXT ·decodeAllAVX2Call(SB), NOSPLIT, $0-68
        MOVQ            dst_base(FP), DI
        MOVQ            src_base+24(FP), SI
        MOVQ            src_len+32(FP), BX
        MOVQ            DI, R15                     // save DI

	TESTQ	        BX, BX
	JLE		exit

        LEAQ            funcTableCall<>(SB), R14    // base of function pointer table
        VMOVDQU         write3mask<>(SB), Y14

loop:
        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX
        CALL            AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             loop

exit:
        VZEROUPPER
        SUBQ            R15, DI
        SHRQ            $3, DI
        MOVQ            DI, ret+48(FP)
        RET

// func decodeBytesBigEndianAVX2Core(dst []uint64, src []byte) (value int)
TEXT ·decodeBytesBigEndianAVX2Core(SB), NOSPLIT, $0-68
        MOVQ            dst_base(FP), DI
        MOVQ            src_base+24(FP), SI
        MOVQ            src_len+32(FP), BX
        SHRQ            $3, BX
        MOVQ            DI, R15                     // save DI

	TESTQ	        BX, BX
	JLE		exit

        LEAQ            funcTableCall<>(SB), R14    // base of function pointer table
        VMOVDQU         write3mask<>(SB), Y14

loop:
        MOVQ            (SI), DX
        BSWAPQ          DX
        MOVQ            DX, (SI)
        SHRQ            $60, DX                 // calc selector

        MOVQ            (R14)(DX*8), AX
        CALL            AX

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit
        JMP             loop

exit:
        VZEROUPPER
        SUBQ            R15, DI
        SHRQ            $3, DI
        MOVQ            DI, ret+48(FP)
        RET

// func unpack1AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack1AVX2Call(SB), NOSPLIT, $0-68
        MOVQ            mask1, R8

        ANDQ            (SI), R8            
        MOVQ            R8, (DI)

        ADDQ            $8, DI
        RET

// func unpack2AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack2AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), X0
        VPBROADCASTQ    mask2<>(SB), X15

        VPSRLVQ         shift2<>(SB), X0, X0
        VPAND           X0, X15, X0
        VMOVDQU         X0, (DI)

        ADDQ            $16, DI
        RET

// func unpack3AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack3AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask3<>(SB), Y15

        VPSRLVQ         shift3<>(SB), Y0, Y0
        VPAND           Y0, Y15, Y0
        VPMASKMOVQ      Y0, Y14, (DI)

        ADDQ            $24, DI
        RET

// func unpack4AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack4AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask4<>(SB), Y15

        VPSRLVQ         shift4<>(SB), Y0, Y0
        VPAND           Y0, Y15, Y0
        VMOVDQU         Y0, (DI)

        ADDQ            $32, DI
        RET

// func unpack5AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack5AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask5<>(SB), Y15

        VPSRLVQ         shift5<>+0x00(SB), Y0, Y1
//        VPSRLVQ         shift5<>+0x20(SB), Y0, Y0
//        VPAND           Y0, Y15, Y0
        VPAND           Y1, Y15, Y1
        VMOVDQU         Y1, (DI)
//        VMOVDQU         Y0, 32(DI)

        MOVQ            mask5<>(SB), R8

        MOVQ            (SI), DX
        SHRQ            $48, DX
        ANDQ            DX, R8            
        MOVQ            R8, 32(DI)

        ADDQ            $40, DI
        RET

// func unpack6AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack6AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask6<>(SB), Y15

        VPSRLVQ         shift6<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift6<>+0x20(SB), X0, X0
        VPAND           X0, X15, X0
        VPAND           Y1, Y15, Y1
        VMOVDQU         Y1, (DI)
        VMOVDQU         X0, 32(DI)

        ADDQ            $48, DI
        RET

// func unpack7AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack7AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask7<>(SB), Y15

        VPSRLVQ         shift7<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift7<>+0x20(SB), Y0, Y0
        VPAND           Y0, Y15, Y0
        VPAND           Y1, Y15, Y1
        VMOVDQU         Y1, (DI)
        VPMASKMOVQ      Y0, Y14, 32(DI)

        ADDQ            $56, DI
        RET

// func unpack8AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack8AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask8<>(SB), Y15

        VPSRLVQ         shift8<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift8<>+0x20(SB), Y0, Y0
        VPAND           Y0, Y15, Y0
        VPAND           Y1, Y15, Y1
        VMOVDQU         Y1, (DI)
        VMOVDQU         Y0, 32(DI)

        ADDQ            $64, DI
        RET

// func unpack10AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack10AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask10<>(SB), Y15

        VPSRLVQ         shift10<>+0x00(SB), Y0, Y2
        VPSRLVQ         shift10<>+0x20(SB), Y0, Y1
        VPSRLVQ         shift10<>+0x40(SB), X0, X0
        VPAND           X0, X15, X0
        VPAND           Y1, Y15, Y1
        VPAND           Y2, Y15, Y2
        VMOVDQU         Y2, (DI)
        VMOVDQU         Y1, 32(DI)
        VMOVDQU         X0, 64(DI)

        ADDQ            $80, DI
        RET

// func unpack12AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack12AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask12<>(SB), Y15

        VPSRLVQ         shift12<>+0x00(SB), Y0, Y2
        VPSRLVQ         shift12<>+0x20(SB), Y0, Y1
        VPSRLVQ         shift12<>+0x40(SB), Y0, Y0
        VPAND           Y0, Y15, Y0
        VPAND           Y1, Y15, Y1
        VPAND           Y2, Y15, Y2
        VMOVDQU         Y2, (DI)
        VMOVDQU         Y1, 32(DI)
        VMOVDQU         Y0, 64(DI)

        ADDQ            $96, DI
        RET

// func unpack15AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack15AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask15<>(SB), Y15

        VPSRLVQ         shift15<>+0x00(SB), Y0, Y3
        VPSRLVQ         shift15<>+0x20(SB), Y0, Y2
        VPSRLVQ         shift15<>+0x40(SB), Y0, Y1
        VPSRLVQ         shift15<>+0x60(SB), Y0, Y0
        VPAND           Y0, Y15, Y0
        VPAND           Y1, Y15, Y1
        VPAND           Y2, Y15, Y2
        VPAND           Y3, Y15, Y3
        VMOVDQU         Y3, (DI)
        VMOVDQU         Y2, 32(DI)
        VMOVDQU         Y1, 64(DI)
        VPMASKMOVQ      Y0, Y14, 96(DI)

        ADDQ            $120, DI
        RET

// func unpack20AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack20AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask20<>(SB), Y15

        VPSRLVQ         shift20<>+0x00(SB), Y0, Y4
        VPSRLVQ         shift20<>+0x20(SB), Y0, Y3
        VPSRLVQ         shift20<>+0x40(SB), Y0, Y2
        VPSRLVQ         shift20<>+0x60(SB), Y0, Y1
        VPSRLVQ         shift20<>+0x80(SB), Y0, Y0
        VPAND           Y0, Y15, Y0
        VPAND           Y1, Y15, Y1
        VPAND           Y2, Y15, Y2
        VPAND           Y3, Y15, Y3
        VPAND           Y4, Y15, Y4
        VMOVDQU         Y4, (DI)
        VMOVDQU         Y3, 32(DI)
        VMOVDQU         Y2, 64(DI)
        VMOVDQU         Y1, 96(DI)
        VMOVDQU         Y0, 128(DI)

        ADDQ            $160, DI
        RET

// func unpack30AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack30AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask30<>(SB), Y15

        VPSRLVQ         shift30<>+0x00(SB), Y0, Y7
        VPSRLVQ         shift30<>+0x20(SB), Y0, Y6
        VPSRLVQ         shift30<>+0x40(SB), Y0, Y5
        VPSRLVQ         shift30<>+0x60(SB), Y0, Y4
        VPAND           Y7, Y15, Y7
        VPAND           Y6, Y15, Y6
        VPAND           Y5, Y15, Y5
        VPAND           Y4, Y15, Y4
        VMOVDQU         Y7, (DI)
        VMOVDQU         Y6, 32(DI)
        VMOVDQU         Y5, 64(DI)
        VMOVDQU         Y4, 96(DI)


        VPSRLVQ         shift30<>+0x80(SB), Y0, Y3
        VPSRLVQ         shift30<>+0xa0(SB), Y0, Y2
        VPSRLVQ         shift30<>+0xc0(SB), Y0, Y1
        VPSRLVQ         shift30<>+0xe0(SB), X0, X0
        VPAND           Y3, Y15, Y3
        VPAND           Y2, Y15, Y2
        VPAND           Y1, Y15, Y1
        VPAND           X0, X15, X0
        VMOVDQU         Y3, 128(DI)
        VMOVDQU         Y2, 160(DI)
        VMOVDQU         Y1, 192(DI)
        VMOVDQU         X0, 224(DI)

        ADDQ            $240, DI
        RET

// func unpack60AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack60AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask60<>(SB), Y15

        VPSRLVQ         shift60<>+0x000(SB), Y0, Y1
        VPSRLVQ         shift60<>+0x020(SB), Y0, Y13
        VPSRLVQ         shift60<>+0x040(SB), Y0, Y12
        VPSRLVQ         shift60<>+0x060(SB), Y0, Y11
        VPAND           Y1, Y15, Y1
        VPAND           Y13, Y15, Y13
        VPAND           Y12, Y15, Y12
        VPAND           Y11, Y15, Y11
        VMOVDQU         Y1, (DI)
        VMOVDQU         Y13, 32(DI)
        VMOVDQU         Y12, 64(DI)
        VMOVDQU         Y11, 96(DI)

        VPSRLVQ         shift60<>+0x080(SB), Y0, Y10
        VPSRLVQ         shift60<>+0x0a0(SB), Y0, Y9
        VPSRLVQ         shift60<>+0x0c0(SB), Y0, Y8
        VPSRLVQ         shift60<>+0x0e0(SB), Y0, Y7
        VPAND           Y10, Y15, Y10
        VPAND           Y9, Y15, Y9
        VPAND           Y8, Y15, Y8
        VPAND           Y7, Y15, Y7
        VMOVDQU         Y10, 128(DI)
        VMOVDQU         Y9, 160(DI)
        VMOVDQU         Y8, 192(DI)
        VMOVDQU         Y7, 224(DI)

        VPSRLVQ         shift60<>+0x100(SB), Y0, Y6
        VPSRLVQ         shift60<>+0x120(SB), Y0, Y5
        VPSRLVQ         shift60<>+0x140(SB), Y0, Y4
        VPSRLVQ         shift60<>+0x160(SB), Y0, Y3
        VPAND           Y6, Y15, Y6
        VPAND           Y5, Y15, Y5
        VPAND           Y4, Y15, Y4
        VPAND           Y3, Y15, Y3
        VMOVDQU         Y6, 256(DI)
        VMOVDQU         Y5, 288(DI)
        VMOVDQU         Y4, 320(DI)
        VMOVDQU         Y3, 352(DI)

        VPSRLVQ         shift60<>+0x180(SB), Y0, Y2
        VPSRLVQ         shift60<>+0x1a0(SB), Y0, Y1
        VPSRLVQ         shift60<>+0x1c0(SB), Y0, Y0
        VPAND           Y2, Y15, Y2
        VPAND           Y1, Y15, Y1
        VPAND           Y0, Y15, Y0
        VMOVDQU         Y2, 384(DI)
        VMOVDQU         Y1, 416(DI)
        VMOVDQU         Y0, 448(DI)

        ADDQ            $480, DI
        RET

// func unpack120AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack120AVX2Call(SB), NOSPLIT, $0-68
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
        RET

// func unpack240AVX2(v uint64, dst *[240]uint64)
TEXT ·unpack240AVX2Call(SB), NOSPLIT, $0-68
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
        RET
