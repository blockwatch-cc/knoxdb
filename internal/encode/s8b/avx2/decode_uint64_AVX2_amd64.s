// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

#include "textflag.h"
#include "constants_uint64_AVX2.h"

// allow buffer overflows due to writung full vector even its not full
// caller has to care about
// #define ALLOW_BO

TEXT ·initUint64AVX2(SB), NOSPLIT, $0-0
        LEAQ            ·unpackZerosUint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>(SB)
        LEAQ            ·unpackOnesUint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>+8(SB)
        LEAQ            ·unpack60Uint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>+16(SB)
        LEAQ            ·unpack30Uint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>+24(SB)
        LEAQ            ·unpack20Uint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>+32(SB)
        LEAQ            ·unpack15Uint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>+40(SB)
        LEAQ            ·unpack12Uint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>+48(SB)
        LEAQ            ·unpack10Uint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>+56(SB)
        LEAQ            ·unpack8Uint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>+64(SB)
        LEAQ            ·unpack7Uint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>+72(SB)
        LEAQ            ·unpack6Uint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>+80(SB)
        LEAQ            ·unpack5Uint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>+88(SB)
        LEAQ            ·unpack4Uint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>+96(SB)
        LEAQ            ·unpack3Uint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>+104(SB)
        LEAQ            ·unpack2Uint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>+112(SB)
        LEAQ            ·unpack1Uint64AVX2(SB), DX
        MOVQ            DX, funcTableUint64AVX2<>+120(SB)

        RET

// func decodeUint64AVX2(dst []uint64, src []byte, minv uint64) (value int)
TEXT ·decodeUint64AVX2(SB), NOSPLIT, $0-64
        MOVQ            dst_base(FP), DI
        MOVQ            src_base+24(FP), SI
        MOVQ            src_len+32(FP), BX
        VPBROADCASTQ    minv+48(FP), Y5
        SHRQ            $3, BX
        MOVQ            DI, R15                            // save DI

    	CMPQ	        BX, $0
	JNE		start
        JMP             ·decodeUint64AVX2Exit(SB)
start:
        LEAQ            funcTableUint64AVX2<>(SB), R14            // base of function pointer table
        VMOVDQU         write3mask<>(SB), Y15
        VPBROADCASTQ    mask60<>(SB), Y14
        VPBROADCASTQ    mask30<>(SB), Y13
        VPBROADCASTQ    mask20<>(SB), Y12
        VPBROADCASTQ    mask7<>(SB), Y11
        VPBROADCASTQ    mask6<>(SB), Y10
        VPBROADCASTQ    mask5<>(SB), Y9
        VPBROADCASTQ    mask4<>(SB), Y8
        VPBROADCASTQ    mask3<>(SB), Y7

        MOVQ            (SI), DX
        SHRQ            $60, DX                            // calc selector

        MOVQ            (R14)(DX*8), AX
        JMP             AX

TEXT ·decodeUint64AVX2Exit(SB), NOSPLIT, $0-0
        VZEROUPPER
        SUBQ            R15, DI
        SHRQ            $3, DI
        MOVQ            DI, value+56(FP)
        RET

// func unpack1Uint64AVX2()
TEXT ·unpack1Uint64AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    mask1<>(SB), X6
        VMOVQ           (SI), X0
        VPAND           X0, X6, X0              // reload mask (Y6 shared with unpack2)
        VPADDQ          X0, X5, X0              // add minv
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
        JMP ·decodeUint64AVX2Exit(SB)

// func unpack2Uint64AVX2()
TEXT ·unpack2Uint64AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), X0
        VPBROADCASTQ    mask2<>(SB), X6         // reload mask (Y6 shared with unpack1)

        VPSRLVQ         shift2<>(SB), X0, X0
        VPAND           X0, X6, X0
        VPADDQ          X0, X5, X0              // add minv
        VMOVDQU         X0, (DI)

        ADDQ            $16, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint64AVX2Exit(SB)

// func unpack3Uint64AVX2()
TEXT ·unpack3Uint64AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0

        VPSRLVQ         shift3<>(SB), Y0, Y0
        VPAND           Y0, Y7, Y0
        VPADDQ          Y0, Y5, Y0            // add minv

#ifdef ALLOW_BO
        VMOVDQU         Y0, (DI)
#else
        VPMASKMOVQ      Y0, Y15, (DI)
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
        JMP ·decodeUint64AVX2Exit(SB)

// func unpack4Uint64AVX2()
TEXT ·unpack4Uint64AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0

        VPSRLVQ         shift4<>(SB), Y0, Y0
        VPAND           Y0, Y8, Y0
        VPADDQ          Y0, Y5, Y0            // add minv
        VMOVDQU         Y0, (DI)

        ADDQ            $32, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint64AVX2Exit(SB)

// func unpack5Uint64AVX2()
TEXT ·unpack5Uint64AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0

        VPSRLVQ         shift5<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift5<>+0x20(SB), X0, X0
        VPAND           X0, X9, X0
        VPAND           Y1, Y9, Y1
        VPADDQ          X0, X5, X0            // add minv
        VPADDQ          Y1, Y5, Y1            // add minv
        VMOVDQU         Y1, (DI)
        VMOVQ           X0, 32(DI)

        ADDQ            $40, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint64AVX2Exit(SB)

// func unpack6Uint64AVX2()
TEXT ·unpack6Uint64AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0

        VPSRLVQ         shift6<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift6<>+0x20(SB), X0, X0
        VPAND           X0, X10, X0
        VPAND           Y1, Y10, Y1
        VPADDQ          X0, X5, X0            // add minv
        VPADDQ          Y1, Y5, Y1            // add minv
        VMOVDQU         Y1, (DI)
        VMOVDQU         X0, 32(DI)

        ADDQ            $48, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint64AVX2Exit(SB)

// func unpack7Uint64AVX2()
TEXT ·unpack7Uint64AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0

        VPSRLVQ         shift7<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift7<>+0x20(SB), Y0, Y0
        VPAND           Y0, Y11, Y0
        VPAND           Y1, Y11, Y1
        VPADDQ          Y0, Y5, Y0            // add minv
        VPADDQ          Y1, Y5, Y1            // add minv
        VMOVDQU         Y1, (DI)

#ifdef ALLOW_BO
        VMOVDQU         Y0, 32(DI)
#else
        VPMASKMOVQ      Y0, Y15, 32(DI)
#endif

        ADDQ            $56, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint64AVX2Exit(SB)

// func unpack8Uint64AVX2()
TEXT ·unpack8Uint64AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask8<>(SB), Y4

        VPSRLVQ         shift8<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift8<>+0x20(SB), Y0, Y0
        VPAND           Y0, Y4, Y0
        VPAND           Y1, Y4, Y1
        VPADDQ          Y0, Y5, Y0            // add minv
        VPADDQ          Y1, Y5, Y1            // add minv
        VMOVDQU         Y1, (DI)
        VMOVDQU         Y0, 32(DI)

        ADDQ            $64, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint64AVX2Exit(SB)

// func unpack10Uint64AVX2()
TEXT ·unpack10Uint64AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask10<>(SB), Y4

        VPSRLVQ         shift10<>+0x00(SB), Y0, Y2
        VPSRLVQ         shift10<>+0x20(SB), Y0, Y1
        VPSRLVQ         shift10<>+0x40(SB), X0, X0
        VPAND           X0, X4, X0
        VPAND           Y1, Y4, Y1
        VPAND           Y2, Y4, Y2
        VPADDQ          X0, X5, X0            // add minv
        VPADDQ          Y1, Y5, Y1            // add minv
        VPADDQ          Y2, Y5, Y2            // add minv
        VMOVDQU         Y2, (DI)
        VMOVDQU         Y1, 32(DI)
        VMOVDQU         X0, 64(DI)

        ADDQ            $80, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint64AVX2Exit(SB)

// func unpack12Uint64AVX2()
TEXT ·unpack12Uint64AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask12<>(SB), Y4

        VPSRLVQ         shift12<>+0x00(SB), Y0, Y2
        VPSRLVQ         shift12<>+0x20(SB), Y0, Y1
        VPSRLVQ         shift12<>+0x40(SB), Y0, Y0
        VPAND           Y0, Y4, Y0
        VPAND           Y1, Y4, Y1
        VPAND           Y2, Y4, Y2
        VPADDQ          Y0, Y5, Y0            // add minv
        VPADDQ          Y1, Y5, Y1            // add minv
        VPADDQ          Y2, Y5, Y2            // add minv
        VMOVDQU         Y2, (DI)
        VMOVDQU         Y1, 32(DI)
        VMOVDQU         Y0, 64(DI)

        ADDQ            $96, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint64AVX2Exit(SB)

// func unpack15Uint64AVX2()
TEXT ·unpack15Uint64AVX2(SB), NOSPLIT, $0-0
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
        VPADDQ          Y0, Y5, Y0            // add minv
        VPADDQ          Y1, Y5, Y1            // add minv
        VPADDQ          Y2, Y5, Y2            // add minv
        VPADDQ          Y3, Y5, Y3            // add minv
        VMOVDQU         Y3, (DI)
        VMOVDQU         Y2, 32(DI)
        VMOVDQU         Y1, 64(DI)

#ifdef ALLOW_BO
        VMOVDQU         Y0, 96(DI)
#else
        VPMASKMOVQ      Y0, Y15, 96(DI)
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
        JMP ·decodeUint64AVX2Exit(SB)

// func unpack20Uint64AVX2()
TEXT ·unpack20Uint64AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0

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
        VPADDQ          Y4, Y5, Y4            // add minv
        VPADDQ          Y3, Y5, Y3            // add minv
        VPADDQ          Y2, Y5, Y2            // add minv
        VPADDQ          Y1, Y5, Y1            // add minv
        VPADDQ          Y0, Y5, Y0            // add minv
        VMOVDQU         Y4, (DI)
        VMOVDQU         Y3, 32(DI)
        VMOVDQU         Y2, 64(DI)
        VMOVDQU         Y1, 96(DI)
        VMOVDQU         Y0, 128(DI)

        ADDQ            $160, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint64AVX2Exit(SB)

// func unpack30Uint64AVX2()
TEXT ·unpack30Uint64AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0

        VPSRLVQ         shift30<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift30<>+0x20(SB), Y0, Y2
        VPSRLVQ         shift30<>+0x40(SB), Y0, Y3
        VPSRLVQ         shift30<>+0x60(SB), Y0, Y4
        VPAND           Y1, Y13, Y1
        VPAND           Y2, Y13, Y2
        VPAND           Y3, Y13, Y3
        VPAND           Y4, Y13, Y4
        VPADDQ          Y1, Y5, Y1            // add minv
        VPADDQ          Y2, Y5, Y2            // add minv
        VPADDQ          Y3, Y5, Y3            // add minv
        VPADDQ          Y4, Y5, Y4            // add minv
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
        VPADDQ          Y3, Y5, Y3            // add minv
        VPADDQ          Y2, Y5, Y2            // add minv
        VPADDQ          Y1, Y5, Y1            // add minv
        VPADDQ          X0, X5, X0            // add minv
        VMOVDQU         Y3, 128(DI)
        VMOVDQU         Y2, 160(DI)
        VMOVDQU         Y1, 192(DI)
        VMOVDQU         X0, 224(DI)

        ADDQ            $240, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint64AVX2Exit(SB)

// func unpack60Uint64AVX2()
TEXT ·unpack60Uint64AVX2(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Y0

        VPSRLVQ         shift60<>+0x000(SB), Y0, Y1
        VPSRLVQ         shift60<>+0x020(SB), Y0, Y2
        VPSRLVQ         shift60<>+0x040(SB), Y0, Y3
        VPSRLVQ         shift60<>+0x060(SB), Y0, Y4
        VPAND           Y1, Y14, Y1
        VPAND           Y2, Y14, Y2
        VPAND           Y3, Y14, Y3
        VPAND           Y4, Y14, Y4
        VPADDQ          Y1, Y5, Y1            // add minv
        VPADDQ          Y2, Y5, Y2            // add minv
        VPADDQ          Y3, Y5, Y3            // add minv
        VPADDQ          Y4, Y5, Y4            // add minv
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
        VPADDQ          Y1, Y5, Y1            // add minv
        VPADDQ          Y2, Y5, Y2            // add minv
        VPADDQ          Y3, Y5, Y3            // add minv
        VPADDQ          Y4, Y5, Y4            // add minv
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
        VPADDQ          Y1, Y5, Y1            // add minv
        VPADDQ          Y2, Y5, Y2            // add minv
        VPADDQ          Y3, Y5, Y3            // add minv
        VPADDQ          Y4, Y5, Y4            // add minv
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
        VPADDQ          Y2, Y5, Y2            // add minv
        VPADDQ          Y1, Y5, Y1            // add minv
        VPADDQ          Y0, Y5, Y0            // add minv
        VMOVDQU         Y2, 384(DI)
        VMOVDQU         Y1, 416(DI)
        VMOVDQU         Y0, 448(DI)

        ADDQ            $480, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint64AVX2Exit(SB)

// func unpackOnesUint64AVX2()
TEXT ·unpackOnesUint64AVX2(SB), NOSPLIT, $0-0
        VPCMPEQQ        Y0, Y0, Y0
        VPSRLQ          $63, Y0, Y0             // Y0 = [1,1,...] 
        VPADDQ          Y0, Y5, Y0              // add minv

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

        ADDQ            $1024, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint64AVX2Exit(SB)

// func unpackZerosUint64AVX2()
TEXT ·unpackZerosUint64AVX2(SB), NOSPLIT, $0-0
        // VPXORQ          Y0, Y0, Y0      // Y0 = [1,1,...]

        VMOVDQU         Y5, (DI)           // Y5 = minv
        VMOVDQU         Y5, 32(DI)
        VMOVDQU         Y5, 64(DI)
        VMOVDQU         Y5, 96(DI)
        VMOVDQU         Y5, 128(DI)
        VMOVDQU         Y5, 160(DI)
        VMOVDQU         Y5, 192(DI)
        VMOVDQU         Y5, 224(DI)
        VMOVDQU         Y5, 256(DI)
        VMOVDQU         Y5, 288(DI)
        VMOVDQU         Y5, 320(DI)
        VMOVDQU         Y5, 352(DI)
        VMOVDQU         Y5, 384(DI)
        VMOVDQU         Y5, 416(DI)
        VMOVDQU         Y5, 448(DI)
        VMOVDQU         Y5, 480(DI)
        VMOVDQU         Y5, 512(DI)
        VMOVDQU         Y5, 544(DI)
        VMOVDQU         Y5, 576(DI)
        VMOVDQU         Y5, 608(DI)
        VMOVDQU         Y5, 640(DI)
        VMOVDQU         Y5, 672(DI)
        VMOVDQU         Y5, 704(DI)
        VMOVDQU         Y5, 736(DI)
        VMOVDQU         Y5, 768(DI)
        VMOVDQU         Y5, 800(DI)
        VMOVDQU         Y5, 832(DI)
        VMOVDQU         Y5, 864(DI)
        VMOVDQU         Y5, 896(DI)
        VMOVDQU         Y5, 928(DI)
        VMOVDQU         Y5, 960(DI)
        VMOVDQU         Y5, 992(DI)

        ADDQ            $1024, DI

        ADDQ            $8, SI
        SUBQ            $1, BX
        JZ              exit

        MOVQ            (SI), DX
        SHRQ            $60, DX                 // calc selector
        MOVQ            (R14)(DX*8), AX         // read jump adress
        JMP             AX
exit:
        JMP ·decodeUint64AVX2Exit(SB)
