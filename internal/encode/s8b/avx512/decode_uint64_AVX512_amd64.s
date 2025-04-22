// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

#include "textflag.h"
#include "constants_uint64_AVX512.h"

TEXT ·initUint64AVX512(SB), NOSPLIT, $0-0
        LEAQ            ·unpackZerosUint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>(SB)
        LEAQ            ·unpackOnesUint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>+8(SB)
        LEAQ            ·unpack60Uint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>+16(SB)
        LEAQ            ·unpack30Uint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>+24(SB)
        LEAQ            ·unpack20Uint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>+32(SB)
        LEAQ            ·unpack15Uint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>+40(SB)
        LEAQ            ·unpack12Uint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>+48(SB)
        LEAQ            ·unpack10Uint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>+56(SB)
        LEAQ            ·unpack8Uint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>+64(SB)
        LEAQ            ·unpack7Uint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>+72(SB)
        LEAQ            ·unpack6Uint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>+80(SB)
        LEAQ            ·unpack5Uint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>+88(SB)
        LEAQ            ·unpack4Uint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>+96(SB)
        LEAQ            ·unpack3Uint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>+104(SB)
        LEAQ            ·unpack2Uint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>+112(SB)
        LEAQ            ·unpack1Uint64AVX512(SB), DX
        MOVQ            DX, funcTableUint64AVX512<>+120(SB)

        RET

// func decodeUint64AVX512(dst, src []uint64, minv uint64) (value int)
TEXT ·decodeUint64AVX512(SB), NOSPLIT, $0-0
        MOVQ            dst_base(FP), DI
        MOVQ            src_base+24(FP), SI
        MOVQ            src_len+32(FP), BX
        MOVQ            minv+48(FP), R9
        VPBROADCASTQ    minv+48(FP), Z14
        MOVQ            DI, R15                     // save DI

	TESTQ	        BX, BX
	JLE		exit

        LEAQ            funcTableUint64AVX512<>(SB), R14    // base of function pointer table
        MOVQ            $0x3, AX
        KMOVQ           AX, K2
        MOVQ            $0x7, AX
        KMOVQ           AX, K3
        MOVQ            $0xf, AX
        KMOVQ           AX, K4
        MOVQ            $0x1f, AX
        KMOVQ           AX, K5
        MOVQ            $0x3f, AX
        KMOVQ           AX, K6
        MOVQ            $0x7f, AX
        KMOVQ           AX, K7

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
        MOVQ            DI, value+56(FP)
        RET

// func unpack1Uint64AVX512()
TEXT ·unpack1Uint64AVX512(SB), NOSPLIT, $0-0
        MOVQ            mask1, R8

        ANDQ            (SI), R8
        ADDQ            R9, R8      // add minv
        MOVQ            R8, (DI)

        ADDQ            $8, DI
        RET

// func unpack2Uint64AVX512()
TEXT ·unpack2Uint64AVX512(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask2<>(SB), Z15

        VPSRLVQ         shift2<>+0x00(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPADDQ          Z0, Z14, Z0
        VMOVDQU64       Z0, K2, (DI)

        ADDQ            $16, DI
        RET

// func unpack3Uint64AVX512()
TEXT ·unpack3Uint64AVX512(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask3<>(SB), Z15

        VPSRLVQ         shift3<>+0x00(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPADDQ          Z0, Z14, Z0
        VMOVDQU64       Z0, K3, (DI)

        ADDQ            $24, DI
        RET

// func unpack4Uint64AVX512()
TEXT ·unpack4Uint64AVX512(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask4<>(SB), Z15

        VPSRLVQ         shift4<>+0x00(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPADDQ          Z0, Z14, Z0
        VMOVDQU64       Z0, K4, (DI)

        ADDQ            $32, DI
        RET

// func unpack5Uint64AVX512()
TEXT ·unpack5Uint64AVX512(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask5<>(SB), Z15

        VPSRLVQ         shift5<>+0x00(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPADDQ          Z0, Z14, Z0
        VMOVDQU64       Z0, K5, (DI)

        ADDQ            $40, DI
        RET

// func unpack6Uint64AVX512()
TEXT ·unpack6Uint64AVX512(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask6<>(SB), Z15

        VPSRLVQ         shift6<>+0x00(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPADDQ          Z0, Z14, Z0
        VMOVDQU64       Z0, K6, (DI)

        ADDQ            $48, DI
        RET

// func unpack7Uint64AVX512()
TEXT ·unpack7Uint64AVX512(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask7<>(SB), Z15

        VPSRLVQ         shift7<>+0x00(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPADDQ          Z0, Z14, Z0
        VMOVDQU64       Z0, K7, (DI)

        ADDQ            $56, DI
        RET

// func unpack8Uint64AVX512()
TEXT ·unpack8Uint64AVX512(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask8<>(SB), Z15

        VPSRLVQ         shift8<>+0x00(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPADDQ          Z0, Z14, Z0
        VMOVDQU64       Z0, (DI)

        ADDQ            $64, DI
        RET

// func unpack10Uint64AVX512()
TEXT ·unpack10Uint64AVX512(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask10<>(SB), Z15

        VPSRLVQ         shift10<>+0x00(SB), Z0, Z1
        VPSRLVQ         shift10<>+0x40(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPANDQ          Z1, Z15, Z1
        VPADDQ          Z0, Z14, Z0
        VPADDQ          Z1, Z14, Z1
        VMOVDQU64       Z1, (DI)
        VMOVDQU64       Z0, K2, 64(DI)

        ADDQ            $80, DI
        RET

// func unpack12Uint64AVX512()
TEXT ·unpack12Uint64AVX512(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask12<>(SB), Z15

        VPSRLVQ         shift12<>+0x00(SB), Z0, Z1
        VPSRLVQ         shift12<>+0x40(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPANDQ          Z1, Z15, Z1
        VPADDQ          Z0, Z14, Z0
        VPADDQ          Z1, Z14, Z1
        VMOVDQU64       Z1, (DI)
        VMOVDQU64       Z0, K4, 64(DI)

        ADDQ            $96, DI
        RET

// func unpack15Uint64AVX512()
TEXT ·unpack15Uint64AVX512(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask15<>(SB), Z15

        VPSRLVQ         shift15<>+0x00(SB), Z0, Z1
        VPSRLVQ         shift15<>+0x40(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPANDQ          Z1, Z15, Z1
        VPADDQ          Z0, Z14, Z0
        VPADDQ          Z1, Z14, Z1
        VMOVDQU64       Z1, (DI)
        VMOVDQU64       Z0, K7, 64(DI)

        ADDQ            $120, DI
        RET

// func unpack20Uint64AVX512()
TEXT ·unpack20Uint64AVX512(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask20<>(SB), Z15

        VPSRLVQ         shift20<>+0x00(SB), Z0, Z2
        VPSRLVQ         shift20<>+0x40(SB), Z0, Z1
        VPSRLVQ         shift20<>+0x80(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPANDQ          Z1, Z15, Z1
        VPANDQ          Z2, Z15, Z2
        VPADDQ          Z0, Z14, Z0
        VPADDQ          Z1, Z14, Z1
        VPADDQ          Z2, Z14, Z2
        VMOVDQU64       Z2, (DI)
        VMOVDQU64       Z1, 64(DI)
        VMOVDQU64       Z0, K4, 128(DI)

        ADDQ            $160, DI
        RET

// func unpack30Uint64AVX512()
TEXT ·unpack30Uint64AVX512(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask30<>(SB), Z15

        VPSRLVQ         shift30<>+0x00(SB), Z0, Z3
        VPSRLVQ         shift30<>+0x40(SB), Z0, Z2
        VPSRLVQ         shift30<>+0x80(SB), Z0, Z1
        VPSRLVQ         shift30<>+0xc0(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPANDQ          Z1, Z15, Z1
        VPANDQ          Z2, Z15, Z2
        VPANDQ          Z3, Z15, Z3
        VPADDQ          Z0, Z14, Z0
        VPADDQ          Z1, Z14, Z1
        VPADDQ          Z2, Z14, Z2
        VPADDQ          Z3, Z14, Z3
        VMOVDQU64       Z3, (DI)
        VMOVDQU64       Z2, 64(DI)
        VMOVDQU64       Z1, 128(DI)
        VMOVDQU64       Z0, K6, 192(DI)

        ADDQ            $240, DI
        RET

// func unpack60Uint64AVX512()
TEXT ·unpack60Uint64AVX512(SB), NOSPLIT, $0-0
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask60<>(SB), Z15

        VPSRLVQ         shift60<>+0x00(SB), Z0, Z7
        VPSRLVQ         shift60<>+0x40(SB), Z0, Z6
        VPSRLVQ         shift60<>+0x80(SB), Z0, Z5
        VPSRLVQ         shift60<>+0xc0(SB), Z0, Z4
        VPANDQ          Z7, Z15, Z7
        VPANDQ          Z6, Z15, Z6
        VPANDQ          Z5, Z15, Z5
        VPANDQ          Z4, Z15, Z4
        VPADDQ          Z7, Z14, Z7  // add minv
        VPADDQ          Z6, Z14, Z6
        VPADDQ          Z5, Z14, Z5
        VPADDQ          Z4, Z14, Z4
        VMOVDQU64       Z7, (DI)
        VMOVDQU64       Z6, 64(DI)
        VMOVDQU64       Z5, 128(DI)
        VMOVDQU64       Z4, 192(DI)

        VPSRLVQ         shift60<>+0x100(SB), Z0, Z3
        VPSRLVQ         shift60<>+0x140(SB), Z0, Z2
        VPSRLVQ         shift60<>+0x180(SB), Z0, Z1
        VPSRLVQ         shift60<>+0x1c0(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPANDQ          Z1, Z15, Z1
        VPANDQ          Z2, Z15, Z2
        VPANDQ          Z3, Z15, Z3
        VPADDQ          Z0, Z14, Z0  // add minv
        VPADDQ          Z1, Z14, Z1
        VPADDQ          Z2, Z14, Z2
        VPADDQ          Z3, Z14, Z3
        VMOVDQU64       Z3, 256(DI)
        VMOVDQU64       Z2, 320(DI)
        VMOVDQU64       Z1, 384(DI)
        VMOVDQU64       Z0, K4, 448(DI)

        ADDQ            $480, DI
        RET

// func unpackOnesUint64AVX512()
TEXT ·unpackOnesUint64AVX512(SB), NOSPLIT, $0-0
        MOVQ            $1, AX
        VPBROADCASTQ    AX, Z0          // Z0 = [1,1,...]
        VPADDQ          Z0, Z14, Z0     // add miniv

        VMOVDQU64         Z0, (DI)
        VMOVDQU64         Z0, 64(DI)
        VMOVDQU64         Z0, 128(DI)
        VMOVDQU64         Z0, 192(DI)
        VMOVDQU64         Z0, 256(DI)
        VMOVDQU64         Z0, 320(DI)
        VMOVDQU64         Z0, 384(DI)
        VMOVDQU64         Z0, 448(DI)
        VMOVDQU64         Z0, 512(DI)
        VMOVDQU64         Z0, 576(DI)
        VMOVDQU64         Z0, 640(DI)
        VMOVDQU64         Z0, 704(DI)
        VMOVDQU64         Z0, 768(DI)
        VMOVDQU64         Z0, 832(DI)
        VMOVDQU64         Z0, 896(DI)
        VMOVDQU64         Z0, 960(DI)

        ADDQ            $1024, DI
        RET

// func unpackZerosUint64AVX512()
TEXT ·unpackZerosUint64AVX512(SB), NOSPLIT, $0-0
        //XORQ            AX, AX
        //VPBROADCASTQ    AX, Z0          // Z0 = [0,0,...]

        VMOVDQU64         Z14, (DI)       // Z14 = minv
        VMOVDQU64         Z14, 64(DI)
        VMOVDQU64         Z14, 128(DI)
        VMOVDQU64         Z14, 192(DI)
        VMOVDQU64         Z14, 256(DI)
        VMOVDQU64         Z14, 320(DI)
        VMOVDQU64         Z14, 384(DI)
        VMOVDQU64         Z14, 448(DI)
        VMOVDQU64         Z14, 512(DI)
        VMOVDQU64         Z14, 576(DI)
        VMOVDQU64         Z14, 640(DI)
        VMOVDQU64         Z14, 704(DI)
        VMOVDQU64         Z14, 768(DI)
        VMOVDQU64         Z14, 832(DI)
        VMOVDQU64         Z14, 896(DI)
        VMOVDQU64         Z14, 960(DI)

        ADDQ            $1024, DI
        RET
