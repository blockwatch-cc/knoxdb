// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefanx@blockwatch.cc

#include "textflag.h"
#include "constants_AVX512.h"

TEXT ·initAVX512Call(SB), NOSPLIT, $0-0
        LEAQ            ·unpack240AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>(SB)
        LEAQ            ·unpack120AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>+8(SB)
        LEAQ            ·unpack60AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>+16(SB)
        LEAQ            ·unpack30AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>+24(SB)
        LEAQ            ·unpack20AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>+32(SB)
        LEAQ            ·unpack15AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>+40(SB)
        LEAQ            ·unpack12AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>+48(SB)
        LEAQ            ·unpack10AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>+56(SB)
        LEAQ            ·unpack8AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>+64(SB)
        LEAQ            ·unpack7AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>+72(SB)
        LEAQ            ·unpack6AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>+80(SB)
        LEAQ            ·unpack5AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>+88(SB)
        LEAQ            ·unpack4AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>+96(SB)
        LEAQ            ·unpack3AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>+104(SB)
        LEAQ            ·unpack2AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>+112(SB)
        LEAQ            ·unpack1AVX512Call(SB), DX
        MOVQ            DX, funcTableCall<>+120(SB)

        RET

// func countBytesAVX512Core(src []byte) (count int)
//
// input:
//   SI = src_base
//   BX = src_len
// internal:
//   Y15 = LUT selector -> number of values
//   Y14 = selector mask for using 4x64bit vector
//   Y13 = selector mask for using 8x32bit vector
//   Y12, Y11 = sum registers
//   Y0-Y3 = vector data
//   BX = remaining bytes
//   CX = loop counter 
TEXT ·countBytesAVX512Core(SB), NOSPLIT, $0-32
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX

        XORQ    AX, AX
        CMPQ    BX, $0
	JE	done

prep_avx:
        VPXOR           Y12, Y12, Y12                   // set sum to zero
        VPXOR           Y11, Y11, Y11                   // set sum to zero
	VPBROADCASTQ    sel_mask64<>+0x00(SB), Y14      // load selector mask
	VPBROADCASTD    sel_mask32<>+0x00(SB), Y13      // load selector mask
	VMOVDQU		sel_LUT<>+0x00(SB), Y15         // load LUT

prep_big:
        MOVQ    BX, CX
        ANDQ    $0x7f, BX                   // number of bytes left
        SHRQ    $7, CX                      // number of runs of big loop
        JZ      prep_small

loop_big:
        // for big endian
        VPAND           (SI), Y14, Y0   // determine selector
        VPAND           32(SI), Y14, Y1   // determine selector
        VPAND           64(SI), Y14, Y2   // determine selector
        VPAND           96(SI), Y14, Y3   // determine selector
	VPSRLQ	        $4, Y0, Y0      // determine selector
	VPSRLQ	        $4, Y1, Y1      // determine selector
	VPSRLQ	        $4, Y2, Y2      // determine selector
	VPSRLQ	        $4, Y3, Y3      // determine selector

/*      // for little endian 
	VPSRLQ	        $60, Y0, Y0      // determine selector
	VPSRLQ	        $60, Y1, Y1      // determine selector
	VPSRLQ	        $60, Y2, Y2      // determine selector
	VPSRLQ	        $60, Y3, Y3     // determine selector
*/
	VPSLLQ	        $32, Y1, Y1      // combine selector vectors
        VPOR            Y1, Y0, Y0
	VPSLLQ	        $32, Y3, Y3      // combine selector vectors
        VPOR            Y3, Y2, Y2

	VPSHUFB	        Y0, Y15, Y0     // look up number of values
        VPAND           Y0, Y13, Y0     // clear unused values
	VPSHUFB	        Y2, Y15, Y2     // look up number of values
        VPAND           Y2, Y13, Y2     // clear unused values

        VPADDD          Y12, Y0, Y12    // add number of values
        VPADDD          Y11, Y2, Y11    // add number of values

	ADDQ		$128, SI
        SUBQ            $1, CX
	JZ	 	exit_big
	JMP	 	loop_big

exit_big:
        VPADDD          Y11, Y12, Y12

prep_small:
        MOVQ    BX, CX
        ANDQ    $0x1f, BX               // number of bytes left
        SHRQ    $5, CX                  // number of runs of small loop
        JZ      exit_small

loop_small:
        // for big endian
        VPAND           (SI), Y14, Y0   // determine selector
	VPSRLQ	        $4, Y0, Y0      // determine selector

/*      // for little endian 
	VPSRLQ	        $60, Y0, Y0      // determine selector
*/
	VPSHUFB	        Y0, Y15, Y0     // look up number of values
        VPAND           Y0, Y14, Y0     // clear unused values

        VPADDD          Y12, Y0, Y12    // add number of values

	ADDQ		$32, SI
        SUBQ            $1, CX
	JZ	 	exit_small
	JMP	 	loop_small

exit_small:
        SHRQ            $3, BX                  // number of 64bit words
        VMOVDQU         countdown<>(SB), Y2
        MOVQ            BX, X0
        VPBROADCASTQ    X0, Y1                  // broadcast BX
        VPCMPGTQ        Y2, Y1, Y1              // mask remaining values

        VPMASKMOVQ      (SI), Y1, Y0            // load remaining values

        // for big endian
        VPAND           Y0, Y14, Y0     // determine selector
	VPSRLQ	        $4, Y0, Y0      // determine selector

/*      // for little endian 
	VPSRLQ	        $60, Y0, Y0      // determine selector
*/
	VPSHUFB	        Y0, Y15, Y0     // look up number of values
        VPAND           Y0, Y14, Y0     // clear unused values
        VPAND           Y0, Y1, Y0      // cut vector

        VPADDD          Y12, Y0, Y12    // add number of values

        // finish: add all values in Y12
        VPHADDD         Y12, Y12, Y12
        VPHADDD         Y12, Y12, Y12
        VEXTRACTI128    $1, Y12, X0
        VPADDD          X0, X12, X12

	VMOVD	        X12, AX
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
        MOVQ            AX, ret+24(FP)
	RET

// func decodeAllAVX512Call(dst, src []uint64) (value int)
TEXT ·decodeAllAVX512Call(SB), NOSPLIT, $0-68
        MOVQ            dst_base(FP), DI
        MOVQ            src_base+24(FP), SI
        MOVQ            src_len+32(FP), BX
        MOVQ            DI, R15                     // save DI

	TESTQ	        BX, BX
	JLE		exit

        LEAQ            funcTableCall<>(SB), R14    // base of function pointer table
        VMOVDQU         write3mask<>(SB), Y14
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
        MOVQ            DI, ret+48(FP)
        RET

// func decodeBytesBigEndianAVX512Core(dst []uint64, src []byte) (value int)
TEXT ·decodeBytesBigEndianAVX512Core(SB), NOSPLIT, $0-68
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

// func unpack1AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack1AVX512Call(SB), NOSPLIT, $0-68
        MOVQ            mask1, R8

        ANDQ            (SI), R8            
        MOVQ            R8, (DI)

        ADDQ            $8, DI
        RET

// func unpack2AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack2AVX512Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask2<>(SB), Z15

        VPSRLVQ         shift2<>+0x00(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VMOVDQU64       Z0, K2, (DI)

        ADDQ            $16, DI
        RET

// func unpack3AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack3AVX512Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask3<>(SB), Z15

        VPSRLVQ         shift3<>+0x00(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VMOVDQU64       Z0, K3, (DI)

        ADDQ            $24, DI
        RET

// func unpack4AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack4AVX512Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask4<>(SB), Z15

        VPSRLVQ         shift4<>+0x00(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VMOVDQU64       Z0, K4, (DI)

        ADDQ            $32, DI
        RET

// func unpack5AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack5AVX512Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask5<>(SB), Z15

        VPSRLVQ         shift5<>+0x00(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VMOVDQU64       Z0, K5, (DI)

        ADDQ            $40, DI
        RET

// func unpack6AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack6AVX512Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask6<>(SB), Z15

        VPSRLVQ         shift6<>+0x00(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VMOVDQU64       Z0, K6, (DI)

        ADDQ            $48, DI
        RET

// func unpack7AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack7AVX512Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask7<>(SB), Z15

        VPSRLVQ         shift7<>+0x00(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VMOVDQU64       Z0, K7, (DI)

        ADDQ            $56, DI
        RET

// func unpack8AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack8AVX512Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask8<>(SB), Z15

        VPSRLVQ         shift8<>+0x00(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VMOVDQU64       Z0, (DI)

        ADDQ            $64, DI
        RET

// func unpack10AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack10AVX512Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask10<>(SB), Z15

        VPSRLVQ         shift10<>+0x00(SB), Z0, Z1
        VPSRLVQ         shift10<>+0x40(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPANDQ          Z1, Z15, Z1
        VMOVDQU64       Z1, (DI)
        VMOVDQU64       Z0, K2, 64(DI)

        ADDQ            $80, DI
        RET

// func unpack12AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack12AVX512Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask12<>(SB), Z15

        VPSRLVQ         shift12<>+0x00(SB), Z0, Z1
        VPSRLVQ         shift12<>+0x40(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPANDQ          Z1, Z15, Z1
        VMOVDQU64       Z1, (DI)
        VMOVDQU64       Z0, K4, 64(DI)

        ADDQ            $96, DI
        RET

// func unpack15AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack15AVX512Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask15<>(SB), Z15

        VPSRLVQ         shift15<>+0x00(SB), Z0, Z1
        VPSRLVQ         shift15<>+0x40(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPANDQ          Z1, Z15, Z1
        VMOVDQU64       Z1, (DI)
        VMOVDQU64       Z0, K7, 64(DI)

        ADDQ            $120, DI
        RET

// func unpack20AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack20AVX512Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Z0
        VPBROADCASTQ    mask20<>(SB), Z15

        VPSRLVQ         shift20<>+0x00(SB), Z0, Z2
        VPSRLVQ         shift20<>+0x40(SB), Z0, Z1
        VPSRLVQ         shift20<>+0x80(SB), Z0, Z0
        VPANDQ          Z0, Z15, Z0
        VPANDQ          Z1, Z15, Z1
        VPANDQ          Z2, Z15, Z2
        VMOVDQU64       Z2, (DI)
        VMOVDQU64       Z1, 64(DI)
        VMOVDQU64       Z0, K4, 128(DI)

        ADDQ            $160, DI
        RET

// func unpack30AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack30AVX512Call(SB), NOSPLIT, $0-68
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
        VMOVDQU64       Z3, (DI)
        VMOVDQU64       Z2, 64(DI)
        VMOVDQU64       Z1, 128(DI)
        VMOVDQU64       Z0, K6, 192(DI)

        ADDQ            $240, DI
        RET

// func unpack60AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack60AVX512Call(SB), NOSPLIT, $0-68
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
        VMOVDQU64       Z3, 256(DI)
        VMOVDQU64       Z2, 320(DI)
        VMOVDQU64       Z1, 384(DI)
        VMOVDQU64       Z0, K4, 448(DI)

        ADDQ            $480, DI
        RET

// func unpack120AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack120AVX512Call(SB), NOSPLIT, $0-68
        MOVQ            $1, AX
        VPBROADCASTQ    AX, Z0          // Z0 = [1,1,...]

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

        ADDQ            $960, DI
        RET

// func unpack240AVX512(v uint64, dst *[240]uint64)
TEXT ·unpack240AVX512Call(SB), NOSPLIT, $0-68
        MOVQ            $1, AX
        VPBROADCASTQ    AX, Z0          // Z0 = [1,1,...]

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
        VMOVDQU64         Z0, 1024(DI)
        VMOVDQU64         Z0, 1088(DI)
        VMOVDQU64         Z0, 1152(DI)
        VMOVDQU64         Z0, 1216(DI)
        VMOVDQU64         Z0, 1280(DI)
        VMOVDQU64         Z0, 1344(DI)
        VMOVDQU64         Z0, 1408(DI)
        VMOVDQU64         Z0, 1472(DI)
        VMOVDQU64         Z0, 1536(DI)
        VMOVDQU64         Z0, 1600(DI)
        VMOVDQU64         Z0, 1664(DI)
        VMOVDQU64         Z0, 1728(DI)
        VMOVDQU64         Z0, 1792(DI)
        VMOVDQU64         Z0, 1856(DI)

        ADDQ            $1920, DI
        RET
