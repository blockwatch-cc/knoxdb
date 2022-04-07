// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefanx@blockwatch.cc

#include "textflag.h"
#include "constants_32bit_AVX.h"

TEXT ·init32bitAVX2Call(SB), NOSPLIT, $0-0
        LEAQ            ·unpack32bit240AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>(SB)
        LEAQ            ·unpack32bit120AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>+8(SB)
        LEAQ            ·unpack32bit60AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>+16(SB)
        LEAQ            ·unpack32bit30AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>+24(SB)
        LEAQ            ·unpack32bit20AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>+32(SB)
        LEAQ            ·unpack32bit15AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>+40(SB)
        LEAQ            ·unpack32bit12AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>+48(SB)
        LEAQ            ·unpack32bit10AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>+56(SB)
        LEAQ            ·unpack32bit8AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>+64(SB)
        LEAQ            ·unpack32bit7AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>+72(SB)
        LEAQ            ·unpack32bit6AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>+80(SB)
        LEAQ            ·unpack32bit5AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>+88(SB)
        LEAQ            ·unpack32bit4AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>+96(SB)
        LEAQ            ·unpack32bit3AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>+104(SB)
        LEAQ            ·unpack32bit2AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>+112(SB)
        LEAQ            ·unpack32bit1AVX2Call(SB), DX
        MOVQ            DX, funcTable32bitCall<>+120(SB)

        RET

// func countBytes32bitAVX2Core(src []byte) (count int)
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
TEXT ·countBytes32bitAVX2Core(SB), NOSPLIT, $0-32
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

// func decodeAll32bitAVX2(dst []uint32, src []uint64) (value int)
TEXT ·decodeAll32bitAVX2(SB), NOSPLIT, $0-56
        MOVQ            dst_base(FP), DI
        MOVQ            src_base+24(FP), SI
        MOVQ            src_len+32(FP), BX
        MOVQ            DI, R15                     // save DI

	TESTQ	        BX, BX
	JLE		exit

        LEAQ            funcTable32bitCall<>(SB), R14    // base of function pointer table

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
        SHRQ            $2, DI
        MOVQ            DI, ret+48(FP)
        RET

// func decodeBytesBigEndian32bitAVX2Core(dst []uint64, src []byte) (value int)
TEXT ·decodeBytesBigEndian32bitAVX2Core(SB), NOSPLIT, $0-68
        MOVQ            dst_base(FP), DI
        MOVQ            src_base+24(FP), SI
        MOVQ            src_len+32(FP), BX
        SHRQ            $3, BX
        MOVQ            DI, R15                     // save DI

	TESTQ	        BX, BX
	JLE		exit

        LEAQ            funcTableCall<>(SB), R14    // base of function pointer table

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
        SHRQ            $2, DI
        MOVQ            DI, ret+48(FP)
        RET

// func unpack32bit1AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit1AVX2Call(SB), NOSPLIT, $0-68
        MOVQ            mask1, R8

        ANDQ            (SI), R8            
        MOVD            R8, (DI)

        ADDQ            $4, DI
        RET

// func unpack32bit2AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit2AVX2Call(SB), NOSPLIT, $0-68
        MOVQ            mask2, R8

        MOVQ            (SI), R9
        MOVQ            R9, R10
        SHRQ            $30, R10

        ANDQ            R8, R9            
        ANDQ            R8, R10            
        MOVD            R9, (DI)
        MOVD            R10, 4(DI)


        ADDQ            $8, DI
        RET

// func unpack32bit3AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit3AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), X0
        VPBROADCASTQ    mask3<>(SB), X15
        VMOVDQU         write3mask<>(SB), X14

        VPSRLVQ         shift3<>(SB), X0, X1
        VPSRLVQ         shift3<>+0x10(SB), X0, X0
        VPAND           X0, X15, X0
        VPAND           X1, X15, X1

        VPSLLQ          $32, X0, X0
        VPBLENDW        $(0x33), X1, X0, X0

        VPMASKMOVD      X0, X14, (DI)

        ADDQ            $12, DI
        RET

// func unpack32bit4AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit4AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), X0
        VPBROADCASTQ    mask4<>(SB), X15
//        VMOVDQU         perm4<>(SB), Y13         

        VPSRLVQ         shift4<>(SB), X0, X1
        VPSRLVQ         shift4<>+0x10(SB), X0, X0
        VPAND           X0, X15, X0
        VPAND           X1, X15, X1

        VPSLLQ          $32, X0, X0
        VPBLENDW        $(0x33), X1, X0, X0
//        VPERMD          Y0, Y13, Y0

        VMOVDQU         X0, (DI)

        ADDQ            $16, DI
        RET

// func unpack32bit5AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit5AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask5<>(SB), Y15
        VMOVDQU         write5mask<>(SB), Y14

        VPSRLVQ         shift5<>(SB), Y0, Y1
        VPSRLVQ         shift5<>+0x20(SB), Y0, Y0
        VPAND           Y0, Y15, Y0
        VPAND           Y1, Y15, Y1

        VPSLLQ          $32, Y0, Y0
        VPBLENDW        $(0x33), Y1, Y0, Y0

        VPMASKMOVD      Y0, Y14, (DI)

        ADDQ            $20, DI
        RET

// func unpack32bit6AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit6AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask6<>(SB), Y15
        VMOVDQU         write6mask<>(SB), Y14

        VPSRLVQ         shift6<>(SB), Y0, Y1
        VPSRLVQ         shift6<>+0x20(SB), Y0, Y0
        VPAND           Y0, Y15, Y0
        VPAND           Y1, Y15, Y1

        VPSLLQ          $32, Y0, Y0
        VPBLENDW        $(0x33), Y1, Y0, Y0

        VPMASKMOVD      Y0, Y14, (DI)

        ADDQ            $24, DI
        RET

// func unpack32bit7AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit7AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask7<>(SB), Y15
        VMOVDQU         write7mask<>(SB), Y14

        VPSRLVQ         shift7<>(SB), Y0, Y1
        VPSRLVQ         shift7<>+0x20(SB), Y0, Y0
        VPAND           Y0, Y15, Y0
        VPAND           Y1, Y15, Y1

        VPSLLQ          $32, Y0, Y0
        VPBLENDW        $(0x33), Y1, Y0, Y0

        VPMASKMOVD      Y0, Y14, (DI)

        ADDQ            $28, DI
        RET

// func unpack32bit8AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit8AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask8<>(SB), Y15

        VPSRLVQ         shift8<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift8<>+0x20(SB), Y0, Y0
        VPAND           Y0, Y15, Y0
        VPAND           Y1, Y15, Y1

        VPSLLQ          $32, Y0, Y0
        VPBLENDW        $(0xcc), Y0, Y1, Y0

        VMOVDQU         Y0, (DI)

        ADDQ            $32, DI
        RET

// func unpack32bit10AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit10AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask10<>(SB), Y15

        VPSRLVQ         shift10<>+0x00(SB), Y0, Y1
        VPSRLVQ         shift10<>+0x20(SB), Y0, Y0
        VPAND           Y0, Y15, Y0
        VPAND           Y1, Y15, Y1

        VPSLLQ          $32, Y0, Y0
        VPBLENDW        $(0xcc), Y0, Y1, Y0

        VMOVDQU         Y0, (DI)

        MOVQ            mask10<>(SB), R8
        MOVQ            (SI), R9
        MOVQ            R9, R10
        SHRQ            $48, R9
        SHRQ            $54, R10

        ANDQ            R8, R9            
        ANDQ            R8, R10            
        MOVD            R9, 32(DI)
        MOVD            R10, 36(DI)

        ADDQ            $40, DI
        RET

// func unpack32bit12AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit12AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask12<>(SB), Y15

        VPSRLVQ         shift12<>+0x00(SB), Y0, Y3
        VPSRLVQ         shift12<>+0x20(SB), Y0, Y2
        VPSRLVQ         shift12<>+0x40(SB), X0, X1
        VPSRLVQ         shift12<>+0x50(SB), X0, X0
        VPAND           Y3, Y15, Y3
        VPAND           Y2, Y15, Y2
        VPAND           X1, X15, X1
        VPAND           X0, X15, X0

        VPSLLQ          $32, Y2, Y2
        VPBLENDW        $(0xcc), Y2, Y3, Y2
        VPSLLQ          $32, X0, X0
        VPBLENDW        $(0xcc), X0, X1, X0

        VMOVDQU         Y2, (DI)
        VMOVDQU         X0, 32(DI)

        ADDQ            $48, DI
        RET

// func unpack32bit15AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit15AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask15<>(SB), Y15
        VMOVDQU         write7mask<>(SB), Y14

        VPSRLVQ         shift15<>+0x00(SB), Y0, Y3
        VPSRLVQ         shift15<>+0x20(SB), Y0, Y2
        VPSRLVQ         shift15<>+0x40(SB), Y0, Y1
        VPSRLVQ         shift15<>+0x60(SB), Y0, Y0
        VPAND           Y3, Y15, Y3
        VPAND           Y2, Y15, Y2
        VPAND           Y1, Y15, Y1
        VPAND           Y0, Y15, Y0

        VPSLLQ          $32, Y2, Y2
        VPBLENDW        $(0xcc), Y2, Y3, Y2
        VPSLLQ          $32, Y0, Y0
        VPBLENDW        $(0xcc), Y0, Y1, Y0

        VMOVDQU         Y2, (DI)
        VPMASKMOVD      Y0, Y14, 32(DI)

        ADDQ            $60, DI
        RET

// func unpack32bit20AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit20AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask20<>(SB), Y15

        VPSRLVQ         shift20<>+0x00(SB), Y0, Y5
        VPSRLVQ         shift20<>+0x20(SB), Y0, Y4
        VPSRLVQ         shift20<>+0x40(SB), Y0, Y3
        VPSRLVQ         shift20<>+0x60(SB), Y0, Y2
        VPSRLVQ         shift20<>+0x80(SB), X0, X1
        VPSRLVQ         shift20<>+0x90(SB), X0, X0
        VPAND           Y5, Y15, Y5
        VPAND           Y4, Y15, Y4
        VPAND           Y3, Y15, Y3
        VPAND           Y2, Y15, Y2
        VPAND           X1, X15, X1
        VPAND           X0, X15, X0

        VPSLLQ          $32, Y4, Y4
        VPBLENDW        $(0xcc), Y4, Y5, Y4
        VPSLLQ          $32, Y2, Y2
        VPBLENDW        $(0xcc), Y2, Y3, Y2
        VPSLLQ          $32, X0, X0
        VPBLENDW        $(0xcc), X0, X1, X0

        VMOVDQU         Y4, (DI)
        VMOVDQU         Y2, 32(DI)
        VMOVDQU         X0, 64(DI)

        ADDQ            $80, DI
        RET

// func unpack32bit30AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit30AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask30<>(SB), Y15
        VMOVDQU         write6mask<>(SB), Y14

        VPSRLVQ         shift30<>+0x00(SB), Y0, Y7
        VPSRLVQ         shift30<>+0x20(SB), Y0, Y6
        VPSRLVQ         shift30<>+0x40(SB), Y0, Y5
        VPSRLVQ         shift30<>+0x60(SB), Y0, Y4
        VPSRLVQ         shift30<>+0x80(SB), Y0, Y3
        VPSRLVQ         shift30<>+0xa0(SB), Y0, Y2
        VPSRLVQ         shift30<>+0xc0(SB), Y0, Y1
        VPSRLVQ         shift30<>+0xe0(SB), Y0, Y0
        VPAND           Y7, Y15, Y7
        VPAND           Y6, Y15, Y6
        VPAND           Y5, Y15, Y5
        VPAND           Y4, Y15, Y4
        VPAND           Y3, Y15, Y3
        VPAND           Y2, Y15, Y2
        VPAND           Y1, Y15, Y1
        VPAND           Y0, Y15, Y0

        VPSLLQ          $32, Y6, Y6
        VPBLENDW        $(0xcc), Y6, Y7, Y6
        VPSLLQ          $32, Y4, Y4
        VPBLENDW        $(0xcc), Y4, Y5, Y4
        VPSLLQ          $32, Y2, Y2
        VPBLENDW        $(0xcc), Y2, Y3, Y2
        VPSLLQ          $32, Y0, Y0
        VPBLENDW        $(0xcc), Y0, Y1, Y0

        VMOVDQU         Y6, (DI)
        VMOVDQU         Y4, 32(DI)
        VMOVDQU         Y2, 64(DI)
        VPMASKMOVD      Y0, Y14, 96(DI)

        ADDQ            $120, DI
        RET

// func unpack32bit60AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit60AVX2Call(SB), NOSPLIT, $0-68
        VPBROADCASTQ    (SI), Y0
        VPBROADCASTQ    mask60<>(SB), Y15

        VPSRLVQ         shift60<>+0x00(SB), Y0, Y7
        VPSRLVQ         shift60<>+0x20(SB), Y0, Y6
        VPSRLVQ         shift60<>+0x40(SB), Y0, Y5
        VPSRLVQ         shift60<>+0x60(SB), Y0, Y4
        VPSRLVQ         shift60<>+0x80(SB), Y0, Y3
        VPSRLVQ         shift60<>+0xa0(SB), Y0, Y2
        VPSRLVQ         shift60<>+0xc0(SB), Y0, Y1
        VPSRLVQ         shift60<>+0xe0(SB), Y0, Y8
        VPAND           Y7, Y15, Y7
        VPAND           Y6, Y15, Y6
        VPAND           Y5, Y15, Y5
        VPAND           Y4, Y15, Y4
        VPAND           Y3, Y15, Y3
        VPAND           Y2, Y15, Y2
        VPAND           Y1, Y15, Y1
        VPAND           Y8, Y15, Y8

        VPSLLQ          $32, Y6, Y6
        VPBLENDW        $(0xcc), Y6, Y7, Y6
        VPSLLQ          $32, Y4, Y4
        VPBLENDW        $(0xcc), Y4, Y5, Y4
        VPSLLQ          $32, Y2, Y2
        VPBLENDW        $(0xcc), Y2, Y3, Y2
        VPSLLQ          $32, Y8, Y8
        VPBLENDW        $(0xcc), Y8, Y1, Y8

        VMOVDQU         Y6, (DI)
        VMOVDQU         Y4, 32(DI)
        VMOVDQU         Y2, 64(DI)
        VMOVDQU         Y8, 96(DI)

        VPSRLVQ         shift60<>+0x100(SB), Y0, Y7
        VPSRLVQ         shift60<>+0x120(SB), Y0, Y6
        VPSRLVQ         shift60<>+0x140(SB), Y0, Y5
        VPSRLVQ         shift60<>+0x160(SB), Y0, Y4
        VPSRLVQ         shift60<>+0x180(SB), Y0, Y3
        VPSRLVQ         shift60<>+0x1a0(SB), Y0, Y2
        VPSRLVQ         shift60<>+0x1c0(SB), X0, X1
        VPSRLVQ         shift60<>+0x1d0(SB), X0, X0
        VPAND           Y7, Y15, Y7
        VPAND           Y6, Y15, Y6
        VPAND           Y5, Y15, Y5
        VPAND           Y4, Y15, Y4
        VPAND           Y3, Y15, Y3
        VPAND           Y2, Y15, Y2
        VPAND           X1, X15, X1
        VPAND           X0, X15, X0

        VPSLLQ          $32, Y6, Y6
        VPBLENDW        $(0xcc), Y6, Y7, Y6
        VPSLLQ          $32, Y4, Y4
        VPBLENDW        $(0xcc), Y4, Y5, Y4
        VPSLLQ          $32, Y2, Y2
        VPBLENDW        $(0xcc), Y2, Y3, Y2
        VPSLLQ          $32, X0, X0
        VPBLENDW        $(0xcc), X0, X1, X0

        VMOVDQU         Y6, 128(DI)
        VMOVDQU         Y4, 160(DI)
        VMOVDQU         Y2, 192(DI)
        VMOVDQU         X0, 224(DI)

        ADDQ            $240, DI
        RET

// func unpack32bit120AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit120AVX2Call(SB), NOSPLIT, $0-68
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
        RET

// func unpack32bit240AVX2(v uint64, dst *[240]uint32)
TEXT ·unpack32bit240AVX2Call(SB), NOSPLIT, $0-68
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
        RET
