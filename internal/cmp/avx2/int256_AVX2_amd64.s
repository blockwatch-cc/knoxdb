// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build amd64

#include "textflag.h"
#include "constants_AVX2.h"

// func cmp_i256_eq_x2(src Int256LLSlice, val Int256, bits []byte) int64
//
// input:
//   SI = src_X0_base (1st qwords)
//   DX = src_X1_base (2nd qwords)
//   R14 = src_X2_base (3rd qwords)
//   R15 = src_X3_base (4th qwords)
//   DI = bits_base
//   BX = src_X0_len
//   (Y0,Y13,Y14,Y15) = comparison value for AVX2 (4 qwords)
// internal:
//   AX = intermediate
//   R9 = population count
//   Y11 = permute control mask
//   Y12 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·cmp_i256_eq_x2(SB), NOSPLIT, $0-160
	MOVQ	src_X0_base+0(FP), SI
    MOVQ    src_X1_base+24(FP), DX
    MOVQ    src_X2_base+48(FP), R14
    MOVQ    src_X3_base+72(FP), R15
	MOVQ	src_X0_len+8(FP), BX
	MOVQ	bits_base+128(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD val_0+96(FP), Y0              // load 1st qword of val into AVX2 reg
	VBROADCASTSD val_1+104(FP), Y13            // load 2nd qword of val into AVX2 reg
	VBROADCASTSD val_2+112(FP), Y14            // load 3rd qword of val into AVX2 reg
	VBROADCASTSD val_3+120(FP), Y15            // load 4th qword of val into AVX2 reg

	VMOVDQU		crosslane<>+0x00(SB), Y11   // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y12    // load shuffle control mask
	CMPQ	BX, $31      // slices smaller than 64 byte are handled in small loop
	JBE		prep_scalar

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffe0, CX     // number of values processed in big blocks
    ANDQ    $0x1f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_big:
    VPCMPEQQ	    0(SI), Y0, Y1     
	VPCMPEQQ	    0(DX), Y13, Y2
    VPCMPEQQ	    0(R14), Y14, Y3
	VPCMPEQQ	    0(R15), Y15, Y4
    VPAND       Y1, Y2, Y1
    VPAND       Y3, Y4, Y3
    VPAND       Y1, Y3, Y1

    VPCMPEQQ	    32(SI), Y0, Y2
	VPCMPEQQ	    32(DX), Y13, Y3
    VPCMPEQQ	    32(R14), Y14, Y4
	VPCMPEQQ	    32(R15), Y15, Y5
    VPAND       Y2, Y3, Y2
    VPAND       Y4, Y5, Y4
    VPAND       Y2, Y4, Y2

    VPCMPEQQ	    64(SI), Y0, Y3
	VPCMPEQQ	    64(DX), Y13, Y4
    VPCMPEQQ	    64(R14), Y14, Y5
	VPCMPEQQ	    64(R15), Y15, Y6
    VPAND       Y3, Y4, Y3
    VPAND       Y5, Y6, Y5
    VPAND       Y3, Y5, Y3

    VPCMPEQQ	    96(SI), Y0, Y4
	VPCMPEQQ	    96(DX), Y13, Y5
    VPCMPEQQ	    96(R14), Y14, Y6
	VPCMPEQQ	    96(R15), Y15, Y7
    VPAND       Y4, Y5, Y4
    VPAND       Y6, Y7, Y6
    VPAND       Y4, Y6, Y4

    VPCMPEQQ	    128(SI), Y0, Y5
	VPCMPEQQ	    128(DX), Y13, Y6
    VPCMPEQQ	    128(R14), Y14, Y7
	VPCMPEQQ	    128(R15), Y15, Y8
    VPAND       Y5, Y6, Y5
    VPAND       Y7, Y8, Y7
    VPAND       Y5, Y7, Y5

    VPCMPEQQ	    160(SI), Y0, Y6
	VPCMPEQQ	    160(DX), Y13, Y7
    VPCMPEQQ	    160(R14), Y14, Y8
	VPCMPEQQ	    160(R15), Y15, Y9
    VPAND       Y6, Y7, Y6
    VPAND       Y8, Y9, Y8
    VPAND       Y6, Y8, Y6

    VPCMPEQQ	    192(SI), Y0, Y7
	VPCMPEQQ	    192(DX), Y13, Y8
    VPCMPEQQ	    192(R14), Y14, Y9
	VPCMPEQQ	    192(R15), Y15, Y10
    VPAND       Y7, Y8, Y7
    VPAND       Y9, Y10, Y9
    VPAND       Y7, Y9, Y7

	VPACKSSDW	Y1, Y2, Y1

    VPCMPEQQ	    224(SI), Y0, Y8
	VPCMPEQQ	    224(DX), Y13, Y9
    VPCMPEQQ	    224(R14), Y14, Y10
	VPCMPEQQ	    224(R15), Y15, Y2
    VPAND       Y8, Y9, Y8
    VPAND       Y10, Y2, Y10
    VPAND       Y8, Y10, Y8

	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
  	VPERMD		Y1, Y11, Y1
	VPSHUFB		Y12, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$256, DX
	ADDQ		$256, R14
	ADDQ		$256, R15
	ADDQ		$4, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

prep_scalar: 

done:
	MOVQ	R9, ret+152(FP)
	RET

// func cmp_i256_ne_x2(src Int256LLSlice, val Int256, bits []byte) int64
//
// input:
//   SI = src_X0_base (1st qwords)
//   DX = src_X1_base (2nd qwords)
//   R14 = src_X2_base (3rd qwords)
//   R15 = src_X3_base (4th qwords)
//   DI = bits_base
//   BX = src_X0_len
//   (Y0,Y13,Y14,Y15) = comparison value for AVX2 (4 qwords)
// internal:
//   AX = intermediate
//   R9 = population count
//   Y11 = permute control mask
//   Y12 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·cmp_i256_ne_x2(SB), NOSPLIT, $0-160
	MOVQ	src_X0_base+0(FP), SI
    MOVQ    src_X1_base+24(FP), DX
    MOVQ    src_X2_base+48(FP), R14
    MOVQ    src_X3_base+72(FP), R15
	MOVQ	src_X0_len+8(FP), BX
	MOVQ	bits_base+128(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD val_0+96(FP), Y0              // load 1st qword of val into AVX2 reg
	VBROADCASTSD val_1+104(FP), Y13            // load 2nd qword of val into AVX2 reg
	VBROADCASTSD val_2+112(FP), Y14            // load 3rd qword of val into AVX2 reg
	VBROADCASTSD val_3+120(FP), Y15            // load 4th qword of val into AVX2 reg

	VMOVDQU		crosslane<>+0x00(SB), Y11   // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y12    // load shuffle control mask
	CMPQ	BX, $31      // slices smaller than 64 byte are handled in small loop
	JBE		prep_scalar

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffe0, CX     // number of values processed in big blocks
    ANDQ    $0x1f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_big:
    VPCMPEQQ	    0(SI), Y0, Y1
	VPCMPEQQ	    0(DX), Y13, Y2
    VPCMPEQQ	    0(R14), Y14, Y3
	VPCMPEQQ	    0(R15), Y15, Y4
    VPAND       Y1, Y2, Y1
    VPAND       Y3, Y4, Y3
    VPAND       Y1, Y3, Y1

    VPCMPEQQ	    32(SI), Y0, Y2
	VPCMPEQQ	    32(DX), Y13, Y3
    VPCMPEQQ	    32(R14), Y14, Y4
	VPCMPEQQ	    32(R15), Y15, Y5
    VPAND       Y2, Y3, Y2
    VPAND       Y4, Y5, Y4
    VPAND       Y2, Y4, Y2

    VPCMPEQQ	    64(SI), Y0, Y3
	VPCMPEQQ	    64(DX), Y13, Y4
    VPCMPEQQ	    64(R14), Y14, Y5
	VPCMPEQQ	    64(R15), Y15, Y6
    VPAND       Y3, Y4, Y3
    VPAND       Y5, Y6, Y5
    VPAND       Y3, Y5, Y3

    VPCMPEQQ	    96(SI), Y0, Y4
	VPCMPEQQ	    96(DX), Y13, Y5
    VPCMPEQQ	    96(R14), Y14, Y6
	VPCMPEQQ	    96(R15), Y15, Y7
    VPAND       Y4, Y5, Y4
    VPAND       Y6, Y7, Y6
    VPAND       Y4, Y6, Y4

    VPCMPEQQ	    128(SI), Y0, Y5
	VPCMPEQQ	    128(DX), Y13, Y6
    VPCMPEQQ	    128(R14), Y14, Y7
	VPCMPEQQ	    128(R15), Y15, Y8
    VPAND       Y5, Y6, Y5
    VPAND       Y7, Y8, Y7
    VPAND       Y5, Y7, Y5

    VPCMPEQQ	    160(SI), Y0, Y6
	VPCMPEQQ	    160(DX), Y13, Y7
    VPCMPEQQ	    160(R14), Y14, Y8
	VPCMPEQQ	    160(R15), Y15, Y9
    VPAND       Y6, Y7, Y6
    VPAND       Y8, Y9, Y8
    VPAND       Y6, Y8, Y6

    VPCMPEQQ	    192(SI), Y0, Y7
	VPCMPEQQ	    192(DX), Y13, Y8
    VPCMPEQQ	    192(R14), Y14, Y9
	VPCMPEQQ	    192(R15), Y15, Y10
    VPAND       Y7, Y8, Y7
    VPAND       Y9, Y10, Y9
    VPAND       Y7, Y9, Y7

	VPACKSSDW	Y1, Y2, Y1

    VPCMPEQQ	    224(SI), Y0, Y8
	VPCMPEQQ	    224(DX), Y13, Y9
    VPCMPEQQ	    224(R14), Y14, Y10
	VPCMPEQQ	    224(R15), Y15, Y2
    VPAND       Y8, Y9, Y8
    VPAND       Y10, Y2, Y10
    VPAND       Y8, Y10, Y8

	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
  	VPERMD		Y1, Y11, Y1
	VPSHUFB		Y12, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    NOTL        AX
	MOVL		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$256, DX
	ADDQ		$256, R14
	ADDQ		$256, R15
	ADDQ		$4, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

prep_scalar: 

done:
	MOVQ	R9, ret+152(FP)
	RET

// func cmp_i256_lt_x2(src Int256LLSlice, val Int256, bits []byte) int64
//
// input:
//   SI = src_X0_base (1st qwords)
//   DX = src_X1_base (2nd qwords)
//   R14 = src_X2_base (3rd qwords)
//   R15 = src_X3_base (4th qwords)
//   DI = bits_base
//   BX = src_X0_len
//   (Y0,Y13,Y14,Y15) = comparison value for AVX2 (4 qwords)
// internal:
//   AX = intermediate
//   R9 = population count
//   Y11 = permute control mask
//   Y12 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·cmp_i256_lt_x2(SB), NOSPLIT, $0-160
	MOVQ	src_X0_base+0(FP), SI
    MOVQ    src_X1_base+24(FP), DX
    MOVQ    src_X2_base+48(FP), R14
    MOVQ    src_X3_base+72(FP), R15
	MOVQ	src_X0_len+8(FP), BX
	MOVQ	bits_base+128(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
   	VPCMPEQQ		Y10, Y10, Y10                   // create 0x8000.. mask (for unsigned comparision)
	VPSLLQ			$63, Y10, Y10                   // create 0x8000.. mask

	VBROADCASTSD    val_0+96(FP), Y0              // load 1st qword of val into AVX2 reg (signed)
	VBROADCASTSD    val_1+104(FP), Y13            // load 2nd qword of val into AVX2 reg
    VPXOR           Y10, Y13, Y13               // flip sign bit (for unsigned comparision)
	VBROADCASTSD    val_2+112(FP), Y14            // load 3rd qword of val into AVX2 reg
    VPXOR           Y10, Y14, Y14               // flip sign bit (for unsigned comparision)
	VBROADCASTSD    val_3+120(FP), Y15            // load 4th qword of val into AVX2 reg
    VPXOR           Y10, Y15, Y15               // flip sign bit (for unsigned comparision)

	VMOVDQU		    crosslane<>+0x00(SB), Y11   // load permute control mask
	VMOVDQU		    shuffle64<>+0x00(SB), Y12    // load shuffle control mask



	CMPQ	BX, $31      // slices smaller than 64 byte are handled in small loop
	JBE		prep_scalar

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffe0, CX     // number of values processed in big blocks
    ANDQ    $0x1f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_big:
    VMOVDQU	    0(R15), Y1
    VPXOR       Y10, Y1, Y1               // flip sign bit (for unsigned comparision)
	VMOVDQU	    0(R14), Y6
    VPXOR       Y10, Y6, Y6               // flip sign bit (for unsigned comparision)
    VMOVDQU	    0(DX), Y7
    VPXOR       Y10, Y7, Y7               // flip sign bit (for unsigned comparision)
	VMOVDQU	    0(SI), Y8

	VPCMPGTQ	Y1, Y15, Y1         // Y1 < Y15? 

	VPCMPEQQ	Y6, Y14, Y9         // Y6 == Y14?
	VPCMPGTQ	Y6, Y14, Y6         // Y6 < Y14? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y6, Y1

	VPCMPEQQ	Y7, Y13, Y9         // Y7 == Y13?
	VPCMPGTQ	Y7, Y13, Y7         // Y7 < Y13? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y7, Y1

	VPCMPEQQ	Y8, Y0, Y9         // Y8 == Y0?
	VPCMPGTQ	Y8, Y0, Y8         // Y8 < Y0? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y8, Y1

    VMOVDQU	    32(R15), Y2
    VPXOR       Y10, Y2, Y2            
    VMOVDQU	    32(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    32(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    32(SI), Y8

	VPCMPGTQ	Y2, Y15, Y2      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y6, Y14, Y6
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y6, Y2

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y7, Y13, Y7
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y7, Y2

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y8, Y0, Y8     
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y8, Y2

    VMOVDQU	    64(R15), Y3
    VPXOR       Y10, Y3, Y3            
    VMOVDQU	    64(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    64(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    64(SI), Y8

	VPCMPGTQ	Y3, Y15, Y3      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y6, Y14, Y6
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y6, Y3

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y7, Y13, Y7
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y7, Y3

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y8, Y0, Y8     
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y8, Y3

    VMOVDQU	    96(R15), Y4
    VPXOR       Y10, Y4, Y4            
    VMOVDQU	    96(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    96(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    96(SI), Y8

	VPCMPGTQ	Y4, Y15, Y4      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y6, Y14, Y6
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y6, Y4

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y7, Y13, Y7
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y7, Y4

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y8, Y0, Y8     
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y8, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1

    VMOVDQU	    128(R15), Y5
    VPXOR       Y10, Y5, Y5            
    VMOVDQU	    128(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    128(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    128(SI), Y4

	VPCMPGTQ	Y5, Y15, Y5      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y2, Y14, Y2
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y2, Y5

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y3, Y13, Y3
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y3, Y5

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y4, Y0, Y4     
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y4, Y5

    VMOVDQU	    160(R15), Y6
    VPXOR       Y10, Y6, Y6            
    VMOVDQU	    160(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    160(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    160(SI), Y4

	VPCMPGTQ	Y6, Y15, Y6      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y2, Y14, Y2
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y2, Y6

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y3, Y13, Y3
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y3, Y6

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y4, Y0, Y4     
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y4, Y6

    VMOVDQU	    192(R15), Y7
    VPXOR       Y10, Y7, Y7            
    VMOVDQU	    192(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    192(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    192(SI), Y4

	VPCMPGTQ	Y7, Y15, Y7      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y2, Y14, Y2
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y2, Y7

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y3, Y13, Y3
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y3, Y7

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y4, Y0, Y4     
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y4, Y7

    VMOVDQU	    224(R15), Y8
    VPXOR       Y10, Y8, Y8            
    VMOVDQU	    224(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    224(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    224(SI), Y4

	VPCMPGTQ	Y8, Y15, Y8      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y2, Y14, Y2
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y2, Y8

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y3, Y13, Y3
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y3, Y8

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y4, Y0, Y4     
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y4, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
  	VPERMD		Y1, Y11, Y1
	VPSHUFB		Y12, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$256, DX
	ADDQ		$256, R14
	ADDQ		$256, R15
	ADDQ		$4, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

prep_scalar: 

done:
	MOVQ	R9, ret+152(FP)
	RET

// func cmp_i256_le_x2(src Int256LLSlice, val Int256, bits []byte) int64
//
// input:
//   SI = src_X0_base (1st qwords)
//   DX = src_X1_base (2nd qwords)
//   R14 = src_X2_base (3rd qwords)
//   R15 = src_X3_base (4th qwords)
//   DI = bits_base
//   BX = src_X0_len
//   (Y0,Y13,Y14,Y15) = comparison value for AVX2 (4 qwords)
// internal:
//   AX = intermediate
//   R9 = population count
//   Y11 = permute control mask
//   Y12 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·cmp_i256_le_x2(SB), NOSPLIT, $0-160
	MOVQ	src_X0_base+0(FP), SI
    MOVQ    src_X1_base+24(FP), DX
    MOVQ    src_X2_base+48(FP), R14
    MOVQ    src_X3_base+72(FP), R15
	MOVQ	src_X0_len+8(FP), BX
	MOVQ	bits_base+128(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
   	VPCMPEQQ		Y10, Y10, Y10                   // create 0x8000.. mask (for unsigned comparision)
	VPSLLQ			$63, Y10, Y10                   // create 0x8000.. mask

	VBROADCASTSD    val_0+96(FP), Y0              // load 1st qword of val into AVX2 reg (signed)
	VBROADCASTSD    val_1+104(FP), Y13            // load 2nd qword of val into AVX2 reg
    VPXOR           Y10, Y13, Y13               // flip sign bit (for unsigned comparision)
	VBROADCASTSD    val_2+112(FP), Y14            // load 3rd qword of val into AVX2 reg
    VPXOR           Y10, Y14, Y14               // flip sign bit (for unsigned comparision)
	VBROADCASTSD    val_3+120(FP), Y15            // load 4th qword of val into AVX2 reg
    VPXOR           Y10, Y15, Y15               // flip sign bit (for unsigned comparision)

	VMOVDQU		    crosslane<>+0x00(SB), Y11   // load permute control mask
	VMOVDQU		    shuffle64<>+0x00(SB), Y12    // load shuffle control mask



	CMPQ	BX, $31      // slices smaller than 64 byte are handled in small loop
	JBE		prep_scalar

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffe0, CX     // number of values processed in big blocks
    ANDQ    $0x1f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_big:
    VMOVDQU	    0(R15), Y1
    VPXOR       Y10, Y1, Y1               // flip sign bit (for unsigned comparision)
	VMOVDQU	    0(R14), Y6
    VPXOR       Y10, Y6, Y6               // flip sign bit (for unsigned comparision)
    VMOVDQU	    0(DX), Y7
    VPXOR       Y10, Y7, Y7               // flip sign bit (for unsigned comparision)
	VMOVDQU	    0(SI), Y8

	VPCMPGTQ	Y15, Y1, Y1         // Y1 > Y15? 

	VPCMPEQQ	Y6, Y14, Y9         // Y6 == Y14?
	VPCMPGTQ	Y14, Y6, Y6         // Y6 > Y14? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y6, Y1

	VPCMPEQQ	Y7, Y13, Y9         // Y7 == Y13?
	VPCMPGTQ	Y13, Y7, Y7         // Y7 > Y13? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y7, Y1

	VPCMPEQQ	Y8, Y0, Y9         // Y8 == Y0?
	VPCMPGTQ	Y0, Y8, Y8         // Y8 > Y0? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y8, Y1

    VMOVDQU	    32(R15), Y2
    VPXOR       Y10, Y2, Y2            
    VMOVDQU	    32(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    32(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    32(SI), Y8

	VPCMPGTQ	Y15, Y2, Y2      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y14, Y6, Y6
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y6, Y2

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y13, Y7, Y7
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y7, Y2

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y0, Y8, Y8     
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y8, Y2

    VMOVDQU	    64(R15), Y3
    VPXOR       Y10, Y3, Y3            
    VMOVDQU	    64(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    64(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    64(SI), Y8

	VPCMPGTQ	Y15, Y3, Y3      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y14, Y6, Y6
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y6, Y3

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y13, Y7, Y7
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y7, Y3

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y0, Y8, Y8     
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y8, Y3

    VMOVDQU	    96(R15), Y4
    VPXOR       Y10, Y4, Y4            
    VMOVDQU	    96(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    96(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    96(SI), Y8

	VPCMPGTQ	Y15, Y4, Y4      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y14, Y6, Y6
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y6, Y4

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y13, Y7, Y7
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y7, Y4

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y0, Y8, Y8     
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y8, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1

    VMOVDQU	    128(R15), Y5
    VPXOR       Y10, Y5, Y5            
    VMOVDQU	    128(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    128(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    128(SI), Y4

	VPCMPGTQ	Y15, Y5, Y5      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y14, Y2, Y2
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y2, Y5

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y13, Y3, Y3
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y3, Y5

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y0, Y4, Y4     
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y4, Y5

    VMOVDQU	    160(R15), Y6
    VPXOR       Y10, Y6, Y6            
    VMOVDQU	    160(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    160(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    160(SI), Y4

	VPCMPGTQ	Y15, Y6, Y6      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y14, Y2, Y2
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y2, Y6

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y13, Y3, Y3
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y3, Y6

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y0, Y4, Y4     
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y4, Y6

    VMOVDQU	    192(R15), Y7
    VPXOR       Y10, Y7, Y7            
    VMOVDQU	    192(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    192(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    192(SI), Y4

	VPCMPGTQ	Y15, Y7, Y7      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y14, Y2, Y2
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y2, Y7

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y13, Y3, Y3
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y3, Y7

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y0, Y4, Y4     
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y4, Y7

    VMOVDQU	    224(R15), Y8
    VPXOR       Y10, Y8, Y8            
    VMOVDQU	    224(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    224(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    224(SI), Y4

	VPCMPGTQ	Y15, Y8, Y8      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y14, Y2, Y2
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y2, Y8

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y13, Y3, Y3
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y3, Y8

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y0, Y4, Y4     
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y4, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
  	VPERMD		Y1, Y11, Y1
	VPSHUFB		Y12, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    NOTL        AX
	MOVL		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$256, DX
	ADDQ		$256, R14
	ADDQ		$256, R15
	ADDQ		$4, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

prep_scalar: 

done:
	MOVQ	R9, ret+152(FP)
	RET

// func cmp_i256_gt_x2(src Int256LLSlice, val Int256, bits []byte) int64
//
// input:
//   SI = src_X0_base (1st qwords)
//   DX = src_X1_base (2nd qwords)
//   R14 = src_X2_base (3rd qwords)
//   R15 = src_X3_base (4th qwords)
//   DI = bits_base
//   BX = src_X0_len
//   (Y0,Y13,Y14,Y15) = comparison value for AVX2 (4 qwords)
// internal:
//   AX = intermediate
//   R9 = population count
//   Y11 = permute control mask
//   Y12 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·cmp_i256_gt_x2(SB), NOSPLIT, $0-160
	MOVQ	src_X0_base+0(FP), SI
    MOVQ    src_X1_base+24(FP), DX
    MOVQ    src_X2_base+48(FP), R14
    MOVQ    src_X3_base+72(FP), R15
	MOVQ	src_X0_len+8(FP), BX
	MOVQ	bits_base+128(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
   	VPCMPEQQ		Y10, Y10, Y10                   // create 0x8000.. mask (for unsigned comparision)
	VPSLLQ			$63, Y10, Y10                   // create 0x8000.. mask

	VBROADCASTSD    val_0+96(FP), Y0              // load 1st qword of val into AVX2 reg (signed)
	VBROADCASTSD    val_1+104(FP), Y13            // load 2nd qword of val into AVX2 reg
    VPXOR           Y10, Y13, Y13               // flip sign bit (for unsigned comparision)
	VBROADCASTSD    val_2+112(FP), Y14            // load 3rd qword of val into AVX2 reg
    VPXOR           Y10, Y14, Y14               // flip sign bit (for unsigned comparision)
	VBROADCASTSD    val_3+120(FP), Y15            // load 4th qword of val into AVX2 reg
    VPXOR           Y10, Y15, Y15               // flip sign bit (for unsigned comparision)

	VMOVDQU		    crosslane<>+0x00(SB), Y11   // load permute control mask
	VMOVDQU		    shuffle64<>+0x00(SB), Y12    // load shuffle control mask



	CMPQ	BX, $31      // slices smaller than 64 byte are handled in small loop
	JBE		prep_scalar

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffe0, CX     // number of values processed in big blocks
    ANDQ    $0x1f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_big:
    VMOVDQU	    0(R15), Y1
    VPXOR       Y10, Y1, Y1               // flip sign bit (for unsigned comparision)
	VMOVDQU	    0(R14), Y6
    VPXOR       Y10, Y6, Y6               // flip sign bit (for unsigned comparision)
    VMOVDQU	    0(DX), Y7
    VPXOR       Y10, Y7, Y7               // flip sign bit (for unsigned comparision)
	VMOVDQU	    0(SI), Y8

	VPCMPGTQ	Y15, Y1, Y1         // Y1 > Y15? 

	VPCMPEQQ	Y6, Y14, Y9         // Y6 == Y14?
	VPCMPGTQ	Y14, Y6, Y6         // Y6 > Y14? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y6, Y1

	VPCMPEQQ	Y7, Y13, Y9         // Y7 == Y13?
	VPCMPGTQ	Y13, Y7, Y7         // Y7 > Y13? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y7, Y1

	VPCMPEQQ	Y8, Y0, Y9         // Y8 == Y0?
	VPCMPGTQ	Y0, Y8, Y8         // Y8 > Y0? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y8, Y1

    VMOVDQU	    32(R15), Y2
    VPXOR       Y10, Y2, Y2            
    VMOVDQU	    32(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    32(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    32(SI), Y8

	VPCMPGTQ	Y15, Y2, Y2      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y14, Y6, Y6
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y6, Y2

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y13, Y7, Y7
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y7, Y2

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y0, Y8, Y8     
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y8, Y2

    VMOVDQU	    64(R15), Y3
    VPXOR       Y10, Y3, Y3            
    VMOVDQU	    64(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    64(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    64(SI), Y8

	VPCMPGTQ	Y15, Y3, Y3      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y14, Y6, Y6
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y6, Y3

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y13, Y7, Y7
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y7, Y3

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y0, Y8, Y8     
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y8, Y3

    VMOVDQU	    96(R15), Y4
    VPXOR       Y10, Y4, Y4            
    VMOVDQU	    96(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    96(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    96(SI), Y8

	VPCMPGTQ	Y15, Y4, Y4      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y14, Y6, Y6
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y6, Y4

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y13, Y7, Y7
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y7, Y4

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y0, Y8, Y8     
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y8, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1

    VMOVDQU	    128(R15), Y5
    VPXOR       Y10, Y5, Y5            
    VMOVDQU	    128(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    128(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    128(SI), Y4

	VPCMPGTQ	Y15, Y5, Y5      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y14, Y2, Y2
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y2, Y5

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y13, Y3, Y3
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y3, Y5

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y0, Y4, Y4     
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y4, Y5

    VMOVDQU	    160(R15), Y6
    VPXOR       Y10, Y6, Y6            
    VMOVDQU	    160(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    160(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    160(SI), Y4

	VPCMPGTQ	Y15, Y6, Y6      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y14, Y2, Y2
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y2, Y6

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y13, Y3, Y3
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y3, Y6

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y0, Y4, Y4     
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y4, Y6

    VMOVDQU	    192(R15), Y7
    VPXOR       Y10, Y7, Y7            
    VMOVDQU	    192(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    192(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    192(SI), Y4

	VPCMPGTQ	Y15, Y7, Y7      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y14, Y2, Y2
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y2, Y7

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y13, Y3, Y3
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y3, Y7

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y0, Y4, Y4     
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y4, Y7

    VMOVDQU	    224(R15), Y8
    VPXOR       Y10, Y8, Y8            
    VMOVDQU	    224(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    224(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    224(SI), Y4

	VPCMPGTQ	Y15, Y8, Y8      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y14, Y2, Y2
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y2, Y8

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y13, Y3, Y3
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y3, Y8

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y0, Y4, Y4     
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y4, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
  	VPERMD		Y1, Y11, Y1
	VPSHUFB		Y12, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$256, DX
	ADDQ		$256, R14
	ADDQ		$256, R15
	ADDQ		$4, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

prep_scalar: 

done:
	MOVQ	R9, ret+152(FP)
	RET

// func cmp_i256_ge_x2(src Int256LLSlice, val Int256, bits []byte) int64
//
// input:
//   SI = src_X0_base (1st qwords)
//   DX = src_X1_base (2nd qwords)
//   R14 = src_X2_base (3rd qwords)
//   R15 = src_X3_base (4th qwords)
//   DI = bits_base
//   BX = src_X0_len
//   (Y0,Y13,Y14,Y15) = comparison value for AVX2 (4 qwords)
// internal:
//   AX = intermediate
//   R9 = population count
//   Y11 = permute control mask
//   Y12 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·cmp_i256_ge_x2(SB), NOSPLIT, $0-160
	MOVQ	src_X0_base+0(FP), SI
    MOVQ    src_X1_base+24(FP), DX
    MOVQ    src_X2_base+48(FP), R14
    MOVQ    src_X3_base+72(FP), R15
	MOVQ	src_X0_len+8(FP), BX
	MOVQ	bits_base+128(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
   	VPCMPEQQ		Y10, Y10, Y10                   // create 0x8000.. mask (for unsigned comparision)
	VPSLLQ			$63, Y10, Y10                   // create 0x8000.. mask

	VBROADCASTSD    val_0+96(FP), Y0              // load 1st qword of val into AVX2 reg (signed)
	VBROADCASTSD    val_1+104(FP), Y13            // load 2nd qword of val into AVX2 reg
    VPXOR           Y10, Y13, Y13               // flip sign bit (for unsigned comparision)
	VBROADCASTSD    val_2+112(FP), Y14            // load 3rd qword of val into AVX2 reg
    VPXOR           Y10, Y14, Y14               // flip sign bit (for unsigned comparision)
	VBROADCASTSD    val_3+120(FP), Y15            // load 4th qword of val into AVX2 reg
    VPXOR           Y10, Y15, Y15               // flip sign bit (for unsigned comparision)

	VMOVDQU		    crosslane<>+0x00(SB), Y11   // load permute control mask
	VMOVDQU		    shuffle64<>+0x00(SB), Y12    // load shuffle control mask



	CMPQ	BX, $31      // slices smaller than 64 byte are handled in small loop
	JBE		prep_scalar

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffe0, CX     // number of values processed in big blocks
    ANDQ    $0x1f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_big:
    VMOVDQU	    0(R15), Y1
    VPXOR       Y10, Y1, Y1               // flip sign bit (for unsigned comparision)
	VMOVDQU	    0(R14), Y6
    VPXOR       Y10, Y6, Y6               // flip sign bit (for unsigned comparision)
    VMOVDQU	    0(DX), Y7
    VPXOR       Y10, Y7, Y7               // flip sign bit (for unsigned comparision)
	VMOVDQU	    0(SI), Y8

	VPCMPGTQ	Y1, Y15, Y1         // Y1 < Y15? 

	VPCMPEQQ	Y6, Y14, Y9         // Y6 == Y14?
	VPCMPGTQ	Y6, Y14, Y6         // Y6 < Y14? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y6, Y1

	VPCMPEQQ	Y7, Y13, Y9         // Y7 == Y13?
	VPCMPGTQ	Y7, Y13, Y7         // Y7 < Y13? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y7, Y1

	VPCMPEQQ	Y8, Y0, Y9         // Y8 == Y0?
	VPCMPGTQ	Y8, Y0, Y8         // Y8 < Y0? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y8, Y1

    VMOVDQU	    32(R15), Y2
    VPXOR       Y10, Y2, Y2            
    VMOVDQU	    32(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    32(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    32(SI), Y8

	VPCMPGTQ	Y2, Y15, Y2      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y6, Y14, Y6
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y6, Y2

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y7, Y13, Y7
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y7, Y2

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y8, Y0, Y8     
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y8, Y2

    VMOVDQU	    64(R15), Y3
    VPXOR       Y10, Y3, Y3            
    VMOVDQU	    64(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    64(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    64(SI), Y8

	VPCMPGTQ	Y3, Y15, Y3      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y6, Y14, Y6
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y6, Y3

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y7, Y13, Y7
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y7, Y3

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y8, Y0, Y8     
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y8, Y3

    VMOVDQU	    96(R15), Y4
    VPXOR       Y10, Y4, Y4            
    VMOVDQU	    96(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    96(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    96(SI), Y8

	VPCMPGTQ	Y4, Y15, Y4      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y6, Y14, Y6
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y6, Y4

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y7, Y13, Y7
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y7, Y4

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y8, Y0, Y8     
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y8, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1

    VMOVDQU	    128(R15), Y5
    VPXOR       Y10, Y5, Y5            
    VMOVDQU	    128(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    128(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    128(SI), Y4

	VPCMPGTQ	Y5, Y15, Y5      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y2, Y14, Y2
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y2, Y5

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y3, Y13, Y3
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y3, Y5

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y4, Y0, Y4     
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y4, Y5

    VMOVDQU	    160(R15), Y6
    VPXOR       Y10, Y6, Y6            
    VMOVDQU	    160(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    160(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    160(SI), Y4

	VPCMPGTQ	Y6, Y15, Y6      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y2, Y14, Y2
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y2, Y6

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y3, Y13, Y3
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y3, Y6

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y4, Y0, Y4     
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y4, Y6

    VMOVDQU	    192(R15), Y7
    VPXOR       Y10, Y7, Y7            
    VMOVDQU	    192(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    192(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    192(SI), Y4

	VPCMPGTQ	Y7, Y15, Y7      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y2, Y14, Y2
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y2, Y7

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y3, Y13, Y3
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y3, Y7

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y4, Y0, Y4     
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y4, Y7

    VMOVDQU	    224(R15), Y8
    VPXOR       Y10, Y8, Y8            
    VMOVDQU	    224(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    224(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    224(SI), Y4

	VPCMPGTQ	Y8, Y15, Y8      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y2, Y14, Y2
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y2, Y8

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y3, Y13, Y3
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y3, Y8

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y4, Y0, Y4     
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y4, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
  	VPERMD		Y1, Y11, Y1
	VPSHUFB		Y12, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    NOTL        AX
	MOVL		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$256, DX
	ADDQ		$256, R14
	ADDQ		$256, R15
	ADDQ		$4, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

prep_scalar: 

done:
	MOVQ	R9, ret+152(FP)
	RET

// func cmp_i256_bw_x2(src Int256LLSlice, a, b Int256, bits []byte) int64
//
// input:
//   SI = src_X0_base (1st qwords)
//   DX = src_X1_base (2nd qwords)
//   R14 = src_X2_base (3rd qwords)
//   R15 = src_X3_base (4th qwords)
//   DI = bits_base
//   BX = src_X0_len
//   (Y0,Y13,Y14,Y15) = comparison value for AVX2 (4 qwords)
// internal:
//   AX = intermediate
//   R9 = population count
//   Y11 = permute control mask
//   Y12 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·cmp_i256_bw_x2(SB), NOSPLIT, $0-192
	MOVQ	src_X0_base+0(FP), SI
    MOVQ    src_X1_base+24(FP), DX
    MOVQ    src_X2_base+48(FP), R14
    MOVQ    src_X3_base+72(FP), R15
	MOVQ	src_X0_len+8(FP), BX
	MOVQ	bits_base+160(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
   	VPCMPEQQ		Y10, Y10, Y10                   // create 0x8000.. mask (for unsigned comparision)
	VPSLLQ			$63, Y10, Y10                   // create 0x8000.. mask

	VMOVDQU		    crosslane<>+0x00(SB), Y11   // load permute control mask
	VMOVDQU		    shuffle64<>+0x00(SB), Y12    // load shuffle control mask

	CMPQ	BX, $31      // slices smaller than 64 byte are handled in small loop
	JBE		prep_scalar

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffe0, CX     // number of values processed in big blocks
    ANDQ    $0x1f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_big:
    // first we check for each block if v > b
	VBROADCASTSD    b_0+128(FP), Y0              // load 1st qword of val into AVX2 reg (signed)
	VBROADCASTSD    b_1+136(FP), Y13            // load 2nd qword of val into AVX2 reg
    VPXOR           Y10, Y13, Y13               // flip sign bit (for unsigned comparision)
	VBROADCASTSD    b_2+144(FP), Y14            // load 3rd qword of val into AVX2 reg
    VPXOR           Y10, Y14, Y14               // flip sign bit (for unsigned comparision)
	VBROADCASTSD    b_3+152(FP), Y15            // load 4th qword of val into AVX2 reg
    VPXOR           Y10, Y15, Y15               // flip sign bit (for unsigned comparision)

    VMOVDQU	    0(R15), Y1
    VPXOR       Y10, Y1, Y1               // flip sign bit (for unsigned comparision)
	VMOVDQU	    0(R14), Y6
    VPXOR       Y10, Y6, Y6               // flip sign bit (for unsigned comparision)
    VMOVDQU	    0(DX), Y7
    VPXOR       Y10, Y7, Y7               // flip sign bit (for unsigned comparision)
	VMOVDQU	    0(SI), Y8

	VPCMPGTQ	Y15, Y1, Y1         // Y1 > Y15? 

	VPCMPEQQ	Y6, Y14, Y9         // Y6 == Y14?
	VPCMPGTQ	Y14, Y6, Y6         // Y6 > Y14? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y6, Y1

	VPCMPEQQ	Y7, Y13, Y9         // Y7 == Y13?
	VPCMPGTQ	Y13, Y7, Y7         // Y7 > Y13? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y7, Y1

	VPCMPEQQ	Y8, Y0, Y9         // Y8 == Y0?
	VPCMPGTQ	Y0, Y8, Y8         // Y8 > Y0? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y8, Y1

    VMOVDQU	    32(R15), Y2
    VPXOR       Y10, Y2, Y2            
    VMOVDQU	    32(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    32(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    32(SI), Y8

	VPCMPGTQ	Y15, Y2, Y2      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y14, Y6, Y6
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y6, Y2

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y13, Y7, Y7
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y7, Y2

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y0, Y8, Y8     
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y8, Y2

    VMOVDQU	    64(R15), Y3
    VPXOR       Y10, Y3, Y3            
    VMOVDQU	    64(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    64(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    64(SI), Y8

	VPCMPGTQ	Y15, Y3, Y3      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y14, Y6, Y6
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y6, Y3

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y13, Y7, Y7
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y7, Y3

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y0, Y8, Y8     
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y8, Y3

    VMOVDQU	    96(R15), Y4
    VPXOR       Y10, Y4, Y4            
    VMOVDQU	    96(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    96(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    96(SI), Y8

	VPCMPGTQ	Y15, Y4, Y4      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y14, Y6, Y6
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y6, Y4

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y13, Y7, Y7
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y7, Y4

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y0, Y8, Y8     
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y8, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1

    VMOVDQU	    128(R15), Y5
    VPXOR       Y10, Y5, Y5            
    VMOVDQU	    128(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    128(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    128(SI), Y4

	VPCMPGTQ	Y15, Y5, Y5      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y14, Y2, Y2
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y2, Y5

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y13, Y3, Y3
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y3, Y5

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y0, Y4, Y4     
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y4, Y5

    VMOVDQU	    160(R15), Y6
    VPXOR       Y10, Y6, Y6            
    VMOVDQU	    160(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    160(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    160(SI), Y4

	VPCMPGTQ	Y15, Y6, Y6      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y14, Y2, Y2
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y2, Y6

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y13, Y3, Y3
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y3, Y6

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y0, Y4, Y4     
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y4, Y6

    VMOVDQU	    192(R15), Y7
    VPXOR       Y10, Y7, Y7            
    VMOVDQU	    192(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    192(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    192(SI), Y4

	VPCMPGTQ	Y15, Y7, Y7      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y14, Y2, Y2
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y2, Y7

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y13, Y3, Y3
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y3, Y7

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y0, Y4, Y4     
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y4, Y7

    VMOVDQU	    224(R15), Y8
    VPXOR       Y10, Y8, Y8            
    VMOVDQU	    224(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    224(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    224(SI), Y4

	VPCMPGTQ	Y15, Y8, Y8      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y14, Y2, Y2
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y2, Y8

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y13, Y3, Y3
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y3, Y8

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y0, Y4, Y4     
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y4, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
  	VPERMD		Y1, Y11, Y1
	VPSHUFB		Y12, Y1, Y1

	VPMOVMSKB	Y1, R10      // move per byte MSBs into packed bitmask to r32 or r64

    // second we check for each block if v < a
	VBROADCASTSD    a_0+96(FP), Y0              // load 1st qword of val into AVX2 reg (signed)
	VBROADCASTSD    a_1+104(FP), Y13            // load 2nd qword of val into AVX2 reg
    VPXOR           Y10, Y13, Y13               // flip sign bit (for unsigned comparision)
	VBROADCASTSD    a_2+112(FP), Y14            // load 3rd qword of val into AVX2 reg
    VPXOR           Y10, Y14, Y14               // flip sign bit (for unsigned comparision)
	VBROADCASTSD    a_3+120(FP), Y15            // load 4th qword of val into AVX2 reg
    VPXOR           Y10, Y15, Y15               // flip sign bit (for unsigned comparision)

    VMOVDQU	    0(R15), Y1
    VPXOR       Y10, Y1, Y1               // flip sign bit (for unsigned comparision)
	VMOVDQU	    0(R14), Y6
    VPXOR       Y10, Y6, Y6               // flip sign bit (for unsigned comparision)
    VMOVDQU	    0(DX), Y7
    VPXOR       Y10, Y7, Y7               // flip sign bit (for unsigned comparision)
	VMOVDQU	    0(SI), Y8

	VPCMPGTQ	Y1, Y15, Y1         // Y1 < Y15? 

	VPCMPEQQ	Y6, Y14, Y9         // Y6 == Y14?
	VPCMPGTQ	Y6, Y14, Y6         // Y6 < Y14? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y6, Y1

	VPCMPEQQ	Y7, Y13, Y9         // Y7 == Y13?
	VPCMPGTQ	Y7, Y13, Y7         // Y7 < Y13? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y7, Y1

	VPCMPEQQ	Y8, Y0, Y9         // Y8 == Y0?
	VPCMPGTQ	Y8, Y0, Y8         // Y8 < Y0? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y8, Y1

    VMOVDQU	    32(R15), Y2
    VPXOR       Y10, Y2, Y2            
    VMOVDQU	    32(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    32(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    32(SI), Y8

	VPCMPGTQ	Y2, Y15, Y2      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y6, Y14, Y6
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y6, Y2

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y7, Y13, Y7
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y7, Y2

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y8, Y0, Y8     
    VPAND       Y2, Y9, Y2
    VPOR        Y2, Y8, Y2

    VMOVDQU	    64(R15), Y3
    VPXOR       Y10, Y3, Y3            
    VMOVDQU	    64(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    64(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    64(SI), Y8

	VPCMPGTQ	Y3, Y15, Y3      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y6, Y14, Y6
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y6, Y3

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y7, Y13, Y7
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y7, Y3

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y8, Y0, Y8     
    VPAND       Y3, Y9, Y3
    VPOR        Y3, Y8, Y3

    VMOVDQU	    96(R15), Y4
    VPXOR       Y10, Y4, Y4            
    VMOVDQU	    96(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    96(DX), Y7
    VPXOR       Y10, Y7, Y7         
	VMOVDQU	    96(SI), Y8

	VPCMPGTQ	Y4, Y15, Y4      

	VPCMPEQQ	Y6, Y14, Y9        
	VPCMPGTQ	Y6, Y14, Y6
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y6, Y4

	VPCMPEQQ	Y7, Y13, Y9
	VPCMPGTQ	Y7, Y13, Y7
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y7, Y4

	VPCMPEQQ	Y8, Y0, Y9
	VPCMPGTQ	Y8, Y0, Y8     
    VPAND       Y4, Y9, Y4
    VPOR        Y4, Y8, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1

    VMOVDQU	    128(R15), Y5
    VPXOR       Y10, Y5, Y5            
    VMOVDQU	    128(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    128(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    128(SI), Y4

	VPCMPGTQ	Y5, Y15, Y5      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y2, Y14, Y2
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y2, Y5

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y3, Y13, Y3
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y3, Y5

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y4, Y0, Y4     
    VPAND       Y5, Y9, Y5
    VPOR        Y5, Y4, Y5

    VMOVDQU	    160(R15), Y6
    VPXOR       Y10, Y6, Y6            
    VMOVDQU	    160(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    160(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    160(SI), Y4

	VPCMPGTQ	Y6, Y15, Y6      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y2, Y14, Y2
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y2, Y6

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y3, Y13, Y3
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y3, Y6

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y4, Y0, Y4     
    VPAND       Y6, Y9, Y6
    VPOR        Y6, Y4, Y6

    VMOVDQU	    192(R15), Y7
    VPXOR       Y10, Y7, Y7            
    VMOVDQU	    192(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    192(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    192(SI), Y4

	VPCMPGTQ	Y7, Y15, Y7      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y2, Y14, Y2
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y2, Y7

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y3, Y13, Y3
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y3, Y7

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y4, Y0, Y4     
    VPAND       Y7, Y9, Y7
    VPOR        Y7, Y4, Y7

    VMOVDQU	    224(R15), Y8
    VPXOR       Y10, Y8, Y8            
    VMOVDQU	    224(R14), Y2
    VPXOR       Y10, Y2, Y2          
    VMOVDQU	    224(DX), Y3
    VPXOR       Y10, Y3, Y3         
	VMOVDQU	    224(SI), Y4

	VPCMPGTQ	Y8, Y15, Y8      

	VPCMPEQQ	Y2, Y14, Y9        
	VPCMPGTQ	Y2, Y14, Y2
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y2, Y8

	VPCMPEQQ	Y3, Y13, Y9
	VPCMPGTQ	Y3, Y13, Y3
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y3, Y8

	VPCMPEQQ	Y4, Y0, Y9
	VPCMPGTQ	Y4, Y0, Y4     
    VPAND       Y8, Y9, Y8
    VPOR        Y8, Y4, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
  	VPERMD		Y1, Y11, Y1
	VPSHUFB		Y12, Y1, Y1

	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    ORL         R10, AX      // v < a or v > b
    NOTL        AX          // v >= a and v <= b
	MOVL		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$256, DX
	ADDQ		$256, R14
	ADDQ		$256, R15
	ADDQ		$4, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

prep_scalar: 

done:
	MOVQ	R9, ret+184(FP)
	RET

