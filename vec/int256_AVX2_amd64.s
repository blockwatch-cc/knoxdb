// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants_AVX2.h"

// func matchInt256EqualAVX2Core(src Int256LLSlice, val Int256, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·matchInt256EqualAVX2Core(SB), NOSPLIT, $0-160
	MOVQ	src0_base+0(FP), SI
    MOVQ    src1_base+24(FP), BP
    MOVQ    src2_base+48(FP), R14
    MOVQ    src3_base+72(FP), R15
	MOVQ	src_len+8(FP), BX
	MOVQ	dest_base+128(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD val+96(FP), Y0              // load 1st qword of val into AVX2 reg
	VBROADCASTSD val+104(FP), Y13            // load 2nd qword of val into AVX2 reg
	VBROADCASTSD val+112(FP), Y14            // load 3rd qword of val into AVX2 reg
	VBROADCASTSD val+120(FP), Y15            // load 4th qword of val into AVX2 reg

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
	VPCMPEQQ	    0(BP), Y13, Y2
    VPCMPEQQ	    0(R14), Y14, Y3
	VPCMPEQQ	    0(R15), Y15, Y4
    VPAND       Y1, Y2, Y1
    VPAND       Y3, Y4, Y3
    VPAND       Y1, Y3, Y1

    VPCMPEQQ	    32(SI), Y0, Y2
	VPCMPEQQ	    32(BP), Y13, Y3
    VPCMPEQQ	    32(R14), Y14, Y4
	VPCMPEQQ	    32(R15), Y15, Y5
    VPAND       Y2, Y3, Y2
    VPAND       Y4, Y5, Y4
    VPAND       Y2, Y4, Y2

    VPCMPEQQ	    64(SI), Y0, Y3
	VPCMPEQQ	    64(BP), Y13, Y4
    VPCMPEQQ	    64(R14), Y14, Y5
	VPCMPEQQ	    64(R15), Y15, Y6
    VPAND       Y3, Y4, Y3
    VPAND       Y5, Y6, Y5
    VPAND       Y3, Y5, Y3

    VPCMPEQQ	    96(SI), Y0, Y4
	VPCMPEQQ	    96(BP), Y13, Y5
    VPCMPEQQ	    96(R14), Y14, Y6
	VPCMPEQQ	    96(R15), Y15, Y7
    VPAND       Y4, Y5, Y4
    VPAND       Y6, Y7, Y6
    VPAND       Y4, Y6, Y4

    VPCMPEQQ	    128(SI), Y0, Y5
	VPCMPEQQ	    128(BP), Y13, Y6
    VPCMPEQQ	    128(R14), Y14, Y7
	VPCMPEQQ	    128(R15), Y15, Y8
    VPAND       Y5, Y6, Y5
    VPAND       Y7, Y8, Y7
    VPAND       Y5, Y7, Y5

    VPCMPEQQ	    160(SI), Y0, Y6
	VPCMPEQQ	    160(BP), Y13, Y7
    VPCMPEQQ	    160(R14), Y14, Y8
	VPCMPEQQ	    160(R15), Y15, Y9
    VPAND       Y6, Y7, Y6
    VPAND       Y8, Y9, Y8
    VPAND       Y6, Y8, Y6

    VPCMPEQQ	    192(SI), Y0, Y7
	VPCMPEQQ	    192(BP), Y13, Y8
    VPCMPEQQ	    192(R14), Y14, Y9
	VPCMPEQQ	    192(R15), Y15, Y10
    VPAND       Y7, Y8, Y7
    VPAND       Y9, Y10, Y9
    VPAND       Y7, Y9, Y7

	VPACKSSDW	Y1, Y2, Y1

    VPCMPEQQ	    224(SI), Y0, Y8
	VPCMPEQQ	    224(BP), Y13, Y9
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
	ADDQ		$256, BP
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

// func matchInt256LessThanAVX2Core(src Int256LLSlice, val Int256, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·matchInt256LessThanAVX2Core(SB), NOSPLIT, $0-160
	MOVQ	src0_base+0(FP), SI
    MOVQ    src1_base+24(FP), BP
    MOVQ    src2_base+48(FP), R14
    MOVQ    src3_base+72(FP), R15
	MOVQ	src_len+8(FP), BX
	MOVQ	dest_base+128(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
   	VPCMPEQQ		Y10, Y10, Y10                   // create 0x8000.. mask (for unsigned comparision)
	VPSLLQ			$63, Y10, Y10                   // create 0x8000.. mask

	VBROADCASTSD    val+96(FP), Y0              // load 1st qword of val into AVX2 reg (signed)
	VBROADCASTSD    val+104(FP), Y13            // load 2nd qword of val into AVX2 reg
    VPXOR           Y10, Y13, Y13               // flip sign bit (for unsigned comparision)
	VBROADCASTSD    val+112(FP), Y14            // load 3rd qword of val into AVX2 reg
    VPXOR           Y10, Y14, Y14               // flip sign bit (for unsigned comparision)
	VBROADCASTSD    val+120(FP), Y15            // load 4th qword of val into AVX2 reg
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
    VMOVDQU	    0(BP), Y7
    VPXOR       Y10, Y7, Y7               // flip sign bit (for unsigned comparision)
	VMOVDQU	    0(SI), Y8

	VPCMPGTQ	Y1, Y15, Y1         // Y1 < Y15? 

	VPCMPEQQ	Y6, Y14, Y9         // Y2 == Y14?
	VPCMPGTQ	Y6, Y14, Y6         // Y2 < Y14? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y6, Y1

	VPCMPEQQ	Y7, Y13, Y9         // Y3 == Y13?
	VPCMPGTQ	Y7, Y13, Y7         // Y3 < Y13? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y7, Y1

	VPCMPEQQ	Y8, Y0, Y9         // Y4 == Y0?
	VPCMPGTQ	Y8, Y0, Y8         // Y4 < Y0? 
    VPAND       Y1, Y9, Y1
    VPOR        Y1, Y8, Y1

    VMOVDQU	    32(R15), Y2
    VPXOR       Y10, Y2, Y2            
    VMOVDQU	    32(R14), Y6
    VPXOR       Y10, Y6, Y6          
    VMOVDQU	    32(BP), Y7
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
    VMOVDQU	    64(BP), Y7
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
    VMOVDQU	    96(BP), Y7
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
    VMOVDQU	    128(BP), Y3
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
    VMOVDQU	    160(BP), Y3
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
    VMOVDQU	    192(BP), Y3
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
    VMOVDQU	    224(BP), Y3
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
	ADDQ		$256, BP
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

/*
// func matchInt256LessThanAVX2(src []Int256, val Int256, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·matchInt256LessThanAVX2(SB), NOSPLIT, $0-72
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	dest_base+40(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD val+24(FP), Y0            // load upper qword of val into AVX2 reg
   	VPCMPEQQ		Y14, Y14, Y14                   // create 0x8000.. mask
	VPSLLQ			$63, Y14, Y14                   // create 0x8000.. mask
	VBROADCASTSD val+32(FP), Y15                    // load lower qword of val into AVX2 reg
	VPXOR			Y14, Y15, Y15                     // flip sign bit
	VMOVDQU		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU		shuffle128<>+0x00(SB), Y10    // load shuffle control mask
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
    VMOVDQU	     0(SI), Y1
	VMOVDQU	    32(SI), Y11
    VPSRLDQ     $8, Y1, Y12
    VPSLLDQ     $8, Y11, Y13
    VPBLENDW    $0x0f, Y1, Y13, Y1
    VPBLENDW    $0xf0, Y11, Y12, Y11

	VPCMPGTQ	Y1, Y0, Y12
	VPCMPEQQ	Y1, Y0, Y1
	VPXOR		Y14, Y11, Y11 
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y1,Y11, Y1
    VPOR        Y1, Y12, Y1

	VMOVDQU	    64(SI), Y2
	VMOVDQU	    96(SI), Y11
    VPSRLDQ     $8, Y2, Y12
    VPSLLDQ     $8, Y11, Y13
    VPBLENDW    $0x0f, Y2, Y13, Y2
    VPBLENDW    $0xf0, Y11, Y12, Y11

	VPCMPGTQ	Y2, Y0, Y12
	VPCMPEQQ	Y2, Y0, Y2
	VPXOR		Y14, Y11, Y11 
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y2,Y11, Y2
    VPOR        Y2, Y12, Y2

	VMOVDQU	   128(SI), Y3
	VMOVDQU	   160(SI), Y11
    VPSRLDQ     $8, Y3, Y12
    VPSLLDQ     $8, Y11, Y13
    VPBLENDW    $0x0f, Y3, Y13, Y3
    VPBLENDW    $0xf0, Y11, Y12, Y11

	VPCMPGTQ	Y3, Y0, Y12
	VPCMPEQQ	Y3, Y0, Y3
	VPXOR		Y14, Y11, Y11 
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y3,Y11, Y3
    VPOR        Y3, Y12, Y3

	VMOVDQU	   192(SI), Y4
	VMOVDQU	   224(SI), Y11
    VPSRLDQ     $8, Y4, Y12
    VPSLLDQ     $8, Y11, Y13
    VPBLENDW    $0x0f, Y4, Y13, Y4
    VPBLENDW    $0xf0, Y11, Y12, Y11

	VPCMPGTQ	Y4, Y0, Y12
	VPCMPEQQ	Y4, Y0, Y4
	VPXOR		Y14, Y11, Y11 
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y4,Y11, Y4
    VPOR        Y4, Y12, Y4

	VMOVDQU	   256(SI), Y5
	VMOVDQU	   288(SI), Y11
    VPSRLDQ     $8, Y5, Y12
    VPSLLDQ     $8, Y11, Y13
    VPBLENDW    $0x0f, Y5, Y13, Y5
    VPBLENDW    $0xf0, Y11, Y12, Y11

	VPCMPGTQ	Y5, Y0, Y12
	VPCMPEQQ	Y5, Y0, Y5
	VPXOR		Y14, Y11, Y11 
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y5,Y11, Y5
    VPOR        Y5, Y12, Y5

	VMOVDQU	   320(SI), Y6
	VMOVDQU	   352(SI), Y11
    VPSRLDQ     $8, Y6, Y12
    VPSLLDQ     $8, Y11, Y13
    VPBLENDW    $0x0f, Y6, Y13, Y6
    VPBLENDW    $0xf0, Y11, Y12, Y11

	VPCMPGTQ	Y6, Y0, Y12
	VPCMPEQQ	Y6, Y0, Y6
	VPXOR		Y14, Y11, Y11 
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y6,Y11, Y6
    VPOR        Y6, Y12, Y6

	VMOVDQU	   384(SI), Y7
	VMOVDQU	   416(SI), Y11
    VPSRLDQ     $8, Y7, Y12
    VPSLLDQ     $8, Y11, Y13
    VPBLENDW    $0x0f, Y7, Y13, Y7
    VPBLENDW    $0xf0, Y11, Y12, Y11

	VPCMPGTQ	Y7, Y0, Y12
	VPCMPEQQ	Y7, Y0, Y7
	VPXOR		Y14, Y11, Y11 
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y7,Y11, Y7
    VPOR        Y7, Y12, Y7

	VMOVDQU	   448(SI), Y8
	VMOVDQU	   480(SI), Y11
    VPSRLDQ     $8, Y8, Y12
    VPSLLDQ     $8, Y11, Y13
    VPBLENDW    $0x0f, Y8, Y13, Y8
    VPBLENDW    $0xf0, Y11, Y12, Y11

	VPCMPGTQ	Y8, Y0, Y12
	VPCMPEQQ	Y8, Y0, Y8
	VPXOR		Y14, Y11, Y11 
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y8,Y11, Y8
    VPOR        Y8, Y12, Y8

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    // MOVD        $0x1234567812345678, AX
	MOVL		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI
	ADDQ		$4, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

prep_scalar: 

done:
	MOVQ	R9, ret+64(FP)
	RET

// func matchInt256LessThanAVX2New(src0 []int64, src1 []uint64, val Int256, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·matchInt256LessThanAVX2New(SB), NOSPLIT, $0-96
	MOVQ	src0_base+0(FP), SI
    MOVQ    src1_base+24(FP), BP
	MOVQ	src_len+8(FP), BX
	MOVQ	dest_base+64(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD val+48(FP), Y0            // load upper qword of val into AVX2 reg
    
   	VPCMPEQQ		Y14, Y14, Y14                   // create 0x8000.. mask
	VPSLLQ			$63, Y14, Y14                   // create 0x8000.. mask
	VBROADCASTSD val+56(FP), Y15                    // load lower qword of val into AVX2 reg
	VPXOR			Y14, Y15, Y15                     // flip sign bit
    
	VMOVDQU		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10    // load shuffle control mask
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
    VMOVDQU	     0(SI), Y1
	VMOVDQU	    0(BP), Y11

	VPCMPGTQ	Y1, Y0, Y12         // Y1 < Y0?  (signed)
	VPCMPEQQ	Y1, Y0, Y1          // Y1 == Y0?
	VPXOR		Y14, Y11, Y11       // flip sign bits
	VPCMPGTQ	Y11, Y15, Y11       // Y11 < Y15 (unsigned)
    VPAND       Y1,Y11, Y1
    VPOR        Y1, Y12, Y1

	VMOVDQU	    32(SI), Y2
	VMOVDQU	    32(BP), Y11

	VPCMPGTQ	Y2, Y0, Y12
	VPCMPEQQ	Y2, Y0, Y2
	VPXOR		Y14, Y11, Y11 
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y2,Y11, Y2
    VPOR        Y2, Y12, Y2

	VMOVDQU	   64(SI), Y3
	VMOVDQU	   64(BP), Y11

	VPCMPGTQ	Y3, Y0, Y12
	VPCMPEQQ	Y3, Y0, Y3
	VPXOR		Y14, Y11, Y11 
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y3,Y11, Y3
    VPOR        Y3, Y12, Y3

	VMOVDQU	   96(SI), Y4
	VMOVDQU	   96(BP), Y11

	VPCMPGTQ	Y4, Y0, Y12
	VPCMPEQQ	Y4, Y0, Y4
	VPXOR		Y14, Y11, Y11 
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y4,Y11, Y4
    VPOR        Y4, Y12, Y4

	VMOVDQU	   128(SI), Y5
	VMOVDQU	   128(BP), Y11

	VPCMPGTQ	Y5, Y0, Y12
	VPCMPEQQ	Y5, Y0, Y5
	VPXOR		Y14, Y11, Y11 
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y5,Y11, Y5
    VPOR        Y5, Y12, Y5

	VMOVDQU	   160(SI), Y6
	VMOVDQU	   160(BP), Y11
    
	VPCMPGTQ	Y6, Y0, Y12
	VPCMPEQQ	Y6, Y0, Y6
	VPXOR		Y14, Y11, Y11 
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y6,Y11, Y6
    VPOR        Y6, Y12, Y6

	VMOVDQU	   192(SI), Y7
	VMOVDQU	   192(BP), Y11

	VPCMPGTQ	Y7, Y0, Y12
	VPCMPEQQ	Y7, Y0, Y7
	VPXOR		Y14, Y11, Y11 
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y7,Y11, Y7
    VPOR        Y7, Y12, Y7

	VMOVDQU	   224(SI), Y8
	VMOVDQU	   224(BP), Y11

	VPCMPGTQ	Y8, Y0, Y12
	VPCMPEQQ	Y8, Y0, Y8
	VPXOR		Y14, Y11, Y11 
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y8,Y11, Y8
    VPOR        Y8, Y12, Y8

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    // MOVD        $0x1234567812345678, AX
	MOVL		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$256, BP
	ADDQ		$4, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

prep_scalar: 

done:
	MOVQ	R9, ret+88(FP)
	RET

/*
// func matchInt64EqualAVX2(src []int64, val int64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·matchInt64EqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD val+24(FP), Y0            // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10    // load shuffle control mask
	CMPQ	BX, $63      // slices smaller than 64 byte are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_big:
	VPCMPEQQ	0(SI), Y0, Y1
	VPCMPEQQ	32(SI), Y0, Y2
	VPCMPEQQ	64(SI), Y0, Y3
	VPCMPEQQ	96(SI), Y0, Y4
	VPCMPEQQ	128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VPCMPEQQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPEQQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPEQQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPEQQ	256(SI), Y0, Y1
	VPCMPEQQ	288(SI), Y0, Y2
	VPCMPEQQ	320(SI), Y0, Y3
	VPCMPEQQ	352(SI), Y0, Y4
	VPCMPEQQ	384(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VPCMPEQQ	416(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPEQQ	448(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPEQQ	480(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32,DX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VPCMPEQQ	0(SI), Y0, Y1
	VPCMPEQQ	32(SI), Y0, Y2
	VPCMPEQQ	64(SI), Y0, Y3
	VPCMPEQQ	96(SI), Y0, Y4
	VPCMPEQQ	128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VPCMPEQQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPEQQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPEQQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$4, DI
    SUBQ        $32, BX

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETEQ	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func matchInt64NotEqualAVX2(src []int64, val int64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·matchInt64NotEqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD val+24(FP), Y0            // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10    // load shuffle control mask
	CMPQ	BX, $63      // slices smaller than 64 byte are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_big:
	VPCMPEQQ	0(SI), Y0, Y1
	VPCMPEQQ	32(SI), Y0, Y2
	VPCMPEQQ	64(SI), Y0, Y3
	VPCMPEQQ	96(SI), Y0, Y4
	VPCMPEQQ	128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VPCMPEQQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPEQQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPEQQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPEQQ	256(SI), Y0, Y1
	VPCMPEQQ	288(SI), Y0, Y2
	VPCMPEQQ	320(SI), Y0, Y3
	VPCMPEQQ	352(SI), Y0, Y4
	VPCMPEQQ	384(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VPCMPEQQ	416(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPEQQ	448(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPEQQ	480(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32,DX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VPCMPEQQ	0(SI), Y0, Y1
	VPCMPEQQ	32(SI), Y0, Y2
	VPCMPEQQ	64(SI), Y0, Y3
	VPCMPEQQ	96(SI), Y0, Y4
	VPCMPEQQ	128(SI), Y0, Y5
	VPACKSSDW	Y1, Y2, Y1
	VPCMPEQQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPEQQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPEQQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    NOTL        AX
	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$4, DI
    SUBQ        $32, BX

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETNE	R10
	ADDL	R10, R9
	ORL 	R10, AX
	RORL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func matchInt64LessThanAVX2(src []int64, val int64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·matchInt64LessThanAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD 	val+24(FP), Y0                  // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9            // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $63      // slices smaller than 64 byte are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_big:
	VPCMPGTQ	0(SI), Y0, Y1   // check using GT with switched operands
	VPCMPGTQ	32(SI), Y0, Y2
	VPCMPGTQ	64(SI), Y0, Y3
	VPCMPGTQ	96(SI), Y0, Y4
	VPCMPGTQ	128(SI), Y0, Y5
    VPACKSSDW	Y1, Y2, Y1
	VPCMPGTQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
  	VPCMPGTQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPGTQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPGTQ	256(SI), Y0, Y1     
	VPCMPGTQ	288(SI), Y0, Y2
	VPCMPGTQ	320(SI), Y0, Y3
	VPCMPGTQ	352(SI), Y0, Y4
	VPCMPGTQ	384(SI), Y0, Y5
    VPACKSSDW	Y1, Y2, Y1
	VPCMPGTQ	416(SI), Y0, Y6     
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPGTQ	448(SI), Y0, Y7     
	VPACKSSDW	Y5, Y6, Y5
	VPCMPGTQ	480(SI), Y0, Y8     
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32,DX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
    VPCMPGTQ	0(SI), Y0, Y1   // check using GT with switched operands
	VPCMPGTQ	32(SI), Y0, Y2
	VPCMPGTQ	64(SI), Y0, Y3
	VPCMPGTQ	96(SI), Y0, Y4
	VPCMPGTQ	128(SI), Y0, Y5
    VPACKSSDW	Y1, Y2, Y1
	VPCMPGTQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPGTQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPGTQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$4, DI
    SUBQ        $32, BX

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETLT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func matchInt64LessThanEqualAVX2(src []int64, val int64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·matchInt64LessThanEqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD 	val+24(FP), Y0                   // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10    // load shuffle control mask
	CMPQ	BX, $63      // slices smaller than 64 byte are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_big:
	VMOVDQU		0(SI), Y1       // load values (necessary to switch operands)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
	VPCMPGTQ	Y0, Y1, Y1     // signed compare
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		160(SI), Y6     
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		192(SI), Y7     
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		224(SI), Y8     
	VPCMPGTQ	Y0, Y8, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VMOVDQU		256(SI), Y1       // load values (necessary to switch operands)
	VMOVDQU		288(SI), Y2
	VMOVDQU		320(SI), Y3
	VMOVDQU		352(SI), Y4
	VMOVDQU		384(SI), Y5
	VPCMPGTQ	Y0, Y1, Y1     // signed compare
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		416(SI), Y6     
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		448(SI), Y7     
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		480(SI), Y8     
	VPCMPGTQ	Y0, Y8, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32,DX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU		0(SI), Y1       // load values (necessary to switch operands)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
	VPCMPGTQ	Y0, Y1, Y1     // signed compare
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		160(SI), Y6     
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		192(SI), Y7     
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		224(SI), Y8     
	VPCMPGTQ	Y0, Y8, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    NOTL        AX
	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$4, DI
    SUBQ        $32, BX

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETLE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar
    
scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func matchInt64GreaterThanEqualAVX2(src []int64, val int64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·matchInt64GreaterThanEqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD 	val+24(FP), Y0                  // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9            // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10       // load shuffle control mask
	CMPQ	BX, $63      // slices smaller than 64 byte are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_big:
	VPCMPGTQ	0(SI), Y0, Y1   // check using GT with switched operands
	VPCMPGTQ	32(SI), Y0, Y2
	VPCMPGTQ	64(SI), Y0, Y3
	VPCMPGTQ	96(SI), Y0, Y4
	VPCMPGTQ	128(SI), Y0, Y5
    VPACKSSDW	Y1, Y2, Y1
	VPCMPGTQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
  	VPCMPGTQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPGTQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VPCMPGTQ	256(SI), Y0, Y1      // load values (necessary to flip sign bit)
	VPCMPGTQ	288(SI), Y0, Y2
	VPCMPGTQ	320(SI), Y0, Y3
	VPCMPGTQ	352(SI), Y0, Y4
	VPCMPGTQ	384(SI), Y0, Y5
    VPACKSSDW	Y1, Y2, Y1
	VPCMPGTQ	416(SI), Y0, Y6     
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPGTQ	448(SI), Y0, Y7     
	VPACKSSDW	Y5, Y6, Y5
	VPCMPGTQ	480(SI), Y0, Y8     
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32,DX
    ORQ         DX, AX
    NOTQ        AX
	MOVQ		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
    VPCMPGTQ	0(SI), Y0, Y1   // check using GT with switched operands
	VPCMPGTQ	32(SI), Y0, Y2
	VPCMPGTQ	64(SI), Y0, Y3
	VPCMPGTQ	96(SI), Y0, Y4
	VPCMPGTQ	128(SI), Y0, Y5
    VPACKSSDW	Y1, Y2, Y1
	VPCMPGTQ	160(SI), Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPCMPGTQ	192(SI), Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VPCMPGTQ	224(SI), Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    NOTL        AX
	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$4, DI
    SUBQ        $32, BX

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETGE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func matchInt64GreaterThanAVX2(src []int64, val int64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   DX = comparison value for scalar
//   Y0 = comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·matchInt64GreaterThanAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		prep_scalar

prep_avx:
	VBROADCASTSD 	val+24(FP), Y0                   // load val into AVX2 reg
	VMOVDQU		crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10    // load shuffle control mask
	CMPQ	BX, $63      // slices smaller than 64 byte are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_big:
	VMOVDQU		0(SI), Y1       // load values (necessary to switch operands)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
	VPCMPGTQ	Y0, Y1, Y1     // signed compare
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		160(SI), Y6     
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		192(SI), Y7     
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		224(SI), Y8     
	VPCMPGTQ	Y0, Y8, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VMOVDQU		256(SI), Y1       // load values (necessary to switch operands)
	VMOVDQU		288(SI), Y2
	VMOVDQU		320(SI), Y3
	VMOVDQU		352(SI), Y4
	VMOVDQU		384(SI), Y5
	VPCMPGTQ	Y0, Y1, Y1     // signed compare
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		416(SI), Y6     
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		448(SI), Y7     
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		480(SI), Y8     
	VPCMPGTQ	Y0, Y8, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32,DX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU		0(SI), Y1       // load values (necessary to switch operands)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
	VPCMPGTQ	Y0, Y1, Y1     // signed compare
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		160(SI), Y6     
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		192(SI), Y7     
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		224(SI), Y8     
	VPCMPGTQ	Y0, Y8, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$4, DI
    SUBQ        $32, BX

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX   // load val for comparison
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETGT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar
    
scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func matchInt64BetweenAVX2(src []int64, a, b int64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   R10 = lower comparison value for scalar
//   R11 = upper comparison value for scalar
//   Y0 = lower comparison value for AVX2
//   Y11 = upper comparison value for AVX2
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·matchInt64BetweenAVX2(SB), NOSPLIT, $0-72
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+40(FP), DI
	XORQ	R9, R9

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31      // slices smaller than 32 byte are handled separately
	JBE		prep_scalar

// works for >= 32 int64 (i.e. 256 bytes of data)
// check is using GT with switched operands and add
// the diff method to avoid jumps:
// 	diff := b - a + 1
//  v-a < diff
prep_avx:
	VPCMPEQQ		Y11, Y11, Y11                   // create 0x8000.. mask
	VPSLLQ			$63, Y11, Y11                   // create 0x8000.. mask
	VPCMPEQQ		Y13, Y13, Y13                   // create 1 for adding
	VPSRLQ			$63, Y13, Y13
	VBROADCASTSD 	a+24(FP), Y12                   // load val a into AVX2 reg
	VBROADCASTSD 	b+32(FP), Y0                    // load val b into AVX2 reg
	VPSUBQ			Y12, Y0, Y0                     // compute diff
	VPADDQ			Y13, Y0, Y0
	VPXOR			Y11, Y0, Y0                     // flip sign bit
	VMOVDQU			crosslane<>+0x00(SB), Y9        // load permute control mask
	VMOVDQU			shuffle64<>+0x00(SB), Y10       // load shuffle control mask

	CMPQ	BX, $63      // slices smaller than 64 byte are handled in small loop
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 32 int64 (i.e. 256 bytes of data)
loop_big:
	VMOVDQU		0(SI), Y1      // load values (necessary to flip sign bit)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
	VPSUBQ		Y12, Y1, Y1
	VPSUBQ		Y12, Y2, Y2
	VPSUBQ		Y12, Y3, Y3
	VPSUBQ		Y12, Y4, Y4
	VPSUBQ		Y12, Y5, Y5
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPXOR		Y11, Y5, Y5
	VPCMPGTQ	Y1, Y0, Y1     // signed compare
	VPCMPGTQ	Y2, Y0, Y2
	VPCMPGTQ	Y3, Y0, Y3
	VPCMPGTQ	Y4, Y0, Y4
	VPCMPGTQ	Y5, Y0, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		160(SI), Y6     
	VPSUBQ		Y12, Y6, Y6
	VPXOR		Y11, Y6, Y6
	VPCMPGTQ	Y6, Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		192(SI), Y7     
	VPSUBQ		Y12, Y7, Y7
	VPXOR		Y11, Y7, Y7
	VPCMPGTQ	Y7, Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		224(SI), Y8     
	VPSUBQ		Y12, Y8, Y8
	VPXOR		Y11, Y8, Y8
	VPCMPGTQ	Y8, Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	VMOVDQU		256(SI), Y1      // load values (necessary to flip sign bit)
	VMOVDQU		288(SI), Y2
	VMOVDQU		320(SI), Y3
	VMOVDQU		352(SI), Y4
	VMOVDQU		384(SI), Y5
	VPSUBQ		Y12, Y1, Y1
	VPSUBQ		Y12, Y2, Y2
	VPSUBQ		Y12, Y3, Y3
	VPSUBQ		Y12, Y4, Y4
	VPSUBQ		Y12, Y5, Y5
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPXOR		Y11, Y5, Y5
	VPCMPGTQ	Y1, Y0, Y1     // signed compare
	VPCMPGTQ	Y2, Y0, Y2
	VPCMPGTQ	Y3, Y0, Y3
	VPCMPGTQ	Y4, Y0, Y4
	VPCMPGTQ	Y5, Y0, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		416(SI), Y6     
	VPSUBQ		Y12, Y6, Y6
	VPXOR		Y11, Y6, Y6
	VPCMPGTQ	Y6, Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		448(SI), Y7     
	VPSUBQ		Y12, Y7, Y7
	VPXOR		Y11, Y7, Y7
	VPCMPGTQ	Y7, Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		480(SI), Y8     
	VPSUBQ		Y12, Y8, Y8
	VPXOR		Y11, Y8, Y8
	VPCMPGTQ	Y8, Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, DX      // move per byte MSBs into packed bitmask to r32 or r64

    SHLQ        $32,DX
    ORQ         DX, AX
	MOVQ		AX, (DI)(CX*1)    // write the 64 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	CMPQ	BX, $31      // slices smaller than 32 byte are handled in scalar loop
	JBE		exit_small

prep_small:

loop_small:
	VMOVDQU		0(SI), Y1      // load values (necessary to flip sign bit)
	VMOVDQU		32(SI), Y2
	VMOVDQU		64(SI), Y3
	VMOVDQU		96(SI), Y4
	VMOVDQU		128(SI), Y5
	VPSUBQ		Y12, Y1, Y1
	VPSUBQ		Y12, Y2, Y2
	VPSUBQ		Y12, Y3, Y3
	VPSUBQ		Y12, Y4, Y4
	VPSUBQ		Y12, Y5, Y5
	VPXOR		Y11, Y1, Y1    // flip sign bits
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPXOR		Y11, Y5, Y5
	VPCMPGTQ	Y1, Y0, Y1     // signed compare
	VPCMPGTQ	Y2, Y0, Y2
	VPCMPGTQ	Y3, Y0, Y3
	VPCMPGTQ	Y4, Y0, Y4
	VPCMPGTQ	Y5, Y0, Y5	
    VPACKSSDW	Y1, Y2, Y1
	VMOVDQU		160(SI), Y6     
	VPSUBQ		Y12, Y6, Y6
	VPXOR		Y11, Y6, Y6
	VPCMPGTQ	Y6, Y0, Y6
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VMOVDQU		192(SI), Y7     
	VPSUBQ		Y12, Y7, Y7
	VPXOR		Y11, Y7, Y7
	VPCMPGTQ	Y7, Y0, Y7
	VPACKSSDW	Y5, Y6, Y5
	VMOVDQU		224(SI), Y8     
	VPSUBQ		Y12, Y8, Y8
	VPXOR		Y11, Y8, Y8
	VPCMPGTQ	Y8, Y0, Y8
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)    // write the lower 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$4, DI
    SUBQ        $32, BX

exit_small:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	a+24(FP), R13   // load val a
	MOVQ	b+32(FP), DX    // load val b
	SUBQ	R13, DX
	INCQ	DX
	MOVQ    $1, R12          // create 0x80... mask
	SHLQ    $63, R12
	XORQ    R12, DX          // flip sign bit
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$32, CX          // remember how many extra shifts we need at the end
	SUBQ	BX, CX

// for remainders of <32 int64
scalar:
	MOVQ	(SI), R8
	SUBQ	R13, R8          // v - a < diff
	XORQ    R12, R8          // flip sign bit
	CMPQ	R8, DX
	SETLT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	RORL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	RORL	CX, AX        // fill 32bits by shifting
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+64(FP)
	RET
*/
