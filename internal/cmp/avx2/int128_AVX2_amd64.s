// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants_AVX2.h"

// func cmp_i128_eq_x2(src Int128LLSlice, val Int128, bits []byte) int64
//
// input:
//   SI = src_X0_base (upper qwords)
//   BP = src_X1_base (lower qwords)
//   DI = bits_base
//   BX = src_X0_len
//   (Y0,Y15) = comparison value for AVX2 (upper/lower qword)
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·cmp_i128_eq_x2(SB), NOSPLIT, $0-96
	MOVQ	src_X0_base+0(FP), SI
    MOVQ    src_X1_base+24(FP), BP
	MOVQ	src_X0_len+8(FP), BX
	MOVQ	bits_base+64(FP), DI
	XORQ	R9, R9

	CMPQ	BX, $31     // function handles only blocks of 32 values
    JBE		done        // smaller slices and tails are not handled
    
prep_avx:
	VBROADCASTSD    val_0+48(FP), Y0              // load upper qword of val into AVX2 reg
	VBROADCASTSD    val_1+56(FP), Y15             // load lower qword of val into AVX2 reg
	VMOVDQU		    crosslane<>+0x00(SB), Y9    // load permute control mask
	VMOVDQU		    shuffle64<>+0x00(SB), Y10   // load shuffle control mask

    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffe0, CX     // number of values processed in big blocks
    ANDQ    $0x1f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_avx:
    VPCMPEQQ    0(SI), Y0, Y1           // compare upper qword
	VPCMPEQQ    0(BP), Y15, Y11         // compare lower qword
    VPAND       Y1,Y11, Y1              // both equal?

	VPCMPEQQ    32(SI), Y0, Y2
	VPCMPEQQ    32(BP), Y15, Y11
    VPAND       Y2,Y11, Y2

	VPCMPEQQ   64(SI), Y0, Y3
	VPCMPEQQ   64(BP), Y15, Y11
    VPAND       Y3,Y11, Y3

	VPCMPEQQ   96(SI), Y0, Y4
	VPCMPEQQ   96(BP), Y15, Y11
    VPAND       Y4,Y11, Y4

	VPCMPEQQ   128(SI), Y0, Y5
	VPCMPEQQ   128(BP), Y15, Y11
    VPAND       Y5,Y11, Y5

	VPCMPEQQ   160(SI), Y0, Y6
	VPCMPEQQ   160(BP), Y15, Y11
    VPAND       Y6,Y11, Y6

	VPCMPEQQ   192(SI), Y0, Y7
	VPCMPEQQ   192(BP), Y15, Y11
    VPAND       Y7,Y11, Y7

	VPCMPEQQ   224(SI), Y0, Y8
	VPCMPEQQ   224(BP), Y15, Y11
    VPAND       Y8,Y11, Y8

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX              // move per byte MSBs into packed bitmask to r32 or r64

	MOVL		AX, (DI)(CX*1)      // write the 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$256, BP
	ADDQ		$4, CX
	JZ		 	exit_avx
	JMP		 	loop_avx

exit_avx:
	VZEROUPPER                      // clear upper part of Y regs, prevents AVX-SSE penalty

done:
	MOVQ	R9, ret+88(FP)
	RET

// func cmp_i128_ne_x2(src Int128LLSlice, val Int128, bits []byte) int64
//
// input:
//   SI = src_X0_base (upper qwords)
//   BP = src_X1_base (lower qwords)
//   DI = bits_base
//   BX = src_X0_len
//   (Y0,Y15) = comparison value for AVX2 (upper/lower qword)
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y1-Y8 = vector data
TEXT ·cmp_i128_ne_x2(SB), NOSPLIT, $0-96
	MOVQ	src_X0_base+0(FP), SI
    MOVQ    src_X1_base+24(FP), BP
	MOVQ	src_X0_len+8(FP), BX
	MOVQ	bits_base+64(FP), DI
	XORQ	R9, R9

	CMPQ	BX, $31     // function handles only blocks of 32 values
    JBE		done        // smaller slices and tails are not handled

prep_avx:
	VBROADCASTSD    val_0+48(FP), Y0              // load upper qword of val into AVX2 reg
	VBROADCASTSD    val_1+56(FP), Y15             // load lower qword of val into AVX2 reg
	VMOVDQU		    crosslane<>+0x00(SB), Y9    // load permute control mask
	VMOVDQU		    shuffle64<>+0x00(SB), Y10   // load shuffle control mask

    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffe0, CX     // number of values processed in big blocks
    ANDQ    $0x1f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_avx:
    VPCMPEQQ    0(SI), Y0, Y1           // compare upper qword
	VPCMPEQQ    0(BP), Y15, Y11         // compare lower qword
    VPAND       Y1,Y11, Y1              // both equal (will be negated later)

	VPCMPEQQ    32(SI), Y0, Y2
	VPCMPEQQ    32(BP), Y15, Y11
    VPAND       Y2,Y11, Y2

	VPCMPEQQ   64(SI), Y0, Y3
	VPCMPEQQ   64(BP), Y15, Y11
    VPAND       Y3,Y11, Y3

	VPCMPEQQ   96(SI), Y0, Y4
	VPCMPEQQ   96(BP), Y15, Y11
    VPAND       Y4,Y11, Y4

	VPCMPEQQ   128(SI), Y0, Y5
	VPCMPEQQ   128(BP), Y15, Y11
    VPAND       Y5,Y11, Y5

	VPCMPEQQ   160(SI), Y0, Y6
	VPCMPEQQ   160(BP), Y15, Y11
    VPAND       Y6,Y11, Y6

	VPCMPEQQ   192(SI), Y0, Y7
	VPCMPEQQ   192(BP), Y15, Y11
    VPAND       Y7,Y11, Y7

	VPCMPEQQ   224(SI), Y0, Y8
	VPCMPEQQ   224(BP), Y15, Y11
    VPAND       Y8,Y11, Y8

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1
	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX              // move per byte MSBs into packed bitmask to r32 or r64

    NOTL        AX
	MOVL		AX, (DI)(CX*1)      // write the 32 bits to the output slice
	POPCNTQ		AX, AX              // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$256, BP
	ADDQ		$4, CX
	JZ		 	exit_avx
	JMP		 	loop_avx

exit_avx:
	VZEROUPPER                      // clear upper part of Y regs, prevents AVX-SSE penalty

done:
	MOVQ	R9, ret+88(FP)
	RET

// func cmp_i128_lt_x2(src Int128LLSlice, val Int128, bits []byte) int64
//
// input:
//   SI = src_X0_base (upper qwords)
//   BP = src_X1_base (lower qwords)
//   DI = bits_base
//   BX = src_X0_len
//   (Y0,Y15) = comparison value for AVX2 (upper/lower qword)
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y14 = bitmask to flip sign bit (to perform unsigned comparision)
//   Y1-Y8 = vector data
TEXT ·cmp_i128_lt_x2(SB), NOSPLIT, $0-96
	MOVQ	src_X0_base+0(FP), SI
    MOVQ    src_X1_base+24(FP), BP
	MOVQ	src_X0_len+8(FP), BX
	MOVQ	bits_base+64(FP), DI
	XORQ	R9, R9

	CMPQ	BX, $31     // function handles only blocks of 32 values
    JBE		done        // smaller slices and tails are not handled

prep_avx:
   	VPCMPEQQ		Y14, Y14, Y14           // create 0x8000.. mask
	VPSLLQ			$63, Y14, Y14           // create 0x8000.. mask
	VBROADCASTSD    val_0+48(FP), Y0          // load upper qword of val into AVX2 reg
	VBROADCASTSD    val_1+56(FP), Y15         // load lower qword of val into AVX2 reg
	VPXOR			Y14, Y15, Y15           // flip sign bit
    
	VMOVDQU		crosslane<>+0x00(SB), Y9    // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10   // load shuffle control mask

    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffe0, CX     // number of values processed in big blocks
    ANDQ    $0x1f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_avx:
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

	MOVL		AX, (DI)(CX*1)    // write the 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$256, BP
	ADDQ		$4, CX
	JZ		 	exit_avx
	JMP		 	loop_avx

exit_avx:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
	MOVQ	R9, ret+88(FP)
	RET

// func cmp_i128_le_x2(src Int128LLSlice, val Int128, bits []byte) int64
//
// input:
//   SI = src_X0_base (upper qwords)
//   BP = src_X1_base (lower qwords)
//   DI = bits_base
//   BX = src_X0_len
//   (Y0,Y15) = comparison value for AVX2 (upper/lower qword)
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y14 = bitmask to flip sign bit (to perform unsigned comparision)
//   Y1-Y8 = vector data
TEXT ·cmp_i128_le_x2(SB), NOSPLIT, $0-96
	MOVQ	src_X0_base+0(FP), SI
    MOVQ    src_X1_base+24(FP), BP
	MOVQ	src_X0_len+8(FP), BX
	MOVQ	bits_base+64(FP), DI
	XORQ	R9, R9

	CMPQ	BX, $31     // function handles only blocks of 32 values
    JBE		done        // smaller slices and tails are not handled

prep_avx:
   	VPCMPEQQ		Y14, Y14, Y14           // create 0x8000.. mask
	VPSLLQ			$63, Y14, Y14           // create 0x8000.. mask
	VBROADCASTSD    val_0+48(FP), Y0          // load upper qword of val into AVX2 reg
	VBROADCASTSD    val_1+56(FP), Y15         // load lower qword of val into AVX2 reg
	VPXOR			Y14, Y15, Y15           // flip sign bit
    
	VMOVDQU		crosslane<>+0x00(SB), Y9    // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10   // load shuffle control mask

    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffe0, CX     // number of values processed in big blocks
    ANDQ    $0x1f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_avx:
    VMOVDQU	     0(SI), Y1
	VMOVDQU	    0(BP), Y11
	VPXOR		Y14, Y11, Y11       // flip sign bits

	VPCMPGTQ	Y0, Y1, Y12         // Y1 > Y0?  (signed)
	VPCMPEQQ	Y1, Y0, Y1          // Y1 == Y0?
	VPCMPGTQ	Y15, Y11, Y11       // Y11 > Y15 (unsigned)
    VPAND       Y1,Y11, Y1
    VPOR        Y1, Y12, Y1

	VMOVDQU	    32(SI), Y2
	VMOVDQU	    32(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y0, Y2, Y12
	VPCMPEQQ	Y2, Y0, Y2
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y2,Y11, Y2
    VPOR        Y2, Y12, Y2

	VMOVDQU	    64(SI), Y3
	VMOVDQU	    64(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y0, Y3, Y12
	VPCMPEQQ	Y3, Y0, Y3
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y3,Y11, Y3
    VPOR        Y3, Y12, Y3

	VMOVDQU	    96(SI), Y4
	VMOVDQU	    96(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y0, Y4, Y12
	VPCMPEQQ	Y4, Y0, Y4
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y4,Y11, Y4
    VPOR        Y4, Y12, Y4

	VMOVDQU	    128(SI), Y5
	VMOVDQU	    128(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y0, Y5, Y12
	VPCMPEQQ	Y5, Y0, Y5
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y5,Y11, Y5
    VPOR        Y5, Y12, Y5

	VMOVDQU	    160(SI), Y6
	VMOVDQU	    160(BP), Y11
	VPXOR		Y14, Y11, Y11 
    
	VPCMPGTQ	Y0, Y6, Y12
	VPCMPEQQ	Y6, Y0, Y6
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y6,Y11, Y6
    VPOR        Y6, Y12, Y6

	VMOVDQU	    192(SI), Y7
	VMOVDQU	    192(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y0, Y7, Y12
	VPCMPEQQ	Y7, Y0, Y7
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y7,Y11, Y7
    VPOR        Y7, Y12, Y7

	VMOVDQU	    224(SI), Y8
	VMOVDQU	    224(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y0, Y8, Y12
	VPCMPEQQ	Y8, Y0, Y8
	VPCMPGTQ	Y15, Y11, Y11
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

    NOTL        AX
	MOVL		AX, (DI)(CX*1)    // write the 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$256, BP
	ADDQ		$4, CX
	JZ		 	exit_avx
	JMP		 	loop_avx

exit_avx:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
	MOVQ	R9, ret+88(FP)
	RET

// func cmp_i128_gt_x2(src Int128LLSlice, val Int128, bits []byte) int64
//
// input:
//   SI = src_X0_base (upper qwords)
//   BP = src_X1_base (lower qwords)
//   DI = bits_base
//   BX = src_X0_len
//   (Y0,Y15) = comparison value for AVX2 (upper/lower qword)
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y14 = bitmask to flip sign bit (to perform unsigned comparision)
//   Y1-Y8 = vector data
TEXT ·cmp_i128_gt_x2(SB), NOSPLIT, $0-96
	MOVQ	src_X0_base+0(FP), SI
    MOVQ    src_X1_base+24(FP), BP
	MOVQ	src_X0_len+8(FP), BX
	MOVQ	bits_base+64(FP), DI
	XORQ	R9, R9

	CMPQ	BX, $31     // function handles only blocks of 32 values
    JBE		done        // smaller slices and tails are not handled

prep_avx:
   	VPCMPEQQ		Y14, Y14, Y14           // create 0x8000.. mask
	VPSLLQ			$63, Y14, Y14           // create 0x8000.. mask
	VBROADCASTSD    val_0+48(FP), Y0          // load upper qword of val into AVX2 reg
	VBROADCASTSD    val_1+56(FP), Y15         // load lower qword of val into AVX2 reg
	VPXOR			Y14, Y15, Y15           // flip sign bit
    
	VMOVDQU		crosslane<>+0x00(SB), Y9    // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10   // load shuffle control mask

    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffe0, CX     // number of values processed in big blocks
    ANDQ    $0x1f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_avx:
    VMOVDQU	    0(SI), Y1
	VMOVDQU	    0(BP), Y11
	VPXOR		Y14, Y11, Y11       // flip sign bits

	VPCMPGTQ	Y0, Y1, Y12         // Y1 > Y0?  (signed)
	VPCMPEQQ	Y1, Y0, Y1          // Y1 == Y0?
	VPCMPGTQ	Y15, Y11, Y11       // Y11 > Y15 (unsigned)
    VPAND       Y1,Y11, Y1
    VPOR        Y1, Y12, Y1

	VMOVDQU	    32(SI), Y2
	VMOVDQU	    32(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y0, Y2, Y12
	VPCMPEQQ	Y2, Y0, Y2
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y2,Y11, Y2
    VPOR        Y2, Y12, Y2

	VMOVDQU	   64(SI), Y3
	VMOVDQU	   64(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y0, Y3, Y12
	VPCMPEQQ	Y3, Y0, Y3
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y3,Y11, Y3
    VPOR        Y3, Y12, Y3

	VMOVDQU	    96(SI), Y4
	VMOVDQU	    96(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y0, Y4, Y12
	VPCMPEQQ	Y4, Y0, Y4
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y4,Y11, Y4
    VPOR        Y4, Y12, Y4

	VMOVDQU	    128(SI), Y5
	VMOVDQU	    128(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y0, Y5, Y12
	VPCMPEQQ	Y5, Y0, Y5
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y5,Y11, Y5
    VPOR        Y5, Y12, Y5

	VMOVDQU	    160(SI), Y6
	VMOVDQU	    160(BP), Y11
	VPXOR		Y14, Y11, Y11 
    
	VPCMPGTQ	Y0, Y6, Y12
	VPCMPEQQ	Y6, Y0, Y6
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y6,Y11, Y6
    VPOR        Y6, Y12, Y6

	VMOVDQU	    192(SI), Y7
	VMOVDQU	    192(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y0, Y7, Y12
	VPCMPEQQ	Y7, Y0, Y7
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y7,Y11, Y7
    VPOR        Y7, Y12, Y7

	VMOVDQU	    224(SI), Y8
	VMOVDQU	    224(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y0, Y8, Y12
	VPCMPEQQ	Y8, Y0, Y8
	VPCMPGTQ	Y15, Y11, Y11
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

	MOVL		AX, (DI)(CX*1)    // write the 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$256, BP
	ADDQ		$4, CX
	JZ		 	exit_avx
	JMP		 	loop_avx

exit_avx:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
	MOVQ	R9, ret+88(FP)
	RET

// func cmp_i128_ge_x2(src Int128LLSlice, val Int128, bits []byte) int64
//
// input:
//   SI = src_X0_base (upper qwords)
//   BP = src_X1_base (lower qwords)
//   DI = bits_base
//   BX = src_X0_len
//   (Y0,Y15) = comparison value for AVX2 (upper/lower qword)
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y14 = bitmask to flip sign bit (to perform unsigned comparision)
//   Y1-Y8 = vector data
TEXT ·cmp_i128_ge_x2(SB), NOSPLIT, $0-96
	MOVQ	src_X0_base+0(FP), SI
    MOVQ    src_X1_base+24(FP), BP
	MOVQ	src_X0_len+8(FP), BX
	MOVQ	bits_base+64(FP), DI
	XORQ	R9, R9

	CMPQ	BX, $31     // function handles only blocks of 32 values
    JBE		done        // smaller slices and tails are not handled

prep_avx:
   	VPCMPEQQ		Y14, Y14, Y14           // create 0x8000.. mask
	VPSLLQ			$63, Y14, Y14           // create 0x8000.. mask
	VBROADCASTSD    val_0+48(FP), Y0          // load upper qword of val into AVX2 reg
	VBROADCASTSD    val_1+56(FP), Y15         // load lower qword of val into AVX2 reg
	VPXOR			Y14, Y15, Y15           // flip sign bit
    
	VMOVDQU		crosslane<>+0x00(SB), Y9    // load permute control mask
	VMOVDQU		shuffle64<>+0x00(SB), Y10   // load shuffle control mask

    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffe0, CX     // number of values processed in big blocks
    ANDQ    $0x1f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_avx:
    VMOVDQU	    0(SI), Y1
	VMOVDQU	    0(BP), Y11
	VPXOR		Y14, Y11, Y11       // flip sign bits

	VPCMPGTQ	Y1, Y0, Y12         // Y1 < Y0?  (signed)
	VPCMPEQQ	Y1, Y0, Y1          // Y1 == Y0?
	VPCMPGTQ	Y11, Y15, Y11       // Y11 < Y15 (unsigned)
    VPAND       Y1,Y11, Y1
    VPOR        Y1, Y12, Y1

	VMOVDQU	    32(SI), Y2
	VMOVDQU	    32(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y2, Y0, Y12
	VPCMPEQQ	Y2, Y0, Y2
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y2,Y11, Y2
    VPOR        Y2, Y12, Y2

	VMOVDQU	    64(SI), Y3
	VMOVDQU	    64(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y3, Y0, Y12
	VPCMPEQQ	Y3, Y0, Y3
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y3,Y11, Y3
    VPOR        Y3, Y12, Y3

	VMOVDQU	    96(SI), Y4
	VMOVDQU	    96(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y4, Y0, Y12
	VPCMPEQQ	Y4, Y0, Y4
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y4,Y11, Y4
    VPOR        Y4, Y12, Y4

	VMOVDQU	    128(SI), Y5
	VMOVDQU	    128(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y5, Y0, Y12
	VPCMPEQQ	Y5, Y0, Y5
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y5,Y11, Y5
    VPOR        Y5, Y12, Y5

	VMOVDQU	    160(SI), Y6
	VMOVDQU	    160(BP), Y11
	VPXOR		Y14, Y11, Y11 
    
	VPCMPGTQ	Y6, Y0, Y12
	VPCMPEQQ	Y6, Y0, Y6
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y6,Y11, Y6
    VPOR        Y6, Y12, Y6

	VMOVDQU	    192(SI), Y7
	VMOVDQU	    192(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y7, Y0, Y12
	VPCMPEQQ	Y7, Y0, Y7
	VPCMPGTQ	Y11, Y15, Y11
    VPAND       Y7,Y11, Y7
    VPOR        Y7, Y12, Y7

	VMOVDQU	    224(SI), Y8
	VMOVDQU	    224(BP), Y11
	VPXOR		Y14, Y11, Y11 

	VPCMPGTQ	Y8, Y0, Y12
	VPCMPEQQ	Y8, Y0, Y8
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

    NOTL        AX
	MOVL		AX, (DI)(CX*1)    // write the 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$256, BP
	ADDQ		$4, CX
	JZ		 	exit_avx
	JMP		 	loop_avx

exit_avx:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
	MOVQ	R9, ret+88(FP)
	RET

// func cmp_i128_bw_x2(src Int128LLSlice, a, b Int128, bits []byte) int64
//
// input:
//   SI = src_X0_base (upper qwords)
//   BP = src_X1_base (lower qwords)
//   DI = bits_base
//   BX = src_X0_len
//   (Y0,Y15) = upper bound: b
//   (Y13,Y14) = lower bound: a
// internal:
//   AX = intermediate
//   R9 = population count
//   Y9 = permute control mask
//   Y10 = shuffle control mask
//   Y12 = bit mask for sign bit (used for unsigned comparision)
//   Y1-Y8 = vector data
TEXT ·cmp_i128_bw_x2(SB), NOSPLIT, $0-112
	MOVQ	src_X0_base+0(FP), SI
    MOVQ    src_X1_base+24(FP), BP
	MOVQ	src_X0_len+8(FP), BX
	MOVQ	bits_base+80(FP), DI
	XORQ	R9, R9

	CMPQ	BX, $31     // function handles only blocks of 32 values
    JBE		done        // smaller slices and tails are not handled

prep_avx:
   	VPCMPEQQ		Y12, Y12, Y12           // create 0x8000.. mask
	VPSLLQ			$63, Y12, Y12           // create 0x8000.. mask

	VBROADCASTSD    a_0+48(FP), Y13           // load upper qword of a into AVX2 reg
	VBROADCASTSD    a_1+56(FP), Y14           // load lower qword of a into AVX2 reg
	VPXOR			Y12, Y14, Y14           // flip sign bit of lower qword
	VBROADCASTSD    b_0+64(FP), Y0            // load upper qword of b into AVX2 reg
	VBROADCASTSD    b_1+72(FP), Y15           // load lower qword of b into AVX2 reg
	VPXOR			Y12, Y15, Y15           // flip sign bit of lower qword
    
	VMOVDQU		    crosslane<>+0x00(SB), Y9    // load permute control mask
	VMOVDQU		    shuffle64<>+0x00(SB), Y10   // load shuffle control mask

    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffe0, CX     // number of values processed in big blocks
    ANDQ    $0x1f, BX                   // number of values processed in small blocks/scalar
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

loop_avx:
    VMOVDQU	    0(SI), Y1
	VMOVDQU	    0(BP), Y11
	VPXOR		Y12, Y11, Y11       // flip sign bits

    // v < a
	VPCMPGTQ	Y1, Y13, Y5         // Y1 < Y13?  (signed)
	VPCMPEQQ	Y1, Y13, Y6          // Y1 == Y13?
	VPCMPGTQ	Y11, Y14, Y7       // Y11 < Y14 (unsigned)
    VPAND       Y6, Y7, Y6
    VPOR        Y6, Y5, Y5

    // v > b
	VPCMPGTQ	Y0, Y1, Y6          // Y1 > Y0?  (signed)
	VPCMPEQQ	Y0, Y1, Y1          // Y1 == Y0?
	VPCMPGTQ	Y15, Y11, Y11       // Y11 > Y15 (unsigned)
    VPAND       Y1, Y11, Y1
    VPOR        Y1, Y6, Y1

    VPOR        Y1, Y5, Y1          // v < a or v > b
    
	VMOVDQU	    32(SI), Y2
	VMOVDQU	    32(BP), Y11
	VPXOR		Y12, Y11, Y11

	VPCMPGTQ	Y2, Y13, Y5 
	VPCMPEQQ	Y2, Y13, Y6
	VPCMPGTQ	Y11, Y14, Y7
    VPAND       Y6, Y7, Y6
    VPOR        Y5, Y6, Y5

	VPCMPGTQ	Y0, Y2, Y6
	VPCMPEQQ	Y0, Y2, Y2
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y2, Y11, Y2
    VPOR        Y2, Y6, Y2

    VPOR        Y2, Y5, Y2

	VMOVDQU	    64(SI), Y3
	VMOVDQU	    64(BP), Y11
	VPXOR		Y12, Y11, Y11

	VPCMPGTQ	Y3, Y13, Y5 
	VPCMPEQQ	Y3, Y13, Y6
	VPCMPGTQ	Y11, Y14, Y7
    VPAND       Y6, Y7, Y6
    VPOR        Y6, Y5, Y5

	VPCMPGTQ	Y0, Y3, Y6
	VPCMPEQQ	Y0, Y3, Y3
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y3, Y11, Y3
    VPOR        Y3, Y6, Y3

    VPOR        Y3, Y5, Y3

	VMOVDQU	   96(SI), Y4
	VMOVDQU	   96(BP), Y11
	VPXOR		Y12, Y11, Y11

	VPCMPGTQ	Y4, Y13, Y5 
	VPCMPEQQ	Y4, Y13, Y6
	VPCMPGTQ	Y11, Y14, Y7
    VPAND       Y6, Y7, Y6
    VPOR        Y6, Y5, Y5

	VPCMPGTQ	Y0, Y4, Y6
	VPCMPEQQ	Y0, Y4, Y4
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y4, Y11, Y4
    VPOR        Y4, Y6, Y4

    VPOR        Y4, Y5, Y4

	VPACKSSDW	Y1, Y2, Y1
	VPACKSSDW	Y3, Y4, Y3
	VPACKSSDW	Y1, Y3, Y1

	VMOVDQU	   128(SI), Y5
	VMOVDQU	   128(BP), Y11
	VPXOR		Y12, Y11, Y11

	VPCMPGTQ	Y5, Y13, Y2 
	VPCMPEQQ	Y5, Y13, Y3
	VPCMPGTQ	Y11, Y14, Y4
    VPAND       Y3, Y4, Y3
    VPOR        Y3, Y2, Y2

	VPCMPGTQ	Y0, Y5, Y3
	VPCMPEQQ	Y0, Y5, Y5
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y5, Y11, Y5
    VPOR        Y5, Y3, Y5

    VPOR        Y5, Y2, Y5

	VMOVDQU	   160(SI), Y6
	VMOVDQU	   160(BP), Y11
	VPXOR		Y12, Y11, Y11

	VPCMPGTQ	Y6, Y13, Y2 
	VPCMPEQQ	Y6, Y13, Y3
	VPCMPGTQ	Y11, Y14, Y4
    VPAND       Y3, Y4, Y3
    VPOR        Y3, Y2, Y2

	VPCMPGTQ	Y0, Y6, Y3
	VPCMPEQQ	Y0, Y6, Y6
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y6, Y11, Y6
    VPOR        Y6, Y3, Y6

    VPOR        Y6, Y2, Y6

	VMOVDQU	   192(SI), Y7
	VMOVDQU	   192(BP), Y11
	VPXOR		Y12, Y11, Y11

	VPCMPGTQ	Y7, Y13, Y2 
	VPCMPEQQ	Y7, Y13, Y3
	VPCMPGTQ	Y11, Y14, Y4
    VPAND       Y3, Y4, Y3
    VPOR        Y3, Y2, Y2

	VPCMPGTQ	Y0, Y7, Y3
	VPCMPEQQ	Y0, Y7, Y7
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y7, Y11, Y7
    VPOR        Y7, Y3, Y7

    VPOR        Y7, Y2, Y7

	VMOVDQU	   224(SI), Y8
	VMOVDQU	   224(BP), Y11
	VPXOR		Y12, Y11, Y11

	VPCMPGTQ	Y8, Y13, Y2 
	VPCMPEQQ	Y8, Y13, Y3
	VPCMPGTQ	Y11, Y14, Y4
    VPAND       Y3, Y4, Y3
    VPOR        Y3, Y2, Y2

	VPCMPGTQ	Y0, Y8, Y3
	VPCMPEQQ	Y0, Y8, Y8
	VPCMPGTQ	Y15, Y11, Y11
    VPAND       Y8, Y11, Y8
    VPOR        Y8, Y3, Y8

    VPOR        Y8, Y2, Y8

	VPACKSSDW	Y5, Y6, Y5
	VPACKSSDW	Y7, Y8, Y7
	VPACKSSDW	Y5, Y7, Y5
	VPACKSSWB	Y1, Y5, Y1
  	VPERMD		Y1, Y9, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX      // move per byte MSBs into packed bitmask to r32 or r64

    NOTL        AX
	MOVL		AX, (DI)(CX*1)    // write the 32 bits to the output slice
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9

	ADDQ		$256, SI
	ADDQ		$256, BP
	ADDQ		$4, CX
	JZ		 	exit_avx
	JMP		 	loop_avx

exit_avx:
	VZEROUPPER           // clear upper part of Y regs, prevents AVX-SSE penalty

done:
	MOVQ	R9, ret+104(FP)
	RET
    
