// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants_AVX512.h"

// func matchInt64EqualAVX512(src []int64, val int64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   Z0 = comparison value
// internal:
//   AX = intermediate
//   R9 = population count
//   Z10 = permute control mask
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·matchInt64EqualAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSD    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		shuffle64<>+0x00(SB), Z10    // load shuffle control mask

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $63      // slices smaller than 64 values are handled separately
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
// works for >= 64 int64 (i.e. 512 bytes of data)
loop_big:
	VPERMQ   	0(SI), Z10, Z1 
	VPCMPEQQ	Z1, Z0, K1
    
	VPERMQ   	64(SI), Z10, Z2
	VPCMPEQQ	Z2, Z0, K2
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VPERMQ   	128(SI), Z10, Z3
	VPCMPEQQ	Z3, Z0, K3
    KSHIFTLD    $16, K3, K3
    KORQ        K1, K3, K1

	VPERMQ   	192(SI), Z10, Z4
	VPCMPEQQ	Z4, Z0, K4
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VPERMQ   	256(SI), Z10, Z5 
	VPCMPEQQ	Z5, Z0, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VPERMQ   	320(SI), Z10, Z6
	VPCMPEQQ	Z6, Z0, K6
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VPERMQ   	384(SI), Z10, Z7
	VPCMPEQQ	Z7, Z0, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VPERMQ   	448(SI), Z10, Z8
	VPCMPEQQ	Z8, Z0, K2
    KSHIFTLQ    $56, K2, K2
    KORQ        K1, K2, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMQ.Z   	0(SI), Z10, K2, Z1 
	VPCMPEQQ	Z1, Z0, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$64, SI
	ADDQ		$1, DI
	SUBQ		$8, BX 
	JBE		 	exit_small
	JMP		 	loop_small    
    
exit_small:
    // nothings to do
    
done:
	VZEROUPPER           // clear upper part of Z regs, prevents AVX-SSE penalty
	MOVQ	R9, ret+56(FP)
	RET

// func matchInt64NotEqualAVX512(src []int64, val int64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   Z0 = comparison value
// internal:
//   AX = intermediate
//   R9 = population count
//   Z10 = permute control mask
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·matchInt64NotEqualAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSD    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		shuffle64<>+0x00(SB), Z10    // load shuffle control mask

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $63      // slices smaller than 64 values are handled separately
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 64 int64 (i.e. 512 bytes of data)
loop_big:
	VPERMQ   	0(SI), Z10, Z1 
	VPCMPEQQ	Z1, Z0, K1
    
	VPERMQ   	64(SI), Z10, Z2
	VPCMPEQQ	Z2, Z0, K2
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VPERMQ   	128(SI), Z10, Z3
	VPCMPEQQ	Z3, Z0, K3
    KSHIFTLD    $16, K3, K3
    KORQ        K1, K3, K1

	VPERMQ   	192(SI), Z10, Z4
	VPCMPEQQ	Z4, Z0, K4
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VPERMQ   	256(SI), Z10, Z5 
	VPCMPEQQ	Z5, Z0, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VPERMQ   	320(SI), Z10, Z6
	VPCMPEQQ	Z6, Z0, K6
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VPERMQ   	384(SI), Z10, Z7
	VPCMPEQQ	Z7, Z0, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VPERMQ   	448(SI), Z10, Z8
	VPCMPEQQ	Z8, Z0, K2
    KSHIFTLQ    $56, K2, K2
    KORQ        K1, K2, K1

    KNOTQ       K1, K1              // make EQ to NE
	KMOVQ		K1, (DI)(CX*1)      // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMQ.Z   	0(SI), Z10, K2, Z1 
	VPCMPEQQ	Z1, Z0, K2, K1
    KNOTB       K1, K1      // make EQ to NE
    KANDB       K1, K2, K1  // delete the unused bits
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$64, SI
	ADDQ		$1, DI
	SUBQ		$8, BX 
	JBE		 	exit_small
	JMP		 	loop_small    
    
exit_small:
    // nothings to do
    
done:
	VZEROUPPER           // clear upper part of Z regs, prevents AVX-SSE penalty
	MOVQ	R9, ret+56(FP)
	RET

// func matchInt64LessThanAVX512(src []int64, val int64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   Z0 = comparison value
// internal:
//   AX = intermediate
//   R9 = population count
//   Z10 = permute control mask
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·matchInt64LessThanAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSD    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		shuffle64<>+0x00(SB), Z10    // load shuffle control mask

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $63      // slices smaller than 64 values are handled separately
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 64 int64 (i.e. 512 bytes of data)
loop_big:
	VPERMQ   	0(SI), Z10, Z1 
	VPCMPQ	    $1, Z0, Z1, K1    // $1 means compare less than
    
	VPERMQ   	64(SI), Z10, Z2
	VPCMPQ	    $1, Z0, Z2, K2
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VPERMQ   	128(SI), Z10, Z3
	VPCMPQ	    $1, Z0, Z3, K3
    KSHIFTLD    $16, K3, K3
    KORQ        K1, K3, K1

	VPERMQ   	192(SI), Z10, Z4
	VPCMPQ	    $1, Z0, Z4, K4
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VPERMQ   	256(SI), Z10, Z5 
	VPCMPQ	    $1, Z0, Z5, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VPERMQ   	320(SI), Z10, Z6
	VPCMPQ	    $1, Z0, Z6, K6
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VPERMQ   	384(SI), Z10, Z7
	VPCMPQ	    $1, Z0, Z7, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VPERMQ   	448(SI), Z10, Z8
	VPCMPQ	    $1, Z0, Z8, K2
    KSHIFTLQ    $56, K2, K2
    KORQ        K1, K2, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMQ.Z   	0(SI), Z10, K2, Z1 
	VPCMPQ	    $1, Z0, Z1, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$64, SI
	ADDQ		$1, DI
	SUBQ		$8, BX 
	JBE		 	exit_small
	JMP		 	loop_small    
    
exit_small:
    // nothings to do
    
done:
	VZEROUPPER           // clear upper part of Z regs, prevents AVX-SSE penalty
	MOVQ	R9, ret+56(FP)
	RET

// func matchInt64LessThanEqualAVX512(src []int64, val int64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   Z0 = comparison value
// internal:
//   AX = intermediate
//   R9 = population count
//   Z10 = permute control mask
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·matchInt64LessThanEqualAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSD    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		shuffle64<>+0x00(SB), Z10    // load shuffle control mask

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $63      // slices smaller than 64 values are handled separately
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 64 int64 (i.e. 512 bytes of data)
loop_big:
	VPERMQ   	0(SI), Z10, Z1 
	VPCMPQ	    $2, Z0, Z1, K1    // $2 means compare less equal
    
	VPERMQ   	64(SI), Z10, Z2
	VPCMPQ	    $2, Z0, Z2, K2
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VPERMQ   	128(SI), Z10, Z3
	VPCMPQ	    $2, Z0, Z3, K3
    KSHIFTLD    $16, K3, K3
    KORQ        K1, K3, K1

	VPERMQ   	192(SI), Z10, Z4
	VPCMPQ	    $2, Z0, Z4, K4
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VPERMQ   	256(SI), Z10, Z5 
	VPCMPQ	    $2, Z0, Z5, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VPERMQ   	320(SI), Z10, Z6
	VPCMPQ	    $2, Z0, Z6, K6
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VPERMQ   	384(SI), Z10, Z7
	VPCMPQ	    $2, Z0, Z7, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VPERMQ   	448(SI), Z10, Z8
	VPCMPQ	    $2, Z0, Z8, K2
    KSHIFTLQ    $56, K2, K2
    KORQ        K1, K2, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMQ.Z   	0(SI), Z10, K2, Z1 
	VPCMPQ	    $2, Z0, Z1, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$64, SI
	ADDQ		$1, DI
	SUBQ		$8, BX 
	JBE		 	exit_small
	JMP		 	loop_small    
    
exit_small:
    // nothings to do
    
done:
	VZEROUPPER           // clear upper part of Z regs, prevents AVX-SSE penalty
	MOVQ	R9, ret+56(FP)
	RET

// func matchInt64GreaterThanAVX512(src []int64, val int64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   Z0 = comparison value
// internal:
//   AX = intermediate
//   R9 = population count
//   Z10 = permute control mask
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·matchInt64GreaterThanAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSD    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		shuffle64<>+0x00(SB), Z10    // load shuffle control mask

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $63      // slices smaller than 64 values are handled separately
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 64 int64 (i.e. 512 bytes of data)
loop_big:
	VPERMQ   	0(SI), Z10, Z1 
	VPCMPQ	    $6, Z0, Z1, K1    // $6 means compare not less equal (or greater than)
    
	VPERMQ   	64(SI), Z10, Z2
	VPCMPQ	    $6, Z0, Z2, K2
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VPERMQ   	128(SI), Z10, Z3
	VPCMPQ	    $6, Z0, Z3, K3
    KSHIFTLD    $16, K3, K3
    KORQ        K1, K3, K1

	VPERMQ   	192(SI), Z10, Z4
	VPCMPQ	    $6, Z0, Z4, K4
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VPERMQ   	256(SI), Z10, Z5 
	VPCMPQ	    $6, Z0, Z5, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VPERMQ   	320(SI), Z10, Z6
	VPCMPQ	    $6, Z0, Z6, K6
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VPERMQ   	384(SI), Z10, Z7
	VPCMPQ	    $6, Z0, Z7, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VPERMQ   	448(SI), Z10, Z8
	VPCMPQ	    $6, Z0, Z8, K2
    KSHIFTLQ    $56, K2, K2
    KORQ        K1, K2, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMQ.Z   	0(SI), Z10, K2, Z1 
	VPCMPQ	    $6, Z0, Z1, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$64, SI
	ADDQ		$1, DI
	SUBQ		$8, BX 
	JBE		 	exit_small
	JMP		 	loop_small    
    
exit_small:
    // nothings to do
    
done:
	VZEROUPPER           // clear upper part of Z regs, prevents AVX-SSE penalty
	MOVQ	R9, ret+56(FP)
	RET

// func matchInt64GreaterThanEqualAVX512(src []int64, val int64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   Z0 = comparison value
// internal:
//   AX = intermediate
//   R9 = population count
//   Z10 = permute control mask
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·matchInt64GreaterThanEqualAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSD    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		shuffle64<>+0x00(SB), Z10    // load shuffle control mask

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $63      // slices smaller than 64 values are handled separately
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX

// works for >= 64 int64 (i.e. 512 bytes of data)
loop_big:
	VPERMQ   	0(SI), Z10, Z1 
	VPCMPQ	    $5, Z0, Z1, K1    // $5 means compare not less (or greater equal)
    
	VPERMQ   	64(SI), Z10, Z2
	VPCMPQ	    $5, Z0, Z2, K2
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VPERMQ   	128(SI), Z10, Z3
	VPCMPQ	    $5, Z0, Z3, K3
    KSHIFTLD    $16, K3, K3
    KORQ        K1, K3, K1

	VPERMQ   	192(SI), Z10, Z4
	VPCMPQ	    $5, Z0, Z4, K4
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VPERMQ   	256(SI), Z10, Z5 
	VPCMPQ	    $5, Z0, Z5, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VPERMQ   	320(SI), Z10, Z6
	VPCMPQ	    $5, Z0, Z6, K6
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VPERMQ   	384(SI), Z10, Z7
	VPCMPQ	    $5, Z0, Z7, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VPERMQ   	448(SI), Z10, Z8
	VPCMPQ	    $5, Z0, Z8, K2
    KSHIFTLQ    $56, K2, K2
    KORQ        K1, K2, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMQ.Z   	0(SI), Z10, K2, Z1 
	VPCMPQ	    $5, Z0, Z1, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$64, SI
	ADDQ		$1, DI
	SUBQ		$8, BX 
	JBE		 	exit_small
	JMP		 	loop_small    
    
exit_small:
    // nothings to do
    
done:
	VZEROUPPER           // clear upper part of Z regs, prevents AVX-SSE penalty
	MOVQ	R9, ret+56(FP)
	RET

// func matchInt64BetweenAVX512(src []int64, a, b int64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
// internal:
//   Z0 = comparison value
//   AX = intermediate
//   R9 = population count
//   Z10 = permute control mask
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·matchInt64BetweenAVX512(SB), NOSPLIT, $0-72
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+40(FP), DI
	XORQ	R9, R9

    MOVQ            $1, AX
	VPBROADCASTQ 	AX, Z13                  // 1 into AVX512 reg
	VBROADCASTSD 	a+24(FP), Z12            // load val a into AVX512 reg
	VBROADCASTSD 	b+32(FP), Z0             // load val b into AVX512 reg
	VPSUBQ			Z12, Z0, Z0              // compute diff
	VPADDQ			Z13, Z0, Z0
	VMOVDQU64		shuffle64<>+0x00(SB), Z10    // load shuffle control mask

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $63      // slices smaller than 64 values are handled separately
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
// works for >= 64 int64 (i.e. 512 bytes of data)
loop_big:
	VPERMQ   	0(SI), Z10, Z1 
	VPSUBQ		Z12, Z1, Z1
	VPCMPUQ	    $1, Z0, Z1, K1    // $5 means compare not less (or greater equal)
    
	VPERMQ   	64(SI), Z10, Z2
	VPSUBQ		Z12, Z2, Z2
	VPCMPUQ	    $1, Z0, Z2, K2
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VPERMQ   	128(SI), Z10, Z3
	VPSUBQ		Z12, Z3, Z3
	VPCMPUQ	    $1, Z0, Z3, K3
    KSHIFTLD    $16, K3, K3
    KORQ        K1, K3, K1

	VPERMQ   	192(SI), Z10, Z4
	VPSUBQ		Z12, Z4, Z4
	VPCMPUQ	    $1, Z0, Z4, K4
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VPERMQ   	256(SI), Z10, Z5 
	VPSUBQ		Z12, Z5, Z5
	VPCMPUQ	    $1, Z0, Z5, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VPERMQ   	320(SI), Z10, Z6
	VPSUBQ		Z12, Z6, Z6
    VPCMPUQ	    $1, Z0, Z6, K6
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VPERMQ   	384(SI), Z10, Z7
	VPSUBQ		Z12, Z7, Z7
	VPCMPUQ	    $1, Z0, Z7, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VPERMQ   	448(SI), Z10, Z8
	VPSUBQ		Z12, Z8, Z8
	VPCMPUQ	    $1, Z0, Z8, K2
    KSHIFTLQ    $56, K2, K2
    KORQ        K1, K2, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$512, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMQ.Z   	0(SI), Z10, K2, Z1 
	VPSUBQ		Z12, Z1, K2, Z1
	VPCMPUQ	    $1, Z0, Z1, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$64, SI
	ADDQ		$1, DI
	SUBQ		$8, BX 
	JBE		 	exit_small
	JMP		 	loop_small    
    
exit_small:
    // nothings to do
    
done:
	VZEROUPPER           // clear upper part of Z regs, prevents AVX-SSE penalty
	MOVQ	R9, ret+64(FP)
	RET
