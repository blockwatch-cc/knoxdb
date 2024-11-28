// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build !appengine && gc && !purego && !noasm

#include "textflag.h"
#include "constants_AVX512.h"

// func cmp_u8_eq_x5(src []uint8, val uint8, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   Z0 = comparison value
// internal:
//   AX = intermediate
//   R9 = population count
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_u8_eq_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTB    val+24(FP), Z0            // load val into AVX512 reg

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $63      // slices smaller than 64 values are handled separately
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks
    ADDQ    CX, SI                      // move SI to the end of the array
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
// works for >= 64 uint8 (i.e. 64 bytes of data)
loop_big:
	VPCMPEQB	(SI)(CX*8), Z0, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

// here we process 8 values (8 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX
    
	VPCMPEQB	(SI), Z0, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$8, SI
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
    
// func cmp_u8_ne_x5(src []uint8, val uint8, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   Z0 = comparison value
// internal:
//   AX = intermediate
//   R9 = population count
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_u8_ne_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTB    val+24(FP), Z0            // load val into AVX512 reg

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $63      // slices smaller than 64 values are handled separately
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks
    ADDQ    CX, SI                      // move SI to the end of the array
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
// works for >= 64 uint8 (i.e. 64 bytes of data)
loop_big:
	VPCMPEQB	(SI)(CX*8), Z0, K1
    
    KNOTQ       K1, K1              // make EQ to NE
	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

// here we process 8 values (8 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VPCMPEQB	(SI), Z0, K2, K1
    KNOTB       K1, K1      // make EQ to NE
    KANDB       K1, K2, K1  // delete the unused bits
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$8, SI
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

// func cmp_u8_lt_x5(src []uint8, val uint8, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   Z0 = comparison value
// internal:
//   AX = intermediate
//   R9 = population count
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_u8_lt_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTB    val+24(FP), Z0            // load val into AVX512 reg

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $63      // slices smaller than 64 values are handled separately
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks
    ADDQ    CX, SI                      // move SI to the end of the array
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
// works for >= 64 uint8 (i.e. 64 bytes of data)
loop_big:
	VPCMPUB	    $6, (SI)(CX*8), Z0, K1    // $6 means not less equal, but operands are switched
    
	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

// here we process 8 values (8 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VPCMPUB	    $6, (SI), Z0, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$8, SI
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

// func cmp_u8_le_x5(src []uint8, val uint8, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   Z0 = comparison value
// internal:
//   AX = intermediate
//   R9 = population count
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_u8_le_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTB    val+24(FP), Z0            // load val into AVX512 reg

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $63      // slices smaller than 64 values are handled separately
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks
    ADDQ    CX, SI                      // move SI to the end of the array
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
// works for >= 64 uint8 (i.e. 64 bytes of data)
loop_big:
	VPCMPUB	    $5, (SI)(CX*8), Z0, K1      // $5 means not less than, but operands are switched
 
 	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

// here we process 8 values (8 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11             // broadcast BX
	VPCMPUQ	        $2, Z11, Z9, K2     // mask lower equal than BX
 
	VPCMPUB	    $5, (SI), Z0, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$8, SI
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

// func cmp_u8_gt_x5(src []uint8, val uint8, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   Z0 = comparison value
// internal:
//   AX = intermediate
//   R9 = population count
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_u8_gt_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTB    val+24(FP), Z0            // load val into AVX512 reg

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $63      // slices smaller than 64 values are handled separately
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks
    ADDQ    CX, SI                      // move SI to the end of the array
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
// works for >= 64 uint8 (i.e. 64 bytes of data)
loop_big:
	VPCMPUB	    $1, (SI)(CX*8), Z0, K1    // $1 means less than, but operands are switched
 
	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

// here we process 8 values (8 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11             // broadcast BX
	VPCMPUQ	        $2, Z11, Z9, K2     // mask lower equal than BX

	VPCMPUB	    $1, (SI), Z0, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$8, SI
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

// func cmp_u8_ge_x5(src []uint8, val uint8, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   Z0 = comparison value
// internal:
//   AX = intermediate
//   R9 = population count
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_u8_ge_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTB    val+24(FP), Z0            // load val into AVX512 reg

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $63      // slices smaller than 64 values are handled separately
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks
    ADDQ    CX, SI                      // move SI to the end of the array
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
// works for >= 64 uint8 (i.e. 64 bytes of data)
loop_big:
	VPCMPUB	    $2, (SI)(CX*8), Z0, K1    // $2 means less equal, but operands are switched
    
	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

// here we process 8 values (8 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX
 
	VPCMPUB	    $2, (SI), Z0, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$8, SI
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

// func cmp_u8_bw_x5(src []uint64, a, b uint64, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
// internal:
//   Z0 = comparison value
//   AX = intermediate
//   R9 = population count
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_u8_bw_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

    MOVQ            $1, AX
	VPBROADCASTB 	AX, Z13                  // 1 into AVX512 reg
	VPBROADCASTB 	a+24(FP), Z12            // load val a into AVX512 reg
	VPBROADCASTB 	b+25(FP), Z0             // load val b into AVX512 reg
	VPSUBB			Z12, Z0, Z0              // compute diff
	VPADDB			Z13, Z0, Z0              // Z0 = a - b + 1

	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $63      // slices smaller than 64 values are handled separately
	JBE		prep_small

prep_big:
    MOVQ    BX, CX
    ANDQ    $0xffffffffffffffc0, CX     // number of values processed in big blocks
    ANDQ    $0x3f, BX                   // number of values processed in small blocks
    ADDQ    CX, SI                      // move SI to the end of the array
    SHRQ    $3, CX                      // number of bytes to write to output slice (div by 8)
    ADDQ    CX, DI                      // move DI to the end of the array
    NEGQ    CX
    
// works for >= 64 uint64 (i.e. 512 bytes of data)
loop_big:
    VMOVDQU64   (SI)(CX*8), Z1
	VPSUBB   	Z12, Z1, Z1
	VPCMPUB	    $1, Z0, Z1, K1    // $1 means compare less than
    
	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

// here we process 8 values (8 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

    VMOVDQU8    (SI), Z1
	VPSUBB		Z12, Z1, K2, Z1
	VPCMPUB	    $1, Z0, Z1, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$8, SI
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
