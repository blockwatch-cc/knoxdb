// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants_AVX512.h"

// func cmp_f32_eq_x5(src []float32, val float32, bits []byte) int64
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
TEXT ·cmp_f32_eq_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSS    val+24(FP), Z0            // load val into AVX512 reg

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
    
// works for >= 64 float32 (i.e. 256 bytes of data)
loop_big:
	VCMPPS		$0, (SI), Z0, K1     // $0 means compare equal
    
	VCMPPS		$0, 64(SI), Z0, K2     
    KSHIFTLQ    $16, K2, K2
    KORQ        K1, K2, K1

	VCMPPS		$0, 128(SI), Z0, K3  
    KSHIFTLQ    $32, K3, K3
    KORQ        K1, K3, K1

	VCMPPS		$0, 192(SI), Z0, K4  
    KSHIFTLQ    $48, K4, K4
    KORQ        K1, K4, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$256, SI
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
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VCMPPS		$0, (SI), Z0, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$32, SI
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

// func cmp_f32_ne_x5(src []float32, val float32, bits []byte) int64
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
TEXT ·cmp_f32_ne_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSS    val+24(FP), Z0            // load val into AVX512 reg

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

// works for >= 64 float32 (i.e. 256 bytes of data)
loop_big:
	VCMPPS		$4, (SI), Z0, K1     // $4 means compare not equal
    
	VCMPPS		$4, 64(SI), Z0, K2     
    KSHIFTLQ    $16, K2, K2
    KORQ        K1, K2, K1

	VCMPPS		$4, 128(SI), Z0, K3  
    KSHIFTLQ    $32, K3, K3
    KORQ        K1, K3, K1

	VCMPPS		$4, 192(SI), Z0, K4  
    KSHIFTLQ    $48, K4, K4
    KORQ        K1, K4, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
    ADDQ		AX, R9
	ADDQ		$256, SI
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
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VCMPPS		$4, (SI), Z0, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$32, SI
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

// func cmp_f32_lt_x5(src []float32, val float32, bits []byte) int64
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
TEXT ·cmp_f32_lt_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSS    val+24(FP), Z0            // load val into AVX512 reg

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

// works for >= 64 float32 (i.e. 256 bytes of data)
loop_big:
	VCMPPS		$30, (SI), Z0, K1     // $30 means greater than, but operands are switched
    
	VCMPPS		$30, 64(SI), Z0, K2     
    KSHIFTLQ    $16, K2, K2
    KORQ        K1, K2, K1

	VCMPPS		$30, 128(SI), Z0, K3  
    KSHIFTLQ    $32, K3, K3
    KORQ        K1, K3, K1

	VCMPPS		$30, 192(SI), Z0, K4  
    KSHIFTLQ    $48, K4, K4
    KORQ        K1, K4, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$256, SI
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
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VCMPPS		$30, (SI), Z0, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$32, SI
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

// func cmp_f32_le_x5(src []float32, val float32, bits []byte) int64
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
TEXT ·cmp_f32_le_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSS    val+24(FP), Z0            // load val into AVX512 reg

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

// works for >= 64 float32 (i.e. 256 bytes of data)
loop_big:
	VCMPPS		$29, (SI), Z0, K1     // $29 means greater equal, but operands are switched
    
	VCMPPS		$29, 64(SI), Z0, K2     
    KSHIFTLQ    $16, K2, K2
    KORQ        K1, K2, K1

	VCMPPS		$29, 128(SI), Z0, K3  
    KSHIFTLQ    $32, K3, K3
    KORQ        K1, K3, K1

	VCMPPS		$29, 192(SI), Z0, K4  
    KSHIFTLQ    $48, K4, K4
    KORQ        K1, K4, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$256, SI
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
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VCMPPS		$29, (SI), Z0, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$32, SI
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

// func cmp_f32_gt_x5(src []float32, val float32, bits []byte) int64
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
TEXT ·cmp_f32_gt_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSS    val+24(FP), Z0            // load val into AVX512 reg

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

// works for >= 64 float32 (i.e. 256 bytes of data)
loop_big:
	VCMPPS		$17, (SI), Z0, K1     // $17 means less than, but operands are switched
    
	VCMPPS		$17, 64(SI), Z0, K2     
    KSHIFTLQ    $16, K2, K2
    KORQ        K1, K2, K1

	VCMPPS		$17, 128(SI), Z0, K3  
    KSHIFTLQ    $32, K3, K3
    KORQ        K1, K3, K1

	VCMPPS		$17, 192(SI), Z0, K4  
    KSHIFTLQ    $48, K4, K4
    KORQ        K1, K4, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$256, SI
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
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VCMPPS		$17, (SI), Z0, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$32, SI
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

// func cmp_f32_ge_x5(src []float32, val float32, bits []byte) int64
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
TEXT ·cmp_f32_ge_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSS    val+24(FP), Z0            // load val into AVX512 reg

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

// works for >= 64 float32 (i.e. 256 bytes of data)
loop_big:
	VCMPPS		$18, (SI), Z0, K1     // $18 means less equal, but operands are switched
    
	VCMPPS		$18, 64(SI), Z0, K2     
    KSHIFTLQ    $16, K2, K2
    KORQ        K1, K2, K1

	VCMPPS		$18, 128(SI), Z0, K3  
    KSHIFTLQ    $32, K3, K3
    KORQ        K1, K3, K1

	VCMPPS		$18, 192(SI), Z0, K4  
    KSHIFTLQ    $48, K4, K4
    KORQ        K1, K4, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$256, SI
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
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VCMPPS		$18, (SI), Z0, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$32, SI
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

// func cmp_f32_bw_x5(src []float32, a, b float32, bits []byte) int64
//
// input:
//   SI = src_base
//   DI = bits_base
//   BX = src_len
//   Z0 = lower comparison value
//   Z12 = upper comparison value
// internal:
//   AX = intermediate
//   R9 = population count
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·cmp_f32_bw_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSS    a+24(FP), Z0            // load a into AVX512 reg
	VBROADCASTSS    b+28(FP), Z12            // load a into AVX512 reg

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

// works for >= 64 float32 (i.e. 256 bytes of data)
loop_big:
	VMOVDQU64  	(SI), Z1 
	VCMPPS		$29, Z0, Z1, K1      // => a ?
	VCMPPS		$18, Z12, Z1, K2      // =< b ?
    KANDW       K1, K2, K1
    
	VMOVDQU64	64(SI), Z2
	VCMPPS		$29, Z0, Z2, K2     
	VCMPPS		$18, Z12, Z2, K3     
    KANDW       K2, K3, K2
    KSHIFTLQ    $16, K2, K2
    KORQ        K1, K2, K1

	VMOVDQU64	128(SI), Z3
	VCMPPS		$29, Z0, Z3, K3  
	VCMPPS		$18, Z12, Z3, K4
    KANDW       K3, K4, K3  
    KSHIFTLQ    $32, K3, K3
    KORQ        K1, K3, K1

	VMOVDQU64	192(SI), Z4
	VCMPPS		$29, Z0, Z4, K4  
	VCMPPS		$18, Z12, Z4, K5
    KANDW       K4, K5, K4  
    KSHIFTLQ    $48, K4, K4
    KORQ        K1, K4, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$256, SI
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
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VMOVDQU64 	0(SI), K2, Z1 
	VCMPPS		$29, Z0, Z1, K2, K1   // => a ?
	VCMPPS		$18, Z12, Z1, K2, K3  // =< b ?
    KANDB       K1, K3, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$32, SI
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
