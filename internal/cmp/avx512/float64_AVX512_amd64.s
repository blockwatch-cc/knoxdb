// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants_AVX512.h"

// func cmp_f64_eq_x5(src []float64, val float64, bits []byte) int64
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
TEXT ·cmp_f64_eq_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSD    val+24(FP), Z0            // load val into AVX512 reg

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
    
// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VCMPPD		$0, (SI), Z0, K1     // $0 means compare equal
    
	VCMPPD		$0, 64(SI), Z0, K2     
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VCMPPD		$0, 128(SI), Z0, K3  
    KSHIFTLQ    $16, K3, K3
    KORQ        K1, K3, K1

	VCMPPD		$0, 192(SI), Z0, K4  
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VCMPPD		$0, 256(SI), Z0, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VCMPPD		$0, 320(SI), Z0, K6  
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VCMPPD		$0, 384(SI), Z0, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VCMPPD		$0, 448(SI), Z0, K2  
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
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VCMPPD		$0, (SI), Z0, K2, K1
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

// func cmp_f64_ne_x5(src []float64, val float64, bits []byte) int64
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
TEXT ·cmp_f64_ne_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSD    val+24(FP), Z0            // load val into AVX512 reg

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

// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VCMPPD		$4, (SI), Z0, K1     // $4 means compare not equal
    
	VCMPPD		$4, 64(SI), Z0, K2     
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VCMPPD		$4, 128(SI), Z0, K3  
    KSHIFTLQ    $16, K3, K3
    KORQ        K1, K3, K1

	VCMPPD		$4, 192(SI), Z0, K4  
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VCMPPD		$4, 256(SI), Z0, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VCMPPD		$4, 320(SI), Z0, K6  
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VCMPPD		$4, 384(SI), Z0, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VCMPPD		$4, 448(SI), Z0, K2  
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
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VCMPPD		$4, (SI), Z0, K2, K1
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

// func cmp_f64_lt_x5(src []float64, val float64, bits []byte) int64
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
TEXT ·cmp_f64_lt_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSD    val+24(FP), Z0            // load val into AVX512 reg

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

// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VCMPPD		$30, (SI), Z0, K1     // $30 means greater than, but operands are switched
    
	VCMPPD		$30, 64(SI), Z0, K2     
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VCMPPD		$30, 128(SI), Z0, K3  
    KSHIFTLQ    $16, K3, K3
    KORQ        K1, K3, K1

	VCMPPD		$30, 192(SI), Z0, K4  
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VCMPPD		$30, 256(SI), Z0, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VCMPPD		$30, 320(SI), Z0, K6  
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VCMPPD		$30, 384(SI), Z0, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VCMPPD		$30, 448(SI), Z0, K2  
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
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VCMPPD		$30, (SI), Z0, K2, K1
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

// func cmp_f64_le_x5(src []float64, val float64, bits []byte) int64
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
TEXT ·cmp_f64_le_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSD    val+24(FP), Z0            // load val into AVX512 reg

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

// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VCMPPD		$29, (SI), Z0, K1     // $29 means greater equal, but operands are switched
    
	VCMPPD		$29, 64(SI), Z0, K2     
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VCMPPD		$29, 128(SI), Z0, K3  
    KSHIFTLQ    $16, K3, K3
    KORQ        K1, K3, K1

	VCMPPD		$29, 192(SI), Z0, K4  
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VCMPPD		$29, 256(SI), Z0, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VCMPPD		$29, 320(SI), Z0, K6  
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VCMPPD		$29, 384(SI), Z0, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VCMPPD		$29, 448(SI), Z0, K2  
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
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VCMPPD		$29, (SI), Z0, K2, K1
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

// func cmp_f64_gt_x5(src []float64, val float64, bits []byte) int64
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
TEXT ·cmp_f64_gt_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSD    val+24(FP), Z0            // load val into AVX512 reg

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

// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VCMPPD		$17, (SI), Z0, K1     // $17 means less than, but operands are switched
    
	VCMPPD		$17, 64(SI), Z0, K2     
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VCMPPD		$17, 128(SI), Z0, K3  
    KSHIFTLQ    $16, K3, K3
    KORQ        K1, K3, K1

	VCMPPD		$17, 192(SI), Z0, K4  
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VCMPPD		$17, 256(SI), Z0, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VCMPPD		$17, 320(SI), Z0, K6  
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VCMPPD		$17, 384(SI), Z0, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VCMPPD		$17, 448(SI), Z0, K2  
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
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VCMPPD		$17, (SI), Z0, K2, K1
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

// func cmp_f64_ge_x5(src []float64, val float64, bits []byte) int64
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
TEXT ·cmp_f64_ge_x5(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VBROADCASTSD    val+24(FP), Z0            // load val into AVX512 reg

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

// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VCMPPD		$18, (SI), Z0, K1     // $18 means less equal, but operands are switched
    
	VCMPPD		$18, 64(SI), Z0, K2     
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VCMPPD		$18, 128(SI), Z0, K3  
    KSHIFTLQ    $16, K3, K3
    KORQ        K1, K3, K1

	VCMPPD		$18, 192(SI), Z0, K4  
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VCMPPD		$18, 256(SI), Z0, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VCMPPD		$18, 320(SI), Z0, K6  
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VCMPPD		$18, 384(SI), Z0, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VCMPPD		$18, 448(SI), Z0, K2  
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
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VCMPPD		$18, (SI), Z0, K2, K1
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

// func cmp_f64_bw_x5(src []float64, a, b float64, bits []byte) int64
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
TEXT ·cmp_f64_bw_x5(SB), NOSPLIT, $0-72
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+40(FP), DI
	XORQ	R9, R9
	VBROADCASTSD    a+24(FP), Z0            // load a into AVX512 reg
	VBROADCASTSD    b+32(FP), Z12            // load a into AVX512 reg

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

// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VMOVDQU64  	(SI), Z1 
	VCMPPD		$29, Z0, Z1, K1      // => a ?
	VCMPPD		$18, Z12, Z1, K2      // =< b ?
    KANDB       K1, K2, K1
    
	VMOVDQU64 	64(SI), Z2
	VCMPPD		$29, Z0, Z2, K2     
	VCMPPD		$18, Z12, Z2, K3     
    KANDB       K2, K3, K2
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VMOVDQU64	128(SI), Z3
	VCMPPD		$29, Z0, Z3, K3  
	VCMPPD		$18, Z12, Z3, K4
    KANDB       K3, K4, K3  
    KSHIFTLQ    $16, K3, K3
    KORQ        K1, K3, K1

	VMOVDQU64 	192(SI), Z4
	VCMPPD		$29, Z0, Z4, K4  
	VCMPPD		$18, Z12, Z4, K5
    KANDB       K4, K5, K4  
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VMOVDQU64 	256(SI), Z5 
	VCMPPD		$29, Z0, Z5, K5
	VCMPPD		$18, Z12, Z5, K6
    KANDB       K5, K6, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VMOVDQU64	320(SI), Z6
	VCMPPD		$29, Z0, Z6, K6  
	VCMPPD		$18, Z12, Z6, K7
    KANDB       K6, K7, K6  
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VMOVDQU64	384(SI), Z7
	VCMPPD		$29, Z0, Z7, K7
	VCMPPD		$18, Z12, Z7, K2
    KANDB       K7, K2, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VMOVDQU64	448(SI), Z8
	VCMPPD		$29, Z0, Z8, K2  
	VCMPPD		$18, Z12, Z8, K3
    KANDB       K2, K3, K2  
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
	VPCMPUQ	    $2, Z11, Z9, K2     // mask lower equal than BX

	VMOVDQU64 	(SI), K2, Z1 
	VCMPPD		$29, Z0, Z1, K2, K1   // => a ?
	VCMPPD		$18, Z12, Z1, K2, K3  // =< b ?
    KANDB       K1, K3, K1
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
