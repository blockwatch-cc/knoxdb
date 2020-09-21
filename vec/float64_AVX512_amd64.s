// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants_AVX512.h"

// func matchFloat64EqualAVX512(src []float64, val float64, bits []byte) int64
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
TEXT ·matchFloat64EqualAVX512(SB), NOSPLIT, $0-64
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
    
// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VPERMQ   	0(SI), Z10, Z1 
	VCMPPD		$0, Z0, Z1, K1     // $0 means compare equal
    
	VPERMQ   	64(SI), Z10, Z2
	VCMPPD		$0, Z0, Z2, K2     
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VPERMQ   	128(SI), Z10, Z3
	VCMPPD		$0, Z0, Z3, K3  
    KSHIFTLD    $16, K3, K3
    KORQ        K1, K3, K1

	VPERMQ   	192(SI), Z10, Z4
	VCMPPD		$0, Z0, Z4, K4  
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VPERMQ   	256(SI), Z10, Z5 
	VCMPPD		$0, Z0, Z5, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VPERMQ   	320(SI), Z10, Z6
	VCMPPD		$0, Z0, Z6, K6  
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VPERMQ   	384(SI), Z10, Z7
	VCMPPD		$0, Z0, Z7, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VPERMQ   	448(SI), Z10, Z8
	VCMPPD		$0, Z0, Z8, K2  
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
	VCMPPD		$0, Z0, Z1, K2, K1
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

// func matchFloat64NotEqualAVX512(src []float64, val float64, bits []byte) int64
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
TEXT ·matchFloat64NotEqualAVX512(SB), NOSPLIT, $0-64
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

// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VPERMQ   	0(SI), Z10, Z1 
	VCMPPD		$4, Z0, Z1, K1     // $4 means compare not equal
    
	VPERMQ   	64(SI), Z10, Z2
	VCMPPD		$4, Z0, Z2, K2     
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VPERMQ   	128(SI), Z10, Z3
	VCMPPD		$4, Z0, Z3, K3  
    KSHIFTLD    $16, K3, K3
    KORQ        K1, K3, K1

	VPERMQ   	192(SI), Z10, Z4
	VCMPPD		$4, Z0, Z4, K4  
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VPERMQ   	256(SI), Z10, Z5 
	VCMPPD		$4, Z0, Z5, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VPERMQ   	320(SI), Z10, Z6
	VCMPPD		$4, Z0, Z6, K6  
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VPERMQ   	384(SI), Z10, Z7
	VCMPPD		$4, Z0, Z7, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VPERMQ   	448(SI), Z10, Z8
	VCMPPD		$4, Z0, Z8, K2  
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
	VCMPPD		$4, Z0, Z1, K2, K1
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

// func matchFloat64LessThanAVX512(src []float64, val float64, bits []byte) int64
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
TEXT ·matchFloat64LessThanAVX512(SB), NOSPLIT, $0-64
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

// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VPERMQ   	0(SI), Z10, Z1 
	VCMPPD		$17, Z0, Z1, K1     // $17 means compare less
    
	VPERMQ   	64(SI), Z10, Z2
	VCMPPD		$17, Z0, Z2, K2     
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VPERMQ   	128(SI), Z10, Z3
	VCMPPD		$17, Z0, Z3, K3  
    KSHIFTLD    $16, K3, K3
    KORQ        K1, K3, K1

	VPERMQ   	192(SI), Z10, Z4
	VCMPPD		$17, Z0, Z4, K4  
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VPERMQ   	256(SI), Z10, Z5 
	VCMPPD		$17, Z0, Z5, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VPERMQ   	320(SI), Z10, Z6
	VCMPPD		$17, Z0, Z6, K6  
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VPERMQ   	384(SI), Z10, Z7
	VCMPPD		$17, Z0, Z7, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VPERMQ   	448(SI), Z10, Z8
	VCMPPD		$17, Z0, Z8, K2  
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
	VCMPPD		$17, Z0, Z1, K2, K1
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

// func matchFloat64LessThanEqualAVX512(src []float64, val float64, bits []byte) int64
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
TEXT ·matchFloat64LessThanEqualAVX512(SB), NOSPLIT, $0-64
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

// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VPERMQ   	0(SI), Z10, Z1 
	VCMPPD		$18, Z0, Z1, K1     // $18 means compare less or equal
    
	VPERMQ   	64(SI), Z10, Z2
	VCMPPD		$18, Z0, Z2, K2     
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VPERMQ   	128(SI), Z10, Z3
	VCMPPD		$18, Z0, Z3, K3  
    KSHIFTLD    $16, K3, K3
    KORQ        K1, K3, K1

	VPERMQ   	192(SI), Z10, Z4
	VCMPPD		$18, Z0, Z4, K4  
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VPERMQ   	256(SI), Z10, Z5 
	VCMPPD		$18, Z0, Z5, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VPERMQ   	320(SI), Z10, Z6
	VCMPPD		$18, Z0, Z6, K6  
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VPERMQ   	384(SI), Z10, Z7
	VCMPPD		$18, Z0, Z7, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VPERMQ   	448(SI), Z10, Z8
	VCMPPD		$18, Z0, Z8, K2  
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
	VCMPPD		$18, Z0, Z1, K2, K1
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

// func matchFloat64GreaterThanAVX512(src []float64, val float64, bits []byte) int64
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
TEXT ·matchFloat64GreaterThanAVX512(SB), NOSPLIT, $0-64
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

// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VPERMQ   	0(SI), Z10, Z1 
	VCMPPD		$30, Z0, Z1, K1     // $30 means compare greater
    
	VPERMQ   	64(SI), Z10, Z2
	VCMPPD		$30, Z0, Z2, K2     
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VPERMQ   	128(SI), Z10, Z3
	VCMPPD		$30, Z0, Z3, K3  
    KSHIFTLD    $16, K3, K3
    KORQ        K1, K3, K1

	VPERMQ   	192(SI), Z10, Z4
	VCMPPD		$30, Z0, Z4, K4  
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VPERMQ   	256(SI), Z10, Z5 
	VCMPPD		$30, Z0, Z5, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VPERMQ   	320(SI), Z10, Z6
	VCMPPD		$30, Z0, Z6, K6  
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VPERMQ   	384(SI), Z10, Z7
	VCMPPD		$30, Z0, Z7, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VPERMQ   	448(SI), Z10, Z8
	VCMPPD		$30, Z0, Z8, K2  
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
	VCMPPD		$30, Z0, Z1, K2, K1
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

// func matchFloat64GreaterThanEqualAVX512(src []float64, val float64, bits []byte) int64
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
TEXT ·matchFloat64GreaterThanEqualAVX512(SB), NOSPLIT, $0-64
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

// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VPERMQ   	0(SI), Z10, Z1 
	VCMPPD		$29, Z0, Z1, K1     // $29 means compare greater or equal
    
	VPERMQ   	64(SI), Z10, Z2
	VCMPPD		$29, Z0, Z2, K2     
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VPERMQ   	128(SI), Z10, Z3
	VCMPPD		$29, Z0, Z3, K3  
    KSHIFTLD    $16, K3, K3
    KORQ        K1, K3, K1

	VPERMQ   	192(SI), Z10, Z4
	VCMPPD		$29, Z0, Z4, K4  
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VPERMQ   	256(SI), Z10, Z5 
	VCMPPD		$29, Z0, Z5, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VPERMQ   	320(SI), Z10, Z6
	VCMPPD		$29, Z0, Z6, K6  
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VPERMQ   	384(SI), Z10, Z7
	VCMPPD		$29, Z0, Z7, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VPERMQ   	448(SI), Z10, Z8
	VCMPPD		$29, Z0, Z8, K2  
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
	VCMPPD		$29, Z0, Z1, K2, K1
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

// func matchFloat64BetweenAVX512(src []float64, a, b float64, bits []byte) int64
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
//   Z10 = permute control mask
//   Z1-Z8 = vector data
//   K1-K7 = comparision results
//   CX = loop counter (counts 1/8 values or bytes writen to output slice, runs from neg. to zero)
TEXT ·matchFloat64BetweenAVX512(SB), NOSPLIT, $0-72
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+40(FP), DI
	XORQ	R9, R9
	VBROADCASTSD    a+24(FP), Z0            // load a into AVX512 reg
	VBROADCASTSD    b+32(FP), Z12            // load a into AVX512 reg
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

// works for >= 64 float64 (i.e. 512 bytes of data)
loop_big:
	VPERMQ   	0(SI), Z10, Z1 
	VCMPPD		$29, Z0, Z1, K1      // => a ?
	VCMPPD		$18, Z1, Z1, K2      // =< b ?
    KANDB       K1, K2, K1
    
	VPERMQ   	64(SI), Z10, Z2
	VCMPPD		$29, Z0, Z2, K2     
	VCMPPD		$18, Z12, Z2, K3     
    KANDB       K2, K3, K2
    KSHIFTLQ    $8, K2, K2
    KORQ        K1, K2, K1

	VPERMQ   	128(SI), Z10, Z3
	VCMPPD		$29, Z0, Z3, K3  
	VCMPPD		$18, Z12, Z3, K4
    KANDB       K3, K4, K3  
    KSHIFTLD    $16, K3, K3
    KORQ        K1, K3, K1

	VPERMQ   	192(SI), Z10, Z4
	VCMPPD		$29, Z0, Z4, K4  
	VCMPPD		$18, Z12, Z4, K5
    KANDB       K4, K5, K4  
    KSHIFTLQ    $24, K4, K4
    KORQ        K1, K4, K1

	VPERMQ   	256(SI), Z10, Z5 
	VCMPPD		$29, Z0, Z5, K5
	VCMPPD		$18, Z12, Z5, K6
    KANDB       K5, K6, K5
    KSHIFTLQ    $32, K5, K5
    KORQ        K1, K5, K1

	VPERMQ   	320(SI), Z10, Z6
	VCMPPD		$29, Z0, Z6, K6  
	VCMPPD		$18, Z12, Z6, K7
    KANDB       K6, K7, K6  
    KSHIFTLQ    $40, K6, K6
    KORQ        K1, K6, K1

	VPERMQ   	384(SI), Z10, Z7
	VCMPPD		$29, Z0, Z7, K7
	VCMPPD		$18, Z12, Z7, K2
    KANDB       K7, K2, K7
    KSHIFTLQ    $48, K7, K7
    KORQ        K1, K7, K1

	VPERMQ   	448(SI), Z10, Z8
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
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMQ.Z   	0(SI), Z10, K2, Z1 
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
