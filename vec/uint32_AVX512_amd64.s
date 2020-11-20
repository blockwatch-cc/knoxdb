// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants_AVX512.h"

// func matchUint32EqualAVX512(src []uint32, val uint32, bits []byte) int64
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
TEXT ·matchUint32EqualAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTD    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		perm32<>+0x00(SB), Z10    // load shuffle control mask

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
    
// works for >= 64 uint32 (i.e. 256 bytes of data)
loop_big:
	VPERMD   	0(SI), Z10, Z1 
	VPCMPEQD	Z1, Z0, K1
    
	VPERMD   	64(SI), Z10, Z2
	VPCMPEQD	Z2, Z0, K2
    KSHIFTLQ    $16, K2, K2
    KORQ        K1, K2, K1

	VPERMD   	128(SI), Z10, Z3
	VPCMPEQD	Z3, Z0, K3
    KSHIFTLQ    $32, K3, K3
    KORQ        K1, K3, K1

	VPERMD   	192(SI), Z10, Z4
	VPCMPEQD	Z4, Z0, K4
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

// here we process 8 values (32 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMD.Z   	0(SI), Z10, K2, Z1 
	VPCMPEQD	Z1, Z0, K2, K1
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
    
// func matchUint32NotEqualAVX512(src []uint32, val uint32, bits []byte) int64
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
TEXT ·matchUint32NotEqualAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTD    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		perm32<>+0x00(SB), Z10    // load shuffle control mask

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
    
// works for >= 64 uint32 (i.e. 256 bytes of data)
loop_big:
	VPERMD   	0(SI), Z10, Z1 
	VPCMPEQD	Z1, Z0, K1
    
	VPERMD   	64(SI), Z10, Z2
	VPCMPEQD	Z2, Z0, K2
    KSHIFTLQ    $16, K2, K2
    KORQ        K1, K2, K1

	VPERMD   	128(SI), Z10, Z3
	VPCMPEQD	Z3, Z0, K3
    KSHIFTLQ    $32, K3, K3
    KORQ        K1, K3, K1

	VPERMD   	192(SI), Z10, Z4
	VPCMPEQD	Z4, Z0, K4
    KSHIFTLQ    $48, K4, K4
    KORQ        K1, K4, K1

    KNOTQ       K1, K1              // make EQ to NE
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

// here we process 8 values (32 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMD.Z   	0(SI), Z10, K2, Z1 
	VPCMPEQD	Z1, Z0, K2, K1
    KNOTB       K1, K1      // make EQ to NE
    KANDB       K1, K2, K1  // delete the unused bits
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

// func matchUint32LessThanAVX512(src []uint32, val uint32, bits []byte) int64
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
TEXT ·matchUint32LessThanAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTD    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		perm32<>+0x00(SB), Z10    // load shuffle control mask

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
    
// works for >= 64 uint32 (i.e. 256 bytes of data)
loop_big:
	VPERMD   	0(SI), Z10, Z1 
	VPCMPUD	    $1, Z0, Z1, K1    // $1 means compare less than
    
	VPERMD   	64(SI), Z10, Z2
	VPCMPUD	    $1, Z0, Z2, K2 
    KSHIFTLQ    $16, K2, K2
    KORQ        K1, K2, K1

	VPERMD   	128(SI), Z10, Z3
	VPCMPUD	    $1, Z0, Z3, K3 
    KSHIFTLQ    $32, K3, K3
    KORQ        K1, K3, K1

	VPERMD   	192(SI), Z10, Z4
	VPCMPUD	    $1, Z0, Z4, K4 
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

// here we process 8 values (32 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMD.Z   	0(SI), Z10, K2, Z1 
	VPCMPUD	    $1, Z0, Z1, K2, K1
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

// func matchUint32LessThanEqualAVX512(src []uint32, val uint32, bits []byte) int64
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
TEXT ·matchUint32LessThanEqualAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTD    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		perm32<>+0x00(SB), Z10    // load shuffle control mask

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
    
// works for >= 64 uint32 (i.e. 256 bytes of data)
loop_big:
	VPERMD   	0(SI), Z10, Z1 
	VPCMPUD	    $2, Z0, Z1, K1      // $2 means compare less equal
    
	VPERMD   	64(SI), Z10, Z2
	VPCMPUD	    $2, Z0, Z2, K2 
    KSHIFTLQ    $16, K2, K2
    KORQ        K1, K2, K1

	VPERMD   	128(SI), Z10, Z3
	VPCMPUD	    $2, Z0, Z3, K3 
    KSHIFTLQ    $32, K3, K3
    KORQ        K1, K3, K1

	VPERMD   	192(SI), Z10, Z4
	VPCMPUD	    $2, Z0, Z4, K4 
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

// here we process 8 values (32 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMD.Z   	0(SI), Z10, K2, Z1 
	VPCMPUD	    $2, Z0, Z1, K2, K1
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

// func matchUint32GreaterThanAVX512(src []uint32, val uint32, bits []byte) int64
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
TEXT ·matchUint32GreaterThanAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTD    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		perm32<>+0x00(SB), Z10    // load shuffle control mask

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
    
// works for >= 64 uint32 (i.e. 256 bytes of data)
loop_big:
	VPERMD   	0(SI), Z10, Z1 
	VPCMPUD	    $6, Z0, Z1, K1    // $6 means compare not less equal (or greater than)
    
	VPERMD   	64(SI), Z10, Z2
	VPCMPUD	    $6, Z0, Z2, K2 
    KSHIFTLQ    $16, K2, K2
    KORQ        K1, K2, K1

	VPERMD   	128(SI), Z10, Z3
	VPCMPUD	    $6, Z0, Z3, K3 
    KSHIFTLQ    $32, K3, K3
    KORQ        K1, K3, K1

	VPERMD   	192(SI), Z10, Z4
	VPCMPUD	    $6, Z0, Z4, K4 
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

// here we process 8 values (32 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMD.Z   	0(SI), Z10, K2, Z1 
	VPCMPUD	    $6, Z0, Z1, K2, K1
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

// func matchUint32GreaterThanEqualAVX512(src []uint32, val uint32, bits []byte) int64
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
TEXT ·matchUint32GreaterThanEqualAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTD    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		perm32<>+0x00(SB), Z10    // load shuffle control mask

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
    
// works for >= 64 uint32 (i.e. 256 bytes of data)
loop_big:
	VPERMD   	0(SI), Z10, Z1 
	VPCMPUD	    $5, Z0, Z1, K1    // $5 means compare not less (or greater equal)
    
	VPERMD   	64(SI), Z10, Z2
	VPCMPUD	    $5, Z0, Z2, K2 
    KSHIFTLQ    $16, K2, K2
    KORQ        K1, K2, K1

	VPERMD   	128(SI), Z10, Z3
	VPCMPUD	    $5, Z0, Z3, K3 
    KSHIFTLQ    $32, K3, K3
    KORQ        K1, K3, K1

	VPERMD   	192(SI), Z10, Z4
	VPCMPUD	    $5, Z0, Z4, K4 
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

// here we process 8 values (32 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMD.Z   	0(SI), Z10, K2, Z1 
	VPCMPUD	    $5, Z0, Z1, K2, K1
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

// func matchUint32BetweenAVX512(src []uint64, a, b uint64, bits []byte) int64
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
TEXT ·matchUint32BetweenAVX512(SB), NOSPLIT, $0-72
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

    MOVQ            $1, AX
	VPBROADCASTD 	AX, Z13                  // 1 into AVX512 reg
	VPBROADCASTD 	a+24(FP), Z12            // load val a into AVX512 reg
	VPBROADCASTD 	b+28(FP), Z0             // load val b into AVX512 reg
	VPSUBD			Z12, Z0, Z0              // compute diff
	VPADDD			Z13, Z0, Z0
	VMOVDQU64		perm32<>+0x00(SB), Z10    // load shuffle control mask

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
    
// works for >= 64 uint64 (i.e. 512 bytes of data)
loop_big:
	VPERMD   	0(SI), Z10, Z1 
	VPSUBD		Z12, Z1, Z1
	VPCMPUD	    $1, Z0, Z1, K1    // $1 means compare less than
    
	VPERMD   	64(SI), Z10, Z2
	VPSUBD		Z12, Z2, Z2
	VPCMPUD	    $1, Z0, Z2, K2
    KSHIFTLQ    $16, K2, K2
    KORQ        K1, K2, K1

	VPERMD   	128(SI), Z10, Z3
	VPSUBD		Z12, Z3, Z3
	VPCMPUD	    $1, Z0, Z3, K3
    KSHIFTLQ    $32, K3, K3
    KORQ        K1, K3, K1

	VPERMD   	192(SI), Z10, Z4
	VPSUBD		Z12, Z4, Z4
	VPCMPUD	    $1, Z0, Z4, K4
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

// here we process 8 values (32 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMD.Z   	0(SI), Z10, K2, Z1 
	VPSUBD		Z12, Z1, K2, Z1
	VPCMPUD	    $1, Z0, Z1, K2, K1
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
