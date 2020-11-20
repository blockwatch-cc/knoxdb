// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants_AVX512.h"

// func matchInt16EqualAVX512(src []int16, val int16, bits []byte) int64
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
TEXT ·matchInt16EqualAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTW    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		perm16<>+0x00(SB), Z10    // load shuffle control mask

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
    
// works for >= 64 int16 (i.e. 128 bytes of data)
loop_big:
	VPERMW   	0(SI), Z10, Z1 
	VPCMPEQW	Z1, Z0, K1
    
	VPERMW   	64(SI), Z10, Z2
	VPCMPEQW	Z2, Z0, K2
    KSHIFTLQ    $32, K2, K2
    KORQ        K1, K2, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$128, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

// here we process 8 values (16 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMW.Z   	0(SI), Z10, K2, Z1 
	VPCMPEQW	Z1, Z0, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$16, SI
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
    
// func matchInt16NotEqualAVX512(src []int16, val int16, bits []byte) int64
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
TEXT ·matchInt16NotEqualAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTW    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		perm16<>+0x00(SB), Z10    // load shuffle control mask

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
    
// works for >= 64 int16 (i.e. 128 bytes of data)
loop_big:
	VPERMW   	0(SI), Z10, Z1 
	VPCMPEQW	Z1, Z0, K1
    
	VPERMW   	64(SI), Z10, Z2
	VPCMPEQW	Z2, Z0, K2
    KSHIFTLQ    $32, K2, K2
    KORQ        K1, K2, K1

    KNOTQ       K1, K1              // make EQ to NE
	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$128, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

// here we process 8 values (16 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMW.Z   	0(SI), Z10, K2, Z1 
	VPCMPEQW	Z1, Z0, K2, K1
    KNOTB       K1, K1      // make EQ to NE
    KANDB       K1, K2, K1  // delete the unused bits
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$16, SI
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

// func matchInt16LessThanAVX512(src []int16, val int16, bits []byte) int64
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
TEXT ·matchInt16LessThanAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTW    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		perm16<>+0x00(SB), Z10    // load shuffle control mask

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
    
// works for >= 64 int16 (i.e. 128 bytes of data)
loop_big:
	VPERMW   	0(SI), Z10, Z1 
	VPCMPW	    $1, Z0, Z1, K1    // $1 means compare less than
    
	VPERMW   	64(SI), Z10, Z2
	VPCMPW	    $1, Z0, Z2, K2 
    KSHIFTLQ    $32, K2, K2
    KORQ        K1, K2, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$128, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

// here we process 8 values (16 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMW.Z   	0(SI), Z10, K2, Z1 
	VPCMPW	    $1, Z0, Z1, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$16, SI
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

// func matchInt16LessThanEqualAVX512(src []int16, val int16, bits []byte) int64
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
TEXT ·matchInt16LessThanEqualAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTW    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		perm16<>+0x00(SB), Z10    // load shuffle control mask

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
    
// works for >= 64 int16 (i.e. 128 bytes of data)
loop_big:
	VPERMW   	0(SI), Z10, Z1 
	VPCMPW	    $2, Z0, Z1, K1      // $2 means compare less equal
    
	VPERMW   	64(SI), Z10, Z2
	VPCMPW	    $2, Z0, Z2, K2 
    KSHIFTLQ    $32, K2, K2
    KORQ        K1, K2, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$128, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

// here we process 8 values (16 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMW.Z   	0(SI), Z10, K2, Z1 
	VPCMPW	    $2, Z0, Z1, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$16, SI
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

// func matchInt16GreaterThanAVX512(src []int16, val int16, bits []byte) int64
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
TEXT ·matchInt16GreaterThanAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTW    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		perm16<>+0x00(SB), Z10    // load shuffle control mask

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
    
// works for >= 64 int16 (i.e. 128 bytes of data)
loop_big:
	VPERMW   	0(SI), Z10, Z1 
	VPCMPW	    $6, Z0, Z1, K1    // $6 means compare not less equal (or greater than)
    
	VPERMW   	64(SI), Z10, Z2
	VPCMPW	    $6, Z0, Z2, K2 
    KSHIFTLQ    $32, K2, K2
    KORQ        K1, K2, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$128, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

// here we process 8 values (16 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMW.Z   	0(SI), Z10, K2, Z1 
	VPCMPW	    $6, Z0, Z1, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$16, SI
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

// func matchInt16GreaterThanEqualAVX512(src []int16, val int16, bits []byte) int64
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
TEXT ·matchInt16GreaterThanEqualAVX512(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	VPBROADCASTW    val+24(FP), Z0            // load val into AVX512 reg
	VMOVDQU64		perm16<>+0x00(SB), Z10    // load shuffle control mask

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
    
// works for >= 64 int16 (i.e. 128 bytes of data)
loop_big:
	VPERMW   	0(SI), Z10, Z1 
	VPCMPW	    $5, Z0, Z1, K1    // $5 means compare not less (or greater equal)
    
	VPERMW   	64(SI), Z10, Z2
	VPCMPW	    $5, Z0, Z2, K2 
    KSHIFTLQ    $32, K2, K2
    KORQ        K1, K2, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$128, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

// here we process 8 values (16 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMW.Z   	0(SI), Z10, K2, Z1 
	VPCMPW	    $5, Z0, Z1, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$16, SI
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

// func matchInt16BetweenAVX512(src []uint64, a, b uint64, bits []byte) int64
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
TEXT ·matchInt16BetweenAVX512(SB), NOSPLIT, $0-72
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9

    MOVQ            $1, AX
	VPBROADCASTW 	AX, Z13                  // 1 into AVX512 reg
	VPBROADCASTW 	a+24(FP), Z12            // load val a into AVX512 reg
	VPBROADCASTW 	b+26(FP), Z0             // load val b into AVX512 reg
	VPSUBW			Z12, Z0, Z0              // compute diff
	VPADDW			Z13, Z0, Z0
	VMOVDQU64		perm16<>+0x00(SB), Z10    // load shuffle control mask

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
	VPERMW   	0(SI), Z10, Z1 
	VPSUBW		Z12, Z1, Z1
	VPCMPUW	    $1, Z0, Z1, K1    // $1 means compare less than
    
	VPERMW   	64(SI), Z10, Z2
	VPSUBW		Z12, Z2, Z2
	VPCMPUW	    $1, Z0, Z2, K2
    KSHIFTLQ    $32, K2, K2
    KORQ        K1, K2, K1

	KMOVQ		K1, (DI)(CX*1)    // write 64 bits to the output slice
	KMOVQ		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$128, SI
	ADDQ		$8, CX
	JZ		 	exit_big
	JMP		 	loop_big

exit_big:
	TESTQ	BX, BX
	JLE		done

prep_small:
	VMOVDQU64		countup64<>+0x00(SB), Z9   // load counter mask

// here we process 8 values (16 byte) in one step
loop_small:
    // calculate mask
    VPBROADCASTQ    BX, Z11         // broadcast BX
    VPCMPGTQ        Z11, Z9, K2     // mask greater than BX
    KNOTB           K2, K2          // use lower equal than BX

	VPERMW.Z   	0(SI), Z10, K2, Z1 
	VPSUBW		Z12, Z1, K2, Z1
	VPCMPUW	    $1, Z0, Z1, K2, K1
	KMOVB		K1, (DI)    // write the lower 8 bits to the output slice
    KMOVB		K1, AX
	POPCNTQ		AX, AX      // count 1 bits
	ADDQ		AX, R9
	ADDQ		$16, SI
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
