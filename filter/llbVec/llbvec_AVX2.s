// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefanx@blockwatch.cc

#include "textflag.h"
#include "constants_AVX2.h"

#define rol32_1(Y_reg) \
    VPSLLD  $1, Y_reg, Y15 \
    VPSRLD  $31, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol32_7(Y_reg) \
    VPSLLD  $7, Y_reg, Y15 \
    VPSRLD  $25, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol32_12(Y_reg) \
    VPSLLD  $12, Y_reg, Y15 \
    VPSRLD  $20, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol32_13(Y_reg) \
    VPSLLD  $13, Y_reg, Y15 \
    VPSRLD  $19, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol32_17(Y_reg) \
    VPSLLD  $17, Y_reg, Y15 \
    VPSRLD  $15, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol32_18(Y_reg) \
    VPSLLD  $18, Y_reg, Y15 \
    VPSRLD  $14, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol64_1(Y_reg) \
    VPSLLQ  $1, Y_reg, Y15 \
    VPSRLQ  $31, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol64_7(Y_reg) \
    VPSLLQ  $7, Y_reg, Y15 \
    VPSRLQ  $25, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol64_12(Y_reg) \
    VPSLLQ  $12, Y_reg, Y15 \
    VPSRLQ  $20, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol64_13(Y_reg) \
    VPSLLQ  $13, Y_reg, Y15 \
    VPSRLQ  $19, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol64_23(Y_reg) \
    VPSLLQ  $23, Y_reg, Y15 \
    VPSRLQ  $41, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol64_24(Y_reg) \
    VPSLLQ  $24, Y_reg, Y15 \
    VPSRLQ  $40, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol64_27(Y_reg) \
    VPSLLQ  $27, Y_reg, Y15 \
    VPSRLQ  $37, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol64_31(Y_reg) \
    VPSLLQ  $31, Y_reg, Y15 \
    VPSRLQ  $33, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol64_49(Y_reg) \
    VPSLLQ  $49, Y_reg, Y15 \
    VPSRLQ  $15, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define round(Y_seed, Y_input) \
        VPMULLD         Y_input, Y14, Y15 \
        VPADDD          Y_seed, Y15, Y_seed \
        VPSLLD          $13, Y_seed, Y15 \
        VPSRLD          $19, Y_seed, Y_seed \
        VPOR            Y15, Y_seed, Y_seed \
        VPMULLD         Y_seed, Y10, Y_seed

#define mul64(Ya, Yb, Yab) \
    VPSHUFD     $0xb1, Yb, Yab \
    VPMULLD     Ya, Yab, Yab \
    VPSLLQ      $32, Yab, Y15 \
    VPADDD      Yab, Y15, Y15 \
    VPAND       Y15, Y9, Y15 \
    VPMULUDQ    Ya, Yb, Yab \
    VPADDQ      Yab, Y15, Yab \ 

/***************************** xxhash32Uint32 ****************************************************/

// func filterAddManyUint32AVX2Core(f LogLogBeta32, data []uint32, seed uint32)
TEXT ·filterAddManyUint32AVX2Core(SB), NOSPLIT, $0-68
        MOVQ    data_base+48(FP), SI
        MOVQ    data_len+56(FP), BX
        MOVQ    f_buf_base+24(FP), DI

        SHRQ    $3, BX      // less than 8 data values          
        JZ      exit

        VPBROADCASTD    PRIME32_1<>+0x00(SB), Y10  // prime numbers for hash values
        VPBROADCASTD    PRIME32_2<>+0x00(SB), Y11
        VPBROADCASTD    PRIME32_3<>+0x00(SB), Y12
        VPBROADCASTD    PRIME32_4<>+0x00(SB), Y13
        VPBROADCASTD    PRIME32_5<>+0x00(SB), Y14

        VPBROADCASTD    seed+72(FP), Y9
        VPBROADCASTD    f_max+8(FP), Y7
        VPBROADCASTD    f_maxX+12(FP), Y6
        VPBROADCASTD    f_precision+0(FP), Y5
        VPBROADCASTD    constU32_4<>(SB), Y8

        LEAQ    buf_pos<>(SB), R8   // buffer for register numbers
        LEAQ    buf_val<>(SB), R9   // buffer for register values

        // calculate hash
        VPADDD          Y9, Y14, Y0    
        VPADDD          Y8, Y0, Y0

        VMOVDQU         (SI), Y1
        VPMULLD         Y1, Y12, Y1
        VPADDD          Y0, Y1, Y0
        rol32_17           (Y0)
        VPMULLD         Y0, Y13, Y0

        VPSRLD          $15, Y0, Y1
        VPXOR           Y0, Y1, Y0
        VPMULLD         Y0, Y11, Y0
        VPSRLD          $13, Y0, Y1
        VPXOR           Y0, Y1, Y0
        VPMULLD         Y0, Y12, Y0
        VPSRLD          $16, Y0, Y1
        VPXOR           Y0, Y1, Y0      // now 8 hashes in Y0

        VPSRLVD         Y7, Y0, Y1        // calculate register number
        VPSLLVD         Y5, Y0, Y0        // calculate register value
        VPXOR           Y6, Y0, Y0
        
        // now we have 8 register numbers in Y1
        // with apropriate values in Y0
                
        // put the values into the registers, this cannot be vectorized
        VMOVDQU         Y1, 0(R8)
        VMOVDQU         Y0, 0(R9)

        SUBQ    $1, BX          
        JZ      exit_avx

loop_avx:
        MOVL            (R8), AX      // read register number
        LZCNTL          (R9), DX      // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, (DI)(AX*1)      // compare value with register
        JBE             jmp_no_write0        
        MOVB            DX, (DI)(AX*1)      // write if greater
jmp_no_write0:
       ADDQ            $32, SI
        // calculate hash
        VPADDD          Y9, Y14, Y0    
        VPADDD          Y8, Y0, Y0

        MOVL            4(R8), AX      // read register number
        LZCNTL          4(R9), DX      // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, (DI)(AX*1)      // compare value with register
        JBE             jmp_no_write1        
        MOVB            DX, (DI)(AX*1)      // write if greater
jmp_no_write1:
        VMOVDQU         (SI), Y1
        VPMULLD         Y1, Y12, Y1
        VPADDD          Y0, Y1, Y0

        MOVL            8(R8), AX      // read register number
        LZCNTL          8(R9), DX      // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, (DI)(AX*1)      // compare value with register
        JBE             jmp_no_write2        
        MOVB            DX, (DI)(AX*1)      // write if greater
jmp_no_write2:
        rol32_17           (Y0)

        MOVL            12(R8), AX      // read register number
        LZCNTL          12(R9), DX      // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, (DI)(AX*1)      // compare value with register
        JBE             jmp_no_write3        
        MOVB            DX, (DI)(AX*1)      // write if greater
jmp_no_write3:
        VPMULLD         Y0, Y13, Y0
        VPSRLD          $15, Y0, Y1
        VPXOR           Y0, Y1, Y0

        MOVL            16(R8), AX      // read register number
        LZCNTL          16(R9), DX      // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, (DI)(AX*1)      // compare value with register
        JBE             jmp_no_write4        
        MOVB            DX, (DI)(AX*1)      // write if greater
jmp_no_write4:
        VPMULLD         Y0, Y11, Y0
        VPSRLD          $13, Y0, Y1
        VPXOR           Y0, Y1, Y0
 
        MOVL            20(R8), AX      // read register number
        LZCNTL          20(R9), DX      // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, (DI)(AX*1)      // compare value with register
        JBE             jmp_no_write5        
        MOVB            DX, (DI)(AX*1)      // write if greater
jmp_no_write5:
        VPMULLD         Y0, Y12, Y0
        VPSRLD          $16, Y0, Y1
        VPXOR           Y0, Y1, Y0      // now 8 hashes in Y0

        MOVL            24(R8), AX      // read register number
        LZCNTL          24(R9), DX      // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, (DI)(AX*1)      // compare value with register
        JBE             jmp_no_write6        
        MOVB            DX, (DI)(AX*1)      // write if greater
jmp_no_write6:
        VPSRLVD         Y7, Y0, Y1        // calculate register number
        VPSLLVD         Y5, Y0, Y0        // calculate register value
        VPXOR           Y6, Y0, Y0

        MOVL            28(R8), AX      // read register number
        LZCNTL          28(R9), DX      // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, (DI)(AX*1)      // compare value with register
        JBE             jmp_no_write7        
        MOVB            DX, (DI)(AX*1)      // write if greater
jmp_no_write7:
        // now we have again 8 register numbers in Y1
        // with apropriate values in Y0
                
        // put the values into the registers, this cannot be vectorized
        VMOVDQU         Y1, 0(R8)
        VMOVDQU         Y0, 0(R9)

        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        
        XORQ            CX, CX
loop:
        MOVL            (R8)(CX*4), AX      // read register number
        LZCNTL          (R9)(CX*4), DX      // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, (DI)(AX*1)      // compare value with register
        JBE             jmp_no_write        
        MOVB            DX, (DI)(AX*1)      // write if greater

jmp_no_write:
        ADDQ            $1, CX
        CMPQ            CX, $8
        JNE             loop

exit:
        RET

// func xxhash32Uint32SliceAVX512Core(src []uint32, res []uint32, seed uint32)
TEXT ·xxhash32Uint32SliceAVX512Core(SB), NOSPLIT, $0-52
        MOVQ    src_base+0(FP), SI
        MOVQ    src_len+8(FP), BX
        MOVQ    res_base+24(FP), DI

        VPBROADCASTD    PRIME32_1<>+0x00(SB), Z10
        VPBROADCASTD    PRIME32_2<>+0x00(SB), Z11
        VPBROADCASTD    PRIME32_3<>+0x00(SB), Z12
        VPBROADCASTD    PRIME32_4<>+0x00(SB), Z13
        VPBROADCASTD    PRIME32_5<>+0x00(SB), Z14

        VPBROADCASTD    seed+48(FP), Z9
        VPBROADCASTD    constU32_4<>(SB), Z8

        SHRQ    $4, BX          
        JZ      exit_avx

loop_avx:
        VPADDD          Z9, Z14, Z0    
        VPADDD          Z8, Z0, Z0

        VMOVDQU64         (SI), Z1
        VPMULLD         Z1, Z12, Z1
        VPADDD          Z0, Z1, Z0
        VPROLD          $17, Z0, Z0
        VPMULLD         Z0, Z13, Z0

        ADDQ            $64, SI

        VPSRLD          $15, Z0, Z1
        VPXORD          Z0, Z1, Z0
        VPMULLD         Z0, Z11, Z0
        VPSRLD          $13, Z0, Z1
        VPXORD          Z0, Z1, Z0
        VPMULLD         Z0, Z12, Z0
        VPSRLD          $16, Z0, Z1
        VPXORD          Z0, Z1, Z0

        VMOVDQU64       Z0, (DI)
        ADDQ            $64, DI
        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        RET

/***************************** xxhash32Uint64 ****************************************************/

// func xxhash32Uint64SliceAVX2Core(src []uint64, res []uint32, seed uint32)
TEXT ·xxhash32Uint64SliceAVX2Core(SB), NOSPLIT, $0-52
        MOVQ    src_base+0(FP), SI
        MOVQ    src_len+8(FP), BX
        MOVQ    res_base+24(FP), DI

        VPBROADCASTD    PRIME32_1<>+0x00(SB), Y10
        VPBROADCASTD    PRIME32_2<>+0x00(SB), Y11
        VPBROADCASTD    PRIME32_3<>+0x00(SB), Y12
        VPBROADCASTD    PRIME32_4<>+0x00(SB), Y13
        VPBROADCASTD    PRIME32_5<>+0x00(SB), Y14

        VPBROADCASTD    seed+48(FP), Y9
        VPBROADCASTD    constU32_8<>(SB), Y8
        VMOVDQU	        perm<>(SB), Y7

        SHRQ    $3, BX          
        JZ      exit_avx

loop_avx:
        VPADDD          Y9, Y14, Y0    
        VPADDD          Y8, Y0, Y0

        VMOVDQU	         0(SI), Y1
        VMOVDQU	        32(SI), Y2
        VPSRLQ          $32, Y1, Y3
        VPSLLQ          $32, Y2, Y4
        VPBLENDD        $0x55, Y1, Y4, Y1
        VPBLENDD        $0xaa, Y2, Y3, Y2

        VPMULLD         Y1, Y12, Y1
        VPADDD          Y0, Y1, Y0
        rol32_17           (Y0)
        VPMULLD         Y0, Y13, Y0

        VPMULLD         Y2, Y12, Y2
        VPADDD          Y0, Y2, Y0
        rol32_17           (Y0)
        VPMULLD         Y0, Y13, Y0

        ADDQ            $64, SI

        VPSRLD          $15, Y0, Y1
        VPXOR           Y0, Y1, Y0
        VPMULLD         Y0, Y11, Y0
        VPSRLD          $13, Y0, Y1
        VPXOR           Y0, Y1, Y0
        VPMULLD         Y0, Y12, Y0
        VPSRLD          $16, Y0, Y1
        VPXOR           Y0, Y1, Y0

        VPERMD          Y0, Y7, Y0

        VMOVDQU         Y0, (DI)
        ADDQ            $32, DI
        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        RET

// func xxhash32Uint64SliceAVX512Core(src []uint64, res []uint32, seed uint32)
TEXT ·xxhash32Uint64SliceAVX512Core(SB), NOSPLIT, $0-52
        MOVQ    src_base+0(FP), SI
        MOVQ    src_len+8(FP), BX
        MOVQ    res_base+24(FP), DI

        VPBROADCASTD    PRIME32_1<>+0x00(SB), Z10
        VPBROADCASTD    PRIME32_2<>+0x00(SB), Z11
        VPBROADCASTD    PRIME32_3<>+0x00(SB), Z12
        VPBROADCASTD    PRIME32_4<>+0x00(SB), Z13
        VPBROADCASTD    PRIME32_5<>+0x00(SB), Z14

        VPBROADCASTD    seed+48(FP), Z9
        VPBROADCASTD    constU32_8<>(SB), Z8
        VMOVDQU64       perm512<>(SB), Z7
        MOVW            $0x5555, AX
        KMOVW           AX, K1
        MOVW            $0xaaaa, AX
        KMOVW           AX, K2

        SHRQ    $4, BX          
        JZ      exit_avx

loop_avx:
        VPADDD          Z9, Z14, Z0    
        VPADDD          Z8, Z0, Z0

        VMOVDQU64	     0(SI), Z1
        VMOVDQU64	    64(SI), Z2
        VPSRLQ          $32, Z1, Z3
        VPSLLQ          $32, Z2, Z4
        VPBLENDMD       Z1, Z4, K1, Z1
        VPBLENDMD       Z2, Z3, K2, Z2

        VPMULLD         Z1, Z12, Z1
        VPADDD          Z0, Z1, Z0
        VPROLD          $17, Z0, Z0
        VPMULLD         Z0, Z13, Z0

        VPMULLD         Z2, Z12, Z2
        VPADDD          Z0, Z2, Z0
        VPROLD          $17, Z0, Z0
        VPMULLD         Z0, Z13, Z0

        ADDQ            $128, SI

        VPSRLD          $15, Z0, Z1
        VPXORD          Z0, Z1, Z0
        VPMULLD         Z0, Z11, Z0
        VPSRLD          $13, Z0, Z1
        VPXORD          Z0, Z1, Z0
        VPMULLD         Z0, Z12, Z0
        VPSRLD          $16, Z0, Z1
        VPXORD          Z0, Z1, Z0

        VPERMD          Z0, Z7, Z0

        VMOVDQU64       Z0, (DI)
        ADDQ            $64, DI
        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        RET
