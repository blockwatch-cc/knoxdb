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

#define rol64_27(Y_reg) \
    VPSLLQ  $27, Y_reg, Y15 \
    VPSRLQ  $37, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol64_31(Y_reg) \
    VPSLLQ  $31, Y_reg, Y15 \
    VPSRLQ  $33, Y_reg, Y_reg \
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

// func xxhash32Uint32SliceAVX2Core(src []uint32, res []uint32, seed uint32)
TEXT ·xxhash32Uint32SliceAVX2Core(SB), NOSPLIT, $0-52
        MOVQ    src_base+0(FP), SI
        MOVQ    src_len+8(FP), BX
        MOVQ    res_base+24(FP), DI

        VPBROADCASTD    PRIME32_1<>+0x00(SB), Y10
        VPBROADCASTD    PRIME32_2<>+0x00(SB), Y11
        VPBROADCASTD    PRIME32_3<>+0x00(SB), Y12
        VPBROADCASTD    PRIME32_4<>+0x00(SB), Y13
        VPBROADCASTD    PRIME32_5<>+0x00(SB), Y14

        VPBROADCASTD    seed+48(FP), Y9
        VPBROADCASTD    constU32_4<>(SB), Y8

        SHRQ    $3, BX          
        JZ      exit_avx

loop_avx:
        VPADDD          Y9, Y14, Y0    
        VPADDD          Y8, Y0, Y0

        VMOVDQU         (SI), Y1
        VPMULLD         Y1, Y12, Y1
        VPADDD          Y0, Y1, Y0
        rol32_17           (Y0)
        VPMULLD         Y0, Y13, Y0

        ADDQ            $32, SI

        VPSRLD          $15, Y0, Y1
        VPXOR           Y0, Y1, Y0
        VPMULLD         Y0, Y11, Y0
        VPSRLD          $13, Y0, Y1
        VPXOR           Y0, Y1, Y0
        VPMULLD         Y0, Y12, Y0
        VPSRLD          $16, Y0, Y1
        VPXOR           Y0, Y1, Y0

        VMOVDQU         Y0, (DI)
        ADDQ            $32, DI
        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        RET

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

// func xxhash64Uint64SliceAVX512Core(src []uint64, res []uint64)
TEXT ·xxhash64Uint64SliceAVX512Core(SB), NOSPLIT, $0-48
        MOVQ    src_base+0(FP), SI
        MOVQ    src_len+8(FP), BX
        MOVQ    res_base+24(FP), DI

        VPBROADCASTQ    PRIME64_1<>+0x00(SB), Z10
        VPBROADCASTQ    PRIME64_2<>+0x00(SB), Z11
        VPBROADCASTQ    PRIME64_3<>+0x00(SB), Z12
        VPBROADCASTQ    PRIME64_4<>+0x00(SB), Z13
        VPBROADCASTQ    PRIME64_5<>+0x00(SB), Z14

        VPBROADCASTQ    maskHighD<>+0x00(SB), Z9   // 0xffffffff00000000 mask
        VPBROADCASTQ    constU64_8<>(SB), Z8 // const 8

        SHRQ    $3, BX          
        JZ      exit_avx

loop_avx:
        VPADDQ          Z8, Z14, Z0    
        VMOVDQU64	    (SI), Z1

        VPMULLQ         Z1, Z11, Z2
        VPROLQ          $31, Z2, Z2
        VPMULLQ         Z2, Z10, Z1

        VPXORQ          Z1, Z0, Z0
        VPROLQ          $27, Z0, Z0
        VPMULLQ         Z0, Z10, Z1
        VPADDQ          Z1, Z13, Z0

        
        ADDQ            $64, SI

        VPSRLQ          $33, Z0, Z1
        VPXORQ          Z0, Z1, Z0
        VPMULLQ         Z0, Z11, Z1
        VPSRLQ          $29, Z1, Z0
        VPXORQ          Z0, Z1, Z0
        VPMULLQ         Z0, Z12, Z1
        VPSRLQ          $32, Z1, Z0
        VPXORQ          Z0, Z1, Z0

        VMOVDQU64       Z0, (DI)
        ADDQ            $64, DI
        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        RET
        
// func xxhash64Uint64SliceAVX2Core(src []uint64, res []uint64)
TEXT ·xxhash64Uint64SliceAVX2Core(SB), NOSPLIT, $0-48
        MOVQ    src_base+0(FP), SI
        MOVQ    src_len+8(FP), BX
        MOVQ    res_base+24(FP), DI

        VPBROADCASTQ    PRIME64_1<>+0x00(SB), Y10
        VPBROADCASTQ    PRIME64_2<>+0x00(SB), Y11
        VPBROADCASTQ    PRIME64_3<>+0x00(SB), Y12
        VPBROADCASTQ    PRIME64_4<>+0x00(SB), Y13
        VPBROADCASTQ    PRIME64_5<>+0x00(SB), Y14

        VPBROADCASTQ    maskHighD<>+0x00(SB), Y9   // 0xffffffff00000000 mask
        VPBROADCASTQ    constU64_8<>(SB), Y8 // const 8

        SHRQ    $2, BX          
        JZ      exit_avx

loop_avx:
        VPADDQ          Y8, Y14, Y0    
        VMOVDQU	        (SI), Y1

        mul64           (Y1, Y11, Y2)
        rol64_31        (Y2)
        mul64           (Y2, Y10, Y1)

        VPXOR           Y1, Y0, Y0
        rol64_27        (Y0)
        mul64           (Y0, Y10, Y1)
        VPADDQ          Y1, Y13, Y0

        
        ADDQ            $32, SI

        VPSRLQ          $33, Y0, Y1
        VPXOR           Y0, Y1, Y0
        mul64           (Y0, Y11, Y1)
        VPSRLQ          $29, Y1, Y0
        VPXOR           Y0, Y1, Y0
        mul64           (Y0, Y12, Y1)
        VPSRLQ          $32, Y1, Y0
        VPXOR           Y0, Y1, Y0

        VMOVDQU         Y0, (DI)
        ADDQ            $32, DI
        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        RET
        
