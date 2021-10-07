// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefanx@blockwatch.cc

#include "textflag.h"
#include "constants_AVX2.h"

#define rol1(Y_reg) \
    VPSLLD  $1, Y_reg, Y15 \
    VPSRLD  $31, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol7(Y_reg) \
    VPSLLD  $7, Y_reg, Y15 \
    VPSRLD  $25, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol12(Y_reg) \
    VPSLLD  $12, Y_reg, Y15 \
    VPSRLD  $20, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol13(Y_reg) \
    VPSLLD  $13, Y_reg, Y15 \
    VPSRLD  $19, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol17(Y_reg) \
    VPSLLD  $17, Y_reg, Y15 \
    VPSRLD  $15, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define rol18(Y_reg) \
    VPSLLD  $18, Y_reg, Y15 \
    VPSRLD  $14, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define round(Y_seed, Y_input) \
        VPMULLD         Y_input, Y14, Y15 \
        VPADDD          Y_seed, Y15, Y_seed \
        VPSLLD          $13, Y_seed, Y15 \
        VPSRLD          $19, Y_seed, Y_seed \
        VPOR            Y15, Y_seed, Y_seed \
        VPMULLD         Y_seed, Y10, Y_seed

// func parallel(keys []uint32, seed uint32, res []uint32, SizeWords int)
TEXT ·parallel(SB), NOSPLIT, $0-64
        MOVQ    keys_base+0(FP), SI
        MOVQ    keys_len+8(FP), BX
        SHRQ    $3, BX          
        MOVQ    res_base+32(FP), DI
        MOVQ    SizeWords+56(FP), R8

        VPBROADCASTD    PRIME32_1<>+0x00(SB), Y10
        VPBROADCASTD    PRIME32_2<>+0x00(SB), Y11
        VPBROADCASTD    PRIME32_3<>+0x00(SB), Y12
        VPBROADCASTD    PRIME32_4<>+0x00(SB), Y13
        VPBROADCASTD    PRIME32_5<>+0x00(SB), Y14

        VPBROADCASTD    seed+24(FP), Y9
        VPBROADCASTD    SizeWords+56(FP), Y8

loop_avx:
        VPADDD          Y9, Y14, Y0    


        CMPQ            R8, $4
        JB              skip_loop2

        // __m256i v1 = _mm256_set1_epi32(seed + PRIME32_1 + PRIME32_2);
        VPADDD          Y9, Y10, Y0
        VPADDD          Y0, Y11, Y0 
        //    __m256i v2 = _mm256_set1_epi32(seed + PRIME32_2);
        VPADDD          Y9, Y11, Y1
        //    __m256i v3 = _mm256_set1_epi32(seed);
        VMOVDQU         Y9, Y2
        //    __m256i v4 = _mm256_set1_epi32(seed - PRIME32_1);
        VPSUBD          Y9, Y10, Y3
        //    for (int i = 0; i < (SizeWords & ~3); i += 4) {
        MOVQ            R8, CX
        ANDQ            $0xfffffffffffffffc, CX
loop2:        

        //        __m256i k1 = _mm256_loadu_si256((__m256i*) (keys + (i + 0) * 8));
        VMOVDQU         0(SI), Y4
        //        __m256i k2 = _mm256_loadu_si256((__m256i*) (keys + (i + 1) * 8));
        VMOVDQU         32(SI), Y5
        //        __m256i k3 = _mm256_loadu_si256((__m256i*) (keys + (i + 2) * 8));
        VMOVDQU         64(SI), Y6
        //        __m256i k4 = _mm256_loadu_si256((__m256i*) (keys + (i + 3) * 8));
        VMOVDQU         96(SI), Y7

        //        v1 = mm256_round(v1, k1);
        round           (Y0, Y4)
        //        v2 = mm256_round(v2, k2);
        round           (Y1, Y5)
        //        v3 = mm256_round(v3, k3);
        round           (Y2, Y6)
        //        v4 = mm256_round(v4, k4);
        round           (Y3, Y7)
        //    }
        ADDQ            $128, SI
        SUBQ            $4, CX
        JZ              exit_loop2
        JMP             loop2

exit_loop2:
        //    h = mm256_rol32<1>(v1) + mm256_rol32<7>(v2) + mm256_rol32<12>(v3) + mm256_rol32<18>(v4);
        rol1            (Y0)
        rol7            (Y1)
        VPADDD          Y0, Y1, Y0
        rol12           (Y2)
        rol18           (Y3)
        VPADDD          Y2, Y3, Y2
        VPADDD          Y0, Y2, Y0

skip_loop2:

        VPSLLD          $2, Y8, Y1
        VPADDD          Y1, Y0, Y0

        MOVQ            R8, CX
        ANDQ            $3, CX
        JZ              loop1_exit

loop1:
        VMOVDQU         (SI), Y1
        VPMULLD         Y1, Y12, Y1
        VPADDD          Y0, Y1, Y0

        rol17           (Y0)
        VPMULLD         Y0, Y13, Y0


        ADDQ            $32, SI
        SUBQ            $1, CX
        JZ              loop1_exit
        JMP             loop1

loop1_exit: 
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
        SUBQ            R8, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        RET

// func xxhash32Uint32SliceAVX2Core(src []uint32, res []uint32, seed uint32)
TEXT ·xxhash32Uint32SliceAVX2Core(SB), NOSPLIT, $0-56
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
        rol17           (Y0)
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
TEXT ·xxhash32Uint64SliceAVX2Core(SB), NOSPLIT, $0-56
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
        rol17           (Y0)
        VPMULLD         Y0, Y13, Y0

        VPMULLD         Y2, Y12, Y2
        VPADDD          Y0, Y2, Y0
        rol17           (Y0)
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
