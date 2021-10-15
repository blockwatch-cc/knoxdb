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

// func xxhash32Uint64SliceAVX2UnrollCore(src []uint64, res []uint32, seed uint32)
TEXT ·xxhash32Uint64SliceAVX2UnrollCore(SB), NOSPLIT, $0-52
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

        SHRQ    $4, BX          
        JZ      exit_avx

loop_avx:

        VMOVDQU	         0(SI), Y1
        VMOVDQU	        32(SI), Y2
        VMOVDQU	        64(SI), Y5
        VMOVDQU	        96(SI), Y6
        VPSRLQ          $32, Y1, Y3
        VPSLLQ          $32, Y2, Y4
        VPBLENDD        $0x55, Y1, Y4, Y1
        VPBLENDD        $0xaa, Y2, Y3, Y2
        VPSRLQ          $32, Y5, Y3
        VPSLLQ          $32, Y6, Y4
        VPBLENDD        $0x55, Y5, Y4, Y4
        VPBLENDD        $0xaa, Y2, Y3, Y5

        VPADDD          Y9, Y14, Y0    
        VPADDD          Y9, Y14, Y3    
        VPADDD          Y8, Y0, Y0
        VPADDD          Y8, Y3, Y3

        VPMULLD         Y1, Y12, Y1
        VPMULLD         Y4, Y12, Y4
        VPADDD          Y0, Y1, Y0
        VPADDD          Y3, Y4, Y3
        rol32_17           (Y0)
        rol32_17           (Y3)
        VPMULLD         Y0, Y13, Y0
        VPMULLD         Y3, Y13, Y3

        VPMULLD         Y2, Y12, Y2
        VPMULLD         Y5, Y12, Y5
        VPADDD          Y0, Y2, Y0
        VPADDD          Y3, Y5, Y3
        rol32_17           (Y0)
        rol32_17           (Y3)
        VPMULLD         Y0, Y13, Y0
        VPMULLD         Y3, Y13, Y3

        ADDQ            $128, SI

        VPSRLD          $15, Y0, Y1
        VPSRLD          $15, Y3, Y4
        VPXOR           Y0, Y1, Y0
        VPXOR           Y3, Y4, Y3
        VPMULLD         Y0, Y11, Y0
        VPMULLD         Y3, Y11, Y3
        VPSRLD          $13, Y0, Y1
        VPSRLD          $13, Y3, Y4
        VPXOR           Y0, Y1, Y0
        VPXOR           Y3, Y4, Y3
        VPMULLD         Y0, Y12, Y0
        VPMULLD         Y3, Y12, Y3
        VPSRLD          $16, Y0, Y1
        VPSRLD          $16, Y3, Y4
        VPXOR           Y0, Y1, Y0
        VPXOR           Y3, Y4, Y3

        VPERMD          Y0, Y7, Y0
        VPERMD          Y3, Y7, Y3

        VMOVDQU         Y0, (DI)
        VMOVDQU         Y3, 32(DI)
        ADDQ            $64, DI
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

/***************************** xxhash64Uint32 ****************************************************/

// func xxhash64Uint32SliceAVX2Core(src []uint32, res []uint64)
TEXT ·xxhash64Uint32SliceAVX2Core(SB), NOSPLIT, $0-48
        MOVQ    src_base+0(FP), SI
        MOVQ    src_len+8(FP), BX
        MOVQ    res_base+24(FP), DI

        VPBROADCASTQ    PRIME64_1<>+0x00(SB), Y10
        VPBROADCASTQ    PRIME64_2<>+0x00(SB), Y11
        VPBROADCASTQ    PRIME64_3<>+0x00(SB), Y12
        VPBROADCASTQ    PRIME64_4<>+0x00(SB), Y13
        VPBROADCASTQ    PRIME64_5<>+0x00(SB), Y14

        VPBROADCASTQ    maskHighD<>+0x00(SB), Y9   // 0xffffffff00000000 mask
        VPBROADCASTQ    constU64_4<>(SB), Y8 // const 4
        VMOVDQU         exp32_64<>(SB), Y7
        
        VPXOR   Y1, Y1, Y1
        SHRQ    $2, BX          
        JZ      exit_avx

loop_avx:
        VPADDQ          Y8, Y14, Y0    
        VMOVDQU	        (SI), X1
        VPERMD          Y1, Y7, Y1

        mul64           (Y1, Y10, Y2)
        VPXOR           Y2, Y0, Y0
        rol64_23        (Y0)
        mul64           (Y0, Y11, Y1)
        VPADDQ          Y1, Y12, Y0

        ADDQ            $16, SI

        VPSRLQ          $33, Y0, Y1
        VPXOR           Y0, Y1, Y0
        mul64           (Y0, Y11, Y1)
        VPSRLQ          $29, Y1, Y0
        VPXOR           Y0, Y1, Y0
        mul64           (Y0, Y12, Y1)
        VPSRLQ          $32, Y1, Y0
        VPXOR           Y0, Y1, Y0

        VPXOR           Y1, Y1, Y1
        VMOVDQU         Y0, (DI)
        ADDQ            $32, DI
        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        RET

// func xxhash64Uint32SliceAVX512Core(src []uint32, res []uint64)
TEXT ·xxhash64Uint32SliceAVX512Core(SB), NOSPLIT, $0-48
        MOVQ    src_base+0(FP), SI
        MOVQ    src_len+8(FP), BX
        MOVQ    res_base+24(FP), DI

        VPBROADCASTQ    PRIME64_1<>+0x00(SB), Z10
        VPBROADCASTQ    PRIME64_2<>+0x00(SB), Z11
        VPBROADCASTQ    PRIME64_3<>+0x00(SB), Z12
        VPBROADCASTQ    PRIME64_4<>+0x00(SB), Z13
        VPBROADCASTQ    PRIME64_5<>+0x00(SB), Z14

        VPBROADCASTQ    maskLowD<>+0x00(SB), Z9   // 0x00000000ffffffff mask
        VPBROADCASTQ    constU64_4<>(SB), Z8 // const 4
        VMOVDQU64       dbl32_64<>(SB), Z7

        SHRQ    $3, BX          
        JZ      exit_avx

loop_avx:
        VPADDQ          Z8, Z14, Z0    
        VPERMD	        (SI), Z7, Z1
        VPANDQ          Z1, Z9, Z1

        VPMULLQ         Z1, Z10, Z2
        VPXORQ          Z2, Z0, Z0
        VPROLQ          $23, Z0, Z0
        VPMULLQ         Z0, Z11, Z1
        VPADDQ          Z1, Z12, Z0

        ADDQ            $32, SI

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

/***************************** xxhash64Uint64 ****************************************************/

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

/***************************** xxh3Uint32 ****************************************************/

// func xxh3Uint32SliceAVX2Core(src []uint32, res []uint64)
TEXT ·xxh3Uint32SliceAVX2Core(SB), NOSPLIT, $0-48
        MOVQ    src_base+0(FP), SI
        MOVQ    src_len+8(FP), BX
        MOVQ    res_base+24(FP), DI

        VPBROADCASTQ    key64_008<>(SB), Y11           
        VPBROADCASTQ    key64_016<>(SB), Y12           
        VPXOR           Y11, Y12, Y11

        VMOVDQU         dbl32_64<>(SB), Y12
        VPBROADCASTQ    con64_1<>(SB), Y10
        VPBROADCASTQ    maskHighD<>+0x00(SB), Y9   // 0xffffffff00000000 mask
        VPBROADCASTQ    constU64_4<>(SB), Y8 // const 4

        SHRQ    $2, BX          
        JZ      exit_avx

loop_avx:
		// input64 := u64(val) + u64(val)<<32
        VPERMD          (SI), Y12, Y0
		// h := input64 ^ (key64_008 ^ key64_016)
        VPXOR           Y11, Y0, Y0           

		// h ^= rol64_49(h) ^ rol64_24(h)
        VMOVDQU        Y0, Y1
        VMOVDQU        Y0, Y2
        rol64_49        (Y1)
        rol64_24        (Y2)
        VPXOR           Y1, Y2, Y1
        VPXOR           Y0, Y1, Y0
		// h *= 0x9fb21c651e98df25
        mul64           (Y0, Y10, Y1)
		// h ^= (h >> 35) + 8
        VPSRLQ          $35, Y1, Y0
        VPADDQ          Y0, Y8, Y0
        VPXOR           Y1, Y0, Y0
		// h *= 0x9fb21c651e98df25
        mul64           (Y0, Y10, Y1)
		// h ^= (h >> 28)
        VPSRLQ          $28, Y1, Y0
        VPXOR           Y1, Y0, Y0
        
        ADDQ            $16, SI

        VMOVDQU         Y0, (DI)
        ADDQ            $32, DI
        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        RET
        
// func xxh3Uint32SliceAVX512Core(src []uint32, res []uint64)
TEXT ·xxh3Uint32SliceAVX512Core(SB), NOSPLIT, $0-48
        MOVQ    src_base+0(FP), SI
        MOVQ    src_len+8(FP), BX
        MOVQ    res_base+24(FP), DI

        VPBROADCASTQ    key64_008<>(SB), Z11           
        VPBROADCASTQ    key64_016<>(SB), Z12           
        VPXORQ          Z11, Z12, Z11

        VMOVDQU64         dbl32_64<>(SB), Z12
        VPBROADCASTQ    con64_1<>(SB), Z10
        VPBROADCASTQ    maskHighD<>+0x00(SB), Z9   // 0xffffffff00000000 mask
        VPBROADCASTQ    constU64_4<>(SB), Z8 // const 4

        SHRQ    $3, BX          
        JZ      exit_avx

loop_avx:
		// input64 := u64(val) + u64(val)<<32
        VPERMD          (SI), Z12, Z0
		// h := input64 ^ (key64_008 ^ key64_016)
        VPXORQ          Z11, Z0, Z0           

		// h ^= rol64_49(h) ^ rol64_24(h)
        VPROLQ          $49, Z0, Z1
        VPROLQ          $24, Z0, Z2
        VPXORQ          Z1, Z2, Z1
        VPXORQ          Z0, Z1, Z0
		// h *= 0x9fb21c651e98df25
        VPMULLQ         Z0, Z10, Z1
		// h ^= (h >> 35) + 8
        VPSRLQ          $35, Z1, Z0
        VPADDQ          Z0, Z8, Z0
        VPXORQ          Z1, Z0, Z0
		// h *= 0x9fb21c651e98df25
        VPMULLQ         Z0, Z10, Z1
		// h ^= (h >> 28)
        VPSRLQ          $28, Z1, Z0
        VPXORQ          Z1, Z0, Z0
        
        ADDQ            $32, SI

        VMOVDQU64       Z0, (DI)
        ADDQ            $64, DI
        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        RET

/***************************** xxh3Uint64 ****************************************************/
                
// func xxh3Uint64SliceAVX2Core(src []uint64, res []uint64)
TEXT ·xxh3Uint64SliceAVX2Core(SB), NOSPLIT, $0-48
        MOVQ    src_base+0(FP), SI
        MOVQ    src_len+8(FP), BX
        MOVQ    res_base+24(FP), DI

        VPBROADCASTQ    key64_008<>(SB), Y11           
        VPBROADCASTQ    key64_016<>(SB), Y12           
        VPXOR           Y11, Y12, Y11

        VPBROADCASTQ    con64_1<>(SB), Y10
        VPBROADCASTQ    maskHighD<>+0x00(SB), Y9   // 0xffffffff00000000 mask
        VPBROADCASTQ    constU64_8<>(SB), Y8 // const 8

        SHRQ    $2, BX          
        JZ      exit_avx

loop_avx:
        //VMOVDQU	        (SI), Y1

		//input64 := val>>32 + val<<32
        VPSHUFD         $0xb1, (SI), Y0
		// h := input64 ^ (key64_008 ^ key64_016)
        VPXOR           Y11, Y0, Y0           

		// h ^= rol64_49(h) ^ rol64_24(h)
        VMOVDQU        Y0, Y1
        VMOVDQU        Y0, Y2
        rol64_49        (Y1)
        rol64_24        (Y2)
        VPXOR           Y1, Y2, Y1
        VPXOR           Y0, Y1, Y0
		// h *= 0x9fb21c651e98df25
        mul64           (Y0, Y10, Y1)
		// h ^= (h >> 35) + 8
        VPSRLQ          $35, Y1, Y0
        VPADDQ          Y0, Y8, Y0
        VPXOR           Y1, Y0, Y0
		// h *= 0x9fb21c651e98df25
        mul64           (Y0, Y10, Y1)
		// h ^= (h >> 28)
        VPSRLQ          $28, Y1, Y0
        VPXOR           Y1, Y0, Y0
        
        ADDQ            $32, SI

        VMOVDQU         Y0, (DI)
        ADDQ            $32, DI
        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        RET
        
// func xxh3Uint64SliceAVX512Core(src []uint64, res []uint64)
TEXT ·xxh3Uint64SliceAVX512Core(SB), NOSPLIT, $0-48
        MOVQ    src_base+0(FP), SI
        MOVQ    src_len+8(FP), BX
        MOVQ    res_base+24(FP), DI

        VPBROADCASTQ    key64_008<>(SB), Z11           
        VPBROADCASTQ    key64_016<>(SB), Z12           
        VPXORQ          Z11, Z12, Z11

        VPBROADCASTQ    con64_1<>(SB), Z10
        VPBROADCASTQ    maskHighD<>+0x00(SB), Z9   // 0xffffffff00000000 mask
        VPBROADCASTQ    constU64_8<>(SB), Z8 // const 8

        SHRQ    $3, BX          
        JZ      exit_avx

loop_avx:
		//input64 := val>>32 + val<<32
        VPSHUFD         $0xb1, (SI), Z0
		// h := input64 ^ (key64_008 ^ key64_016)
        VPXORQ          Z11, Z0, Z0           

		// h ^= rol64_49(h) ^ rol64_24(h)
        VPROLQ          $49, Z0, Z1
        VPROLQ          $24, Z0, Z2
        VPXORQ          Z1, Z2, Z1
        VPXORQ          Z0, Z1, Z0
		// h *= 0x9fb21c651e98df25
        VPMULLQ         Z0, Z10, Z1
		// h ^= (h >> 35) + 8
        VPSRLQ          $35, Z1, Z0
        VPADDQ          Z0, Z8, Z0
        VPXORQ          Z1, Z0, Z0
		// h *= 0x9fb21c651e98df25
        VPMULLQ         Z0, Z10, Z1
		// h ^= (h >> 28)
        VPSRLQ          $28, Z1, Z0
        VPXORQ          Z1, Z0, Z0
        
        ADDQ            $64, SI

        VMOVDQU64       Z0, (DI)
        ADDQ            $64, DI
        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        RET
        
// func xxh3Uint64SliceAVX512UnrollCore(src []uint64, res []uint64)
TEXT ·xxh3Uint64SliceAVX512UnrollCore(SB), NOSPLIT, $0-48
        MOVQ    src_base+0(FP), SI
        MOVQ    src_len+8(FP), BX
        MOVQ    res_base+24(FP), DI

        VPBROADCASTQ    key64_008<>(SB), Z11           
        VPBROADCASTQ    key64_016<>(SB), Z12           
        VPXORQ          Z11, Z12, Z11

        VPBROADCASTQ    con64_1<>(SB), Z10
        VPBROADCASTQ    maskHighD<>+0x00(SB), Z9   // 0xffffffff00000000 mask
        VPBROADCASTQ    constU64_8<>(SB), Z8 // const 8

        SHRQ    $4, BX          
        JZ      exit_avx

loop_avx:
		//input64 := val>>32 + val<<32
        VPSHUFD         $0xb1, (SI), Z0
        VPSHUFD         $0xb1, 64(SI), Z3
		// h := input64 ^ (key64_008 ^ key64_016)
        VPXORQ          Z11, Z0, Z0           
        VPXORQ          Z11, Z3, Z3           

		// h ^= rol64_49(h) ^ rol64_24(h)
        VPROLQ          $49, Z0, Z1
        VPROLQ          $49, Z3, Z4
        VPROLQ          $24, Z0, Z2
        VPROLQ          $24, Z3, Z5
        VPXORQ          Z1, Z2, Z1
        VPXORQ          Z4, Z5, Z4
        VPXORQ          Z0, Z1, Z0
        VPXORQ          Z3, Z4, Z3
		// h *= 0x9fb21c651e98df25
        VPMULLQ         Z0, Z10, Z1
        VPMULLQ         Z3, Z10, Z4
		// h ^= (h >> 35) + 8
        VPSRLQ          $35, Z1, Z0
        VPSRLQ          $35, Z4, Z3
        VPADDQ          Z0, Z8, Z0
        VPADDQ          Z3, Z8, Z3
        VPXORQ          Z1, Z0, Z0
        VPXORQ          Z4, Z3, Z3
		// h *= 0x9fb21c651e98df25
        VPMULLQ         Z0, Z10, Z1
        VPMULLQ         Z3, Z10, Z4
		// h ^= (h >> 28)
        VPSRLQ          $28, Z1, Z0
        VPSRLQ          $28, Z4, Z3
        VPXORQ          Z1, Z0, Z0
        VPXORQ          Z4, Z3, Z3
        
        ADDQ            $128, SI

        VMOVDQU64       Z0, (DI)
        VMOVDQU64       Z3, 64(DI)
        ADDQ            $128, DI
        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        RET
        
