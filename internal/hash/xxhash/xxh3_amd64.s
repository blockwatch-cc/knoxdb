// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefanx@blockwatch.cc

#include "textflag.h"
#include "constants.h"

/***************************** xxh3Uint32 ****************************************************/

// func xxh3_u32_core_avx2(src []uint32, res []uint64)
TEXT ·xxh3_u32_core_avx2(SB), NOSPLIT, $0-48
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
        
// func xxh3_u32_core_avx512(src []uint32, res []uint64)
TEXT ·xxh3_u32_core_avx512(SB), NOSPLIT, $0-48
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
                
// func xxh3_u64_core_avx2(src []uint64, res []uint64)
TEXT ·xxh3_u64_core_avx2(SB), NOSPLIT, $0-48
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
        //VMOVDQU           (SI), Y1

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
        
// func xxh3_u64_core_avx512(src []uint64, res []uint64)
TEXT ·xxh3_u64_core_avx512(SB), NOSPLIT, $0-48
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
        
// func xxh3_u64_core_avx512_unroll(src []uint64, res []uint64)
TEXT ·xxh3_u64_core_avx512_unroll(SB), NOSPLIT, $0-48
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
