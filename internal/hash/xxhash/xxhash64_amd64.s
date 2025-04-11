// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefanx@blockwatch.cc

#include "textflag.h"
#include "constants.h"

/***************************** xxhash64Uint32 ****************************************************/

// func x64_u32_core_avx2(src []uint32, res []uint64)
TEXT ·x64_u32_core_avx2(SB), NOSPLIT, $0-48
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

// func x64_u32_core_avx512(src []uint32, res []uint64)
TEXT ·x64_u32_core_avx512(SB), NOSPLIT, $0-48
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

// func x64_u64_core_avx2(src []uint64, res []uint64)
TEXT ·x64_u64_core_avx2(SB), NOSPLIT, $0-48
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

// func x64_u64_core_avx512(src []uint64, res []uint64)
TEXT ·x64_u64_core_avx512(SB), NOSPLIT, $0-48
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

