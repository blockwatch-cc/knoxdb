// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefanx@blockwatch.cc

#include "textflag.h"
#include "constants.h"

/***************************** xxhash32Uint32 ****************************************************/

// func x32_u32_core_avx2(src []uint32, res []uint32, seed uint32)
TEXT ·x32_u32_core_avx2(SB), NOSPLIT, $0-52
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

// func x32_u32_core_avx512(src []uint32, res []uint32, seed uint32)
TEXT ·x32_u32_core_avx512(SB), NOSPLIT, $0-52
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

// func x32_u64_core_avx2(src []uint64, res []uint32, seed uint32)
TEXT ·x32_u64_core_avx2(SB), NOSPLIT, $0-52
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
        VMOVDQU         perm<>(SB), Y7

        SHRQ    $3, BX          
        JZ      exit_avx

loop_avx:
        VPADDD          Y9, Y14, Y0    
        VPADDD          Y8, Y0, Y0

        VMOVDQU          0(SI), Y1
        VMOVDQU         32(SI), Y2
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

// func x32_u64_core_avx512(src []uint64, res []uint32, seed uint32)
TEXT ·x32_u64_core_avx512(SB), NOSPLIT, $0-52
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

        VMOVDQU64        0(SI), Z1
        VMOVDQU64       64(SI), Z2
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

