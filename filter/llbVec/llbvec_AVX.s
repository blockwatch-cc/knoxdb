// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefanx@blockwatch.cc

#include "textflag.h"
#include "constants_AVX.h"

#define rol32_17(Y_reg) \
    VPSLLD  $17, Y_reg, Y15 \
    VPSRLD  $15, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

#define mul64(Ya, Yb, Yab) \
    VPSHUFD     $0xb1, Yb, Yab \
    VPMULLD     Ya, Yab, Yab \
    VPSLLQ      $32, Yab, Y15 \
    VPADDD      Yab, Y15, Y15 \
    VPAND       Y15, Y9, Y15 \
    VPMULUDQ    Ya, Yb, Yab \
    VPADDQ      Yab, Y15, Yab \ 

/***************************** filterAddManyUint32 ****************************************************/

// func filterAddManyUint32AVX2Core(f LogLogBeta, data []uint32, seed uint32)
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

        XORQ    R10, R10
        
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
        VMOVDQU         Y1, (R8)
        VMOVDQU         Y0, (R9)

        SUBQ    $1, BX          
        JZ      exit_avx

loop_avx:
        MOVL            (R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          (R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $32, SI
        // calculate hash
        VPADDD          Y9, Y14, Y0    
        VPADDD          Y8, Y0, Y0

        MOVL            4(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          4(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VMOVDQU         (SI), Y1
        VPMULLD         Y1, Y12, Y1
        VPADDD          Y0, Y1, Y0

        MOVL            8(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          8(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back
        rol32_17           (Y0)

        MOVL            12(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          12(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y0, Y13, Y0
        VPSRLD          $15, Y0, Y1
        VPXOR           Y0, Y1, Y0

        MOVL            16(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          16(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y0, Y11, Y0
        VPSRLD          $13, Y0, Y1
        VPXOR           Y0, Y1, Y0
 
        MOVL            20(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          20(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y0, Y12, Y0
        VPSRLD          $16, Y0, Y1
        VPXOR           Y0, Y1, Y0      // now 8 hashes in Y0

        MOVL            24(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          24(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLVD         Y7, Y0, Y1        // calculate register number
        VPSLLVD         Y5, Y0, Y0        // calculate register value
        VPXOR           Y6, Y0, Y0

        MOVL            28(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          28(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        // now we have again 8 register numbers in Y1
        // with apropriate values in Y0
                
        // put the values into the registers, this cannot be vectorized
        VMOVDQU         Y1, (R8)
        VMOVDQU         Y0, (R9)

        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        
        XORQ            CX, CX
loop:
        MOVL            (R8)(CX*4), AX      // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          (R9)(CX*4), DX      // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $1, CX
        CMPQ            CX, $8
        JNE             loop

exit:
        RET

// func filterAddManyUint32AVX512Core(f LogLogBeta, data []uint32, seed uint32)
TEXT ·filterAddManyUint32AVX512Core(SB), NOSPLIT, $0-68
        MOVQ    data_base+48(FP), SI
        MOVQ    data_len+56(FP), BX
        MOVQ    f_buf_base+24(FP), DI

        SHRQ    $4, BX      // less than 16 data values          
        JZ      exit

        VPBROADCASTD    PRIME32_1<>+0x00(SB), Z10  // prime numbers for hash values
        VPBROADCASTD    PRIME32_2<>+0x00(SB), Z11
        VPBROADCASTD    PRIME32_3<>+0x00(SB), Z12
        VPBROADCASTD    PRIME32_4<>+0x00(SB), Z13
        VPBROADCASTD    PRIME32_5<>+0x00(SB), Z14

        VPBROADCASTD    seed+72(FP), Z9
        VPBROADCASTD    f_max+8(FP), Z7
        VPBROADCASTD    f_maxX+12(FP), Z6
        VPBROADCASTD    f_precision+0(FP), Z5
        VPBROADCASTD    constU32_4<>(SB), Z8
        VPBROADCASTD    constU32_1<>(SB), Z4
        
        LEAQ    buf_pos<>(SB), R8   // buffer for register numbers
        LEAQ    buf_val<>(SB), R9   // buffer for register values

        XORQ            R10, R10
        
        // calculate hash
        VPADDD          Z9, Z14, Z0    
        VPADDD          Z8, Z0, Z0

        VMOVDQU64         (SI), Z1
        VPMULLD         Z1, Z12, Z1
        VPADDD          Z0, Z1, Z0
        VPROLD          $17, Z0, Z0
        VPMULLD         Z0, Z13, Z0

        VPSRLD          $15, Z0, Z1
        VPXORD          Z0, Z1, Z0
        VPMULLD         Z0, Z11, Z0
        VPSRLD          $13, Z0, Z1
        VPXORD          Z0, Z1, Z0
        VPMULLD         Z0, Z12, Z0
        VPSRLD          $16, Z0, Z1
        VPXORD          Z0, Z1, Z0        // now 16 hashes in Z0

        VPSRLVD         Z7, Z0, Z1        // calculate register number
        VPSLLVD         Z5, Z0, Z0        // calculate register value
        VPXORD          Z6, Z0, Z0
        
        VPLZCNTD        Z0, Z0          // count leading zeros
        VPADDD          Z4, Z0, Z0      // add 1

        // now we have 16 register numbers in Z1
        // with apropriate values in Z0
                
        // put the values into the registers, this cannot be vectorized
        VMOVDQU32       Z1, (R8)
        VMOVDQU32       Z0, (R9)

        SUBQ    $1, BX          
        JZ      exit_avx

loop_avx:
        MOVL            (R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            (R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $64, SI
        // calculate hash
        VPADDD          Z9, Z14, Z0    

        MOVL            4(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            4(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPADDD          Z8, Z0, Z0
        VMOVDQU64         (SI), Z1

        MOVL            8(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            8(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Z1, Z12, Z1

        MOVL            12(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            12(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPADDD          Z0, Z1, Z0
        VPROLD          $17, Z0, Z0

        MOVL            16(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            16(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Z0, Z13, Z0

        MOVL            20(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            20(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLD          $15, Z0, Z1
        VPXORD          Z0, Z1, Z0

        MOVL            24(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            24(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Z0, Z11, Z0

        MOVL            28(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            28(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLD          $13, Z0, Z1
        VPXORD          Z0, Z1, Z0

        MOVL            32(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            32(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Z0, Z12, Z0

        MOVL            36(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            36(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLD          $16, Z0, Z1
        VPXORD          Z0, Z1, Z0        // now 16 hashes in Z0

        MOVL            40(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            40(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLVD         Z7, Z0, Z1        // calculate register number
        VPSLLVD         Z5, Z0, Z0        // calculate register value

        MOVL            44(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            44(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPXORD          Z6, Z0, Z0

        MOVL            48(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            48(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPLZCNTD        Z0, Z0          // count leading zeros

        MOVL            52(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            52(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPADDD          Z4, Z0, Z0      // add 1

        MOVL            56(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            56(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        MOVL            60(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            60(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        // now we have 16 register numbers in Z1
        // with apropriate values in Z0
        
        // put the values into the registers, this cannot be vectorized
        VMOVDQU32       Z1, (R8)
        VMOVDQU32       Z0, (R9)

        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        
        XORQ            CX, CX
loop:
        MOVL            (R8)(CX*4), AX      // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            (R9)(CX*4), DX      // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $1, CX
        CMPQ            CX, $16
        JNE             loop

exit:
        RET

/***************************** filterAddManyInt32 ****************************************************/

// func filterAddManyInt32AVX2Core(f LogLogBeta, data []int32, seed uint32)
TEXT ·filterAddManyInt32AVX2Core(SB), NOSPLIT, $0-68
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

        XORQ    R10, R10
        
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
        VMOVDQU         Y1, (R8)
        VMOVDQU         Y0, (R9)

        SUBQ    $1, BX          
        JZ      exit_avx

loop_avx:
        MOVL            (R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          (R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

       ADDQ            $32, SI
        // calculate hash
        VPADDD          Y9, Y14, Y0    
        VPADDD          Y8, Y0, Y0

        MOVL            4(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          4(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VMOVDQU         (SI), Y1
        VPMULLD         Y1, Y12, Y1
        VPADDD          Y0, Y1, Y0

        MOVL            8(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          8(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back
        rol32_17           (Y0)

        MOVL            12(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          12(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y0, Y13, Y0
        VPSRLD          $15, Y0, Y1
        VPXOR           Y0, Y1, Y0

        MOVL            16(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          16(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y0, Y11, Y0
        VPSRLD          $13, Y0, Y1
        VPXOR           Y0, Y1, Y0
 
        MOVL            20(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          20(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y0, Y12, Y0
        VPSRLD          $16, Y0, Y1
        VPXOR           Y0, Y1, Y0      // now 8 hashes in Y0

        MOVL            24(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          24(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLVD         Y7, Y0, Y1        // calculate register number
        VPSLLVD         Y5, Y0, Y0        // calculate register value
        VPXOR           Y6, Y0, Y0

        MOVL            28(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          28(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        // now we have again 8 register numbers in Y1
        // with apropriate values in Y0
                
        // put the values into the registers, this cannot be vectorized
        VMOVDQU         Y1, (R8)
        VMOVDQU         Y0, (R9)

        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        
        XORQ            CX, CX
loop:
        MOVL            (R8)(CX*4), AX      // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          (R9)(CX*4), DX      // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $1, CX
        CMPQ            CX, $8
        JNE             loop

exit:
        RET

// func filterAddManyInt32AVX512Core(f LogLogBeta, data []int32, seed uint32)
TEXT ·filterAddManyInt32AVX512Core(SB), NOSPLIT, $0-68
        MOVQ    data_base+48(FP), SI
        MOVQ    data_len+56(FP), BX
        MOVQ    f_buf_base+24(FP), DI

        SHRQ    $4, BX      // less than 16 data values          
        JZ      exit

        VPBROADCASTD    PRIME32_1<>+0x00(SB), Z10  // prime numbers for hash values
        VPBROADCASTD    PRIME32_2<>+0x00(SB), Z11
        VPBROADCASTD    PRIME32_3<>+0x00(SB), Z12
        VPBROADCASTD    PRIME32_4<>+0x00(SB), Z13
        VPBROADCASTD    PRIME32_5<>+0x00(SB), Z14

        VPBROADCASTD    seed+72(FP), Z9
        VPBROADCASTD    f_max+8(FP), Z7
        VPBROADCASTD    f_maxX+12(FP), Z6
        VPBROADCASTD    f_precision+0(FP), Z5
        VPBROADCASTD    constU32_4<>(SB), Z8
        VPBROADCASTD    constU32_1<>(SB), Z4
        
        LEAQ    buf_pos<>(SB), R8   // buffer for register numbers
        LEAQ    buf_val<>(SB), R9   // buffer for register values

        XORQ            R10, R10
        
        // calculate hash
        VPADDD          Z9, Z14, Z0    
        VPADDD          Z8, Z0, Z0

        VMOVDQU64         (SI), Z1
        VPMULLD         Z1, Z12, Z1
        VPADDD          Z0, Z1, Z0
        VPROLD          $17, Z0, Z0
        VPMULLD         Z0, Z13, Z0

        VPSRLD          $15, Z0, Z1
        VPXORD          Z0, Z1, Z0
        VPMULLD         Z0, Z11, Z0
        VPSRLD          $13, Z0, Z1
        VPXORD          Z0, Z1, Z0
        VPMULLD         Z0, Z12, Z0
        VPSRLD          $16, Z0, Z1
        VPXORD          Z0, Z1, Z0        // now 16 hashes in Z0

        VPSRLVD         Z7, Z0, Z1        // calculate register number
        VPSLLVD         Z5, Z0, Z0        // calculate register value
        VPXORD          Z6, Z0, Z0
        
        VPLZCNTD        Z0, Z0          // count leading zeros
        VPADDD          Z4, Z0, Z0      // add 1

        // now we have 16 register numbers in Z1
        // with apropriate values in Z0
                
        // put the values into the registers, this cannot be vectorized
        VMOVDQU32       Z1, (R8)
        VMOVDQU32       Z0, (R9)

        SUBQ    $1, BX          
        JZ      exit_avx

loop_avx:
        MOVL            (R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            (R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $64, SI
        // calculate hash
        VPADDD          Z9, Z14, Z0    

        MOVL            4(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            4(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPADDD          Z8, Z0, Z0
        VMOVDQU64         (SI), Z1

        MOVL            8(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            8(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Z1, Z12, Z1

        MOVL            12(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            12(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPADDD          Z0, Z1, Z0
        VPROLD          $17, Z0, Z0

        MOVL            16(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            16(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Z0, Z13, Z0

        MOVL            20(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            20(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLD          $15, Z0, Z1
        VPXORD          Z0, Z1, Z0

        MOVL            24(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            24(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Z0, Z11, Z0

        MOVL            28(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            28(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLD          $13, Z0, Z1
        VPXORD          Z0, Z1, Z0

        MOVL            32(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            32(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Z0, Z12, Z0

        MOVL            36(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            36(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLD          $16, Z0, Z1
        VPXORD          Z0, Z1, Z0        // now 16 hashes in Z0

        MOVL            40(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            40(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLVD         Z7, Z0, Z1        // calculate register number
        VPSLLVD         Z5, Z0, Z0        // calculate register value

        MOVL            44(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            44(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPXORD          Z6, Z0, Z0

        MOVL            48(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            48(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPLZCNTD        Z0, Z0          // count leading zeros

        MOVL            52(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            52(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPADDD          Z4, Z0, Z0      // add 1

        MOVL            56(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            56(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        MOVL            60(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            60(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        // now we have 16 register numbers in Z1
        // with apropriate values in Z0
        
        // put the values into the registers, this cannot be vectorized
        VMOVDQU32       Z1, (R8)
        VMOVDQU32       Z0, (R9)

        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        
        XORQ            CX, CX
loop:
        MOVL            (R8)(CX*4), AX      // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            (R9)(CX*4), DX      // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $1, CX
        CMPQ            CX, $16
        JNE             loop

exit:
        RET

/***************************** filterAddManyUint64 ****************************************************/

// func filterAddManyUint64AVX2Core(f LogLogBeta, data []uint64, seed uint32)
TEXT ·filterAddManyUint64AVX2Core(SB), NOSPLIT, $0-68
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
        VMOVDQU         Y1, (R8)
        VMOVDQU         Y0, (R9)

        SUBQ    $1, BX          
        JZ      exit_avx

loop_avx:
        ADDQ            $64, SI

        MOVL            (R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          (R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        // calculate hash
        VPADDD          Y9, Y14, Y0    
        VPADDD          Y8, Y0, Y0
        VMOVDQU	         0(SI), Y1
        VMOVDQU	        32(SI), Y2

        MOVL            4(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          4(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLQ          $32, Y1, Y3
        VPSLLQ          $32, Y2, Y4
        VPBLENDD        $0x55, Y1, Y4, Y1
        VPBLENDD        $0xaa, Y2, Y3, Y2

        MOVL            8(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          8(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y1, Y12, Y1
        VPADDD          Y0, Y1, Y0

        MOVL            12(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          12(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        rol32_17           (Y0)
        VPMULLD         Y0, Y13, Y0

        MOVL            16(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          16(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y2, Y12, Y2
        VPADDD          Y0, Y2, Y0
        rol32_17           (Y0)

        MOVL            20(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          20(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y0, Y13, Y0
        VPSRLD          $15, Y0, Y1
        VPXOR           Y0, Y1, Y0

        MOVL            24(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          24(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y0, Y11, Y0
        VPSRLD          $13, Y0, Y1
        VPXOR           Y0, Y1, Y0

        MOVL            28(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          28(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y0, Y12, Y0
        VPSRLD          $16, Y0, Y1
        VPXOR           Y0, Y1, Y0      // now 8 hashes in Y0
        VPSRLVD         Y7, Y0, Y1        // calculate register number
        VPSLLVD         Y5, Y0, Y0        // calculate register value
        VPXOR           Y6, Y0, Y0
        
        // now we have 8 register numbers in Y1
        // with apropriate values in Y0
                
        // put the values into the registers, this cannot be vectorized
        VMOVDQU         Y1, (R8)
        VMOVDQU         Y0, (R9)

        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        
        XORQ            CX, CX
loop:
        MOVL            (R8)(CX*4), AX      // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          (R9)(CX*4), DX      // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $1, CX
        CMPQ            CX, $8
        JNE             loop

exit:
        RET

// func XYZfilterAddManyUint64AVX512Core(f LogLogBeta, data []uint64, seed uint32)
TEXT ·filterAddManyUint64AVX512Core(SB), NOSPLIT, $0-68
        MOVQ    data_base+48(FP), SI
        MOVQ    data_len+56(FP), BX
        MOVQ    f_buf_base+24(FP), DI

        SHRQ    $4, BX      // less than 16 data values          
        JZ      exit

        VPBROADCASTD    PRIME32_1<>+0x00(SB), Z10  // prime numbers for hash values
        VPBROADCASTD    PRIME32_2<>+0x00(SB), Z11
        VPBROADCASTD    PRIME32_3<>+0x00(SB), Z12
        VPBROADCASTD    PRIME32_4<>+0x00(SB), Z13
        VPBROADCASTD    PRIME32_5<>+0x00(SB), Z14

        VPBROADCASTD    seed+72(FP), Z9
        VPBROADCASTD    f_max+8(FP), Z7
        VPBROADCASTD    f_maxX+12(FP), Z6
        VPBROADCASTD    f_precision+0(FP), Z5
        VPBROADCASTD    constU32_8<>(SB), Z8
        VPBROADCASTD    constU32_1<>(SB), Z16
        VMOVDQU64       perm512<>(SB), Z17

        MOVW            $0x5555, AX
        KMOVW           AX, K1
        MOVW            $0xaaaa, AX
        KMOVW           AX, K2
        
        LEAQ    buf_pos<>(SB), R8   // buffer for register numbers
        LEAQ    buf_val<>(SB), R9   // buffer for register values

        // calculate hash
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

        VPSRLD          $15, Z0, Z1
        VPXORD          Z0, Z1, Z0
        VPMULLD         Z0, Z11, Z0
        VPSRLD          $13, Z0, Z1
        VPXORD          Z0, Z1, Z0
        VPMULLD         Z0, Z12, Z0
        VPSRLD          $16, Z0, Z1
        VPXORD          Z0, Z1, Z0

        VPERMD          Z0, Z17, Z0          // now 16 hashes in Z0

        VPSRLVD         Z7, Z0, Z1        // calculate register number
        VPSLLVD         Z5, Z0, Z0        // calculate register value
        VPXORD          Z6, Z0, Z0
        
        VPLZCNTD        Z0, Z0          // count leading zeros
        VPADDD          Z16, Z0, Z0      // add 1

        // now we have 16 register numbers in Z1
        // with apropriate values in Z0
                
        // put the values into the registers, this cannot be vectorized
        VMOVDQU32       Z1, (R8)
        VMOVDQU32       Z0, (R9)

        SUBQ    $1, BX          
        JZ      exit_avx

loop_avx:
        MOVL            (R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            (R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $128, SI
        // calculate hash
        VPADDD          Z9, Z14, Z0    
        VPADDD          Z8, Z0, Z0

        MOVL            4(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            4(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VMOVDQU64	     0(SI), Z1
        VMOVDQU64	    64(SI), Z2
        VPSRLQ          $32, Z1, Z3

        MOVL            8(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            8(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSLLQ          $32, Z2, Z4
        VPBLENDMD       Z1, Z4, K1, Z1
        VPBLENDMD       Z2, Z3, K2, Z2

        MOVL            12(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            12(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Z1, Z12, Z1
        VPADDD          Z0, Z1, Z0

        MOVL            16(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            16(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPROLD          $17, Z0, Z0
        VPMULLD         Z0, Z13, Z0

        MOVL            20(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            20(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Z2, Z12, Z2
        VPADDD          Z0, Z2, Z0

        MOVL            24(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            24(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPROLD          $17, Z0, Z0
        VPMULLD         Z0, Z13, Z0

        MOVL            28(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            28(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLD          $15, Z0, Z1
        VPXORD          Z0, Z1, Z0

        MOVL            32(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            32(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Z0, Z11, Z0
        VPSRLD          $13, Z0, Z1

        MOVL            36(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            36(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPXORD          Z0, Z1, Z0
        VPMULLD         Z0, Z12, Z0

        MOVL            40(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            40(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLD          $16, Z0, Z1
        VPXORD          Z0, Z1, Z0

        MOVL            44(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            44(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPERMD          Z0, Z17, Z0          // now 16 hashes in Z0

        MOVL            48(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            48(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLVD         Z7, Z0, Z1        // calculate register number
        VPSLLVD         Z5, Z0, Z0        // calculate register value

        MOVL            52(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            52(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPXORD          Z6, Z0, Z0

        MOVL            56(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            56(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPLZCNTD        Z0, Z0          // count leading zeros
        VPADDD          Z16, Z0, Z0      // add 1

        MOVL            60(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            60(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        // now we have 16 register numbers in Z1
        // with apropriate values in Z0
        
        // put the values into the registers, this cannot be vectorized
        VMOVDQU32       Z1, (R8)
        VMOVDQU32       Z0, (R9)

        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        
        XORQ            CX, CX
loop:
        MOVL            (R8)(CX*4), AX      // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            (R9)(CX*4), DX      // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $1, CX
        CMPQ            CX, $16
        JNE             loop

exit:
        RET

/***************************** filterAddManyInt64 ****************************************************/

// func filterAddManyInt64AVX2Core(f LogLogBeta, data []int64, seed uint32)
TEXT ·filterAddManyInt64AVX2Core(SB), NOSPLIT, $0-68
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
        VMOVDQU         Y1, (R8)
        VMOVDQU         Y0, (R9)

        SUBQ    $1, BX          
        JZ      exit_avx

loop_avx:
        ADDQ            $64, SI

        MOVL            (R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          (R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        // calculate hash
        VPADDD          Y9, Y14, Y0    
        VPADDD          Y8, Y0, Y0
        VMOVDQU	         0(SI), Y1
        VMOVDQU	        32(SI), Y2

        MOVL            4(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          4(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLQ          $32, Y1, Y3
        VPSLLQ          $32, Y2, Y4
        VPBLENDD        $0x55, Y1, Y4, Y1
        VPBLENDD        $0xaa, Y2, Y3, Y2

        MOVL            8(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          8(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y1, Y12, Y1
        VPADDD          Y0, Y1, Y0

        MOVL            12(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          12(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        rol32_17           (Y0)
        VPMULLD         Y0, Y13, Y0

        MOVL            16(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          16(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y2, Y12, Y2
        VPADDD          Y0, Y2, Y0
        rol32_17           (Y0)

        MOVL            20(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          20(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y0, Y13, Y0
        VPSRLD          $15, Y0, Y1
        VPXOR           Y0, Y1, Y0

        MOVL            24(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          24(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y0, Y11, Y0
        VPSRLD          $13, Y0, Y1
        VPXOR           Y0, Y1, Y0

        MOVL            28(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          28(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Y0, Y12, Y0
        VPSRLD          $16, Y0, Y1
        VPXOR           Y0, Y1, Y0      // now 8 hashes in Y0
        VPSRLVD         Y7, Y0, Y1        // calculate register number
        VPSLLVD         Y5, Y0, Y0        // calculate register value
        VPXOR           Y6, Y0, Y0
        
        // now we have 8 register numbers in Y1
        // with apropriate values in Y0
                
        // put the values into the registers, this cannot be vectorized
        VMOVDQU         Y1, (R8)
        VMOVDQU         Y0, (R9)

        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        
        XORQ            CX, CX
loop:
        MOVL            (R8)(CX*4), AX      // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          (R9)(CX*4), DX      // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $1, CX
        CMPQ            CX, $8
        JNE             loop

exit:
        RET

// func XYZfilterAddManyInt64AVX512Core(f LogLogBeta, data []int64, seed uint32)
TEXT ·filterAddManyInt64AVX512Core(SB), NOSPLIT, $0-68
        MOVQ    data_base+48(FP), SI
        MOVQ    data_len+56(FP), BX
        MOVQ    f_buf_base+24(FP), DI

        SHRQ    $4, BX      // less than 16 data values          
        JZ      exit

        VPBROADCASTD    PRIME32_1<>+0x00(SB), Z10  // prime numbers for hash values
        VPBROADCASTD    PRIME32_2<>+0x00(SB), Z11
        VPBROADCASTD    PRIME32_3<>+0x00(SB), Z12
        VPBROADCASTD    PRIME32_4<>+0x00(SB), Z13
        VPBROADCASTD    PRIME32_5<>+0x00(SB), Z14

        VPBROADCASTD    seed+72(FP), Z9
        VPBROADCASTD    f_max+8(FP), Z7
        VPBROADCASTD    f_maxX+12(FP), Z6
        VPBROADCASTD    f_precision+0(FP), Z5
        VPBROADCASTD    constU32_8<>(SB), Z8
        VPBROADCASTD    constU32_1<>(SB), Z16
        VMOVDQU64       perm512<>(SB), Z17

        MOVW            $0x5555, AX
        KMOVW           AX, K1
        MOVW            $0xaaaa, AX
        KMOVW           AX, K2
        
        LEAQ    buf_pos<>(SB), R8   // buffer for register numbers
        LEAQ    buf_val<>(SB), R9   // buffer for register values

        // calculate hash
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

        VPSRLD          $15, Z0, Z1
        VPXORD          Z0, Z1, Z0
        VPMULLD         Z0, Z11, Z0
        VPSRLD          $13, Z0, Z1
        VPXORD          Z0, Z1, Z0
        VPMULLD         Z0, Z12, Z0
        VPSRLD          $16, Z0, Z1
        VPXORD          Z0, Z1, Z0

        VPERMD          Z0, Z17, Z0          // now 16 hashes in Z0

        VPSRLVD         Z7, Z0, Z1        // calculate register number
        VPSLLVD         Z5, Z0, Z0        // calculate register value
        VPXORD          Z6, Z0, Z0
        
        VPLZCNTD        Z0, Z0          // count leading zeros
        VPADDD          Z16, Z0, Z0      // add 1

        // now we have 16 register numbers in Z1
        // with apropriate values in Z0
                
        // put the values into the registers, this cannot be vectorized
        VMOVDQU32       Z1, (R8)
        VMOVDQU32       Z0, (R9)

        SUBQ    $1, BX          
        JZ      exit_avx

loop_avx:
        MOVL            (R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            (R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $128, SI
        // calculate hash
        VPADDD          Z9, Z14, Z0    
        VPADDD          Z8, Z0, Z0

        MOVL            4(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            4(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VMOVDQU64	     0(SI), Z1
        VMOVDQU64	    64(SI), Z2
        VPSRLQ          $32, Z1, Z3

        MOVL            8(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            8(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSLLQ          $32, Z2, Z4
        VPBLENDMD       Z1, Z4, K1, Z1
        VPBLENDMD       Z2, Z3, K2, Z2

        MOVL            12(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            12(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Z1, Z12, Z1
        VPADDD          Z0, Z1, Z0

        MOVL            16(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            16(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPROLD          $17, Z0, Z0
        VPMULLD         Z0, Z13, Z0

        MOVL            20(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            20(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Z2, Z12, Z2
        VPADDD          Z0, Z2, Z0

        MOVL            24(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            24(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPROLD          $17, Z0, Z0
        VPMULLD         Z0, Z13, Z0

        MOVL            28(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            28(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLD          $15, Z0, Z1
        VPXORD          Z0, Z1, Z0

        MOVL            32(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            32(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPMULLD         Z0, Z11, Z0
        VPSRLD          $13, Z0, Z1

        MOVL            36(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            36(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPXORD          Z0, Z1, Z0
        VPMULLD         Z0, Z12, Z0

        MOVL            40(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            40(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLD          $16, Z0, Z1
        VPXORD          Z0, Z1, Z0

        MOVL            44(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            44(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPERMD          Z0, Z17, Z0          // now 16 hashes in Z0

        MOVL            48(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            48(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPSRLVD         Z7, Z0, Z1        // calculate register number
        VPSLLVD         Z5, Z0, Z0        // calculate register value

        MOVL            52(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            52(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPXORD          Z6, Z0, Z0

        MOVL            56(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            56(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        VPLZCNTD        Z0, Z0          // count leading zeros
        VPADDD          Z16, Z0, Z0      // add 1

        MOVL            60(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            60(R9), DX            // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        // now we have 16 register numbers in Z1
        // with apropriate values in Z0
        
        // put the values into the registers, this cannot be vectorized
        VMOVDQU32       Z1, (R8)
        VMOVDQU32       Z0, (R9)

        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        
        XORQ            CX, CX
loop:
        MOVL            (R8)(CX*4), AX      // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        MOVL            (R9)(CX*4), DX      // read value
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $1, CX
        CMPQ            CX, $16
        JNE             loop

exit:
        RET

/************************************************************************************************+
 * Because the functions here are little bit sophisticated, we show their evolution for better understanding
 * for further develeopment
 *************************************************************************************************

 ****************************************************************************************************
 * This works like the generic implementation:
 * first it generates 8 hashes with vectorized code
 * then it puts the 8 hashes in the filter with an small loop. This cannot be vectorized
 
// func filterAddManyUint32AVX2Core(f LogLogBeta, data []uint32, seed uint32)
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

        XORQ            R10, R10

loop_avx:
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
        VMOVDQU         Y1, (R8)
        VMOVDQU         Y0, (R9)
        ADDQ            $32, SI
        
        XORQ            CX, CX
loop:
        MOVL            (R8)(CX*4), AX      // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          (R9)(CX*4), DX      // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $1, CX
        CMPQ            CX, $8
        JNE             loop

        SUBQ            $1, BX          
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER

exit:
        RET

 *************************************************************************************************
 * Here we break the data dependency between calculating the hashes and putting them into the filter
 * in the main loop first we put the last hashes in the filter and then we calculate the new hashes
 * furthermore the small loop for putting the hashes into the filter is unrolled
 *
 * next evolution step would be to interleave the code for for processing the old hashes and calculating
 * the new ones to achieve instruction level parallism. This is how the functions above are working

// func filterAddManyUint32AVX2Core(f LogLogBeta, data []uint32, seed uint32)
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
        VMOVDQU         Y1, (R8)
        VMOVDQU         Y0, (R9)

        SUBQ    $1, BX          
        JZ      exit_avx

loop_avx:
        MOVL            (R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          (R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        MOVL            4(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          4(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        MOVL            8(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          8(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        MOVL            12(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          12(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        MOVL            16(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          16(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        MOVL            20(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          20(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        MOVL            24(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          24(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        MOVL            28(R8), AX            // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          28(R9), DX            // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $32, SI
        
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
        VMOVDQU         Y1, (R8)
        VMOVDQU         Y0, (R9)

        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER
        
        XORQ            CX, CX
loop:
        MOVL            (R8)(CX*4), AX      // read register number
        MOVB            (DI)(AX*1), R10     // get old register value
        LZCNTL          (R9)(CX*4), DX      // read value and count leading zeros
        ADDL            $1, DX
        CMPB            DX, R10             // compare new and old value 
        CMOVLLE         R10, DX             // keep old value if greater       
        MOVB            DX, (DI)(AX*1)      // write value back

        ADDQ            $1, CX
        CMPQ            CX, $8
        JNE             loop

exit:
        RET

**********************************************************************************************/
