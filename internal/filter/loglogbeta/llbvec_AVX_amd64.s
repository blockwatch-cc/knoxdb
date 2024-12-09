// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefanx@blockwatch.cc

#include "textflag.h"
#include "constants_AVX.h"

#define rol32_17(Y_reg) \
    VPSLLD  $17, Y_reg, Y15 \
    VPSRLD  $15, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

/***************************** filterAddManyUint32 ****************************************************/

// func filterAddManyUint32AVX2Core(f LogLogBeta, data []uint32, seed uint32)
TEXT ·filterAddManyUint32AVX2Core(SB), NOSPLIT, $0-76
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
TEXT ·filterAddManyUint32AVX512Core(SB), NOSPLIT, $0-76
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
TEXT ·filterAddManyInt32AVX2Core(SB), NOSPLIT, $0-76
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
TEXT ·filterAddManyInt32AVX512Core(SB), NOSPLIT, $0-76
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
TEXT ·filterAddManyUint64AVX2Core(SB), NOSPLIT, $0-76
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
TEXT ·filterAddManyUint64AVX512Core(SB), NOSPLIT, $0-76
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
TEXT ·filterAddManyInt64AVX2Core(SB), NOSPLIT, $0-76
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
TEXT ·filterAddManyInt64AVX512Core(SB), NOSPLIT, $0-76
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

/***************************** filterMerge ****************************************************/

// func filterMergeAVX2(dst, src []byte)
TEXT ·filterMergeAVX2(SB), NOSPLIT, $0-48
	MOVQ	dst_base+0(FP), SI
	MOVQ	dst_len+8(FP), BX
	MOVQ	src_base+24(FP), DI

	TESTQ	BX, BX
	JLE		done
    
	CMPQ	BX, $64     // slices smaller than 64 byte are handled separately
	JB		prep_i8

	// works for data size 64 byte
loop_avx2:
	VMOVDQU		0(DI), Y0
	VMOVDQU		32(DI), Y1
	VMOVDQU		0(SI), Y2
	VMOVDQU		32(SI), Y3

        VPCMPGTB    Y2, Y0, Y4
        VPCMPGTB    Y3, Y1, Y5

        VPBLENDVB   Y4, Y0, Y2, Y0   
        VPBLENDVB   Y5, Y1, Y3, Y1   

	VMOVDQU		Y0, 0(SI)
	VMOVDQU		Y1, 32(SI)

	LEAQ		64(DI), DI
	LEAQ		64(SI), SI
	SUBQ		$64, BX
	CMPQ		BX, $64
	JB			exit_avx2
	JMP			loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ	BX, BX
	JLE		done

prep_i8:
	XORQ	AX, AX

loop_i8:
	MOVB	    (DI), AX
	MOVB	    (SI), DX
        CMPB        DX, AX              // compare values 
        CMOVLLE     AX, DX              // keep greater one       
        MOVB        DX, (SI)            // write value back

	INCQ	DI
	INCQ	SI
	DECL	BX
	JZ		done
	JMP		loop_i8

done:
	RET

/***************************** regSumAndZeros ****************************************************/

// func regSumAndZerosAVX2(registers []uint8) (float64, float64)
TEXT ·regSumAndZerosAVX2(SB), NOSPLIT, $0-40
	MOVQ	        registers_base+0(FP), SI
	MOVQ	        registers_len+8(FP), BX

        VPXOR           Y10, Y10, Y10   // sum0
        VPXOR           Y11, Y11, Y11   // sum1
        VPXOR           Y12, Y12, Y12   // sum2
        VPXOR           Y13, Y13, Y13   // sum3
        XORQ            R11, R11        // zero count

	TESTQ	        BX, BX
	JLE	        done

        MOVQ            BX, DX
        ANDQ            $31, DX         // runs of i8 loop
	SHRQ	        $5, BX          // runs of avx2 loop
	JZ	        prep_i8

        VPBROADCASTD    constU32_1<>(SB), Y15
        VPXOR           Y14, Y14, Y14

	// works for data size 32 byte
loop_avx2:
        VMOVDQU	        (SI), Y4
        // count zeros
        VPCMPEQB        Y4, Y14, Y4
        VPMOVMSKB	Y4, AX          // move per byte MSBs into packed bitmask to r32 or r64
	POPCNTQ		AX, AX 
        ADDQ            AX, R11

        VPMOVZXBD       (SI), Y0        // load 32 bytes and convert to 32 32bit values
        VPMOVZXBD       8(SI), Y1 
        VPMOVZXBD       16(SI), Y2 
        VPMOVZXBD       24(SI), Y3 
    
    
        // calc sum
        VPSLLVD         Y0, Y15, Y0     // calc 2^r[i]
        VPSLLVD         Y1, Y15, Y1
        VPSLLVD         Y2, Y15, Y2
        VPSLLVD         Y3, Y15, Y3

        VCVTDQ2PS       Y0, Y0          // convert to 32bit float
        VCVTDQ2PS       Y1, Y1
        VCVTDQ2PS       Y2, Y2
        VCVTDQ2PS       Y3, Y3

        VRCPPS          Y0, Y0          // calc 1/(2^r[i])
        VRCPPS          Y1, Y1
        VRCPPS          Y2, Y2
        VRCPPS          Y3, Y3

        VADDPS          Y10, Y0, Y10    // accumulate values
        VADDPS          Y11, Y1, Y11
        VADDPS          Y12, Y2, Y12
        VADDPS          Y13, Y3, Y13

	ADDQ		$32, SI
	SUBQ		$1, BX
	JZ		exit_avx2
	JMP		loop_avx2

exit_avx2:
        // add Y10,...,Y13
        VADDPS          Y10, Y11, Y10
        VADDPS          Y12, Y13, Y12
        VADDPS          Y10, Y12, Y10

        // add all values in Y10
        VHADDPS         Y10, Y10, Y10
        VHADDPS         Y10, Y10, Y10
        VEXTRACTF128    $1, Y10, X0
        VADDSS          X0, X10, X10

	TESTQ	        DX, DX
	JLE		done

prep_i8:
        XORQ            R10, R10

loop_i8:
        MOVB            (SI), CX        // load 1 byte
    
        // count zeros
        CMPB            CX, $0
        SETEQ           R10
        ADDQ            R10, R11

        // calc sum
        MOVQ            $1, AX
        SHLQ            CL, AX          // calc 2^r[i]
        VCVTSI2SSQ      AX, X0, X0      // convert to 32bit float
        VRCPPS          X0, X0          // calc 1/(2^r[i])
        VADDPS          X10, X0, X10    // accumulate values

	INCQ	        SI
	DECL	        DX
	JZ		done
	JMP		loop_i8

done:
        VCVTSS2SD       X10, X10, X10   // convert to float64
        MOVSD           X10, ret+24(FP)

        VCVTSI2SDQ      R11, X0, X0     // convert to float64
        MOVSD           X0, ret+32(FP)
    
        VZEROUPPER
	RET

// func regSumAndZerosAVX512(registers []uint8) (float64, float64)
TEXT ·regSumAndZerosAVX512(SB), NOSPLIT, $0-40
	MOVQ	        registers_base+0(FP), SI
	MOVQ	        registers_len+8(FP), BX

        VPXORQ          Z10, Z10, Z10   // sum0
        VPXORQ          Z11, Z11, Z11   // sum1
        VPXORQ          Z12, Z12, Z12   // sum2
        VPXORQ          Z13, Z13, Z13   // sum3
        XORQ            R11, R11        // zero count

	TESTQ	        BX, BX
	JLE	        done

        MOVQ            BX, DX
        ANDQ            $63, DX         // runs of i8 loop
	SHRQ	        $6, BX          // runs of avx2 loop
	JZ	        prep_i8

        VPBROADCASTD    constU32_1<>(SB), Z15
        VPXORQ          Z14, Z14, Z14

	// works for data size 64 byte
loop_avx2:
        VMOVDQU64       (SI), Z4
        // count zeros
        VPCMPEQB        Z4, Z14, K1
	KMOVQ		K1, AX 
	POPCNTQ		AX, AX 
        ADDQ            AX, R11

        VPMOVZXBD       (SI), Z0        // load 64 bytes and convert to 64 32bit values
        VPMOVZXBD       16(SI), Z1 
        VPMOVZXBD       32(SI), Z2 
        VPMOVZXBD       48(SI), Z3 
    
    
        // calc sum
        VPSLLVD         Z0, Z15, Z0     // calc 2^r[i]
        VPSLLVD         Z1, Z15, Z1
        VPSLLVD         Z2, Z15, Z2
        VPSLLVD         Z3, Z15, Z3

        VCVTDQ2PS       Z0, Z0          // convert to 32bit float
        VCVTDQ2PS       Z1, Z1
        VCVTDQ2PS       Z2, Z2
        VCVTDQ2PS       Z3, Z3

        VRCP14PS        Z0, Z0          // calc 1/(2^r[i])
        VRCP14PS        Z1, Z1
        VRCP14PS        Z2, Z2
        VRCP14PS        Z3, Z3

        VADDPS          Z10, Z0, Z10    // accumulate values
        VADDPS          Z11, Z1, Z11
        VADDPS          Z12, Z2, Z12
        VADDPS          Z13, Z3, Z13

	ADDQ		$64, SI
	SUBQ		$1, BX
	JZ		exit_avx2
	JMP		loop_avx2

exit_avx2:
        // add Z10,...,Z13
        VADDPS          Z10, Z11, Z10
        VADDPS          Z12, Z13, Z12
        VADDPS          Z10, Z12, Z10

        // add all values in Z10
        VEXTRACTF64X4   $1, Z10, Y11
        VADDPS          Y10, Y11, Y10
        VHADDPS         Y10, Y10, Y10
        VHADDPS         Y10, Y10, Y10
        VEXTRACTF128    $1, Y10, X0
        VADDSS          X0, X10, X10

	TESTQ	        DX, DX
	JLE		done

prep_i8:
        XORQ            R10, R10

loop_i8:
        MOVB            (SI), CX        // load 1 byte
    
        // count zeros
        CMPB            CX, $0
        SETEQ           R10
        ADDQ            R10, R11

        // calc sum
        MOVQ            $1, AX
        SHLQ            CL, AX          // calc 2^r[i]
        VCVTSI2SSQ      AX, X0, X0      // convert to 32bit float
        VRCPPS          X0, X0          // calc 1/(2^r[i])
        VADDPS          X10, X0, X10    // accumulate values

	INCQ	        SI
	DECL	        DX
	JZ		done
	JMP		loop_i8

done:
        VCVTSS2SD       X10, X10, X10   // convert to float64
        MOVSD           X10, ret+24(FP)

        VCVTSI2SDQ      R11, X0, X0     // convert to float64
        MOVSD           X0, ret+32(FP)
    
        VZEROUPPER
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
TEXT ·filterAddManyUint32AVX2Core(SB), NOSPLIT, $0-76
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
 * next evolution step would be to interleave the code for processing the old hashes and calculating
 * the new ones to achieve instruction level parallism. This leads to the functions above

// func filterAddManyUint32AVX2Core(f LogLogBeta, data []uint32, seed uint32)
TEXT ·filterAddManyUint32AVX2Core(SB), NOSPLIT, $0-76
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
