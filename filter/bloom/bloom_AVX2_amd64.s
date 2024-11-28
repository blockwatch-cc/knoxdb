// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefanx@blockwatch.cc

#include "textflag.h"
#include "constants_AVX2.h"

#define rol32_17(Y_reg) \
    VPSLLD  $17, Y_reg, Y15 \
    VPSRLD  $15, Y_reg, Y_reg \
    VPOR    Y15, Y_reg, Y_reg

/***************************** filterAddManyUint32 ****************************************************/

// func filterAddManyUint32AVX2Core(f Filter, data []uint32, seed uint32)
TEXT ·filterAddManyUint32AVX2Core(SB), NOSPLIT, $0-64
        MOVQ    data_base+32(FP), SI
        MOVQ    data_len+40(FP), BX
        MOVQ    f_b_base+8(FP), DI

        SHRQ    $3, BX    // less than 8 data values          
        JZ      exit

        VPBROADCASTD    PRIME32_1<>+0x00(SB), Y10  // prime numbers for hash values
        VPBROADCASTD    PRIME32_2<>+0x00(SB), Y11
        VPBROADCASTD    PRIME32_3<>+0x00(SB), Y12
        VPBROADCASTD    PRIME32_4<>+0x00(SB), Y13
        VPBROADCASTD    PRIME32_5<>+0x00(SB), Y14

        VPBROADCASTD    seed+56(FP), Y9
        VPBROADCASTD    constU32_4<>(SB), Y8

        LEAQ    buf_bpos<>(SB), R8   // buffer for byte positions
        LEAQ    buf_mask<>(SB), R9   // buffer for bitmasks

        // calculate 1st hash
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

        // calculate 2ndt hash (seed is zero)
        VPADDD          Y8, Y14, Y2    
 
        VMOVDQU         (SI), Y3
        VPMULLD         Y3, Y12, Y3
        VPADDD          Y2, Y3, Y2
        rol32_17           (Y2)
        VPMULLD         Y2, Y13, Y2

        VPSRLD          $15, Y2, Y3
        VPXOR           Y2, Y3, Y2
        VPMULLD         Y2, Y11, Y2
        VPSRLD          $13, Y2, Y3
        VPXOR           Y2, Y3, Y2
        VPMULLD         Y2, Y12, Y2
        VPSRLD          $16, Y2, Y3
        VPXOR           Y2, Y3, Y3      
       
        // now 8 hashes in Y0=h0 and 8 in Y3=h1
        // we use loc(i) = Yi = h0 + i*h1
        VPBROADCASTD    f_mask+4(FP), Y15      // mask for filter locations
        VPADDD          Y0, Y3, Y1
        VPADDD          Y1, Y3, Y2      
        VPADDD          Y2, Y3, Y3      
        VPAND           Y15, Y0, Y0      
        VPAND           Y15, Y1, Y1      
        VPAND           Y15, Y2, Y2      
        VPAND           Y15, Y3, Y3      
        // now we have 4x8=32 locations in Y0..Y3
        VPCMPEQQ        Y7, Y7, Y7
        VPSRLD          $29, Y7, Y7         // create 0..0111 mask
        VPCMPEQQ        Y15, Y15, Y15
        VPSRLD          $31, Y15, Y15       // Y15 =[1,1,...]
        // calculate bit positions from locations
        VPAND           Y7, Y0, Y4
        VPAND           Y7, Y1, Y5
        VPAND           Y7, Y2, Y6
        VPAND           Y7, Y3, Y7
        // now we have 4x8=32 bit positions in Y4..Y7
        // calculate byte positions from locations
        VPSRLD          $3, Y0, Y0
        VPSRLD          $3, Y1, Y1
        VPSRLD          $3, Y2, Y2
        VPSRLD          $3, Y3, Y3
        // now we have 4x8=32 byte positions in Y0..Y3
        // make bit positions to bitmasks
        VPSLLVD         Y4, Y15, Y4          
        VPSLLVD         Y5, Y15, Y5          
        VPSLLVD         Y6, Y15, Y6          
        VPSLLVD         Y7, Y15, Y7
        // now we have 4x8=32 byte positions in Y0..Y3
        // with apropriate bitmasks in Y4...Y7
    
        // put the locations into the filter, this cannot be vectorized
        VMOVDQU         Y0, 0(R8)
        VMOVDQU         Y1, 32(R8)
        VMOVDQU         Y2, 64(R8)
        VMOVDQU         Y3, 96(R8)
        VMOVDQU         Y4, 0(R9)
        VMOVDQU         Y5, 32(R9)
        VMOVDQU         Y6, 64(R9)
        VMOVDQU         Y7, 96(R9)

        SUBQ    $1, BX          
        JZ      exit_avx

loop_avx:
        ADDQ            $32, SI
        MOVL            (R8), AX
        MOVL            (R9), DX
        ORB             DX, (DI)(AX*1)  
        // calculate 1st hash
        VPADDD          Y9, Y14, Y0    
        VPADDD          Y8, Y0, Y0

        MOVL            4(R8), AX
        MOVL            4(R9), DX
        ORB             DX, (DI)(AX*1)  
        VMOVDQU         (SI), Y1
        VPMULLD         Y1, Y12, Y1

        MOVL            8(R8), AX
        MOVL            8(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPADDD          Y0, Y1, Y0
        rol32_17           (Y0)

        MOVL            12(R8), AX
        MOVL            12(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPMULLD         Y0, Y13, Y0
        VPSRLD          $15, Y0, Y1

        MOVL            16(R8), AX
        MOVL            16(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPXOR           Y0, Y1, Y0
        VPMULLD         Y0, Y11, Y0

        MOVL            20(R8), AX
        MOVL            20(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSRLD          $13, Y0, Y1
        VPXOR           Y0, Y1, Y0

        MOVL            24(R8), AX
        MOVL            24(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPMULLD         Y0, Y12, Y0
        VPSRLD          $16, Y0, Y1

        MOVL            28(R8), AX
        MOVL            28(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPXOR           Y0, Y1, Y0      // now 8 hashes in Y0
        // calculate 2ndt hash (seed is zero)
        VPADDD          Y8, Y14, Y2    

        MOVL            32(R8), AX
        MOVL            32(R9), DX
        ORB             DX, (DI)(AX*1)  
        VMOVDQU         (SI), Y3
        VPMULLD         Y3, Y12, Y3

        MOVL            36(R8), AX
        MOVL            36(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPADDD          Y2, Y3, Y2
        rol32_17           (Y2)

        MOVL            40(R8), AX
        MOVL            40(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPMULLD         Y2, Y13, Y2
        VPSRLD          $15, Y2, Y3

        MOVL            44(R8), AX
        MOVL            44(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPXOR           Y2, Y3, Y2
        VPMULLD         Y2, Y11, Y2

        MOVL            48(R8), AX
        MOVL            48(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSRLD          $13, Y2, Y3
        VPXOR           Y2, Y3, Y2

        MOVL            52(R8), AX
        MOVL            52(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPMULLD         Y2, Y12, Y2
        VPSRLD          $16, Y2, Y3

        MOVL            56(R8), AX
        MOVL            56(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPXOR           Y2, Y3, Y3      
        // now 8 hashes in Y0=h0 and 8 in Y3=h1
        // we use loc(i) = Yi = h0 + i*h1
        VPBROADCASTD    f_mask+4(FP), Y15      // mask for filter locations

        MOVL            60(R8), AX
        MOVL            60(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPADDD          Y0, Y3, Y1
        VPADDD          Y1, Y3, Y2      

        MOVL            64(R8), AX
        MOVL            64(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPADDD          Y2, Y3, Y3      
        VPAND           Y15, Y0, Y0      

        MOVL            68(R8), AX
        MOVL            68(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPAND           Y15, Y1, Y1      
        VPAND           Y15, Y2, Y2      

        MOVL            72(R8), AX
        MOVL            72(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPAND           Y15, Y3, Y3      
        // now we have 4x8=32 locations in Y0..Y3
        VPCMPEQQ        Y7, Y7, Y7

        MOVL            76(R8), AX
        MOVL            76(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSRLD          $29, Y7, Y7         // create 0..0111 mask
        VPCMPEQQ        Y15, Y15, Y15

        MOVL            80(R8), AX
        MOVL            80(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSRLD          $31, Y15, Y15       // Y15 =[1,1,...]
        // calculate bit positions from locations
        VPAND           Y7, Y0, Y4

        MOVL            84(R8), AX
        MOVL            84(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPAND           Y7, Y1, Y5
        VPAND           Y7, Y2, Y6

        MOVL            88(R8), AX
        MOVL            88(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPAND           Y7, Y3, Y7
        // now we have 4x8=32 bit positions in Y4..Y7
        // calculate byte positions from locations
        VPSRLD          $3, Y0, Y0

        MOVL            92(R8), AX
        MOVL            92(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSRLD          $3, Y1, Y1
        VPSRLD          $3, Y2, Y2

        MOVL            96(R8), AX
        MOVL            96(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSRLD          $3, Y3, Y3
        // now we have 4x8=32 byte positions in Y0..Y3
        // make bit positions to bitmasks
        VPSLLVD         Y4, Y15, Y4          

        MOVL            100(R8), AX
        MOVL            100(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSLLVD         Y5, Y15, Y5          
        VPSLLVD         Y6, Y15, Y6          

        MOVL            104(R8), AX
        MOVL            104(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSLLVD         Y7, Y15, Y7

        MOVL            108(R8), AX
        MOVL            108(R9), DX
        ORB             DX, (DI)(AX*1)  

        MOVL            112(R8), AX
        MOVL            112(R9), DX
        ORB             DX, (DI)(AX*1)  

        MOVL            116(R8), AX
        MOVL            116(R9), DX
        ORB             DX, (DI)(AX*1)  

        MOVL            120(R8), AX
        MOVL            120(R9), DX
        ORB             DX, (DI)(AX*1)  

        MOVL            124(R8), AX
        MOVL            124(R9), DX
        ORB             DX, (DI)(AX*1)  
 
        // now we have 4x8=32 byte positions in Y0..Y3
        // with apropriate bitmasks in Y4...Y7
    
        // put the locations into the filter, this cannot be vectorized
        VMOVDQU         Y0, 0(R8)
        VMOVDQU         Y1, 32(R8)
        VMOVDQU         Y2, 64(R8)
        VMOVDQU         Y3, 96(R8)
        VMOVDQU         Y4, 0(R9)
        VMOVDQU         Y5, 32(R9)
        VMOVDQU         Y6, 64(R9)
        VMOVDQU         Y7, 96(R9)

        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER

        XORQ            CX, CX
loop:
        MOVL            (R8)(CX*4), AX
        MOVL            (R9)(CX*4), DX
        ORB             DX, (DI)(AX*1)  

        ADDQ            $1, CX
        CMPQ            CX, $32
        JNE             loop

exit:
        RET

/***************************** filterAddManyInt32 ****************************************************/

// func filterAddManyInt32AVX2Core(f Filter, data []int32, seed uint32)
TEXT ·filterAddManyInt32AVX2Core(SB), NOSPLIT, $0-64
        MOVQ    data_base+32(FP), SI
        MOVQ    data_len+40(FP), BX
        MOVQ    f_b_base+8(FP), DI

        SHRQ    $3, BX    // less than 8 data values          
        JZ      exit

        VPBROADCASTD    PRIME32_1<>+0x00(SB), Y10  // prime numbers for hash values
        VPBROADCASTD    PRIME32_2<>+0x00(SB), Y11
        VPBROADCASTD    PRIME32_3<>+0x00(SB), Y12
        VPBROADCASTD    PRIME32_4<>+0x00(SB), Y13
        VPBROADCASTD    PRIME32_5<>+0x00(SB), Y14

        VPBROADCASTD    seed+56(FP), Y9
        VPBROADCASTD    constU32_4<>(SB), Y8

        LEAQ    buf_bpos<>(SB), R8   // buffer for byte positions
        LEAQ    buf_mask<>(SB), R9   // buffer for bitmasks

        // calculate 1st hash
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

        // calculate 2ndt hash (seed is zero)
        VPADDD          Y8, Y14, Y2    
 
        VMOVDQU         (SI), Y3
        VPMULLD         Y3, Y12, Y3
        VPADDD          Y2, Y3, Y2
        rol32_17           (Y2)
        VPMULLD         Y2, Y13, Y2

        VPSRLD          $15, Y2, Y3
        VPXOR           Y2, Y3, Y2
        VPMULLD         Y2, Y11, Y2
        VPSRLD          $13, Y2, Y3
        VPXOR           Y2, Y3, Y2
        VPMULLD         Y2, Y12, Y2
        VPSRLD          $16, Y2, Y3
        VPXOR           Y2, Y3, Y3      
       
        // now 8 hashes in Y0=h0 and 8 in Y3=h1
        // we use loc(i) = Yi = h0 + i*h1
        VPBROADCASTD    f_mask+4(FP), Y15      // mask for filter locations
        VPADDD          Y0, Y3, Y1
        VPADDD          Y1, Y3, Y2      
        VPADDD          Y2, Y3, Y3      
        VPAND           Y15, Y0, Y0      
        VPAND           Y15, Y1, Y1      
        VPAND           Y15, Y2, Y2      
        VPAND           Y15, Y3, Y3      
        // now we have 4x8=32 locations in Y0..Y3
        VPCMPEQQ        Y7, Y7, Y7
        VPSRLD          $29, Y7, Y7         // create 0..0111 mask
        VPCMPEQQ        Y15, Y15, Y15
        VPSRLD          $31, Y15, Y15       // Y15 =[1,1,...]
        // calculate bit positions from locations
        VPAND           Y7, Y0, Y4
        VPAND           Y7, Y1, Y5
        VPAND           Y7, Y2, Y6
        VPAND           Y7, Y3, Y7
        // now we have 4x8=32 bit positions in Y4..Y7
        // calculate byte positions from locations
        VPSRLD          $3, Y0, Y0
        VPSRLD          $3, Y1, Y1
        VPSRLD          $3, Y2, Y2
        VPSRLD          $3, Y3, Y3
        // now we have 4x8=32 byte positions in Y0..Y3
        // make bit positions to bitmasks
        VPSLLVD         Y4, Y15, Y4          
        VPSLLVD         Y5, Y15, Y5          
        VPSLLVD         Y6, Y15, Y6          
        VPSLLVD         Y7, Y15, Y7
        // now we have 4x8=32 byte positions in Y0..Y3
        // with apropriate bitmasks in Y4...Y7
    
        // put the locations into the filter, this cannot be vectorized
        VMOVDQU         Y0, 0(R8)
        VMOVDQU         Y1, 32(R8)
        VMOVDQU         Y2, 64(R8)
        VMOVDQU         Y3, 96(R8)
        VMOVDQU         Y4, 0(R9)
        VMOVDQU         Y5, 32(R9)
        VMOVDQU         Y6, 64(R9)
        VMOVDQU         Y7, 96(R9)

        SUBQ    $1, BX          
        JZ      exit_avx

loop_avx:
        ADDQ            $32, SI
        MOVL            (R8), AX
        MOVL            (R9), DX
        ORB             DX, (DI)(AX*1)  
        // calculate 1st hash
        VPADDD          Y9, Y14, Y0    
        VPADDD          Y8, Y0, Y0

        MOVL            4(R8), AX
        MOVL            4(R9), DX
        ORB             DX, (DI)(AX*1)  
        VMOVDQU         (SI), Y1
        VPMULLD         Y1, Y12, Y1

        MOVL            8(R8), AX
        MOVL            8(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPADDD          Y0, Y1, Y0
        rol32_17           (Y0)

        MOVL            12(R8), AX
        MOVL            12(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPMULLD         Y0, Y13, Y0
        VPSRLD          $15, Y0, Y1

        MOVL            16(R8), AX
        MOVL            16(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPXOR           Y0, Y1, Y0
        VPMULLD         Y0, Y11, Y0

        MOVL            20(R8), AX
        MOVL            20(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSRLD          $13, Y0, Y1
        VPXOR           Y0, Y1, Y0

        MOVL            24(R8), AX
        MOVL            24(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPMULLD         Y0, Y12, Y0
        VPSRLD          $16, Y0, Y1

        MOVL            28(R8), AX
        MOVL            28(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPXOR           Y0, Y1, Y0      // now 8 hashes in Y0
        // calculate 2ndt hash (seed is zero)
        VPADDD          Y8, Y14, Y2    

        MOVL            32(R8), AX
        MOVL            32(R9), DX
        ORB             DX, (DI)(AX*1)  
        VMOVDQU         (SI), Y3
        VPMULLD         Y3, Y12, Y3

        MOVL            36(R8), AX
        MOVL            36(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPADDD          Y2, Y3, Y2
        rol32_17           (Y2)

        MOVL            40(R8), AX
        MOVL            40(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPMULLD         Y2, Y13, Y2
        VPSRLD          $15, Y2, Y3

        MOVL            44(R8), AX
        MOVL            44(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPXOR           Y2, Y3, Y2
        VPMULLD         Y2, Y11, Y2

        MOVL            48(R8), AX
        MOVL            48(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSRLD          $13, Y2, Y3
        VPXOR           Y2, Y3, Y2

        MOVL            52(R8), AX
        MOVL            52(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPMULLD         Y2, Y12, Y2
        VPSRLD          $16, Y2, Y3

        MOVL            56(R8), AX
        MOVL            56(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPXOR           Y2, Y3, Y3      
        // now 8 hashes in Y0=h0 and 8 in Y3=h1
        // we use loc(i) = Yi = h0 + i*h1
        VPBROADCASTD    f_mask+4(FP), Y15      // mask for filter locations

        MOVL            60(R8), AX
        MOVL            60(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPADDD          Y0, Y3, Y1
        VPADDD          Y1, Y3, Y2      

        MOVL            64(R8), AX
        MOVL            64(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPADDD          Y2, Y3, Y3      
        VPAND           Y15, Y0, Y0      

        MOVL            68(R8), AX
        MOVL            68(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPAND           Y15, Y1, Y1      
        VPAND           Y15, Y2, Y2      

        MOVL            72(R8), AX
        MOVL            72(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPAND           Y15, Y3, Y3      
        // now we have 4x8=32 locations in Y0..Y3
        VPCMPEQQ        Y7, Y7, Y7

        MOVL            76(R8), AX
        MOVL            76(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSRLD          $29, Y7, Y7         // create 0..0111 mask
        VPCMPEQQ        Y15, Y15, Y15

        MOVL            80(R8), AX
        MOVL            80(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSRLD          $31, Y15, Y15       // Y15 =[1,1,...]
        // calculate bit positions from locations
        VPAND           Y7, Y0, Y4

        MOVL            84(R8), AX
        MOVL            84(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPAND           Y7, Y1, Y5
        VPAND           Y7, Y2, Y6

        MOVL            88(R8), AX
        MOVL            88(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPAND           Y7, Y3, Y7
        // now we have 4x8=32 bit positions in Y4..Y7
        // calculate byte positions from locations
        VPSRLD          $3, Y0, Y0

        MOVL            92(R8), AX
        MOVL            92(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSRLD          $3, Y1, Y1
        VPSRLD          $3, Y2, Y2

        MOVL            96(R8), AX
        MOVL            96(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSRLD          $3, Y3, Y3
        // now we have 4x8=32 byte positions in Y0..Y3
        // make bit positions to bitmasks
        VPSLLVD         Y4, Y15, Y4          

        MOVL            100(R8), AX
        MOVL            100(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSLLVD         Y5, Y15, Y5          
        VPSLLVD         Y6, Y15, Y6          

        MOVL            104(R8), AX
        MOVL            104(R9), DX
        ORB             DX, (DI)(AX*1)  
        VPSLLVD         Y7, Y15, Y7

        MOVL            108(R8), AX
        MOVL            108(R9), DX
        ORB             DX, (DI)(AX*1)  

        MOVL            112(R8), AX
        MOVL            112(R9), DX
        ORB             DX, (DI)(AX*1)  

        MOVL            116(R8), AX
        MOVL            116(R9), DX
        ORB             DX, (DI)(AX*1)  

        MOVL            120(R8), AX
        MOVL            120(R9), DX
        ORB             DX, (DI)(AX*1)  

        MOVL            124(R8), AX
        MOVL            124(R9), DX
        ORB             DX, (DI)(AX*1)  
 
        // now we have 4x8=32 byte positions in Y0..Y3
        // with apropriate bitmasks in Y4...Y7
    
        // put the locations into the filter, this cannot be vectorized
        VMOVDQU         Y0, 0(R8)
        VMOVDQU         Y1, 32(R8)
        VMOVDQU         Y2, 64(R8)
        VMOVDQU         Y3, 96(R8)
        VMOVDQU         Y4, 0(R9)
        VMOVDQU         Y5, 32(R9)
        VMOVDQU         Y6, 64(R9)
        VMOVDQU         Y7, 96(R9)

        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER

        XORQ            CX, CX
loop:
        MOVL            (R8)(CX*4), AX
        MOVL            (R9)(CX*4), DX
        ORB             DX, (DI)(AX*1)  

        ADDQ            $1, CX
        CMPQ            CX, $32
        JNE             loop

exit:
        RET

/***************************** filterAddManyUint64 ****************************************************/

// func filterAddManyUint64AVX2Core(f Filter, data []uint64, seed uint32)
TEXT ·filterAddManyUint64AVX2Core(SB), NOSPLIT, $0-64
        MOVQ    data_base+32(FP), SI
        MOVQ    data_len+40(FP), BX
        MOVQ    f_b_base+8(FP), DI

        SHRQ    $3, BX    // less than 8 data values          
        JZ      exit

        VPBROADCASTD    PRIME32_1<>+0x00(SB), Y10  // prime numbers for hash values
        VPBROADCASTD    PRIME32_2<>+0x00(SB), Y11
        VPBROADCASTD    PRIME32_3<>+0x00(SB), Y12
        VPBROADCASTD    PRIME32_4<>+0x00(SB), Y13
        VPBROADCASTD    PRIME32_5<>+0x00(SB), Y14

        VPBROADCASTD    seed+56(FP), Y9
        VPBROADCASTD    constU32_8<>(SB), Y8
        VMOVDQU	        perm<>(SB), Y7

        LEAQ    buf_bpos<>(SB), R8   // buffer for byte positions
        LEAQ    buf_mask<>(SB), R9   // buffer for bitmasks

        // calculate 1st hash
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
        VPXOR           Y0, Y1, Y0

        VPERMD          Y0, Y7, Y0          // now 8 hashes in Y0

        // calculate 2ndt hash (seed is zero)
        VPADDD          Y8, Y14, Y5    
 
        VMOVDQU	         0(SI), Y1
        VMOVDQU	        32(SI), Y2
        VPSRLQ          $32, Y1, Y3
        VPSLLQ          $32, Y2, Y4
        VPBLENDD        $0x55, Y1, Y4, Y1
        VPBLENDD        $0xaa, Y2, Y3, Y2

        VPMULLD         Y1, Y12, Y1
        VPADDD          Y5, Y1, Y5
        rol32_17           (Y5)
        VPMULLD         Y5, Y13, Y5

        VPMULLD         Y2, Y12, Y2
        VPADDD          Y5, Y2, Y5
        rol32_17           (Y5)
        VPMULLD         Y5, Y13, Y5

        VPSRLD          $15, Y5, Y1
        VPXOR           Y5, Y1, Y5
        VPMULLD         Y5, Y11, Y5
        VPSRLD          $13, Y5, Y1
        VPXOR           Y5, Y1, Y5
        VPMULLD         Y5, Y12, Y5
        VPSRLD          $16, Y5, Y1
        VPXOR           Y5, Y1, Y5

        VPERMD          Y5, Y7, Y3          // now 8 hashes in Y5
       
        // now 8 hashes in Y0=h0 and 8 in Y3=h1
        // we use loc(i) = Yi = h0 + i*h1
        VPBROADCASTD    f_mask+4(FP), Y15      // mask for filter locations
        VPADDD          Y0, Y3, Y1
        VPADDD          Y1, Y3, Y2      
        VPADDD          Y2, Y3, Y3      
        VPAND           Y15, Y0, Y0      
        VPAND           Y15, Y1, Y1      
        VPAND           Y15, Y2, Y2      
        VPAND           Y15, Y3, Y3      
        // now we have 4x8=32 locations in Y0..Y3
        VPCMPEQQ        Y7, Y7, Y7
        VPSRLD          $29, Y7, Y7         // create 0..0111 mask
        VPCMPEQQ        Y15, Y15, Y15
        VPSRLD          $31, Y15, Y15       // Y15 =[1,1,...]
        // calculate bit positions from locations
        VPAND           Y7, Y0, Y4
        VPAND           Y7, Y1, Y5
        VPAND           Y7, Y2, Y6
        VPAND           Y7, Y3, Y7
        // now we have 4x8=32 bit positions in Y4..Y7
        // calculate byte positions from locations
        VPSRLD          $3, Y0, Y0
        VPSRLD          $3, Y1, Y1
        VPSRLD          $3, Y2, Y2
        VPSRLD          $3, Y3, Y3
        // now we have 4x8=32 byte positions in Y0..Y3
        // make bit positions to bitmasks
        VPSLLVD         Y4, Y15, Y4          
        VPSLLVD         Y5, Y15, Y5          
        VPSLLVD         Y6, Y15, Y6          
        VPSLLVD         Y7, Y15, Y7
        // now we have 4x8=32 byte positions in Y0..Y3
        // with apropriate bitmasks in Y4...Y7
    
        // put the locations into the filter, this cannot be vectorized
        VMOVDQU         Y0, 0(R8)
        VMOVDQU         Y1, 32(R8)
        VMOVDQU         Y2, 64(R8)
        VMOVDQU         Y3, 96(R8)
        VMOVDQU         Y4, 0(R9)
        VMOVDQU         Y5, 32(R9)
        VMOVDQU         Y6, 64(R9)
        VMOVDQU         Y7, 96(R9)

        SUBQ    $1, BX          
        JZ      exit_avx

loop_avx:
        ADDQ            $64, SI

        MOVL            (R8), AX
        MOVL            (R9), DX
        ORB             DX, (DI)(AX*1)  

        // calculate 1st hash
        VMOVDQU	        perm<>(SB), Y7
        VPADDD          Y9, Y14, Y0    
        VPADDD          Y8, Y0, Y0

        MOVL            4(R8), AX
        MOVL            4(R9), DX
        ORB             DX, (DI)(AX*1)  

        VMOVDQU	         0(SI), Y1
        VMOVDQU	        32(SI), Y2
        VPSRLQ          $32, Y1, Y3

        MOVL            8(R8), AX
        MOVL            8(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPSLLQ          $32, Y2, Y4
        VPBLENDD        $0x55, Y1, Y4, Y1
        VPBLENDD        $0xaa, Y2, Y3, Y2

        MOVL            12(R8), AX
        MOVL            12(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y1, Y12, Y1

        MOVL            16(R8), AX
        MOVL            16(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPADDD          Y0, Y1, Y0
        rol32_17           (Y0)

        MOVL            20(R8), AX
        MOVL            20(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y0, Y13, Y0

        MOVL            24(R8), AX
        MOVL            24(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y2, Y12, Y2

        MOVL            28(R8), AX
        MOVL            28(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPADDD          Y0, Y2, Y0
        rol32_17           (Y0)

        MOVL            32(R8), AX
        MOVL            32(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y0, Y13, Y0

        MOVL            36(R8), AX
        MOVL            36(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPSRLD          $15, Y0, Y1
        VPXOR           Y0, Y1, Y0

        MOVL            40(R8), AX
        MOVL            40(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y0, Y11, Y0

        MOVL            44(R8), AX
        MOVL            44(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPSRLD          $13, Y0, Y1
        VPXOR           Y0, Y1, Y0

        MOVL            48(R8), AX
        MOVL            48(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y0, Y12, Y0

        MOVL            52(R8), AX
        MOVL            52(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPSRLD          $16, Y0, Y1
        VPXOR           Y0, Y1, Y0
        VPERMD          Y0, Y7, Y0          // now 8 hashes in Y0

        MOVL            56(R8), AX
        MOVL            56(R9), DX
        ORB             DX, (DI)(AX*1)  

        // calculate 2ndt hash (seed is zero)
        VPADDD          Y8, Y14, Y5    
        VMOVDQU	         0(SI), Y1
        VMOVDQU	        32(SI), Y2

        MOVL            60(R8), AX
        MOVL            60(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPSRLQ          $32, Y1, Y3
        VPSLLQ          $32, Y2, Y4
        VPBLENDD        $0x55, Y1, Y4, Y1
        VPBLENDD        $0xaa, Y2, Y3, Y2

        MOVL            64(R8), AX
        MOVL            64(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y1, Y12, Y1

        MOVL            68(R8), AX
        MOVL            68(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPADDD          Y5, Y1, Y5
        rol32_17           (Y5)

        MOVL            72(R8), AX
        MOVL            72(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y5, Y13, Y5

        MOVL            76(R8), AX
        MOVL            76(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y2, Y12, Y2

        MOVL            80(R8), AX
        MOVL            80(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPADDD          Y5, Y2, Y5
        rol32_17           (Y5)

        MOVL            84(R8), AX
        MOVL            84(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y5, Y13, Y5
        VPSRLD          $15, Y5, Y1

        MOVL            88(R8), AX
        MOVL            88(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPXOR           Y5, Y1, Y5
        VPMULLD         Y5, Y11, Y5

        MOVL            92(R8), AX
        MOVL            92(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPSRLD          $13, Y5, Y1
        VPXOR           Y5, Y1, Y5

        MOVL            96(R8), AX
        MOVL            96(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y5, Y12, Y5
        VPSRLD          $16, Y5, Y1

        MOVL            100(R8), AX
        MOVL            100(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPXOR           Y5, Y1, Y5
        VPERMD          Y5, Y7, Y3          // now 8 hashes in Y5
        // now 8 hashes in Y0=h0 and 8 in Y3=h1
        // we use loc(i) = Yi = h0 + i*h1
        VPBROADCASTD    f_mask+4(FP), Y15      // mask for filter locations
        VPADDD          Y0, Y3, Y1

        MOVL            104(R8), AX
        MOVL            104(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPADDD          Y1, Y3, Y2      
        VPADDD          Y2, Y3, Y3      
        VPAND           Y15, Y0, Y0      
        VPAND           Y15, Y1, Y1      
        VPAND           Y15, Y2, Y2      
        VPAND           Y15, Y3, Y3      
        // now we have 4x8=32 locations in Y0..Y3

        MOVL            108(R8), AX
        MOVL            108(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPCMPEQQ        Y7, Y7, Y7
        VPSRLD          $29, Y7, Y7         // create 0..0111 mask
        VPCMPEQQ        Y15, Y15, Y15
        VPSRLD          $31, Y15, Y15       // Y15 =[1,1,...]

        MOVL            112(R8), AX
        MOVL            112(R9), DX
        ORB             DX, (DI)(AX*1)  

        // calculate bit positions from locations
        VPAND           Y7, Y0, Y4
        VPAND           Y7, Y1, Y5
        VPAND           Y7, Y2, Y6
        VPAND           Y7, Y3, Y7
        // now we have 4x8=32 bit positions in Y4..Y7

        MOVL            116(R8), AX
        MOVL            116(R9), DX
        ORB             DX, (DI)(AX*1)  

        // calculate byte positions from locations
        VPSRLD          $3, Y0, Y0
        VPSRLD          $3, Y1, Y1
        VPSRLD          $3, Y2, Y2
        VPSRLD          $3, Y3, Y3
        // now we have 4x8=32 byte positions in Y0..Y3

        MOVL            120(R8), AX
        MOVL            120(R9), DX
        ORB             DX, (DI)(AX*1)  

        // make bit positions to bitmasks
        VPSLLVD         Y4, Y15, Y4          
        VPSLLVD         Y5, Y15, Y5          
        VPSLLVD         Y6, Y15, Y6          
        VPSLLVD         Y7, Y15, Y7

        MOVL            124(R8), AX
        MOVL            124(R9), DX
        ORB             DX, (DI)(AX*1)  
 
        // now we have 4x8=32 byte positions in Y0..Y3
        // with apropriate bitmasks in Y4...Y7
    
        // put the locations into the filter, this cannot be vectorized
        VMOVDQU         Y0, 0(R8)
        VMOVDQU         Y1, 32(R8)
        VMOVDQU         Y2, 64(R8)
        VMOVDQU         Y3, 96(R8)
        VMOVDQU         Y4, 0(R9)
        VMOVDQU         Y5, 32(R9)
        VMOVDQU         Y6, 64(R9)
        VMOVDQU         Y7, 96(R9)

        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER

        XORQ            CX, CX
loop:
        MOVL            (R8)(CX*4), AX
        MOVL            (R9)(CX*4), DX
        ORB             DX, (DI)(AX*1)  

        ADDQ            $1, CX
        CMPQ            CX, $32
        JNE             loop

exit:
        RET

/***************************** filterAddManyInt64 ****************************************************/

// func filterAddManyUint64AVX2Core(f Filter, data []int64, seed uint32)
TEXT ·filterAddManyInt64AVX2Core(SB), NOSPLIT, $0-64
        MOVQ    data_base+32(FP), SI
        MOVQ    data_len+40(FP), BX
        MOVQ    f_b_base+8(FP), DI

        SHRQ    $3, BX    // less than 8 data values          
        JZ      exit

        VPBROADCASTD    PRIME32_1<>+0x00(SB), Y10  // prime numbers for hash values
        VPBROADCASTD    PRIME32_2<>+0x00(SB), Y11
        VPBROADCASTD    PRIME32_3<>+0x00(SB), Y12
        VPBROADCASTD    PRIME32_4<>+0x00(SB), Y13
        VPBROADCASTD    PRIME32_5<>+0x00(SB), Y14

        VPBROADCASTD    seed+56(FP), Y9
        VPBROADCASTD    constU32_8<>(SB), Y8
        VMOVDQU	        perm<>(SB), Y7

        LEAQ    buf_bpos<>(SB), R8   // buffer for byte positions
        LEAQ    buf_mask<>(SB), R9   // buffer for bitmasks

        // calculate 1st hash
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
        VPXOR           Y0, Y1, Y0

        VPERMD          Y0, Y7, Y0          // now 8 hashes in Y0

        // calculate 2ndt hash (seed is zero)
        VPADDD          Y8, Y14, Y5    
 
        VMOVDQU	         0(SI), Y1
        VMOVDQU	        32(SI), Y2
        VPSRLQ          $32, Y1, Y3
        VPSLLQ          $32, Y2, Y4
        VPBLENDD        $0x55, Y1, Y4, Y1
        VPBLENDD        $0xaa, Y2, Y3, Y2

        VPMULLD         Y1, Y12, Y1
        VPADDD          Y5, Y1, Y5
        rol32_17           (Y5)
        VPMULLD         Y5, Y13, Y5

        VPMULLD         Y2, Y12, Y2
        VPADDD          Y5, Y2, Y5
        rol32_17           (Y5)
        VPMULLD         Y5, Y13, Y5

        VPSRLD          $15, Y5, Y1
        VPXOR           Y5, Y1, Y5
        VPMULLD         Y5, Y11, Y5
        VPSRLD          $13, Y5, Y1
        VPXOR           Y5, Y1, Y5
        VPMULLD         Y5, Y12, Y5
        VPSRLD          $16, Y5, Y1
        VPXOR           Y5, Y1, Y5

        VPERMD          Y5, Y7, Y3          // now 8 hashes in Y5
       
        // now 8 hashes in Y0=h0 and 8 in Y3=h1
        // we use loc(i) = Yi = h0 + i*h1
        VPBROADCASTD    f_mask+4(FP), Y15      // mask for filter locations
        VPADDD          Y0, Y3, Y1
        VPADDD          Y1, Y3, Y2      
        VPADDD          Y2, Y3, Y3      
        VPAND           Y15, Y0, Y0      
        VPAND           Y15, Y1, Y1      
        VPAND           Y15, Y2, Y2      
        VPAND           Y15, Y3, Y3      
        // now we have 4x8=32 locations in Y0..Y3
        VPCMPEQQ        Y7, Y7, Y7
        VPSRLD          $29, Y7, Y7         // create 0..0111 mask
        VPCMPEQQ        Y15, Y15, Y15
        VPSRLD          $31, Y15, Y15       // Y15 =[1,1,...]
        // calculate bit positions from locations
        VPAND           Y7, Y0, Y4
        VPAND           Y7, Y1, Y5
        VPAND           Y7, Y2, Y6
        VPAND           Y7, Y3, Y7
        // now we have 4x8=32 bit positions in Y4..Y7
        // calculate byte positions from locations
        VPSRLD          $3, Y0, Y0
        VPSRLD          $3, Y1, Y1
        VPSRLD          $3, Y2, Y2
        VPSRLD          $3, Y3, Y3
        // now we have 4x8=32 byte positions in Y0..Y3
        // make bit positions to bitmasks
        VPSLLVD         Y4, Y15, Y4          
        VPSLLVD         Y5, Y15, Y5          
        VPSLLVD         Y6, Y15, Y6          
        VPSLLVD         Y7, Y15, Y7
        // now we have 4x8=32 byte positions in Y0..Y3
        // with apropriate bitmasks in Y4...Y7
    
        // put the locations into the filter, this cannot be vectorized
        VMOVDQU         Y0, 0(R8)
        VMOVDQU         Y1, 32(R8)
        VMOVDQU         Y2, 64(R8)
        VMOVDQU         Y3, 96(R8)
        VMOVDQU         Y4, 0(R9)
        VMOVDQU         Y5, 32(R9)
        VMOVDQU         Y6, 64(R9)
        VMOVDQU         Y7, 96(R9)

        SUBQ    $1, BX          
        JZ      exit_avx

loop_avx:
        ADDQ            $64, SI

        MOVL            (R8), AX
        MOVL            (R9), DX
        ORB             DX, (DI)(AX*1)  

        // calculate 1st hash
        VMOVDQU	        perm<>(SB), Y7
        VPADDD          Y9, Y14, Y0    
        VPADDD          Y8, Y0, Y0

        MOVL            4(R8), AX
        MOVL            4(R9), DX
        ORB             DX, (DI)(AX*1)  

        VMOVDQU	         0(SI), Y1
        VMOVDQU	        32(SI), Y2
        VPSRLQ          $32, Y1, Y3

        MOVL            8(R8), AX
        MOVL            8(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPSLLQ          $32, Y2, Y4
        VPBLENDD        $0x55, Y1, Y4, Y1
        VPBLENDD        $0xaa, Y2, Y3, Y2

        MOVL            12(R8), AX
        MOVL            12(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y1, Y12, Y1

        MOVL            16(R8), AX
        MOVL            16(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPADDD          Y0, Y1, Y0
        rol32_17           (Y0)

        MOVL            20(R8), AX
        MOVL            20(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y0, Y13, Y0

        MOVL            24(R8), AX
        MOVL            24(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y2, Y12, Y2

        MOVL            28(R8), AX
        MOVL            28(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPADDD          Y0, Y2, Y0
        rol32_17           (Y0)

        MOVL            32(R8), AX
        MOVL            32(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y0, Y13, Y0

        MOVL            36(R8), AX
        MOVL            36(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPSRLD          $15, Y0, Y1
        VPXOR           Y0, Y1, Y0

        MOVL            40(R8), AX
        MOVL            40(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y0, Y11, Y0

        MOVL            44(R8), AX
        MOVL            44(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPSRLD          $13, Y0, Y1
        VPXOR           Y0, Y1, Y0

        MOVL            48(R8), AX
        MOVL            48(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y0, Y12, Y0

        MOVL            52(R8), AX
        MOVL            52(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPSRLD          $16, Y0, Y1
        VPXOR           Y0, Y1, Y0
        VPERMD          Y0, Y7, Y0          // now 8 hashes in Y0

        MOVL            56(R8), AX
        MOVL            56(R9), DX
        ORB             DX, (DI)(AX*1)  

        // calculate 2ndt hash (seed is zero)
        VPADDD          Y8, Y14, Y5    
        VMOVDQU	         0(SI), Y1
        VMOVDQU	        32(SI), Y2

        MOVL            60(R8), AX
        MOVL            60(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPSRLQ          $32, Y1, Y3
        VPSLLQ          $32, Y2, Y4
        VPBLENDD        $0x55, Y1, Y4, Y1
        VPBLENDD        $0xaa, Y2, Y3, Y2

        MOVL            64(R8), AX
        MOVL            64(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y1, Y12, Y1

        MOVL            68(R8), AX
        MOVL            68(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPADDD          Y5, Y1, Y5
        rol32_17           (Y5)

        MOVL            72(R8), AX
        MOVL            72(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y5, Y13, Y5

        MOVL            76(R8), AX
        MOVL            76(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y2, Y12, Y2

        MOVL            80(R8), AX
        MOVL            80(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPADDD          Y5, Y2, Y5
        rol32_17           (Y5)

        MOVL            84(R8), AX
        MOVL            84(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y5, Y13, Y5
        VPSRLD          $15, Y5, Y1

        MOVL            88(R8), AX
        MOVL            88(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPXOR           Y5, Y1, Y5
        VPMULLD         Y5, Y11, Y5

        MOVL            92(R8), AX
        MOVL            92(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPSRLD          $13, Y5, Y1
        VPXOR           Y5, Y1, Y5

        MOVL            96(R8), AX
        MOVL            96(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPMULLD         Y5, Y12, Y5
        VPSRLD          $16, Y5, Y1

        MOVL            100(R8), AX
        MOVL            100(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPXOR           Y5, Y1, Y5
        VPERMD          Y5, Y7, Y3          // now 8 hashes in Y5
        // now 8 hashes in Y0=h0 and 8 in Y3=h1
        // we use loc(i) = Yi = h0 + i*h1
        VPBROADCASTD    f_mask+4(FP), Y15      // mask for filter locations
        VPADDD          Y0, Y3, Y1

        MOVL            104(R8), AX
        MOVL            104(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPADDD          Y1, Y3, Y2      
        VPADDD          Y2, Y3, Y3      
        VPAND           Y15, Y0, Y0      
        VPAND           Y15, Y1, Y1      
        VPAND           Y15, Y2, Y2      
        VPAND           Y15, Y3, Y3      
        // now we have 4x8=32 locations in Y0..Y3

        MOVL            108(R8), AX
        MOVL            108(R9), DX
        ORB             DX, (DI)(AX*1)  

        VPCMPEQQ        Y7, Y7, Y7
        VPSRLD          $29, Y7, Y7         // create 0..0111 mask
        VPCMPEQQ        Y15, Y15, Y15
        VPSRLD          $31, Y15, Y15       // Y15 =[1,1,...]

        MOVL            112(R8), AX
        MOVL            112(R9), DX
        ORB             DX, (DI)(AX*1)  

        // calculate bit positions from locations
        VPAND           Y7, Y0, Y4
        VPAND           Y7, Y1, Y5
        VPAND           Y7, Y2, Y6
        VPAND           Y7, Y3, Y7
        // now we have 4x8=32 bit positions in Y4..Y7

        MOVL            116(R8), AX
        MOVL            116(R9), DX
        ORB             DX, (DI)(AX*1)  

        // calculate byte positions from locations
        VPSRLD          $3, Y0, Y0
        VPSRLD          $3, Y1, Y1
        VPSRLD          $3, Y2, Y2
        VPSRLD          $3, Y3, Y3
        // now we have 4x8=32 byte positions in Y0..Y3

        MOVL            120(R8), AX
        MOVL            120(R9), DX
        ORB             DX, (DI)(AX*1)  

        // make bit positions to bitmasks
        VPSLLVD         Y4, Y15, Y4          
        VPSLLVD         Y5, Y15, Y5          
        VPSLLVD         Y6, Y15, Y6          
        VPSLLVD         Y7, Y15, Y7

        MOVL            124(R8), AX
        MOVL            124(R9), DX
        ORB             DX, (DI)(AX*1)  
 
        // now we have 4x8=32 byte positions in Y0..Y3
        // with apropriate bitmasks in Y4...Y7
    
        // put the locations into the filter, this cannot be vectorized
        VMOVDQU         Y0, 0(R8)
        VMOVDQU         Y1, 32(R8)
        VMOVDQU         Y2, 64(R8)
        VMOVDQU         Y3, 96(R8)
        VMOVDQU         Y4, 0(R9)
        VMOVDQU         Y5, 32(R9)
        VMOVDQU         Y6, 64(R9)
        VMOVDQU         Y7, 96(R9)

        SUBQ            $1, BX
        JZ              exit_avx
        JMP             loop_avx

exit_avx:
        VZEROUPPER

        XORQ            CX, CX
loop:
        MOVL            (R8)(CX*4), AX
        MOVL            (R9)(CX*4), DX
        ORB             DX, (DI)(AX*1)  

        ADDQ            $1, CX
        CMPQ            CX, $32
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
	JB		prep_i64

	// works for data size 64 byte
loop_avx2:
	VMOVDQU		0(DI), Y0
	VPOR		0(SI), Y0, Y0
	VMOVDQU		32(DI), Y1
	VPOR		32(SI), Y1, Y1
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

	// works for data size 15 down to single byte
prep_i64:
	TESTQ	BX, BX
	JLE		done
	XORQ	AX, AX
	CMPL	BX, $8
	JB		prep_i8

loop_i64:
	MOVQ	0(DI), AX
	ORQ	    0(SI), AX
	MOVQ	AX, 0(SI)

	LEAQ	8(DI), DI
	LEAQ	8(SI), SI
	SUBL	$8, BX
	CMPL	BX, $8
	JB		prep_i8
	JMP		loop_i64

prep_i8:
	TESTQ	BX, BX
	JLE		done
	XORL	AX, AX

loop_i8:
	MOVB	0(DI), AX
	ORB	    0(SI), AX
	MOVB	AX, 0(SI)

	INCQ	DI
	INCQ	SI
	DECL	BX
	JZ		done
	JMP		loop_i8

done:
	RET

