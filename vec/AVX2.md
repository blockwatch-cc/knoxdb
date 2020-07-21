### Go Assembler

- Go Doc https://golang.org/doc/asm
- Go ASM specialities https://quasilyte.github.io/blog/post/go-asm-complementary-reference/
- AT&T ASM Instructions https://www.felixcloutier.com/x86/
- General ASM tutorial https://www.csc.depauw.edu/~bhoward/asmtut/asmtut2.html
- Handle missing opcodes with YASM https://blog.klauspost.com/adding-unsupported-instructions-in-golang-assembler/

Go slices are 16byte aligned, this works for SSE, but be careful with AVX/AVX2
https://blog.chewxy.com/2016/07/25/on-the-memory-alignment-of-go-slice-values/

A slice is 24 byte
```
type slice struct {
  ptr *ptr     // 8 byte
  len int32    // 4 byte + 4 byte padding
  cap int32    // 4 byte + 4 byte padding
}

In Go ASM, refer to slice data as

MOVQ src_base+0(FP), SI     // ptr to src data
MOVL src_len+8(FP), BX      // len(src) is int32
MOVL src_cap+16(FP), CX     // cap(src) is int32

where src is the Go variable name of the slice passed to the ASM func.

```

Go ASM examples

- https://mmcloughlin.com/posts/geohash-assembly
- https://lemire.me/blog/2018/01/09/how-fast-can-you-bit-interleave-32-bit-integers-simd-edition/
- https://quasilyte.github.io/blog/post/go-asm-complementary-reference/

https://github.com/minio/sha256-simd/blob/master/sha256blockAvx2_amd64.s
https://github.com/golang/crypto/blob/master/blake2b/blake2bAVX2_amd64.s
https://github.com/stuartcarnie/go-simd/blob/master/sum_avx2_amd64.s

move data into regs
```
MOVQ	g(CX), AX     // Move g into AX.
MOVQ	g_m(AX), BX   // Move g.m into BX.
```

Registers
```
32-bit: (E)AX, CX, DX, BX, SP, BP, SI, DI, 8..15
64-bit: (R)AX, CX, DX, BX, SP, BP, SI, DI, 8..15
XMM    registers: X0 .. X15 (128 bit)
AVX/2  registers: Y0 .. Y15 (256 bit)
AVX512 registers: Z0 .. Z15 (512 bit)
```

Jumps
```
Go      Intel
----------------------
JCC     JAE
JCS     JB
JCXZL   JECXZ
JEQ     JE,JZ
JGE     JGE
JGT     JG
JHI     JA
JLE     JLE
JLS     JBE
JLT     JL
JMI     JS
JNE     JNE, JNZ
JOC     JNO
JOS     JO
JPC     JNP, JPO
JPL     JNS
JPS     JP, JPE
```

## General SIMD Literature

- CPU support https://en.wikipedia.org/wiki/Advanced_Vector_Extensions
- Substring search http://0x80.pl/articles/simd-strfind.html
- Popcount http://0x80.pl/articles/sse-popcount.html
- Geohash https://mmcloughlin.com/posts/geohash-assembly
- Ternary functions (AVX512, and replacements for AVX2) http://0x80.pl/articles/avx512-ternary-functions.html https://github.com/WojciechMula/ternary-logic
- AVX2 left pack with mask https://stackoverflow.com/questions/36932240/avx2-what-is-the-most-efficient-way-to-pack-left-based-on-a-mask
- AVX alternative to vector AVX2 shift https://stackoverflow.com/questions/36637315/avx-alternative-of-avx2s-vector-shift

## Interesting Vector Instructions

### Compare Packed Integers for Equality
- PCMPEQ(B|W|D|Q)

### Compare Packed Integers for Greater Than
- vpcmpgt(q|w|b|d)
- PCMPGTB

### Logical AND, AND NOT, OR, XOR
- vpand, vpandn, vpor, vpxor
- PAND, PANDN, VPOR, PXOR, PXORD, PXORQ

### Insert and Extract Packed Integer Values
- vinserti128
- VINSERTI128, VINSERTI32x4, VINSERTI64x4
- vextracti128

### Load Integer and Broadcast
- VPBROADCAST{Q|W|B|D}

### Move Byte Mask (creates mask from MSB bits of each byte in Yn)
- vpmovmskb
- PMOVMSKB

AVX2: Move a 32-bit mask of ymm1 to reg. The upper bits of r64 are filled with zeros.

### Blend Packed Words

PBLENDW

### Packed Shuffle Bytes

PSHUFB

https://software.intel.com/en-us/blogs/2015/01/13/programming-using-avx2-permutations

### Permutate Doublewords/Words Elements

VPERMD/VPERMW


## Examples

### NOT EQUAL

```
PMOVMSKB X1, AX
XORQ	 $0xffff, AX	// convert EQ to NE
```

### int64

pcmpgtd xmm, xmm   SSE2 // 32bit
pcmpgtq xmm, xmm   SSE4 // 64bit
VPCMPGTQ ymm1, ymm2, ymm3/m256  // AVX2 64bit
VPCMPEQQ ymm1, ymm2, ymm3 /m256 // AVX2 64bit

- Lt: VPCMPGTQ with switched operands
- Lte: VPCMPGTQ / VPOR / VPCMPEQQ with switched operands == NOT VPCMPGTQ
- Gt: VPCMPGTQ
- Gte: VPCMPGTQ / VPOR / VPCMPEQQ == NOT LT


To compare as unsigned integers you can add 0x8000000000000000 to both a and b. and use the signed compare.

Or better, XOR instead of ADD (runs on more ports on Haswell/Broadwell). It's the top bit, so add = add-without-carry.

```
__m128i _mm_cmplt_epu8(__m128i a, __m128i b) {
    __m128i as = _mm_add_epi8(a, _mm_set1_epi8((char)0x80));
    __m128i bs = _mm_add_epi8(b, _mm_set1_epi8((char)0x80));
    return _mm_cmplt_epi8(as, bs);
}
```

EFlags are set according to:
https://mudongliang.github.io/x86/html/file_module_x86_id_316.html

See https://www.felixcloutier.com/x86/cmppd for immediate values for predicate

another way to compare unsigned int
```
inline __m128i NotEqual8u(__m128i a, __m128i b)
{
	return _mm_andnot_si128(_mm_cmpeq_epi8(a, b), _mm_set1_epi8(-1));
}

inline __m128i Greater8u(__m128i a, __m128i b)
{
	return _mm_andnot_si128(_mm_cmpeq_epi8(_mm_min_epu8(a, b), a), _mm_set1_epi8(-1));
}

inline __m128i GreaterOrEqual8u(__m128i a, __m128i b)
{
	return _mm_cmpeq_epi8(_mm_max_epu8(a, b), a);
}

inline __m128i Lesser8u(__m128i a, __m128i b)
{
	return _mm_andnot_si128(_mm_cmpeq_epi8(_mm_max_epu8(a, b), a), _mm_set1_epi8(-1));
}

inline __m128i LesserOrEqual8u(__m128i a, __m128i b)
{
	return _mm_cmpeq_epi8(_mm_min_epu8(a, b), a);
}

### range
https://stackoverflow.com/questions/17095324/fastest-way-to-determine-if-an-integer-is-between-two-integers-inclusive-with/17095534#17095534

return ((unsigned)(number-lower) <= (upper-lower))

```

### float64

VCMPPD — Compare Packed Double-Precision Floating-Point Values

```
// serial (using XMM reg)
MOVSD val+40(SP), X0
MOVSD (DI), X1
UCOMISD	X0, X1
then SETcc, Jcc

// vector
VBROADCASTSD val+24(FP), Y0
VCMPPD		 $0, 0(SI), Y0, Y1   // imm8 = $0 (equal, nosignal)
VCMPPD		 $0x11, 0(SI), Y0, Y1   // imm8 = $0x11 (less than, nosignal)
VCMPPD		 $0x12, 0(SI), Y0, Y1   // imm8 = $0x12 (less than or equal, nosignal)
VCMPPD		 $0x1d, 0(SI), Y0, Y1   // imm8 = $0x1d (greater than or equal, nosignal)
VCMPPD		 $0x1e, 0(SI), Y0, Y1   // imm8 = $0x1e (greater than, nosignal)
```


### Float accumulator

https://stackoverflow.com/questions/6996764/fastest-way-to-do-horizontal-float-vector-sum-on-x86

Agner Optimizing book p.108

```
; Example 12.11c, Two XMM vector accumulators
	lea     esi,  list            ; list must be aligned by 16
	movapd  xmm0, [esi]           ; list[0], list[1]
	movapd  xmm1, [esi+16]        ; list[2], list[3]
	add     esi,  800             ; Point to end of list
	mov     eax,  32-800          ; Index to list[4] from end of list
L1:
	addpd   xmm0, [esi+eax]       ; Add list[i],   list[i+1]
	addpd   xmm1, [esi+eax+16]    ; Add list[i+2], list[i+3]
	add     eax,32                ; i += 4
	js      L1                    ; Loop

	addpd   xmm0, xmm1            ; Add the two accumulators together
	movhlps xmm1, xmm0            ; There is no movhlpd instruction
	addsd   xmm0, xmm1            ; Add the two vector elements
	movsd   [sum], xmm0           ; Store the result
```



## Equality Test Algorithm

    |---- Y1 ----|   |---- Y2 ----|   |---- Y3 ----|      |---- Y4 ----|     ....     Y8
CMP s0  s1  s2  s3   s4  s5  s6  s7   s8  s9  s10  s11   s12  s13  s14  s15   ...    s31

4 * 8 = 32 int64 values

Y1  01 01 01 01  02 02 02 02  03 03 03 03  04 04 04 04        [words]
Y2  05 05 05 05  06 06 06 06  07 07 07 07  08 08 08 08
Y3  09 09 09 09  0a 0a 0a 0a  0b 0b 0b 0b  0c 0c 0c 0c
Y4  0d 0d 0d 0d  0e 0e 0e 0e  0f 0f 0f 0f  00 00 00 00
Y5  11 11 11 11  12 12 12 12  13 13 13 13  14 14 14 14
Y6  15 15 15 15  16 16 16 16  17 17 17 17  18 18 18 18
Y7  19 19 19 19  1a 1a 1a 1a  1b 1b 1b 1b  1c 1c 1c 1c
Y8  1d 1d 1d 1d  1e 1e 1e 1e  1f 1f 1f 1f  10 10 10 10


1  VPCMPEQQ

Y1  00 00 00 00  FF FF FF FF  00 00 00 00  FF FF FF FF
Y2  00 00 00 00  FF FF FF FF  FF FF FF FF  00 00 00 00
Y3  00 00 00 00  FF FF FF FF  FF FF FF FF  FF FF FF FF
Y4  FF FF FF FF  00 00 00 00  00 00 00 00  00 00 00 00
Y5  00 00 00 00  00 00 00 00  00 00 00 00  FF FF FF FF
Y6  00 00 00 00  00 00 00 00  FF FF FF FF  00 00 00 00
Y7  00 00 00 00  00 00 00 00  FF FF FF FF  FF FF FF FF
Y8  00 00 00 00  FF FF FF FF  00 00 00 00  00 00 00 00


2  PACKSSDW

#### first pack PACKSSDW (Y1+Y5, Y2+Y6, Y3+Y7, Y4+Y8)

input gets reordered as
1/2/1 := 5, 6, 1, 2, 7, 8, 3, 4 (1-based !!)
2/1/1 := 1, 2, 5, 6, 3, 4, 7, 8 (1-based !!)

50000000 60000000 10000000 20000000 70000000 80000000 30000000 40000000  [nibbles]
d0000000 e0000000 90000000 a0000000 f0000000 00000000 b0000000 c0000000

Y1+Y5
5        6        1        2        7        8        3        4
00000000 00000000 00000000 ffffffff 00000000 ffffffff 00000000 ffffffff
11000000 12000000 01000000 02000000 13000000 14000000 03000000 04000000


Y2+Y6
5        6        1        2        7        8        3        4
00000000 00000000 00000000 ffffffff ffffffff 00000000 ffffffff 00000000
15000000 16000000 05000000 06000000 17000000 18000000 07000000 08000000

Y3+Y7
5        6        1        2        7        8        3        4
00000000 00000000 00000000 ffffffff ffffffff ffffffff ffffffff ffffffff
19000000 1a000000 09000000 0a000000 1b000000 1c000000 0b000000 0c000000

Y4+Y8
5        6        1        2        7        8        3        4
00000000 ffffffff ffffffff 00000000 00000000 00000000 00000000 00000000
1d000000 1e000000 0d000000 0e000000 1f000000 10000000 0f000000 00000000


#### first VPERMD (only double words) - switch lanes

pos   0  1  2  3  4  5  6  7
from: 5, 6, 1, 2, 7, 8, 3, 4
to:   7, 8, 5, 6, 3, 4, 1, 2
mask: 4  5  0  1  6  7  2  3

as LE
to:   3, 4, 1, 2, 7, 8, 5, 6
mask: 6  7  2  3  4  5  0  1


Y1+Y5
13000000 14000000 11000000 12000000 03000000 04000000 01000000 02000000
00000000 ffffffff 00000000 00000000 00000000 ffffffff 00000000 ffffffff

Y2+Y6
17000000 18000000 15000000 16000000 07000000 08000000 05000000 06000000
ffffffff 00000000 00000000 00000000 ffffffff 00000000 00000000 ffffffff

Y3+Y7
1b000000 1c000000 19000000 1a000000 0b000000 0c000000 09000000 0a000000
ffffffff ffffffff 00000000 00000000 ffffffff ffffffff 00000000 ffffffff

Y4+Y8
1f000000 10000000 1d000000 1e000000 0f000000 00000000 0d000000 0e000000
00000000 00000000 00000000 ffffffff 00000000 00000000 ffffffff 00000000

#### second pack PACKSSDW (Y4_8+Y3_7, Y2_6+Y1_5)

Y2_6_1_5
1300 1400 1100 1200 1700 1800 1500 1600 0300 0400 0100 0200 0700 0800 0500 0600
0000 ffff 0000 0000 ffff 0000 0000 0000 0000 ffff 0000 ffff ffff 0000 0000 ffff

Y4_8_3_7
1b00 1c00 1900 1a00 1f00 1000 1d00 1e00 0b00 0c00 0900 0a00 0f00 0000 0d00 0e00
ffff ffff 0000 0000 0000 0000 0000 ffff ffff ffff 0000 ffff 0000 0000 ffff 0000


#### PACKSSWB Y1, Y3, Y1 produces this result

0b0c 090a 0f00 0d0e 0304 0102 0708 0506 1b1c 191a 1f10 1d1e 1314 1112 1718 1516

1b1c 191a 1f10 1d1e 1314 1112 1718 1516 0b0c 090a 0f00 0d0e 0304 0102 0708 0506
ffff 0000 0000 00ff 00ff 0000 ff00 0000 ffff 00ff 0000 ff00 00ff 00ff ff00 00ff

.. almost there, need one more shuffle

#### VPSHUFB

pos    0  1  2  3  4  5  6  7  8  9  10 11 12 13 14 15 (one half only, other is same)
from:  0b 0c 09 0a 0f 00 0d 0e 03 04 01 02 07 08 05 06
to:    08 07 06 05 04 03 02 01 00 0f 0e 0d 0c 0b 0a 09
mask:  13 12 15 14  9  8 11 10  5  4  7  6  1  0  3  2


required order in AX after PACKSSWB & VPSHUFB is

0        4        1        5        2        6        3        7
08070605 04030201 000f0e0d 0c0b0a09 18171615 14131211 101f1e1d 1c1b1a19 [bits/nibble]


Note: AVX vectors are stored in memory Little Endian, but in CPU handled like a
register, so bytes are reordered. Hence when calling VPMOVMSKB we must take care
that all source bytes are ordered in the same way as nibbles are set in AX register.

For the example above this results in

AX   = 34 12 78 56
MEM  = 56 78 12 34   0101 0110 0111 1000 0001 0010 0011 0100

--------------------------------------------------------------------
Working AVX2 Algorithm

```
// func matchInt64EqualAVX2(src []int64, val int64, bits []byte) uint64
TEXT ·matchInt64EqualAVX2(SB), NOSPLIT, $0-64
	MOVQ src_base+0(FP), DI     // ptr to src data
	MOVL src_len+8(FP), SI      // len(src) is int32
	MOVQ bits_base+32(FP), CX   // ptr to bits data (4 byte extra padding to make val 64bit aligned)
	XORQ BX, BX                 // population counter

	WORD $0xf685                // TEST ESI, ESI // if len(src) == 0
	JLE  done

	VBROADCASTSD val+24(FP), Y0             // load val into AVX2 reg
	VMOVDQU      crosslane<>+0x00(SB), Y9   // load permute control mask
	VMOVDQU      shuffle<>+0x00(SB), Y10    // load shuffle control mask

// TODO: check if slice is 32 byte aligned
loop:
	VPCMPEQQ   0(DI), Y0, Y1 // compare int64 at mem against Y0 (val)
	VPCMPEQQ  32(DI), Y0, Y2
	VPCMPEQQ  64(DI), Y0, Y3
	VPCMPEQQ  96(DI), Y0, Y4
	VPCMPEQQ 128(DI), Y0, Y5
	VPCMPEQQ 160(DI), Y0, Y6
	VPCMPEQQ 192(DI), Y0, Y7
	VPCMPEQQ 224(DI), Y0, Y8

	//VMOVDQU     0(DI), Y1  // read test data directly
	//VMOVDQU    32(DI), Y2
	//VMOVDQU    64(DI), Y3
	//VMOVDQU    96(DI), Y4
	//VMOVDQU   128(DI), Y5
	//VMOVDQU   160(DI), Y6
	//VMOVDQU   192(DI), Y7
	//VMOVDQU   224(DI), Y8

	VPACKSSDW  Y1, Y5, Y1
	VPACKSSDW  Y2, Y6, Y2
	VPACKSSDW  Y3, Y7, Y3
	VPACKSSDW  Y4, Y8, Y4
	VPERMD     Y1, Y9, Y1
	VPERMD     Y2, Y9, Y2
	VPERMD     Y3, Y9, Y3
	VPERMD     Y4, Y9, Y4
	VPACKSSDW  Y2, Y1, Y1
	VPACKSSDW  Y4, Y3, Y3
	VPACKSSWB  Y1, Y3, Y1
	VPSHUFB    Y10, Y1, Y1

	//VMOVDQU Y1, (CX)  // DEBUG output (make sure bits slice is min 32byte!)
	VPMOVMSKB Y1, AX    // move per byte MSBs into packed bitmask to r32 or r64
	MOVL    AX, (CX)    // write the lower 32 bits
	POPCNTQ AX, AX
	ADDQ  AX, BX
	ADDQ  $256, DI
	ADDQ  $4, CX
	SUBL  $32, SI
	JNZ  loop

scalar:
// TODO: handle remainder

done:
	MOVQ BX, ret+56(FP)         // FIXME: use faster popcount here
	VZEROUPPER                  // clear upper part of Y regs, prevents AVX-SSE penalty
	RET
```

```
// Test vector
	Int64EqualTest{
		slice: []int64{
			// test vector to find shuffle/perm positions
			0x1, 0x2, 0x3, 0x4, // Y1
			0x5, 0x6, 0x7, 0x8, // Y2
			0x9, 0xa, 0xb, 0xc, // Y3
			0xd, 0xe, 0xf, 0x0, // Y4
			0x11, 0x12, 0x13, 0x14, // Y5
			0x15, 0x16, 0x17, 0x18, // Y6
			0x19, 0x1a, 0x1b, 0x1c, // Y7
			0x1d, 0x1e, 0x1f, 0x10, // Y8

			// read test data to check algo
			0, 5, 3, 5, // Y1
			7, 5, 5, 9, // Y2
			3, 5, 5, 5, // Y3
			5, 0, 113, 12, // Y4

			4, 2, 3, 5, // Y5
			7, 3, 5, 9, // Y6
			3, 13, 5, 5, // Y7
			42, 5, 113, 12, // Y8
		},
		match:  5,
		result: []byte{0x56, 0x78, 0x12, 0x34},
		count:  13,
	},
```