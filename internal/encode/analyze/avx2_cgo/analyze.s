
analyze.o:     file format elf64-x86-64


Disassembly of section .text:

0000000000000000 <analyze_i64_avx2>:
   0:	48 85 d2             	test   %rdx,%rdx
   3:	0f 84 47 02 00 00    	je     250 <analyze_i64_avx2+0x250>
   9:	55                   	push   %rbp
   a:	4c 8b 56 10          	mov    0x10(%rsi),%r10
   e:	48 83 fa 01          	cmp    $0x1,%rdx
  12:	49 89 d0             	mov    %rdx,%r8
  15:	0f 95 c0             	setne  %al
  18:	45 31 c9             	xor    %r9d,%r9d
  1b:	4d 85 d2             	test   %r10,%r10
  1e:	c4 c1 f9 6e fa       	vmovq  %r10,%xmm7
  23:	48 89 e5             	mov    %rsp,%rbp
  26:	41 54                	push   %r12
  28:	41 0f 95 c1          	setne  %r9b
  2c:	c4 e2 7d 59 ef       	vpbroadcastq %xmm7,%ymm5
  31:	53                   	push   %rbx
  32:	4c 8b 27             	mov    (%rdi),%r12
  35:	41 21 c1             	and    %eax,%r9d
  38:	c4 c1 f9 6e f4       	vmovq  %r12,%xmm6
  3d:	c4 e2 7d 59 d6       	vpbroadcastq %xmm6,%ymm2
  42:	48 83 fa 03          	cmp    $0x3,%rdx
  46:	0f 86 14 02 00 00    	jbe    260 <analyze_i64_avx2+0x260>
  4c:	48 8d 5a fd          	lea    -0x3(%rdx),%rbx
  50:	48 8d 42 ff          	lea    -0x1(%rdx),%rax
  54:	c5 fd 6f da          	vmovdqa %ymm2,%ymm3
  58:	b9 01 00 00 00       	mov    $0x1,%ecx
  5d:	48 39 c3             	cmp    %rax,%rbx
  60:	48 0f 47 d8          	cmova  %rax,%rbx
  64:	45 31 db             	xor    %r11d,%r11d
  67:	c4 a1 7e 6f 04 df    	vmovdqu (%rdi,%r11,8),%ymm0
  6d:	4c 89 e0             	mov    %r12,%rax
  70:	c4 e2 6d 37 c8       	vpcmpgtq %ymm0,%ymm2,%ymm1
  75:	c4 e3 6d 4b d0 10    	vblendvpd %ymm1,%ymm0,%ymm2,%ymm2
  7b:	c4 e2 7d 37 cb       	vpcmpgtq %ymm3,%ymm0,%ymm1
  80:	c4 e3 65 4b d8 10    	vblendvpd %ymm1,%ymm0,%ymm3,%ymm3
  86:	4d 85 db             	test   %r11,%r11
  89:	74 05                	je     90 <analyze_i64_avx2+0x90>
  8b:	4a 8b 44 df f8       	mov    -0x8(%rdi,%r11,8),%rax
  90:	c4 e3 fd 00 c8 93    	vpermq $0x93,%ymm0,%ymm1
  96:	c4 e3 f1 22 e0 00    	vpinsrq $0x0,%rax,%xmm1,%xmm4
  9c:	c4 e3 75 38 cc 00    	vinserti128 $0x0,%xmm4,%ymm1,%ymm1
  a2:	c4 e2 75 29 e0       	vpcmpeqq %ymm0,%ymm1,%ymm4
  a7:	c5 fd 50 c4          	vmovmskpd %ymm4,%eax
  ab:	f7 d0                	not    %eax
  ad:	83 e0 0f             	and    $0xf,%eax
  b0:	f3 0f b8 c0          	popcnt %eax,%eax
  b4:	48 01 c1             	add    %rax,%rcx
  b7:	45 85 c9             	test   %r9d,%r9d
  ba:	74 28                	je     e4 <analyze_i64_avx2+0xe4>
  bc:	c5 fd fb c1          	vpsubq %ymm1,%ymm0,%ymm0
  c0:	ba 0f 00 00 00       	mov    $0xf,%edx
  c5:	c4 e2 7d 29 c5       	vpcmpeqq %ymm5,%ymm0,%ymm0
  ca:	c5 fd 50 c0          	vmovmskpd %ymm0,%eax
  ce:	4d 85 db             	test   %r11,%r11
  d1:	75 08                	jne    db <analyze_i64_avx2+0xdb>
  d3:	83 e0 0e             	and    $0xe,%eax
  d6:	ba 0e 00 00 00       	mov    $0xe,%edx
  db:	45 31 c9             	xor    %r9d,%r9d
  de:	39 d0                	cmp    %edx,%eax
  e0:	41 0f 94 c1          	sete   %r9b
  e4:	49 8d 53 04          	lea    0x4(%r11),%rdx
  e8:	48 39 da             	cmp    %rbx,%rdx
  eb:	0f 82 4f 01 00 00    	jb     240 <analyze_i64_avx2+0x240>
  f1:	49 83 c3 07          	add    $0x7,%r11
  f5:	4d 39 c3             	cmp    %r8,%r11
  f8:	73 5a                	jae    154 <analyze_i64_avx2+0x154>
  fa:	66 0f 1f 44 00 00    	nopw   0x0(%rax,%rax,1)
 100:	c5 fe 6f 0c d7       	vmovdqu (%rdi,%rdx,8),%ymm1
 105:	c4 e2 6d 37 c1       	vpcmpgtq %ymm1,%ymm2,%ymm0
 10a:	c4 e3 6d 4b d1 00    	vblendvpd %ymm0,%ymm1,%ymm2,%ymm2
 110:	c4 e2 75 37 c3       	vpcmpgtq %ymm3,%ymm1,%ymm0
 115:	c4 e3 65 4b d9 00    	vblendvpd %ymm0,%ymm1,%ymm3,%ymm3
 11b:	c4 e3 fd 00 c1 93    	vpermq $0x93,%ymm1,%ymm0
 121:	c4 e3 f9 22 64 d7 f8 	vpinsrq $0x0,-0x8(%rdi,%rdx,8),%xmm0,%xmm4
 128:	00 
 129:	c4 e3 7d 38 c4 00    	vinserti128 $0x0,%xmm4,%ymm0,%ymm0
 12f:	c4 e2 7d 29 c1       	vpcmpeqq %ymm1,%ymm0,%ymm0
 134:	c5 fd 50 c0          	vmovmskpd %ymm0,%eax
 138:	f7 d0                	not    %eax
 13a:	83 e0 0f             	and    $0xf,%eax
 13d:	f3 0f b8 c0          	popcnt %eax,%eax
 141:	48 01 c1             	add    %rax,%rcx
 144:	48 89 d0             	mov    %rdx,%rax
 147:	48 83 c2 04          	add    $0x4,%rdx
 14b:	48 83 c0 07          	add    $0x7,%rax
 14f:	4c 39 c0             	cmp    %r8,%rax
 152:	72 ac                	jb     100 <analyze_i64_avx2+0x100>
 154:	49 8d 40 fc          	lea    -0x4(%r8),%rax
 158:	48 83 e0 fc          	and    $0xfffffffffffffffc,%rax
 15c:	48 83 c0 04          	add    $0x4,%rax
 160:	c5 f9 6f c2          	vmovdqa %xmm2,%xmm0
 164:	c4 e3 7d 39 d2 01    	vextracti128 $0x1,%ymm2,%xmm2
 16a:	c4 e2 79 37 ca       	vpcmpgtq %xmm2,%xmm0,%xmm1
 16f:	c4 e3 79 4b c2 10    	vblendvpd %xmm1,%xmm2,%xmm0,%xmm0
 175:	c4 e1 f9 7e c2       	vmovq  %xmm0,%rdx
 17a:	c4 c3 f9 16 c3 01    	vpextrq $0x1,%xmm0,%r11
 180:	c5 f9 6f c3          	vmovdqa %xmm3,%xmm0
 184:	c4 e3 7d 39 db 01    	vextracti128 $0x1,%ymm3,%xmm3
 18a:	49 39 d3             	cmp    %rdx,%r11
 18d:	c4 e2 61 37 c8       	vpcmpgtq %xmm0,%xmm3,%xmm1
 192:	4c 0f 4f da          	cmovg  %rdx,%r11
 196:	4c 89 1e             	mov    %r11,(%rsi)
 199:	c4 e3 79 4b c3 10    	vblendvpd %xmm1,%xmm3,%xmm0,%xmm0
 19f:	c4 e1 f9 7e c3       	vmovq  %xmm0,%rbx
 1a4:	c4 e3 f9 16 c2 01    	vpextrq $0x1,%xmm0,%rdx
 1aa:	48 39 d3             	cmp    %rdx,%rbx
 1ad:	48 0f 4c da          	cmovl  %rdx,%rbx
 1b1:	48 89 5e 08          	mov    %rbx,0x8(%rsi)
 1b5:	4c 39 c0             	cmp    %r8,%rax
 1b8:	73 63                	jae    21d <analyze_i64_avx2+0x21d>
 1ba:	48 8d 14 c7          	lea    (%rdi,%rax,8),%rdx
 1be:	4d 8d 60 ff          	lea    -0x1(%r8),%r12
 1c2:	eb 0b                	jmp    1cf <analyze_i64_avx2+0x1cf>
 1c4:	0f 1f 40 00          	nopl   0x0(%rax)
 1c8:	4c 8b 1e             	mov    (%rsi),%r11
 1cb:	48 8b 5e 08          	mov    0x8(%rsi),%rbx
 1cf:	48 8b 3a             	mov    (%rdx),%rdi
 1d2:	4c 39 df             	cmp    %r11,%rdi
 1d5:	7d 03                	jge    1da <analyze_i64_avx2+0x1da>
 1d7:	48 89 3e             	mov    %rdi,(%rsi)
 1da:	48 39 df             	cmp    %rbx,%rdi
 1dd:	7e 04                	jle    1e3 <analyze_i64_avx2+0x1e3>
 1df:	48 89 7e 08          	mov    %rdi,0x8(%rsi)
 1e3:	48 85 c0             	test   %rax,%rax
 1e6:	74 0d                	je     1f5 <analyze_i64_avx2+0x1f5>
 1e8:	48 8b 5a f8          	mov    -0x8(%rdx),%rbx
 1ec:	48 39 1a             	cmp    %rbx,(%rdx)
 1ef:	74 04                	je     1f5 <analyze_i64_avx2+0x1f5>
 1f1:	48 83 c1 01          	add    $0x1,%rcx
 1f5:	45 85 c9             	test   %r9d,%r9d
 1f8:	74 16                	je     210 <analyze_i64_avx2+0x210>
 1fa:	4c 39 e0             	cmp    %r12,%rax
 1fd:	73 11                	jae    210 <analyze_i64_avx2+0x210>
 1ff:	48 8b 7a 08          	mov    0x8(%rdx),%rdi
 203:	48 2b 3a             	sub    (%rdx),%rdi
 206:	45 31 c9             	xor    %r9d,%r9d
 209:	4c 39 d7             	cmp    %r10,%rdi
 20c:	41 0f 94 c1          	sete   %r9b
 210:	48 83 c0 01          	add    $0x1,%rax
 214:	48 83 c2 08          	add    $0x8,%rdx
 218:	49 39 c0             	cmp    %rax,%r8
 21b:	75 ab                	jne    1c8 <analyze_i64_avx2+0x1c8>
 21d:	31 c0                	xor    %eax,%eax
 21f:	45 85 c9             	test   %r9d,%r9d
 222:	48 89 4e 18          	mov    %rcx,0x18(%rsi)
 226:	4c 0f 44 d0          	cmove  %rax,%r10
 22a:	4c 89 56 10          	mov    %r10,0x10(%rsi)
 22e:	c5 f8 77             	vzeroupper 
 231:	5b                   	pop    %rbx
 232:	41 5c                	pop    %r12
 234:	5d                   	pop    %rbp
 235:	c3                   	retq   
 236:	66 2e 0f 1f 84 00 00 	nopw   %cs:0x0(%rax,%rax,1)
 23d:	00 00 00 
 240:	49 89 d3             	mov    %rdx,%r11
 243:	e9 1f fe ff ff       	jmpq   67 <analyze_i64_avx2+0x67>
 248:	0f 1f 84 00 00 00 00 	nopl   0x0(%rax,%rax,1)
 24f:	00 
 250:	c5 f9 ef c0          	vpxor  %xmm0,%xmm0,%xmm0
 254:	c5 fe 7f 06          	vmovdqu %ymm0,(%rsi)
 258:	c5 f8 77             	vzeroupper 
 25b:	c3                   	retq   
 25c:	0f 1f 40 00          	nopl   0x0(%rax)
 260:	c5 fd 6f da          	vmovdqa %ymm2,%ymm3
 264:	31 c0                	xor    %eax,%eax
 266:	b9 01 00 00 00       	mov    $0x1,%ecx
 26b:	e9 f0 fe ff ff       	jmpq   160 <analyze_i64_avx2+0x160>
