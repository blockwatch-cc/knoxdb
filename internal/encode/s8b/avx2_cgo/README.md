# Simple8b in C using CGO

TL;TR: CLZ and SIMD data extraction too heavy weight in AVX2. Scalar code is faster.

## SIMD using AVX2

This was a trial to see if we could speed up encoding with AVX2 SIMD instructions. Potential areas where this makes sense

- run counts for 1s
- used bits calculation (CLZ)

SIMD run counts did not help performance.

CLZ is unavailable in AVX2 (supported by AVX512) so we have to resort to hacks. Best performing hack so far was

```c
// calculate used bits
__m256i vals = _mm256_loadu_si256((const __m256i*)chunk);
__m256i v = _mm256_sub_epi32(vals, min_vec);

// Prevent rounding up by keeping top 8 bits
v = _mm256_andnot_si256(_mm256_srli_epi32(v, 8), v);

// Convert to float, extract exponent, compute CLZ
v = _mm256_castps_si256(_mm256_cvtepi32_ps(v));
v = _mm256_srli_epi32(v, 23);   // Exponent (biased)
v = _mm256_subs_epu16(v158, v); // CLZ = 158 - exponent
v = _mm256_min_epi16(v, v32);   // Clamp at 32
__m256i used_bits = _mm256_sub_epi32(v32, v); // Used bits = 32 - CLZ
```

Best performing store 

```c
// Store in buffer with wrap-around
uint32_t bits[8];
_mm256_storeu_si256((__m256i*)bits, used_bits);
for (int k = 0; k < chunk_n; k++) {
    buffer[head % 256] = (uint8_t)(bits[k] & 0xFF);
    head++;
}
```

## New encoder structure

The C version aims to be work preserving, storing expensive calculation results (CLZ a.k.a. usedBits per vector element) in a circular buffer and then feed them to the simple8b packing loop from this buffer.

This same optimization makes the Go version slower, but works well with gcc -O3.

The C version of simple8b does not output selectors 0 (240 ones) and 1 (120 ones). We don't even scan for this because we consider the pattern unlikely.
