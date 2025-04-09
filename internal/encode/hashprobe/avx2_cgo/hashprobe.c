#include "hashprobe.h"
#include <immintrin.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

#define HASH_CONST 0x9e3779b97f4a7c15ULL
#define HASH_MASK 0xFFFF // Fixed 16-bit mask
#define HASH_TABLE_SIZE 1 << 16

void ht_build64(uint64_t* vals, uint64_t* ht_keys, uint16_t* ht_values, uint64_t* dict, size_t len, size_t* dict_size) {
    // SIMD initialize ht_values
    __m256i fill = _mm256_set1_epi16(HASH_MASK); // VPBROADCASTW 0xFFFF
    for (size_t i = 0; i < (HASH_TABLE_SIZE); i += 16) { // 16k / 16 = 1024 iterations
        _mm256_storeu_si256((__m256i*)(ht_values + i), fill);
    }

    // Deduplicate
    int i = 0;
    for (; i <= len - 4; i += 4) {
        // hash 4x values
        __m256i kvec = _mm256_loadu_si256((__m256i*)(vals + i));
        __m256i hvec = _mm256_mullo_epi32(kvec, _mm256_set1_epi32((uint32_t)HASH_CONST));

        // extract may or may not be faster than mem roundtrip
        // uint32_t h0 = _mm256_extract_epi16(hvec, 0);
        // uint32_t h1 = _mm256_extract_epi16(hvec, 4);
        // uint32_t h2 = _mm256_extract_epi16(hvec, 8);
        // uint32_t h3 = _mm256_extract_epi16(hvec, 12);

        uint64_t h_vals[4];
        _mm256_storeu_si256((__m256i*)h_vals, hvec);

        uint64_t h0 = h_vals[0] & HASH_MASK;
        uint64_t h1 = h_vals[1] & HASH_MASK;
        uint64_t h2 = h_vals[2] & HASH_MASK;
        uint64_t h3 = h_vals[3] & HASH_MASK;
        int p0 = 0, p1 = 0, p2 = 0, p3 = 0;

        // insert 4x hashes
        while (ht_values[h0] != HASH_MASK && ht_keys[h0] != vals[i]) {
            p0++;
            h0 = (h0 + p0 * p0) & HASH_MASK;
        }
        if (ht_values[h0] == HASH_MASK) {
            ht_keys[h0] = vals[i];
            ht_values[h0] = 0;
        }

        // h = h_vals[1] & HASH_MASK;
        while (ht_values[h1] != HASH_MASK && ht_keys[h1] != vals[i + 1]) {
            p1++;
            h1 = (h1 + p1 * p1) & HASH_MASK;
        }
        if (ht_values[h1] == HASH_MASK) {
            ht_keys[h1] = vals[i + 1];
            ht_values[h1] = 0;
        }

        // h = h_vals[2] & HASH_MASK;
        while (ht_values[h2] != HASH_MASK && ht_keys[h2] != vals[i + 2]) {
            p2++;
            h2 = (h2 + p2 * p2) & HASH_MASK;
        }
        if (ht_values[h2] == HASH_MASK) {
            ht_keys[h2] = vals[i + 2];
            ht_values[h2] = 0;
        }

        // h = h_vals[3] & HASH_MASK;
        while (ht_values[h3] != HASH_MASK && ht_keys[h3] != vals[i + 3]) {
            p3++;
            h3 = (h3 + p3 * p3) & HASH_MASK;
        }
        if (ht_values[h3] == HASH_MASK) {
            ht_keys[h3] = vals[i + 3];
            ht_values[h3] = 0;
        }
    }

    // tail
    for (; i < len; i++) {
        int p = 0;
        uint64_t h = (vals[i] * HASH_CONST) & HASH_MASK;
        while (ht_values[h] != HASH_MASK && ht_keys[h] != vals[i]) {
            p++;
            h = (h + p * p) & HASH_MASK;
        }
        if (ht_values[h] == HASH_MASK) {
            ht_keys[h] = vals[i];
            ht_values[h] = 0;
        }
    }

    // Extract unique keys
    size_t n = 0;
    for (i = 0; i < (1 << 16); i += 16) { // Scan 16 uint16_t at a time
        __m256i v = _mm256_loadu_si256((__m256i*)(ht_values + i));
        __m256i cmp = _mm256_cmpeq_epi16(v, _mm256_set1_epi16(HASH_MASK));
        int mask = _mm256_movemask_epi8(cmp); // 32-bit mask (16 lanes x 2 bytes)

        while (mask != 0xFFFFFFFF) { // Not all 0xFFFF
            int j = __builtin_ctz(~mask) >> 1; // Find first non-0xFFFF (shift by 2 for byte-to-word)
            dict[n++] = ht_keys[i + j];
            mask |= 3 << (j * 2); // Clear processed bit pair
        }
    }

    *dict_size = n; // Return number of unique keys
}

void ht_encode64(uint64_t* vals, uint64_t* ht_keys, uint16_t* ht_values, uint16_t* codes, size_t len) {
    int i = 0;
    for (; i <= len - 4; i += 4) {
        // hash 4x values
        __m256i kvec = _mm256_loadu_si256((__m256i*)(vals + i));
        __m256i hvec = _mm256_mullo_epi32(kvec, _mm256_set1_epi32((uint32_t)HASH_CONST));

        // extract may or may not be faster than mem roundtrip
        // uint32_t h0 = _mm256_extract_epi16(hvec, 0);
        // uint32_t h1 = _mm256_extract_epi16(hvec, 4);
        // uint32_t h2 = _mm256_extract_epi16(hvec, 8);
        // uint32_t h3 = _mm256_extract_epi16(hvec, 12);

        uint64_t h_vals[4];
        _mm256_storeu_si256((__m256i*)h_vals, hvec);

        uint64_t h0 = h_vals[0] & HASH_MASK;
        uint64_t h1 = h_vals[1] & HASH_MASK;
        uint64_t h2 = h_vals[2] & HASH_MASK;
        uint64_t h3 = h_vals[3] & HASH_MASK;
        int p0 = 0, p1 = 0, p2 = 0, p3 = 0;

        // write codes
        while (ht_keys[h0] != vals[i]) {
            p0++;
            h0 = (h0 + p0 * p0) & HASH_MASK;
        }
        codes[i] = ht_values[h0];

        // 1
        while (ht_keys[h1] != vals[i + 1]) {
            p1++;
            h1 = (h1 + p1 * p1) & HASH_MASK;
        }
        codes[i + 1] = ht_values[h1];

        // 2
        while (ht_keys[h2] != vals[i + 2]) {
            p2++;
            h2 = (h2 + p2 * p2) & HASH_MASK;
        }
        codes[i + 2] = ht_values[h2];

        // 3
        while (ht_keys[h3] != vals[i + 3]) {
            p3++;
            h3 = (h3 + p3 * p3) & HASH_MASK;
        }
        codes[i + 3] = ht_values[h3];
    }

    // tail
    for (; i < len; i++) {
        int p = 0;
        uint64_t h = (vals[i] * HASH_CONST) & HASH_MASK;
        while (ht_keys[h] != vals[i]) {
            p++;
            h = (h + p * p) & HASH_MASK;
        }
        codes[i] = ht_values[h];
    }
}

void ht_build32(uint32_t* vals, uint32_t* ht_keys, uint16_t* ht_values, uint32_t* dict, size_t len, size_t* dict_size) {
    // SIMD initialize ht_values to 0xFFFF
    __m256i fill = _mm256_set1_epi16(HASH_MASK); // VPBROADCASTW equivalent
    for (size_t i = 0; i < (1 << 16); i += 16) { // 64k / 16 = 4096 iterations
        _mm256_storeu_si256((__m256i*)(ht_values + i), fill);
    }

    // Deduplicate
    int i = 0;
    for (; i <= len - 8; i += 8) {
        // hash 8x values
        __m256i kvec = _mm256_loadu_si256((__m256i*)(vals + i));
        __m256i hvec = _mm256_mullo_epi32(kvec, _mm256_set1_epi32((uint32_t)HASH_CONST));

        // extract may or may not be faster than store
        // uint32_t h0 = _mm256_extract_epi16(hvec, 0);
        // uint32_t h1 = _mm256_extract_epi16(hvec, 2);
        // uint32_t h2 = _mm256_extract_epi16(hvec, 4);
        // uint32_t h3 = _mm256_extract_epi16(hvec, 6);
        // uint32_t h4 = _mm256_extract_epi16(hvec, 8);
        // uint32_t h5 = _mm256_extract_epi16(hvec, 10);
        // uint32_t h6 = _mm256_extract_epi16(hvec, 12);
        // uint32_t h7 = _mm256_extract_epi16(hvec, 14);

        uint32_t h_vals[8];
        _mm256_storeu_si256((__m256i*)h_vals, hvec);

        uint32_t h0 = h_vals[0] & HASH_MASK;
        uint32_t h1 = h_vals[1] & HASH_MASK;
        uint32_t h2 = h_vals[2] & HASH_MASK;
        uint32_t h3 = h_vals[3] & HASH_MASK;
        uint32_t h4 = h_vals[4] & HASH_MASK;
        uint32_t h5 = h_vals[5] & HASH_MASK;
        uint32_t h6 = h_vals[6] & HASH_MASK;
        uint32_t h7 = h_vals[7] & HASH_MASK;

        int p0 = 0, p1 = 0, p2 = 0, p3 = 0, p4 = 0, p5 = 0, p6 = 0, p7 = 0;

        // insert, unrolled
        // 0
        while (ht_values[h0] != HASH_MASK && ht_keys[h0] != vals[i]) {
            p0++;
            h0 = (h0 + p0 * p0) & HASH_MASK;
        }
        if (ht_values[h0] == HASH_MASK) {
            ht_keys[h0] = vals[i];
            ht_values[h0] = 0;
        }

        // 1
        while (ht_values[h1] != HASH_MASK && ht_keys[h1] != vals[i + 1]) {
            p1++;
            h1 = (h1 + p1 * p1) & HASH_MASK;
        }
        if (ht_values[h1] == HASH_MASK) {
            ht_keys[h1] = vals[i + 1];
            ht_values[h1] = 0;
        }

        // 2
        while (ht_values[h2] != HASH_MASK && ht_keys[h2] != vals[i + 2]) {
            p2++;
            h2 = (h2 + p2 * p2) & HASH_MASK;
        }
        if (ht_values[h2] == HASH_MASK) {
            ht_keys[h2] = vals[i + 2];
            ht_values[h2] = 0;
        }

        // 3
        while (ht_values[h3] != HASH_MASK && ht_keys[h3] != vals[i + 3]) {
            p3++;
            h3 = (h3 + p3 * p3) & HASH_MASK;
        }
        if (ht_values[h3] == HASH_MASK) {
            ht_keys[h3] = vals[i + 3];
            ht_values[h3] = 0;
        }

        // 4
        while (ht_values[h4] != HASH_MASK && ht_keys[h4] != vals[i + 4]) {
            p4++;
            h4 = (h4 + p4 * p4) & HASH_MASK;
        }
        if (ht_values[h4] == HASH_MASK) {
            ht_keys[h4] = vals[i + 4];
            ht_values[h4] = 0;
        }

        // 5
        while (ht_values[h5] != HASH_MASK && ht_keys[h5] != vals[i + 5]) {
            p5++;
            h5 = (h5 + p5 * p5) & HASH_MASK;
        }
        if (ht_values[h5] == HASH_MASK) {
            ht_keys[h5] = vals[i + 5];
            ht_values[h5] = 0;
        }

        // 6
        while (ht_values[h6] != HASH_MASK && ht_keys[h6] != vals[i + 6]) {
            p6++;
            h6 = (h6 + p6 * p6) & HASH_MASK;
        }
        if (ht_values[h6] == HASH_MASK) {
            ht_keys[h6] = vals[i + 6];
            ht_values[h6] = 0;
        }

        // 7
        while (ht_values[h7] != HASH_MASK && ht_keys[h7] != vals[i + 7]) {
            p7++;
            h7 = (h7 + p7 * p7) & HASH_MASK;
        }
        if (ht_values[h7] == HASH_MASK) {
            ht_keys[h7] = vals[i + 7];
            ht_values[h7] = 0;
        }
    }

    // tail
    for (; i < len; i++) {
        int p = 0;
        uint32_t h = (vals[i] * HASH_CONST) & HASH_MASK;
        while (ht_values[h] != HASH_MASK && ht_keys[h] != vals[i]) {
            p++;
            h = (h + p*p) & HASH_MASK;
        }
        if (ht_values[h] == HASH_MASK) {
            ht_keys[h] = vals[i];
            ht_values[h] = 0;
        }
    }

    // Extract unique keys
    size_t n = 0;
    for (i = 0; i < (1 << 16); i += 16) { // Scan 16 uint16_t at a time
        __m256i v = _mm256_loadu_si256((__m256i*)(ht_values + i));
        __m256i cmp = _mm256_cmpeq_epi16(v, _mm256_set1_epi16(HASH_MASK));
        int mask = _mm256_movemask_epi8(cmp); // 32-bit mask (16 lanes x 2 bytes)

        while (mask != 0xFFFFFFFF) { // Not all 0xFFFF
            int j = __builtin_ctz(~mask) >> 1; // Find first non-0xFFFF (shift by 2 for byte-to-word)
            dict[n++] = ht_keys[i + j];
            mask |= 3 << (j * 2); // Clear processed bit pair
        }
    }

    *dict_size = n; // Return number of unique keys
}

void ht_encode32(uint32_t* vals, uint32_t* ht_keys, uint16_t* ht_values, uint16_t* codes, size_t len) {
    int i = 0;
    for (; i <= len - 8; i += 8) {
        // hash 8x values
        __m256i kvec = _mm256_loadu_si256((__m256i*)(vals + i));
        __m256i hvec = _mm256_mullo_epi32(kvec, _mm256_set1_epi32((uint32_t)HASH_CONST));

        // extract may or may not be faster than store
        // uint32_t h0 = _mm256_extract_epi16(hvec, 0);
        // uint32_t h1 = _mm256_extract_epi16(hvec, 2);
        // uint32_t h2 = _mm256_extract_epi16(hvec, 4);
        // uint32_t h3 = _mm256_extract_epi16(hvec, 6);
        // uint32_t h4 = _mm256_extract_epi16(hvec, 8);
        // uint32_t h5 = _mm256_extract_epi16(hvec, 10);
        // uint32_t h6 = _mm256_extract_epi16(hvec, 12);
        // uint32_t h7 = _mm256_extract_epi16(hvec, 14);

        uint32_t h_vals[8];
        _mm256_storeu_si256((__m256i*)h_vals, hvec);

        uint32_t h0 = h_vals[0] & HASH_MASK;
        uint32_t h1 = h_vals[1] & HASH_MASK;
        uint32_t h2 = h_vals[2] & HASH_MASK;
        uint32_t h3 = h_vals[3] & HASH_MASK;
        uint32_t h4 = h_vals[4] & HASH_MASK;
        uint32_t h5 = h_vals[5] & HASH_MASK;
        uint32_t h6 = h_vals[6] & HASH_MASK;
        uint32_t h7 = h_vals[7] & HASH_MASK;

        // write codes
        int p0 = 0, p1 = 0, p2 = 0, p3 = 0, p4 = 0, p5 = 0, p6 = 0, p7 = 0;

        // 0
        while (ht_keys[h0] != vals[i]) {
            p0++;
            h0 = (h0 + p0 * p0) & HASH_MASK;
        }
        codes[i] = ht_values[h0];

        // 1
        while (ht_keys[h1] != vals[i + 1]) {
            p1++;
            h1 = (h1 + p1 * p1) & HASH_MASK;
        }
        codes[i + 1] = ht_values[h1];

        // 2
        while (ht_keys[h2] != vals[i + 2]) {
            p2++;
            h2 = (h2 + p2 * p2) & HASH_MASK;
        }
        codes[i + 2] = ht_values[h2];

        // 3
        while (ht_keys[h3] != vals[i + 3]) {
            p3++;
            h3 = (h3 + p3 * p3) & HASH_MASK;
        }
        codes[i + 3] = ht_values[h3];

        // 4
        while (ht_keys[h4] != vals[i + 4]) {
            p4++;
            h4 = (h4 + p4 * p4) & HASH_MASK;
        }
        codes[i + 4] = ht_values[h4];

        // 5
        while (ht_keys[h5] != vals[i + 5]) {
            p5++;
            h5 = (h5 + p5 * p5) & HASH_MASK;
        }
        codes[i + 5] = ht_values[h5];

        // 6
        while (ht_keys[h6] != vals[i + 6]) {
            p6++;
            h6 = (h6 + p6 * p6) & HASH_MASK;
        }
        codes[i + 6] = ht_values[h6];

        // 7
        while (ht_keys[h7] != vals[i + 7]) {
            p7++;
            h7 = (h7 + p7 * p7) & HASH_MASK;
        }
        codes[i + 7] = ht_values[h7];
    }

    // tail
    for (; i < len; i++) {
        int p = 0;
        uint32_t h = (uint32_t)(vals[i] * HASH_CONST) & HASH_MASK;
        while (ht_keys[h] != vals[i]) {
            p++;
            h = (h + p*p) & HASH_MASK;
        }
        codes[i] = ht_values[h];
    }
}


// void print_m256i(const char* label, __m256i vec) {
//     uint32_t vals[8];
//     _mm256_storeu_si256((__m256i*)vals, vec);
//     printf("%s: [%08x %08x %08x %08x %08x %08x %08x %08x]\n",
//            label, vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], vals[7]);
// }