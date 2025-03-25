#include <immintrin.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static const struct { uint8_t n; uint8_t shift; } packing_info[16] = {
    {240, 0}, {120, 0}, {60, 1}, {30, 2}, {20, 3}, {15, 4}, {12, 5}, {10, 6},
    {8, 7}, {7, 8}, {6, 10}, {5, 12}, {4, 15}, {3, 20}, {2, 30}, {1, 60}
};

static const struct { uint8_t n; uint8_t code; } maxValsPerBits[61] = {
    {60, 2}, {60, 2}, {30, 3}, {20, 4}, {15, 5}, {12, 6}, {10, 7}, {8, 8},
    {7, 9}, {6, 10}, {6, 10}, {5, 11}, {5, 11}, {4, 12}, {4, 12}, {4, 12},
    {3, 13}, {3, 13}, {3, 13}, {3, 13}, {3, 13}, {2, 14}, {2, 14}, {2, 14},
    {2, 14}, {2, 14}, {2, 14}, {2, 14}, {2, 14}, {2, 14}, {2, 14}, {1, 15},
    {1, 15}, {1, 15}, {1, 15}, {1, 15}, {1, 15}, {1, 15}, {1, 15}, {1, 15},
    {1, 15}, {1, 15}, {1, 15}, {1, 15}, {1, 15}, {1, 15}, {1, 15}, {1, 15},
    {1, 15}, {1, 15}, {1, 15}, {1, 15}, {1, 15}, {1, 15}, {1, 15}, {1, 15},
    {1, 15}, {1, 15}, {1, 15}, {1, 15}, {1, 15}
};

static inline int min(int a, int b) { return a < b ? a : b; }

// void print_m256i(const char* label, __m256i v) {
//     uint32_t vals[8];
//     _mm256_storeu_si256((__m256i*)vals, v);
//     printf("%s: [", label);
//     for (int i = 0; i < 8; i++) {
//         printf("%u%s", vals[i], i < 7 ? ", " : "]\n");
//     }
// }

// void print_m128i(const char* label, __m128i v) {
//     uint32_t vals[4];
//     _mm_storeu_si128((__m128i*)vals, v);
//     printf("%s: [", label);
//     for (int i = 0; i < 4; i++) {
//         printf("%u%s", vals[i], i < 3 ? ", " : "]\n");
//     }
// }


// void printBitsBuffer(uint8_t* bits_buffer, int buffer_idx) {
//     printf("bits_buffer: [");
//     for (int k = 0; k < buffer_idx; k++) {
//         printf("%u%s", bits_buffer[k], k < buffer_idx-1 ? ", ": "]\n");
//     }
// }

// void printBuf(uint8_t* buffer, size_t pos, int n) {
//     printf("circ buffer: [");
//     if (pos % 256 + n < 256) {
//         for (int i = 0; i < n; i++) {
//             printf("%u%s", buffer[pos % 256 + i], i < n-1 ? ", " : "]\n");
//         }
//     } else {
//         int first = n - (pos % 256);
//         for (int i = 0; i < first; i++) {
//             printf("%u, ", buffer[pos % 256 + i]);
//         }
//         for (int i = 0; i < n-first; i++) {
//             printf("%u%s", buffer[i], i < n-first-1 ? ", " : "]\n");
//         }
//     }
// }

// ---------------------------------------------------------------------------------
// Grok version SIMD, keep for reference, double output, likely buggy
// cpu: 12th Gen Intel(R) Core(TM) i9-12900K
// BenchmarkEncodeUint32/dups_1K-24              386160          3123 ns/op    1311.41 MB/s          4120 mean_bytes
// BenchmarkEncodeUint32/dups_16K-24              23893         50846 ns/op    1288.91 MB/s         65560 mean_bytes
// BenchmarkEncodeUint32/dups_64K-24               5833        204957 ns/op    1279.02 MB/s        262160 mean_bytes
// BenchmarkEncodeUint32/runs_1K-24              372568          3175 ns/op    1290.02 MB/s          4120 mean_bytes
// BenchmarkEncodeUint32/runs_16K-24              23215         51575 ns/op    1270.69 MB/s         65552 mean_bytes
// BenchmarkEncodeUint32/runs_64K-24               5712        206257 ns/op    1270.96 MB/s        262136 mean_bytes
// BenchmarkEncodeUint32/seq_1K-24              1000000          1063 ns/op    3853.84 MB/s          1296 mean_bytes
// BenchmarkEncodeUint32/seq_16K-24               49522         24419 ns/op    2683.78 MB/s         30800 mean_bytes
// BenchmarkEncodeUint32/seq_64K-24               10000        115144 ns/op    2276.66 MB/s        150928 mean_bytes
//
// size_t encode_u32_avx2(uint8_t* dst, const uint32_t* src, size_t len, uint32_t minv) {
//     uint64_t* out = (uint64_t*)dst;
//     size_t i = 0, j = 0;

//     if (len == 0) return 0;
//     if ((uintptr_t)dst % 8 != 0) {
//         // fprintf(stderr, "Output buffer must be 8-byte aligned\n");
//         return 0;
//     }

//     // load static values
//     __m256i min_vec = _mm256_set1_epi32(minv);
//     __m256i v158 = _mm256_set1_epi32(158);
//     __m256i v32 = _mm256_set1_epi32(32);
//     __m256i v1 = _mm256_set1_epi32(1);
//     __m256i shuffle = _mm256_set_epi8(
//                 12,  8,  4, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
//                 -1, -1, -1, -1, 12, 8, 4, 0, -1, -1, -1, -1, -1, -1, -1, -1);

//     // printf("Starting encode: len=%zu, minv=%u\n", len, minv);

//     while (i < len) {
//         size_t remaining = len - i;
//         const uint32_t* chunk = src + i;
//         // printf("i=%zu, remaining=%zu, chunk=%p\n", i, remaining, chunk);

//         if (remaining >= 120) {
//             size_t k = 0;
//             for (; k < remaining && k < 240 && chunk[k] - minv == 1; k++);
//             // printf("Runs check: k=%zu\n", k);
//             if (k >= 120) {
//                 out[j++] = (k >= 240 ? 0 : 1ULL << 60);
//                 i += (k >= 240 ? 240 : 120);
//                 // printf("Packed run: j=%zu, i=%zu\n", j, i);
//                 continue;
//             }
//         }

//         int n = 0;
//         uint8_t max_bits = 0;

//         // printf("Starting usedBits loop: n=%d\n", n);

//         while (n < 60 && i + n < len) {
//             int chunk_n = min(8, len - (i + n));
//             // printf("Processing chunk: n=%d, chunk_n=%d\n", n, chunk_n);
//             __m256i vals = _mm256_loadu_si256((const __m256i*)(chunk + n));
//             // print_m256i("vals", vals);

//             // calculate used bits
//             __m256i v = _mm256_sub_epi32(vals, min_vec);
//             // Prevent rounding up by keeping top 8 bits
//             v = _mm256_andnot_si256(_mm256_srli_epi32(v, 8), v);

//             // Convert to float, extract exponent, compute CLZ
//             v = _mm256_castps_si256(_mm256_cvtepi32_ps(v));
//             v = _mm256_srli_epi32(v, 23);   // Exponent (biased)
//             v = _mm256_subs_epu16(v158, v); // CLZ = 158 - exponent
//             v = _mm256_min_epi16(v, v32);   // Clamp at 32
//             __m256i used_bits = _mm256_sub_epi32(v32, v); // Used bits = 32 - CLZ
//             // print_m256i("usedBits", used_bits);

//             // Horizontal max reduction
//             __m256i max = _mm256_max_epu32(used_bits, _mm256_shuffle_epi32(used_bits, _MM_SHUFFLE(2, 3, 0, 1)));
//             max = _mm256_max_epu32(max, _mm256_shuffle_epi32(max, _MM_SHUFFLE(1, 0, 3, 2)));
//             max = _mm256_max_epu32(max, _mm256_permute2x128_si256(max, max, 1));
//             uint8_t nbits = _mm256_extract_epi8(max, 0); // Low byte of first lane

//             if (nbits > max_bits) max_bits = nbits;
//             n += chunk_n;
//             // printf("nbits=%d max_bits=%d\n", nbits, max_bits);

//             int sel = 0;
//             for (; sel < 15 && max_bits > packing_info[sel].shift; sel++);
//             if (n > packing_info[sel].n) break;
//             if (n == packing_info[sel].n || i + n >= len) {
//                 if (n < packing_info[sel].n) {
//                     for (; sel < 15 && n < packing_info[sel].n; sel++);
//                 }
//                 // printf("Selector: sel=%d, n=%d, max_bits=%u\n", sel, n, max_bits);

//                 // printf("Packing: n=%d, sel=%d, j=%zu\n", n, sel, j);
//                 n = min(n, packing_info[sel].n);
//                 uint64_t val = (uint64_t)sel << 60;
//                 uint8_t shl = 0;
//                 for (int k = 0; k < n; k++) {
//                     val |= (uint64_t)(chunk[k] - minv) << shl;
//                     shl += packing_info[sel].shift;
//                     // printf("k=%d, val=0x%lx, shl=%u\n", k, val, shl);
//                 }
//                 // if (j >= len) {
//                 //     // fprintf(stderr, "Output buffer overflow: j=%zu, len=%zu\n", j, len);
//                 //     break;
//                 // }
//                 out[j++] = val;
//                 i += n;
//                 // printf("Packed: i=%zu, j=%zu\n", i, j);
//                 goto next;
//             }
//         }

//         int sel = 0;
//         for (; sel < 15 && max_bits > packing_info[sel].shift; sel++);
//         if (n < packing_info[sel].n) {
//             for (; sel < 15 && n < packing_info[sel].n; sel++);
//         }
//         // printf("Selector: sel=%d, n=%d, max_bits=%u\n", sel, n, max_bits);

//         n = min(n, packing_info[sel].n);
//         // printf("Packing: n=%d, sel=%d, j=%zu\n", n, sel, j);
//         uint64_t val = (uint64_t)sel << 60;
//         uint8_t shl = 0;
//         for (int k = 0; k < n; k++) {
//             val |= (uint64_t)(chunk[k] - minv) << shl;
//             shl += packing_info[sel].shift;
//             // printf("k=%d, val=0x%lx, shl=%u\n", k, val, shl);
//         }
//         if (j >= len) {
//             // fprintf(stderr, "Output buffer overflow: j=%zu, len=%zu\n", j, len);
//             break;
//         }
//         out[j++] = val;
//         i += n;
//         // printf("Packed: i=%zu, j=%zu\n", i, j);

//     next:
//         continue;
//     }

//     // printf("Finished: bytes=%zu\n", j * 8);
//     // fflush(stdout);
//     return j * 8;
// }

// -----------------------------------------------------------------------------------------
// New idea, SIMD work preserving
//
// cpu: 12th Gen Intel(R) Core(TM) i9-12900K
// BenchmarkEncodeUint32/dups_1K-24              480795          2506 ns/op    1634.21 MB/s          4096 mean_bytes
// BenchmarkEncodeUint32/dups_16K-24              22035         53810 ns/op    1217.91 MB/s         65536 mean_bytes
// BenchmarkEncodeUint32/dups_64K-24               5419        221402 ns/op    1184.02 MB/s        262144 mean_bytes
// BenchmarkEncodeUint32/runs_1K-24              467750          2548 ns/op    1607.50 MB/s          4096 mean_bytes
// BenchmarkEncodeUint32/runs_16K-24              30553         39345 ns/op    1665.66 MB/s         65536 mean_bytes
// BenchmarkEncodeUint32/runs_64K-24               7360        163972 ns/op    1598.71 MB/s        261920 mean_bytes
// BenchmarkEncodeUint32/seq_1K-24               825376          1416 ns/op    2892.17 MB/s          1288 mean_bytes
// BenchmarkEncodeUint32/seq_16K-24               48478         24801 ns/op    2642.49 MB/s         30776 mean_bytes
// BenchmarkEncodeUint32/seq_64K-24               10000        107351 ns/op    2441.94 MB/s        150928 mean_bytes
//
// size_t encode_u32_avx2(uint8_t* dst, const uint32_t* src, size_t len, uint32_t minv) {
//     uint64_t* out = (uint64_t*)dst;
//     size_t i = 0, j = 0;

//     if (len == 0) return 0;
//     if ((uintptr_t)dst % 8 != 0) return 0;

//     __m256i min_vec = _mm256_set1_epi32(minv);
//     __m256i v158 = _mm256_set1_epi32(158);
//     __m256i v32 = _mm256_set1_epi32(32);
//     __m256i v0 = _mm256_set1_epi32(0);
//     __m256i v1 = _mm256_set1_epi32(1);

//     uint8_t buffer[256] = {0}; // Circular buffer for usedBits
//     size_t head = 0, tail = 0; // Buffer pointers (modulo 256)
//     size_t entries = 0;

//     while (i < len || tail < head ) {
//         size_t remaining = len - i;
//         const uint32_t* chunk = src + i;
//         // printf("i=%zu, remaining=%zu, chunk=%p\n", i, remaining, chunk);

//         // Refill buffer if low
//         while (entries < 60 && i < len) {
//             int chunk_n = min(8, remaining);
//             // printf("Processing chunk: i=%d, entries=%d, chunk_n=%d\n", i, entries, chunk_n);
//             __m256i vals = _mm256_loadu_si256((const __m256i*)chunk);
//             __m256i v = _mm256_sub_epi32(vals, min_vec);

//             // Compute usedBits
//             v = _mm256_andnot_si256(_mm256_srli_epi32(v, 8), v);
//             v = _mm256_castps_si256(_mm256_cvtepi32_ps(v));
//             v = _mm256_srli_epi32(v, 23);
//             v = _mm256_subs_epu16(v158, v);
//             v = _mm256_min_epi16(v, v32);
//             __m256i used_bits = _mm256_sub_epi32(v32, v);
//             // print_m256i("usedBits", used_bits);

//             // Store in buffer with wrap-around
//             uint32_t bits[8];
//             _mm256_storeu_si256((__m256i*)bits, used_bits);
//             for (int k = 0; k < chunk_n; k++) {
//                 // printf("store to head=%d val=0x%lx\n", head%256, bits[k] & 0xFF);
//                 buffer[head % 256] = (uint8_t)(bits[k] & 0xFF);
//                 head++;
//             }
//             i += chunk_n;
//             entries += chunk_n;
//             chunk += (size_t)chunk_n;
//         }

//         // Output one uint64
//         int n = 0;
//         uint8_t max_bits = 0;

//         // Aggregate from buffer
//         for (int k = 0; k < entries; k++) {
//             uint8_t nbits = buffer[(tail + k) % 256];
//             if (nbits > max_bits) {
//                 max_bits = nbits;
//                 if (n > maxValsPerBits[max_bits].n) break;
//             }
//             n++;
//             if (n == maxValsPerBits[max_bits].n) break;
//         }

//         int sel = maxValsPerBits[max_bits].code;
//         for (; sel < 15 && n < packing_info[sel].n; sel++);
//         n = min(n, packing_info[sel].n);
//         // printf("Selector: sel=%d, n=%d, max_bits=%u\n", sel, n, max_bits);

//         // Pack from chunk (rewind i to match buffer)
//         uint64_t val = (uint64_t)sel << 60;
//         uint8_t shl = 0;
//         for (int k = 0; k < n; k++) {
//             val |= (uint64_t)(src[i - entries + k] - minv) << shl;
//             shl += packing_info[sel].shift;
//         }
//         out[j++] = val;
//         tail += n;
//         entries -= n;
//     }

//     // printf("Finished: bytes=%zu\n", j * 8);
//     // fflush(stdout);

//     return j * 8;
// }


// Scalar + work preserving
// cpu: 12th Gen Intel(R) Core(TM) i9-12900K
// BenchmarkEncodeUint32/dups_1K-24              476822          2549 ns/op    1606.63 MB/s          4096 mean_bytes
// BenchmarkEncodeUint32/dups_16K-24              21619         55934 ns/op    1171.66 MB/s         65536 mean_bytes
// BenchmarkEncodeUint32/dups_64K-24               5190        223568 ns/op    1172.55 MB/s        262144 mean_bytes
// BenchmarkEncodeUint32/runs_1K-24              481413          2498 ns/op    1639.95 MB/s          4096 mean_bytes
// BenchmarkEncodeUint32/runs_16K-24              29680         39366 ns/op    1664.80 MB/s         65512 mean_bytes
// BenchmarkEncodeUint32/runs_64K-24               7663        155562 ns/op    1685.15 MB/s        261976 mean_bytes
// BenchmarkEncodeUint32/seq_1K-24               766366          1558 ns/op    2629.61 MB/s          1288 mean_bytes
// BenchmarkEncodeUint32/seq_16K-24               44977         26761 ns/op    2448.89 MB/s         30776 mean_bytes
// BenchmarkEncodeUint32/seq_64K-24                9787        115113 ns/op    2277.28 MB/s        150928 mean_bytes
size_t encode_u32_avx2(uint8_t* dst, const uint32_t* src, size_t len, uint32_t minv) {
    uint64_t* out = (uint64_t*)dst;
    size_t i = 0, j = 0;

    if (len == 0) return 0;
    if ((uintptr_t)dst % 8 != 0) return 0;

    uint8_t buffer[256] = {0}; // Circular buffer for usedBits
    size_t head = 0, tail = 0; // Buffer pointers (modulo 256)
    size_t entries = 0;

    while (i < len || tail < head ) {
        // Refill buffer if low
        while (entries < 60 && i < len) {
            uint32_t nbits = 32 - __builtin_clz(src[i]-minv);
            buffer[head % 256] = (uint8_t)(nbits & 0xFF);
            i++;
            head++;
            entries++;
        }

        // Output one uint64
        int n = 0;
        uint8_t max_bits = 0;

        // Aggregate from buffer
        for (int k = 0; k < entries; k++) {
            uint8_t nbits = buffer[(tail + k) % 256];
            if (nbits > max_bits) {
                max_bits = nbits;
                if (n > maxValsPerBits[max_bits].n) break;
            }
            n++;
            if (n == maxValsPerBits[max_bits].n) break;
        }

        int sel = maxValsPerBits[max_bits].code;
        for (; sel < 15 && n < packing_info[sel].n; sel++);
        n = min(n, packing_info[sel].n);

        // Pack (rewind i to match buffer)
        uint64_t val = (uint64_t)sel << 60;
        uint8_t shl = 0;
        for (int k = 0; k < n; k++) {
            val |= (uint64_t)(src[i - entries + k] - minv) << shl;
            shl += packing_info[sel].shift;
        }
        out[j++] = val;
        tail += n;
        entries -= n;
    }

    return j * 8;
}


// cpu: 12th Gen Intel(R) Core(TM) i9-12900K
// BenchmarkEncodeUint64/dups_1K-24              248612          4335 ns/op    1889.70 MB/s          8192 mean_bytes
// BenchmarkEncodeUint64/dups_16K-24              18060         66039 ns/op    1984.77 MB/s        131072 mean_bytes
// BenchmarkEncodeUint64/dups_64K-24               3982        270387 ns/op    1939.03 MB/s        524288 mean_bytes
// BenchmarkEncodeUint64/runs_1K-24              276679          4374 ns/op    1872.80 MB/s          8192 mean_bytes
// BenchmarkEncodeUint64/runs_16K-24              17581         67882 ns/op    1930.87 MB/s        131072 mean_bytes
// BenchmarkEncodeUint64/runs_64K-24               4500        270990 ns/op    1934.72 MB/s        524288 mean_bytes
// BenchmarkEncodeUint64/seq_1K-24               715282          1639 ns/op    4996.87 MB/s          1288 mean_bytes
// BenchmarkEncodeUint64/seq_16K-24               39654         30203 ns/op    4339.70 MB/s         30776 mean_bytes
// BenchmarkEncodeUint64/seq_64K-24                9188        131045 ns/op    4000.81 MB/s        150928 mean_bytes
size_t encode_u64_avx2(uint8_t* dst, const uint64_t* src, size_t len, uint64_t minv) {
    uint64_t* out = (uint64_t*)dst;
    size_t i = 0, j = 0;

    if (len == 0) return 0;
    if ((uintptr_t)dst % 8 != 0) return 0;

    uint8_t buffer[256] = {0}; // Circular buffer for usedBits
    size_t head = 0, tail = 0; // Buffer pointers (modulo 256)
    size_t entries = 0;

    while (i < len || tail < head ) {
        // Refill buffer if low
        while (entries < 60 && i < len) {
            uint32_t nbits = 64 - __builtin_clzll(src[i]-minv);
            buffer[head % 256] = (uint8_t)(nbits & 0xFF);
            i++;
            head++;
            entries++;
        }

        // Output one uint64
        int n = 0;
        uint8_t max_bits = 0;

        // Aggregate from buffer
        for (int k = 0; k < entries; k++) {
            uint8_t nbits = buffer[(tail + k) % 256];
            if (nbits > max_bits) {
                max_bits = nbits;
                if (n > maxValsPerBits[max_bits].n) break;
            }
            n++;
            if (n == maxValsPerBits[max_bits].n) break;
        }

        if (max_bits > 60) {
            return -(size_t)max_bits;
        }

        int sel = maxValsPerBits[max_bits].code;
        for (; sel < 15 && n < packing_info[sel].n; sel++);
        n = min(n, packing_info[sel].n);

        // Pack (rewind i to match buffer)
        uint64_t val = (uint64_t)sel << 60;
        uint8_t shl = 0;
        for (int k = 0; k < n; k++) {
            val |= (src[i - entries + k] - minv) << shl;
            shl += packing_info[sel].shift;
        }
        out[j++] = val;
        tail += n;
        entries -= n;
    }

    return j * 8;
}

// BenchmarkEncodeUint16/dups_1K-24              526850          2307 ns/op     887.86 MB/s          2688 mean_bytes
// BenchmarkEncodeUint16/dups_16K-24              22267         53434 ns/op     613.25 MB/s         42824 mean_bytes
// BenchmarkEncodeUint16/dups_64K-24               5106        231615 ns/op     565.90 MB/s        171536 mean_bytes
// BenchmarkEncodeUint16/runs_1K-24              552080          2081 ns/op     983.91 MB/s          2408 mean_bytes
// BenchmarkEncodeUint16/runs_16K-24              33154         37007 ns/op     885.45 MB/s         38536 mean_bytes
// BenchmarkEncodeUint16/runs_64K-24               7893        149808 ns/op     874.94 MB/s        153912 mean_bytes
// BenchmarkEncodeUint16/seq_1K-24               732212          1609 ns/op    1273.22 MB/s          1288 mean_bytes
// BenchmarkEncodeUint16/seq_16K-24               43686         27053 ns/op    1211.24 MB/s         30776 mean_bytes
// BenchmarkEncodeUint16/seq_64K-24               10000        117213 ns/op    1118.24 MB/s        150928 mean_bytes
size_t encode_u16_avx2(uint8_t* dst, const uint16_t* src, size_t len, uint16_t minv) {
    uint64_t* out = (uint64_t*)dst;
    size_t i = 0, j = 0;

    if (len == 0) return 0;
    if ((uintptr_t)dst % 8 != 0) return 0;

    uint8_t buffer[256] = {0}; // Circular buffer for usedBits
    size_t head = 0, tail = 0; // Buffer pointers (modulo 256)
    size_t entries = 0;

    while (i < len || tail < head ) {
        // Refill buffer if low
        while (entries < 60 && i < len) {
            uint32_t nbits = 32 - __builtin_clz((uint32_t)(src[i]-minv));
            buffer[head % 256] = (uint8_t)(nbits & 0xFF);
            i++;
            head++;
            entries++;
        }

        // Output one uint64
        int n = 0;
        uint8_t max_bits = 0;

        // Aggregate from buffer
        for (int k = 0; k < entries; k++) {
            uint8_t nbits = buffer[(tail + k) % 256];
            if (nbits > max_bits) {
                max_bits = nbits;
                if (n > maxValsPerBits[max_bits].n) break;
            }
            n++;
            if (n == maxValsPerBits[max_bits].n) break;
        }

        int sel = maxValsPerBits[max_bits].code;
        for (; sel < 15 && n < packing_info[sel].n; sel++);
        n = min(n, packing_info[sel].n);

        // Pack (rewind i to match buffer)
        uint64_t val = (uint64_t)sel << 60;
        uint8_t shl = 0;
        for (int k = 0; k < n; k++) {
            val |= (uint64_t)(src[i - entries + k] - minv) << shl;
            shl += packing_info[sel].shift;
        }
        out[j++] = val;
        tail += n;
        entries -= n;
    }

    return j * 8;
}

// BenchmarkEncodeUint8/dups_1K-24               780676          1548 ns/op     661.66 MB/s          1176 mean_bytes
// BenchmarkEncodeUint8/dups_16K-24               38138         31794 ns/op     515.32 MB/s         18720 mean_bytes
// BenchmarkEncodeUint8/dups_64K-24                8391        136422 ns/op     480.39 MB/s         74856 mean_bytes
// BenchmarkEncodeUint8/runs_1K-24               728962          1613 ns/op     634.95 MB/s          1088 mean_bytes
// BenchmarkEncodeUint8/runs_16K-24               41211         29045 ns/op     564.10 MB/s         17824 mean_bytes
// BenchmarkEncodeUint8/runs_64K-24                9312        126343 ns/op     518.72 MB/s         70888 mean_bytes
// BenchmarkEncodeUint8/seq_1K-24                760858          1562 ns/op     655.37 MB/s          1040 mean_bytes
// BenchmarkEncodeUint8/seq_16K-24                49408         24624 ns/op     665.36 MB/s         16640 mean_bytes
// BenchmarkEncodeUint8/seq_64K-24                12116         98784 ns/op     663.43 MB/s         66560 mean_bytes
size_t encode_u8_avx2(uint8_t* dst, const uint8_t* src, size_t len, uint8_t minv) {
    uint64_t* out = (uint64_t*)dst;
    size_t i = 0, j = 0;

    if (len == 0) return 0;
    if ((uintptr_t)dst % 8 != 0) return 0;

    uint8_t buffer[256] = {0}; // Circular buffer for usedBits
    size_t head = 0, tail = 0; // Buffer pointers (modulo 256)
    size_t entries = 0;

    while (i < len || tail < head ) {
        // Refill buffer if low
        while (entries < 60 && i < len) {
            uint32_t nbits = 32 - __builtin_clz((uint32_t)(src[i]-minv));
            buffer[head % 256] = (uint8_t)(nbits & 0xFF);
            i++;
            head++;
            entries++;
        }

        // Output one uint64
        int n = 0;
        uint8_t max_bits = 0;

        // Aggregate from buffer
        for (int k = 0; k < entries; k++) {
            uint8_t nbits = buffer[(tail + k) % 256];
            if (nbits > max_bits) {
                max_bits = nbits;
                if (n > maxValsPerBits[max_bits].n) break;
            }
            n++;
            if (n == maxValsPerBits[max_bits].n) break;
        }

        int sel = maxValsPerBits[max_bits].code;
        for (; sel < 15 && n < packing_info[sel].n; sel++);
        n = min(n, packing_info[sel].n);

        // Pack (rewind i to match buffer)
        uint64_t val = (uint64_t)sel << 60;
        uint8_t shl = 0;
        for (int k = 0; k < n; k++) {
            val |= (uint64_t)(src[i - entries + k] - minv) << shl;
            shl += packing_info[sel].shift;
        }
        out[j++] = val;
        tail += n;
        entries -= n;
    }

    return j * 8;
}
