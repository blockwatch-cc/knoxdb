#include "analyze.h"
#include <immintrin.h>
#include <stdio.h>

void analyze_i64_avx2(int64_t* vals, Context* ctx, size_t len) {
    if (len == 0) {
        ctx->Min = 0;
        ctx->Max = 0;
        ctx->Delta = 0;
        ctx->NumRuns = 0;
        return;
    }

    __m256i min_vec = _mm256_set1_epi64x(vals[0]);
    __m256i max_vec = min_vec;
    int64_t num_runs = 1;
    int hasDelta = (ctx->Delta != 0 && len > 1);
    __m256i delta_vec = _mm256_set1_epi64x(ctx->Delta);
    __m256i prev_vec = _mm256_setzero_si256();

    size_t i = 0;
    for (; i + 3 < len; i += 4) {
        __m256i curr_vec = _mm256_loadu_si256((__m256i*)&vals[i]);

        // Min/Max
        __m256i gt = _mm256_cmpgt_epi64(min_vec, curr_vec);
        min_vec = _mm256_castpd_si256(_mm256_blendv_pd(_mm256_castsi256_pd(min_vec), _mm256_castsi256_pd(curr_vec), _mm256_castsi256_pd(gt)));
        gt = _mm256_cmpgt_epi64(curr_vec, max_vec);
        max_vec = _mm256_castpd_si256(_mm256_blendv_pd(_mm256_castsi256_pd(max_vec), _mm256_castsi256_pd(curr_vec), _mm256_castsi256_pd(gt)));


        // Runs
        int64_t last_prev = (i == 0) ? vals[0] : vals[i - 1];
        __m256i shifted = _mm256_permute4x64_epi64(curr_vec, _MM_SHUFFLE(2, 1, 0, 3));
        shifted = _mm256_insert_epi64(shifted, last_prev, 0);
        __m256i eq = _mm256_cmpeq_epi64(curr_vec, shifted);
        num_runs += _mm_popcnt_u32(~_mm256_movemask_pd(_mm256_castsi256_pd(eq)) & 0xF);

        // Delta check
        if (hasDelta && i < len - 1) {
            // Compute differences within curr_vec
            __m256i intra_diffs = _mm256_sub_epi64(curr_vec, shifted);
            eq = _mm256_cmpeq_epi64(intra_diffs, delta_vec);
            int mask = _mm256_movemask_pd(_mm256_castsi256_pd(eq));
            // Mask out the first lane if i=0 (last_prev is invalid)
            if (i == 0) mask &= 0xE; // Ignore lane 0
            if (mask != (i == 0 ? 0xE : 0xF)) {
                hasDelta = 0;
            }
        }

        prev_vec = curr_vec;
    }

    // Reduction
    __m128i min_lo = _mm256_castsi256_si128(min_vec);
    __m128i min_hi = _mm256_extracti128_si256(min_vec, 1);
    __m128i gt = _mm_cmpgt_epi64(min_lo, min_hi);
    min_lo = _mm_castpd_si128(_mm_blendv_pd(_mm_castsi128_pd(min_lo), _mm_castsi128_pd(min_hi), _mm_castsi128_pd(gt)));
    int64_t min1 = _mm_extract_epi64(min_lo, 0);
    int64_t min2 = _mm_extract_epi64(min_lo, 1);
    ctx->Min = (min1 < min2) ? min1 : min2;

    __m128i max_lo = _mm256_castsi256_si128(max_vec);
    __m128i max_hi = _mm256_extracti128_si256(max_vec, 1);
    gt = _mm_cmpgt_epi64(max_hi, max_lo);
    max_lo = _mm_castpd_si128(_mm_blendv_pd(_mm_castsi128_pd(max_lo), _mm_castsi128_pd(max_hi), _mm_castsi128_pd(gt)));
    int64_t max1 = _mm_extract_epi64(max_lo, 0);
    int64_t max2 = _mm_extract_epi64(max_lo, 1);
    ctx->Max = (max1 > max2) ? max1 : max2;

    // Tail loop
    for (; i < len; i++) {
        if (vals[i] < ctx->Min) ctx->Min = vals[i];
        if (vals[i] > ctx->Max) ctx->Max = vals[i];
        if (i > 0 && vals[i] != vals[i - 1]) num_runs++;
        if (hasDelta && i < len - 1 && vals[i + 1] - vals[i] != ctx->Delta) hasDelta = 0;
    }

    ctx->NumRuns = num_runs;
    ctx->Delta = hasDelta ? ctx->Delta : 0;
}
