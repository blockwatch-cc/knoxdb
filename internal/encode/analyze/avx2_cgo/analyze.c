#include "analyze.h"
#include <immintrin.h>
#include <stdio.h>

void analyze_i64_avx2(int64_t* vals, I64Context* ctx, size_t len) {
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
    int64_t last_prev = vals[0];

    size_t i = 0;
    for (; i + 3 < len; i += 4) {
        __m256i curr_vec = _mm256_loadu_si256((__m256i*)&vals[i]);
        // Min/Max in loop
        min_vec = _mm256_blendv_epi8(min_vec, curr_vec, _mm256_cmpgt_epi64(min_vec, curr_vec));
        max_vec = _mm256_blendv_epi8(max_vec, curr_vec, _mm256_cmpgt_epi64(curr_vec, max_vec));

        __m256i shifted = _mm256_permute4x64_epi64(curr_vec, _MM_SHUFFLE(2, 1, 0, 3));
        shifted = _mm256_insert_epi64(shifted, last_prev, 0);
        __m256i eq = _mm256_cmpeq_epi64(curr_vec, shifted);
        num_runs += _mm_popcnt_u32(~_mm256_movemask_pd(_mm256_castsi256_pd(eq)) & 0xF);

        if (hasDelta && i < len - 1) {
            __m256i intra_diffs = _mm256_sub_epi64(curr_vec, shifted);
            eq = _mm256_cmpeq_epi64(intra_diffs, delta_vec);
            int mask = _mm256_movemask_pd(_mm256_castsi256_pd(eq));
            int expected = 0xf;
            if (i == 0) {
                mask &= ~1;
                expected &= ~1;
            }
            if ((mask & expected) != expected) {
                hasDelta = 0;
            }
        }

        last_prev = vals[i + 3];
    }

    // Min/Max Reduction
    min_vec = _mm256_blendv_epi8(min_vec, _mm256_permute4x64_epi64(min_vec, _MM_SHUFFLE(2, 3, 0, 1)),
                                 _mm256_cmpgt_epi64(min_vec, _mm256_permute4x64_epi64(min_vec, _MM_SHUFFLE(2, 3, 0, 1))));
    min_vec = _mm256_blendv_epi8(min_vec, _mm256_permute4x64_epi64(min_vec, _MM_SHUFFLE(1, 0, 3, 2)),
                                 _mm256_cmpgt_epi64(min_vec, _mm256_permute4x64_epi64(min_vec, _MM_SHUFFLE(1, 0, 3, 2))));
    ctx->Min = _mm256_extract_epi64(min_vec, 0);

    max_vec = _mm256_blendv_epi8(max_vec, _mm256_permute4x64_epi64(max_vec, _MM_SHUFFLE(2, 3, 0, 1)),
                                 _mm256_cmpgt_epi64(_mm256_permute4x64_epi64(max_vec, _MM_SHUFFLE(2, 3, 0, 1)), max_vec));
    max_vec = _mm256_blendv_epi8(max_vec, _mm256_permute4x64_epi64(max_vec, _MM_SHUFFLE(1, 0, 3, 2)),
                                 _mm256_cmpgt_epi64(_mm256_permute4x64_epi64(max_vec, _MM_SHUFFLE(1, 0, 3, 2)), max_vec));
    ctx->Max = _mm256_extract_epi64(max_vec, 0);

    // Tail loop
    for (; i < len; i++) {
        if (vals[i] < ctx->Min) ctx->Min = vals[i];
        if (vals[i] > ctx->Max) ctx->Max = vals[i];
        if (i > 0 && vals[i] != last_prev) num_runs++;
        if (hasDelta && i > 0 && vals[i] - last_prev != ctx->Delta) hasDelta = 0;
        last_prev = vals[i];
    }

    ctx->NumRuns = num_runs;
    ctx->Delta = hasDelta ? ctx->Delta : 0;
}

void analyze_u64_avx2(uint64_t* vals, U64Context* ctx, size_t len) {
    if (len == 0) {
        ctx->Min = 0;
        ctx->Max = 0;
        ctx->Delta = 0;
        ctx->NumRuns = 0;
        return;
    }

    __m256i min_vec = _mm256_set1_epi64x((int64_t)vals[0]);
    __m256i max_vec = min_vec;
    uint64_t num_runs = 1;
    int hasDelta = (ctx->Delta != 0 && len > 1);
    __m256i delta_vec = _mm256_set1_epi64x((int64_t)ctx->Delta);
    uint64_t last_prev = vals[0];

    size_t i = 0;
    for (; i + 3 < len; i += 4) {
        __m256i curr_vec = _mm256_loadu_si256((__m256i*)&vals[i]);
        // Min/Max in loop
        min_vec = _mm256_blendv_epi8(min_vec, curr_vec, _mm256_cmpgt_epi64(min_vec, curr_vec));
        max_vec = _mm256_blendv_epi8(max_vec, curr_vec, _mm256_cmpgt_epi64(curr_vec, max_vec));

        __m256i shifted = _mm256_permute4x64_epi64(curr_vec, _MM_SHUFFLE(2, 1, 0, 3));
        shifted = _mm256_insert_epi64(shifted, (int64_t)last_prev, 0);
        __m256i eq = _mm256_cmpeq_epi64(curr_vec, shifted);
        num_runs += _mm_popcnt_u32(~_mm256_movemask_pd(_mm256_castsi256_pd(eq)) & 0xF);

        if (hasDelta && i < len - 1) {
            __m256i intra_diffs = _mm256_sub_epi64(curr_vec, shifted);
            eq = _mm256_cmpeq_epi64(intra_diffs, delta_vec);
            int mask = _mm256_movemask_pd(_mm256_castsi256_pd(eq));
            int expected = 0xf;
            if (i == 0) {
                mask &= ~1;
                expected &= ~1;
            }
            if ((mask & expected) != expected) {
                hasDelta = 0;
            }
        }

        last_prev = vals[i + 3];
    }

    // Min/Max Reduction
    min_vec = _mm256_blendv_epi8(min_vec, _mm256_permute4x64_epi64(min_vec, _MM_SHUFFLE(2, 3, 0, 1)),
                                 _mm256_cmpgt_epi64(min_vec, _mm256_permute4x64_epi64(min_vec, _MM_SHUFFLE(2, 3, 0, 1))));
    min_vec = _mm256_blendv_epi8(min_vec, _mm256_permute4x64_epi64(min_vec, _MM_SHUFFLE(1, 0, 3, 2)),
                                 _mm256_cmpgt_epi64(min_vec, _mm256_permute4x64_epi64(min_vec, _MM_SHUFFLE(1, 0, 3, 2))));
    ctx->Min = (uint64_t)_mm256_extract_epi64(min_vec, 0);

    max_vec = _mm256_blendv_epi8(max_vec, _mm256_permute4x64_epi64(max_vec, _MM_SHUFFLE(2, 3, 0, 1)),
                                 _mm256_cmpgt_epi64(_mm256_permute4x64_epi64(max_vec, _MM_SHUFFLE(2, 3, 0, 1)), max_vec));
    max_vec = _mm256_blendv_epi8(max_vec, _mm256_permute4x64_epi64(max_vec, _MM_SHUFFLE(1, 0, 3, 2)),
                                 _mm256_cmpgt_epi64(_mm256_permute4x64_epi64(max_vec, _MM_SHUFFLE(1, 0, 3, 2)), max_vec));
    ctx->Max = (uint64_t)_mm256_extract_epi64(max_vec, 0);

    // Tail loop
    for (; i < len; i++) {
        if (vals[i] < ctx->Min) ctx->Min = vals[i];
        if (vals[i] > ctx->Max) ctx->Max = vals[i];
        if (i > 0 && vals[i] != last_prev) num_runs++;
        if (hasDelta && i > 0 && vals[i] - last_prev != (int64_t)ctx->Delta) hasDelta = 0;
        last_prev = vals[i];
    }

    ctx->NumRuns = num_runs;
    ctx->Delta = hasDelta ? ctx->Delta : 0;
}

void analyze_i32_avx2(int32_t* vals, I32Context* ctx, size_t len) {
    if (len == 0) {
        ctx->Min = 0;
        ctx->Max = 0;
        ctx->Delta = 0;
        ctx->NumRuns = 0;
        return;
    }

    __m256i min_vec = _mm256_set1_epi32(vals[0]);
    __m256i max_vec = min_vec;
    int32_t num_runs = 1;
    int hasDelta = (ctx->Delta != 0 && len > 1);
    __m256i delta_vec = _mm256_set1_epi32(ctx->Delta);
    int32_t last_prev = vals[0];

    size_t i = 0;
    for (; i + 7 < len; i += 8) {
        __m256i curr_vec = _mm256_loadu_si256((__m256i*)&vals[i]);
        min_vec = _mm256_min_epi32(min_vec, curr_vec);
        max_vec = _mm256_max_epi32(max_vec, curr_vec);

        __m256i shifted = _mm256_insert_epi32(_mm256_permutevar8x32_epi32(curr_vec, _mm256_set_epi32(6, 5, 4, 3, 2, 1, 0, 7)), last_prev, 0);
        __m256i eq = _mm256_cmpeq_epi32(curr_vec, shifted);
        num_runs += _mm_popcnt_u32(~_mm256_movemask_ps(_mm256_castsi256_ps(eq)) & 0xFF);

        if (hasDelta && i < len - 1) {
            __m256i intra_diffs = _mm256_sub_epi32(curr_vec, shifted);
            eq = _mm256_cmpeq_epi32(intra_diffs, delta_vec);
            int mask = _mm256_movemask_ps(_mm256_castsi256_ps(eq));
            int expected = 0xff;
            if (i == 0) {
                mask &= ~1;
                expected &= ~1;
            }
            if ((mask & expected) != expected) {
                hasDelta = 0;
            }
        }

        last_prev = vals[i + 7];
    }

    // Min/Max Reduction
    min_vec = _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(2, 3, 0, 1));
    min_vec = _mm256_min_epi32(min_vec, _mm256_permute4x64_epi64(min_vec, _MM_SHUFFLE(1, 0, 3, 2)));
    min_vec = _mm256_min_epi32(min_vec, _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(3, 2, 1, 0)));
    ctx->Min = _mm256_extract_epi32(min_vec, 0);

    max_vec = _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(2, 3, 0, 1));
    max_vec = _mm256_max_epi32(max_vec, _mm256_permute4x64_epi64(max_vec, _MM_SHUFFLE(1, 0, 3, 2)));
    max_vec = _mm256_max_epi32(max_vec, _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(3, 2, 1, 0)));
    ctx->Max = _mm256_extract_epi32(max_vec, 0);

    // Tail loop
    for (; i < len; i++) {
        if (vals[i] < ctx->Min) ctx->Min = vals[i];
        if (vals[i] > ctx->Max) ctx->Max = vals[i];
        if (i > 0 && vals[i] != last_prev) num_runs++;
        if (hasDelta && i > 0 && vals[i] - last_prev != ctx->Delta) hasDelta = 0;
        last_prev = vals[i];
    }

    ctx->NumRuns = num_runs;
    ctx->Delta = hasDelta ? ctx->Delta : 0;
}

void analyze_u32_avx2(uint32_t* vals, U32Context* ctx, size_t len) {
    if (len == 0) {
        ctx->Min = 0;
        ctx->Max = 0;
        ctx->Delta = 0;
        ctx->NumRuns = 0;
        return;
    }

    __m256i min_vec = _mm256_set1_epi32((int32_t)vals[0]);
    __m256i max_vec = min_vec;
    uint32_t num_runs = 1;
    int hasDelta = (ctx->Delta != 0 && len > 1);
    __m256i delta_vec = _mm256_set1_epi32((int32_t)ctx->Delta);
    uint32_t last_prev = vals[0];

    size_t i = 0;
    for (; i + 7 < len; i += 8) {
        __m256i curr_vec = _mm256_loadu_si256((__m256i*)&vals[i]);

        // Min/Max (unsigned)
        min_vec = _mm256_min_epu32(min_vec, curr_vec);
        max_vec = _mm256_max_epu32(max_vec, curr_vec);

        // Runs
        __m256i shifted = _mm256_insert_epi32(_mm256_permutevar8x32_epi32(curr_vec, _mm256_set_epi32(6, 5, 4, 3, 2, 1, 0, 7)), (int32_t)last_prev, 0);
        __m256i eq = _mm256_cmpeq_epi32(curr_vec, shifted);
        num_runs += _mm_popcnt_u32(~_mm256_movemask_ps(_mm256_castsi256_ps(eq)) & 0xFF);

        // Delta check
        if (hasDelta && i < len - 1) {
            __m256i intra_diffs = _mm256_sub_epi32(curr_vec, shifted);
            eq = _mm256_cmpeq_epi32(intra_diffs, delta_vec);
            int mask = _mm256_movemask_ps(_mm256_castsi256_ps(eq));
            int expected = 0xff;
            if (i == 0) {
                mask &= ~1;
                expected &= ~1;
            }
            if ((mask & expected) != expected) {
                hasDelta = 0;
            }
        }

        last_prev = vals[i + 7];
    }

    // Min/Max Reduction
    min_vec = _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(2, 3, 0, 1));
    min_vec = _mm256_min_epu32(min_vec, _mm256_permute4x64_epi64(min_vec, _MM_SHUFFLE(1, 0, 3, 2)));
    min_vec = _mm256_min_epu32(min_vec, _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(3, 2, 1, 0)));
    ctx->Min = (uint32_t)_mm256_extract_epi32(min_vec, 0);

    max_vec = _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(2, 3, 0, 1));
    max_vec = _mm256_max_epu32(max_vec, _mm256_permute4x64_epi64(max_vec, _MM_SHUFFLE(1, 0, 3, 2)));
    max_vec = _mm256_max_epu32(max_vec, _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(3, 2, 1, 0)));
    ctx->Max = (uint32_t)_mm256_extract_epi32(max_vec, 0);

    // Tail loop
    for (; i < len; i++) {
        if (vals[i] < ctx->Min) ctx->Min = vals[i];
        if (vals[i] > ctx->Max) ctx->Max = vals[i];
        if (i > 0 && vals[i] != last_prev) num_runs++;
        if (hasDelta && i > 0 && vals[i] - last_prev != (int32_t)ctx->Delta) hasDelta = 0;
        last_prev = vals[i];
    }

    ctx->NumRuns = num_runs;
    ctx->Delta = hasDelta ? ctx->Delta : 0;
}

void analyze_i16_avx2(int16_t* vals, I16Context* ctx, size_t len) {
    if (len == 0) {
        ctx->Min = 0;
        ctx->Max = 0;
        ctx->Delta = 0;
        ctx->NumRuns = 0;
        return;
    }

    __m256i min_vec = _mm256_set1_epi16(vals[0]);
    __m256i max_vec = min_vec;
    int16_t num_runs = 1;
    int hasDelta = (ctx->Delta != 0 && len > 1);
    __m256i delta_vec = _mm256_set1_epi16(ctx->Delta);
    __m256i prev_vec = _mm256_setzero_si256();

    size_t i = 0;
    for (; i + 15 < len; i += 16) {
        __m256i curr_vec = _mm256_loadu_si256((__m256i*)&vals[i]);
        min_vec = _mm256_min_epi16(min_vec, curr_vec);
        max_vec = _mm256_max_epi16(max_vec, curr_vec);

        __m256i high0low1 = _mm256_permute2f128_si256(prev_vec, curr_vec, 0x21);
        __m256i shifted = _mm256_alignr_epi8(curr_vec, high0low1, 14);
        __m256i eq = _mm256_cmpeq_epi16(curr_vec, shifted);
        __m128i eq_lo = _mm256_castsi256_si128(eq);
        __m128i eq_hi = _mm256_extracti128_si256(eq, 1);
        __m128i packed = _mm_packs_epi16(eq_lo, eq_hi);
        num_runs += _mm_popcnt_u32(~_mm_movemask_epi8(packed) & (0xFFFF - (i == 0 ? 1 : 0)));

        if (hasDelta && i < len - 1) {
            __m256i intra_diffs = _mm256_sub_epi16(curr_vec, shifted);
            eq = _mm256_cmpeq_epi16(intra_diffs, delta_vec);
            eq_lo = _mm256_castsi256_si128(eq);
            eq_hi = _mm256_extracti128_si256(eq, 1);
            packed = _mm_packs_epi16(eq_lo, eq_hi);
            int mask = _mm_movemask_epi8(packed);
            int expected = 0xffff;
            if (i == 0) {
                mask &= ~1;
                expected &= ~1;
            }
            if ((mask & expected) != expected) {
                hasDelta = 0;
            }
        }

        prev_vec = curr_vec;
    }

    // Min/Max Reduction
    min_vec = _mm256_min_epi16(min_vec, _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(2, 3, 0, 1)));
    min_vec = _mm256_min_epi16(min_vec, _mm256_permute4x64_epi64(min_vec, _MM_SHUFFLE(1, 0, 3, 2)));
    min_vec = _mm256_min_epi16(min_vec, _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(3, 2, 1, 0)));
    min_vec = _mm256_min_epi16(min_vec, _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(0, 1, 2, 3)));
    ctx->Min = _mm256_extract_epi16(min_vec, 0);

    max_vec = _mm256_max_epi16(max_vec, _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(2, 3, 0, 1)));
    max_vec = _mm256_max_epi16(max_vec, _mm256_permute4x64_epi64(max_vec, _MM_SHUFFLE(1, 0, 3, 2)));
    max_vec = _mm256_max_epi16(max_vec, _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(3, 2, 1, 0)));
    max_vec = _mm256_max_epi16(max_vec, _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(0, 1, 2, 3)));
    ctx->Max = _mm256_extract_epi16(max_vec, 0);

    // Tail loop
    for (; i < len; i++) {
        if (vals[i] < ctx->Min) ctx->Min = vals[i];
        if (vals[i] > ctx->Max) ctx->Max = vals[i];
        if (i > 0 && vals[i] != vals[i - 1]) num_runs++;
        if (hasDelta && i > 0 && vals[i] - vals[i-1] != ctx->Delta) hasDelta = 0;
    }

    ctx->NumRuns = num_runs;
    ctx->Delta = hasDelta ? ctx->Delta : 0;
}

void analyze_u16_avx2(uint16_t* vals, U16Context* ctx, size_t len) {
    if (len == 0) {
        ctx->Min = 0;
        ctx->Max = 0;
        ctx->Delta = 0;
        ctx->NumRuns = 0;
        return;
    }

    __m256i min_vec = _mm256_set1_epi16((int16_t)vals[0]);
    __m256i max_vec = min_vec;
    uint16_t num_runs = 1;
    int hasDelta = (ctx->Delta != 0 && len > 1);
    __m256i delta_vec = _mm256_set1_epi16((int16_t)ctx->Delta);
    __m256i prev_vec = _mm256_setzero_si256();

    size_t i = 0;
    for (; i + 15 < len; i += 16) {
        __m256i curr_vec = _mm256_loadu_si256((__m256i*)&vals[i]);
        min_vec = _mm256_min_epu16(min_vec, curr_vec);
        max_vec = _mm256_max_epu16(max_vec, curr_vec);

        __m256i high0low1 = _mm256_permute2f128_si256(prev_vec, curr_vec, 0x21);
        __m256i shifted = _mm256_alignr_epi8(curr_vec, high0low1, 14);
        __m256i eq = _mm256_cmpeq_epi16(curr_vec, shifted);
        __m128i eq_lo = _mm256_castsi256_si128(eq);
        __m128i eq_hi = _mm256_extracti128_si256(eq, 1);
        __m128i packed = _mm_packs_epi16(eq_lo, eq_hi);
        num_runs += _mm_popcnt_u32(~_mm_movemask_epi8(packed) & (0xFFFF - (i == 0 ? 1 : 0)));

        if (hasDelta && i < len - 1) {
            __m256i intra_diffs = _mm256_sub_epi16(curr_vec, shifted);
            eq = _mm256_cmpeq_epi16(intra_diffs, delta_vec);
            eq_lo = _mm256_castsi256_si128(eq);
            eq_hi = _mm256_extracti128_si256(eq, 1);
            packed = _mm_packs_epi16(eq_lo, eq_hi);
            int mask = _mm_movemask_epi8(packed);
            int expected = 0xffff;
            if (i == 0) {
                mask &= ~1;
                expected &= ~1;
            }
            if ((mask & expected) != expected) {
                hasDelta = 0;
            }
        }

        prev_vec = curr_vec;
    }

    // Min/Max Reduction
    min_vec = _mm256_min_epu16(min_vec, _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(2, 3, 0, 1)));
    min_vec = _mm256_min_epu16(min_vec, _mm256_permute4x64_epi64(min_vec, _MM_SHUFFLE(1, 0, 3, 2)));
    min_vec = _mm256_min_epu16(min_vec, _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(3, 2, 1, 0)));
    min_vec = _mm256_min_epu16(min_vec, _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(0, 1, 2, 3)));
    ctx->Min = (uint16_t)_mm256_extract_epi16(min_vec, 0);

    max_vec = _mm256_max_epu16(max_vec, _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(2, 3, 0, 1)));
    max_vec = _mm256_max_epu16(max_vec, _mm256_permute4x64_epi64(max_vec, _MM_SHUFFLE(1, 0, 3, 2)));
    max_vec = _mm256_max_epu16(max_vec, _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(3, 2, 1, 0)));
    max_vec = _mm256_max_epu16(max_vec, _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(0, 1, 2, 3)));
    ctx->Max = (uint16_t)_mm256_extract_epi16(max_vec, 0);

    // Tail loop
    for (; i < len; i++) {
        if (vals[i] < ctx->Min) ctx->Min = vals[i];
        if (vals[i] > ctx->Max) ctx->Max = vals[i];
        if (i > 0 && vals[i] != vals[i - 1]) num_runs++;
        if (hasDelta && i > 0 && vals[i] - vals[i-1] != (int16_t)ctx->Delta) hasDelta = 0;
    }

    ctx->NumRuns = num_runs;
    ctx->Delta = hasDelta ? ctx->Delta : 0;
}

void analyze_i8_avx2(int8_t* vals, I8Context* ctx, size_t len) {
    if (len == 0) {
        ctx->Min = 0;
        ctx->Max = 0;
        ctx->Delta = 0;
        ctx->NumRuns = 0;
        return;
    }

    __m256i min_vec = _mm256_set1_epi8(vals[0]);
    __m256i max_vec = min_vec;
    int8_t num_runs = 1;
    int hasDelta = (ctx->Delta != 0 && len > 1);
    __m256i delta_vec = _mm256_set1_epi8(ctx->Delta);
    __m256i prev_vec = _mm256_setzero_si256();

    size_t i = 0;
    for (; i + 31 < len; i += 32) {
        __m256i curr_vec = _mm256_loadu_si256((__m256i*)&vals[i]);
        min_vec = _mm256_min_epi8(min_vec, curr_vec);
        max_vec = _mm256_max_epi8(max_vec, curr_vec);

        __m256i perm = _mm256_permute2f128_si256(prev_vec, curr_vec, 0x21);
        __m256i shifted = _mm256_alignr_epi8(curr_vec, perm, 15);
        __m256i eq = _mm256_cmpeq_epi8(curr_vec, shifted);
        num_runs += _mm_popcnt_u32(~_mm256_movemask_epi8(eq) & (0xFFFFFFFF - (i == 0 ? 1 : 0)));

        if (hasDelta && i < len - 1) {
            __m256i intra_diffs = _mm256_sub_epi8(curr_vec, shifted);
            eq = _mm256_cmpeq_epi8(intra_diffs, delta_vec);
            int mask = _mm256_movemask_epi8(eq);
            int expected = 0xffffffff;
            if (i == 0) {
                mask &= ~1;
                expected &= ~1;
            }
            if ((mask & expected) != expected) {
                hasDelta = 0;
            }
        }

        prev_vec = curr_vec;
    }

    min_vec = _mm256_min_epi8(min_vec, _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(2, 3, 0, 1)));
    min_vec = _mm256_min_epi8(min_vec, _mm256_permute4x64_epi64(min_vec, _MM_SHUFFLE(1, 0, 3, 2)));
    min_vec = _mm256_min_epi8(min_vec, _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(3, 2, 1, 0)));
    min_vec = _mm256_min_epi8(min_vec, _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(0, 1, 2, 3)));
    __m128i min_lo = _mm256_castsi256_si128(min_vec);
    min_lo = _mm_min_epi8(min_lo, _mm256_extracti128_si256(min_vec, 1));
    ctx->Min = _mm_cvtsi128_si32(min_lo);

    max_vec = _mm256_max_epi8(max_vec, _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(2, 3, 0, 1)));
    max_vec = _mm256_max_epi8(max_vec, _mm256_permute4x64_epi64(max_vec, _MM_SHUFFLE(1, 0, 3, 2)));
    max_vec = _mm256_max_epi8(max_vec, _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(3, 2, 1, 0)));
    max_vec = _mm256_max_epi8(max_vec, _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(0, 1, 2, 3)));
    __m128i max_lo = _mm256_castsi256_si128(max_vec);
    max_lo = _mm_max_epi8(max_lo, _mm256_extracti128_si256(max_vec, 1));
    ctx->Max = _mm_cvtsi128_si32(max_lo);

    for (; i < len; i++) {
        if (vals[i] < ctx->Min) ctx->Min = vals[i];
        if (vals[i] > ctx->Max) ctx->Max = vals[i];
        if (i > 0 && vals[i] != vals[i - 1]) num_runs++;
        if (hasDelta && i > 0 && vals[i] - vals[i-1] != ctx->Delta) hasDelta = 0;
    }

    ctx->NumRuns = num_runs;
    ctx->Delta = hasDelta ? ctx->Delta : 0;
}

void analyze_u8_avx2(uint8_t* vals, U8Context* ctx, size_t len) {
    if (len == 0) {
        ctx->Min = 0;
        ctx->Max = 0;
        ctx->Delta = 0;
        ctx->NumRuns = 0;
        return;
    }

    __m256i min_vec = _mm256_set1_epi8((int8_t)vals[0]);
    __m256i max_vec = min_vec;
    uint8_t num_runs = 1;
    int hasDelta = (ctx->Delta != 0 && len > 1);
    __m256i delta_vec = _mm256_set1_epi8((int8_t)ctx->Delta);
    __m256i prev_vec = _mm256_setzero_si256();

    size_t i = 0;
    for (; i + 31 < len; i += 32) {
        __m256i curr_vec = _mm256_loadu_si256((__m256i*)&vals[i]);
        min_vec = _mm256_min_epu8(min_vec, curr_vec);
        max_vec = _mm256_max_epu8(max_vec, curr_vec);

        __m256i perm = _mm256_permute2f128_si256(prev_vec, curr_vec, 0x21);
        __m256i shifted = _mm256_alignr_epi8(curr_vec, perm, 15);

        __m256i eq = _mm256_cmpeq_epi8(curr_vec, shifted);
        num_runs += _mm_popcnt_u32(~_mm256_movemask_epi8(eq) & (0xFFFFFFFF - (i == 0 ? 1 : 0)));

        if (hasDelta && i < len - 1) {
            __m256i intra_diffs = _mm256_sub_epi8(curr_vec, shifted);
            eq = _mm256_cmpeq_epi8(intra_diffs, delta_vec);
            int mask = _mm256_movemask_epi8(eq);
            int expected = 0xffffffff;
            if (i == 0) {
                mask &= ~1;
                expected &= ~1;
            }
            if ((mask & expected) != expected) {
                hasDelta = 0;
            }
        }

        prev_vec = curr_vec;
    }

    min_vec = _mm256_min_epu8(min_vec, _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(2, 3, 0, 1)));
    min_vec = _mm256_min_epu8(min_vec, _mm256_permute4x64_epi64(min_vec, _MM_SHUFFLE(1, 0, 3, 2)));
    min_vec = _mm256_min_epu8(min_vec, _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(3, 2, 1, 0)));
    min_vec = _mm256_min_epu8(min_vec, _mm256_shuffle_epi32(min_vec, _MM_SHUFFLE(0, 1, 2, 3)));
    __m128i min_lo = _mm256_castsi256_si128(min_vec);
    min_lo = _mm_min_epu8(min_lo, _mm256_extracti128_si256(min_vec, 1));
    ctx->Min = (uint8_t)_mm_cvtsi128_si32(min_lo);

    max_vec = _mm256_max_epu8(max_vec, _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(2, 3, 0, 1)));
    max_vec = _mm256_max_epu8(max_vec, _mm256_permute4x64_epi64(max_vec, _MM_SHUFFLE(1, 0, 3, 2)));
    max_vec = _mm256_max_epu8(max_vec, _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(3, 2, 1, 0)));
    max_vec = _mm256_max_epu8(max_vec, _mm256_shuffle_epi32(max_vec, _MM_SHUFFLE(0, 1, 2, 3)));
    __m128i max_lo = _mm256_castsi256_si128(max_vec);
    max_lo = _mm_max_epu8(max_lo, _mm256_extracti128_si256(max_vec, 1));
    ctx->Max = (uint8_t)_mm_cvtsi128_si32(max_lo);

    for (; i < len; i++) {
        if (vals[i] < ctx->Min) ctx->Min = vals[i];
        if (vals[i] > ctx->Max) ctx->Max = vals[i];
        if (i > 0 && vals[i] != vals[i - 1]) num_runs++;
        if (hasDelta && i > 0 && vals[i] - vals[i-1] != (int8_t)ctx->Delta) hasDelta = 0;
    }

    ctx->NumRuns = num_runs;
    ctx->Delta = hasDelta ? ctx->Delta : 0;
}
