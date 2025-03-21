#include "analyze.h"
#include <immintrin.h>
#include <stdio.h>

void analyze_i64_avx512(int64_t* vals, I64Context* ctx, size_t len) {
    if (len == 0) {
        ctx->Min = 0;
        ctx->Max = 0;
        ctx->Delta = 0;
        ctx->NumRuns = 0;
        return;
    }

    __m512i min_vec = _mm512_set1_epi64(vals[0]);
    __m512i max_vec = min_vec;
    int64_t num_runs = 1;
    int hasDelta = (ctx->Delta != 0 && len > 1);
    __m512i delta_vec = _mm512_set1_epi64(ctx->Delta);
    __m512i prev_vec = _mm512_setzero_si512();

    size_t i = 0;
    for (; i + 7 < len; i += 8) {
        __m512i curr_vec = _mm512_loadu_si512((__m512i*)&vals[i]);

        // Min/Max
        min_vec = _mm512_min_epi64(min_vec, curr_vec);
        max_vec = _mm512_max_epi64(max_vec, curr_vec);

        // Runs
        int64_t last_prev = (i == 0) ? vals[0] : vals[i - 1];
        __m512i shifted = _mm512_permutexvar_epi64(_mm512_set_epi64(6, 5, 4, 3, 2, 1, 0, 7), curr_vec);
        shifted = _mm512_mask_set1_epi64(shifted, 0x01, last_prev); // Replace lane 0 with last_prev
        __mmask8 eq = _mm512_cmpeq_epi64_mask(curr_vec, shifted);
        num_runs += _mm_popcnt_u32(~eq & 0xFF);

        // Delta check
        if (hasDelta && i < len - 1) {
            __m512i intra_diffs = _mm512_sub_epi64(curr_vec, shifted);
            __mmask8 mask = _mm512_cmpeq_epi64_mask(intra_diffs, delta_vec);
            if (i == 0) mask &= 0xFE; // Ignore lane 0
            if (mask != (i == 0 ? 0xFE : 0xFF)) {
                hasDelta = 0;
            }
        }

        prev_vec = curr_vec;
    }

    // Reduction using AVX-512 reduce intrinsics
    ctx->Min = _mm512_reduce_min_epi64(min_vec);
    ctx->Max = _mm512_reduce_max_epi64(max_vec);

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
