// analyze.h
#ifndef ANALYZE_H
#define ANALYZE_H

#include <stdint.h>
#include <stddef.h>

typedef struct {
    int64_t Min;
    int64_t Max;
    int64_t Delta;
    int64_t NumRuns;
} Context;

void analyze_i64_avx512(int64_t* vals, Context* ctx, size_t len);

#endif