#ifndef ANALYZE_H
#define ANALYZE_H

#include <stdint.h>
#include <stddef.h>

typedef struct {
    int64_t Min;
    int64_t Max;
    int64_t Delta;
    uint32_t NumRuns;
} I64Context;

typedef struct {
    uint64_t Min;
    uint64_t Max;
    uint64_t Delta;
    uint32_t NumRuns;
} U64Context;

typedef struct {
    int32_t Min;
    int32_t Max;
    int32_t Delta;
    uint32_t NumRuns;
} I32Context;

typedef struct {
    uint32_t Min;
    uint32_t Max;
    uint32_t Delta;
    uint32_t NumRuns;
} U32Context;

typedef struct {
    int16_t Min;
    int16_t Max;
    int16_t Delta;
    uint32_t NumRuns;
} I16Context;

typedef struct {
    uint16_t Min;
    uint16_t Max;
    uint16_t Delta;
    uint32_t NumRuns;
} U16Context;

typedef struct {
    int8_t Min;
    int8_t Max;
    int8_t Delta;
    uint32_t NumRuns;
} I8Context;

typedef struct {
    uint8_t Min;
    uint8_t Max;
    uint8_t Delta;
    uint32_t NumRuns;
} U8Context;

void analyze_i64_avx2(int64_t* vals, I64Context* ctx, size_t len);
void analyze_u64_avx2(uint64_t* vals, U64Context* ctx, size_t len);
void analyze_i32_avx2(int32_t* vals, I32Context* ctx, size_t len);
void analyze_u32_avx2(uint32_t* vals, U32Context* ctx, size_t len);
void analyze_i16_avx2(int16_t* vals, I16Context* ctx, size_t len);
void analyze_u16_avx2(uint16_t* vals, U16Context* ctx, size_t len);
void analyze_i8_avx2(int8_t* vals, I8Context* ctx, size_t len);
void analyze_u8_avx2(uint8_t* vals, U8Context* ctx, size_t len);


#endif